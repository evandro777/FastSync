//! One running account: a worker thread that owns the engine and the DB, fed by three sources through one channel:
//! inotify (local edits), the provider's delta feed (remote edits) and timers (safety nets, retries).
use std::{
    collections::BTreeSet,
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU32, Ordering},
        mpsc::{Receiver, RecvTimeoutError, Sender, channel},
    },
    time::{Duration, Instant},
};

use notify::{EventKind, RecursiveMode, Watcher};

use crate::{
    accounts::{Account, account_dir},
    db::Db,
    delta::{self, Change, Poll},
    engine::{Engine, Progress, Report, ignored},
    ignore::{RULES, Rules},
    rclone::{Rclone, RcloneRemote, exclude_rules, now_secs},
};

#[derive(Clone, Debug, PartialEq)]
pub enum Status {
    /// Epoch seconds of the last finished run.
    Idle(i64),
    /// What is being transferred and its speed in bytes per second; `Progress::default()` while nothing is planned yet.
    Syncing(Progress, f64),
    Paused,
    /// The delete guard stopped a run: (files to delete, files known, the first few of those paths). The account
    /// does nothing until `Runner::resolve`, also after a restart.
    Stopped(usize, usize, Vec<String>),
    /// A technical message from rclone, the OS or the provider.
    Error(String),
}

enum Event {
    /// Local edits from inotify.
    Local(Vec<String>),
    /// The remote feed's changes and the cursor after them.
    Remote(Vec<Change>, String),
    /// The remote feed's cursor was (re)started: compare everything once.
    Reset(String),
    Root(Option<String>),
    /// Compare the local folder with the DB (offline edits, missed inotify events).
    Walk,
    /// Automatic full comparison (timers, providers without a feed).
    Full,
    /// The user pressed "Sync now".
    SyncNow,
    /// The user's answer to a stopped run.
    Resolve(Decision),
    Pause(bool),
    Stop,
    Warn(String),
}

/// What to do with the deletes the guard stopped.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Decision {
    /// Carry them out (local files go to the trash), up to the count the card showed.
    Delete,
    /// Undo them: copy the files back to the side they vanished from.
    Restore,
}

/// A stopped run, kept in the DB (`meta` key `held`) until the user decides: the account runs nothing meanwhile,
/// and a restart must not forget it. JSON, because a file name can hold any character but `/`.
#[derive(Clone, Debug, Default, PartialEq, serde::Serialize, serde::Deserialize)]
struct Held {
    deletes: usize,
    known: usize,
    paths: Vec<String>,
}

impl Held {
    fn status(&self) -> Status {
        Status::Stopped(self.deletes, self.known, self.paths.clone())
    }
    fn load(db: &Db) -> Option<Self> {
        // A value that does not parse still holds the account: ignoring it would release a stopped run.
        db.meta("held")
            .ok()
            .flatten()
            .map(|s| serde_json::from_str(&s).unwrap_or_default())
    }
    fn save(&self, db: &Db) {
        if let Ok(s) = serde_json::to_string(self) {
            let _ = db.set_meta("held", &s);
        }
    }
}

pub struct Runner {
    tx: Sender<Event>,
    handle: Option<std::thread::JoinHandle<()>>,
}

impl Runner {
    /// Stops the worker and waits until it has let go of the DB, so a new runner can take over the account.
    /// A run in progress finishes first: call it off the UI thread.
    pub fn stop_and_wait(mut self) {
        self.stop();
        if let Some(h) = self.handle.take() {
            let _ = h.join();
        }
    }
    /// The user's "Sync now" on one account. Does nothing while the delete guard holds the account.
    pub fn sync_now(&self) {
        let _ = self.tx.send(Event::SyncNow);
    }
    /// The user's answer to `Status::Stopped`: the only way to sync again after the delete guard tripped.
    pub fn resolve(&self, d: Decision) {
        let _ = self.tx.send(Event::Resolve(d));
    }
    /// A plain full comparison (the tray's "Sync now" for every account at once); held accounts ignore it.
    pub fn rescan(&self) {
        let _ = self.tx.send(Event::Full);
    }
    pub fn pause(&self, on: bool) {
        let _ = self.tx.send(Event::Pause(on));
    }
    pub fn stop(&self) {
        let _ = self.tx.send(Event::Stop);
    }
}

/// How often providers without a delta feed are listed in full.
const SCAN_EVERY: Duration = Duration::from_secs(300);
/// The walk event fires once a minute and a tick that finds no walk due costs nothing (no scan). How often the walk
/// itself runs (it stats every file and reads every DB row) is the user's setting, so a change applies at once:
/// `WALK_MINUTES` while inotify watches the folder, `WALK_FALLBACK_MINUTES` when it cannot (no watch left, or it
/// failed to start). Set by the app from the settings.
const WALK_TICK: Duration = Duration::from_secs(60);
pub static WALK_MINUTES: AtomicU32 = AtomicU32::new(5);
pub static WALK_FALLBACK_MINUTES: AtomicU32 = AtomicU32::new(2);

/// True when a walk is due: none yet (start-up finds what changed while the app was closed), or `minutes` have passed
/// since the last. Half a tick of slack, or a 1-minute setting would drift to 2 on a timer that ticks a hair late.
fn walk_due(since_last: Option<Duration>, minutes: u32) -> bool {
    since_last.is_none_or(|d| d + WALK_TICK / 2 >= Duration::from_secs(u64::from(minutes) * 60))
}
const RETRY_AFTER: Duration = Duration::from_secs(60);
const RETRY_MAX: Duration = Duration::from_secs(1800);
/// How long syncing must stay quiet before the conflicts of a burst are announced (once, not per file).
const CONFLICT_QUIET: Duration = Duration::from_secs(10);

/// The wait before the retry after `fails` failed runs in a row: 1, 2, 4, 8, 16 minutes, then 30. A file that can
/// never sync (a name the provider refuses, a quota) would otherwise cost a full listing of the cloud every minute.
fn retry_after(fails: u32) -> Duration {
    (RETRY_AFTER * (1 << fails.saturating_sub(1).min(5))).min(RETRY_MAX)
}

/// When the next full retry is due after failed runs. A failed cloud change leaves nothing to find it again (the feed's
/// paths are dropped, the local walk only sees local edits), so the retry must survive clean runs of other paths: only
/// a clean full scan ends it, and the feed's cursor is not saved before that.
#[derive(Default)]
struct Retry {
    fails: u32,
    at: Option<Instant>,
}

impl Retry {
    fn pending(&self) -> bool {
        self.at.is_some()
    }
    fn wait(&self, now: Instant) -> Option<Duration> {
        self.at.map(|t| t.saturating_duration_since(now))
    }
    /// True once, when the retry is due: the caller then runs a full scan (which sets it again if it fails).
    fn take_due(&mut self, now: Instant) -> bool {
        self.at.is_some_and(|t| now >= t) && self.at.take().is_some()
    }
    fn after_run(&mut self, failed: bool, was_full: bool, now: Instant) {
        if failed {
            self.fails = self.fails.saturating_add(1);
            self.at = Some(now + retry_after(self.fails));
        } else if was_full {
            *self = Self::default();
        }
    }
}

/// Set when inotify ran out of watches (any account); Preferences then shows how to raise the limit. Only a restart
/// clears it: that is also when a raised limit takes effect.
pub static WATCH_LIMIT_HIT: AtomicBool = AtomicBool::new(false);

/// The system's `fs.inotify.max_user_watches` (0 when unreadable).
pub fn watch_limit() -> u64 {
    std::fs::read_to_string("/proc/sys/fs/inotify/max_user_watches")
        .ok()
        .and_then(|s| s.trim().parse().ok())
        .unwrap_or(0)
}

/// The command that raises the limit: at least 524288, or double the current one.
pub fn watch_fix_command(current: u64) -> String {
    let n = current.saturating_mul(2).max(524_288);
    format!("echo fs.inotify.max_user_watches={n} | sudo tee /etc/sysctl.d/60-inotify.conf && sudo sysctl --system")
}

/// `on_conflicts(count, first path)` fires once `CONFLICT_QUIET` after the last run that kept a file twice, so a burst
/// of conflicts is one call. Not part of `Status`: the card, tray and emblem do not change for it.
pub fn spawn(
    acct: Account,
    rc: Arc<Rclone>,
    on_status: impl Fn(Status) + Send + Sync + 'static,
    on_conflicts: impl Fn(usize, String) + Send + Sync + 'static,
) -> Runner {
    let (tx, rx) = channel();
    let on_status = Arc::new(on_status);
    let handle = std::thread::spawn({
        let tx = tx.clone();
        move || {
            // A panic would end this thread silently and leave the card on its last status for ever.
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                work(acct, rc, tx, rx, on_status.clone(), &on_conflicts)
            }));
            match result {
                Ok(Err(e)) => on_status(Status::Error(e)),
                Err(_) => on_status(Status::Error(
                    "sync stopped after an internal error, restart FastSync".into(),
                )),
                Ok(Ok(())) => {}
            }
        }
    });
    Runner {
        tx,
        handle: Some(handle),
    }
}

fn work(
    acct: Account,
    rc: Arc<Rclone>,
    tx: Sender<Event>,
    rx: Receiver<Event>,
    on_status: Arc<dyn Fn(Status) + Send + Sync>,
    on_conflicts: &(dyn Fn(usize, String) + Send + Sync),
) -> Result<(), String> {
    let root = PathBuf::from(&acct.local);
    let dir = account_dir(&acct.id).ok_or("no data folder")?;
    crate::files::create_private_dir(&dir).map_err(|e| e.to_string())?;
    let db = Db::open(&dir.join("state.db")).map_err(|e| e.to_string())?;
    // A folder that vanished after files were synced (an unmounted drive) must not be recreated empty: that looks
    // exactly like "the user deleted everything".
    if !root.exists() && db.all().is_ok_and(|v| !v.is_empty()) {
        return Err(format!("{}: folder not found (unmounted?)", root.display()));
    }
    std::fs::create_dir_all(&root).map_err(|e| format!("{}: {e}", root.display()))?;
    // Half-downloaded files of a run that was killed: nothing else ever removes them.
    let _ = std::fs::remove_dir_all(root.join(crate::engine::HIDDEN).join("tmp"));
    // A login made before shortcuts were skipped: see `Rclone::skip_shortcuts`. Before anything is listed.
    if acct.kind == "drive" {
        rc.skip_shortcuts(&acct.id)?;
    }
    let cursor = db.meta("cursor").ok().flatten();
    let fs = format!("{}:", acct.id);
    let mut engine = Engine::new(
        root.clone(),
        RcloneRemote {
            rc: rc.clone(),
            fs,
            exclude: exclude_rules(&acct.kind, &acct.excluded),
        },
        db,
    );
    engine.excluded = acct.excluded.clone();
    engine.download_only = acct.download_only;
    // The unchecked list the last full scan was made with. When it differs (the user just saved the folder picker),
    // a full scan must run before anything else: it trashes what was unticked and brings back what was re-ticked.
    // A provider with a delta feed would otherwise wait for a cursor reset, and a re-ticked folder whose files the
    // user removed by hand meanwhile would read as "deleted locally" and be deleted in the cloud.
    // The ignore rules ride on the same mechanism: a changed rules file also needs a full scan first.
    let mut rules_text = std::fs::read_to_string(root.join(RULES)).unwrap_or_default();
    engine.ignore = Rules::parse(&rules_text);
    let mut excluded_key = format!("{}\n\0\n{rules_text}", acct.excluded.join("\n"));
    let excluded_changed = engine.db.meta("excluded").ok().flatten().unwrap_or_default() != excluded_key;
    let stop = Arc::new(AtomicBool::new(false));

    // Local edits. If inotify cannot watch the tree (the watch limit), the walk timer runs more often.
    let watcher_tx = tx.clone();
    let watch_root = root.clone();
    let mut watcher = notify::recommended_watcher(move |res: notify::Result<notify::Event>| {
        let ev = match res {
            Ok(ev) => ev,
            // A folder created after start-up found no watch left: say so, the 2-minute walk covers it meanwhile.
            Err(e) if matches!(e.kind, notify::ErrorKind::MaxFilesWatch) => {
                WATCH_LIMIT_HIT.store(true, Ordering::Relaxed);
                let _ = watcher_tx.send(Event::Warn(format!("inotify: {e}")));
                return;
            }
            Err(_) => return,
        };
        if matches!(ev.kind, EventKind::Access(_)) {
            return;
        }
        let paths: Vec<String> = ev.paths.iter().filter_map(|p| rel(&watch_root, p)).collect();
        if !paths.is_empty() {
            let _ = watcher_tx.send(Event::Local(paths));
        }
    })
    .map_err(|e| e.to_string())?;
    let watching = watcher.watch(&root, RecursiveMode::Recursive);
    if let Err(e) = &watching {
        if matches!(e.kind, notify::ErrorKind::MaxFilesWatch) {
            WATCH_LIMIT_HIT.store(true, Ordering::Relaxed);
        }
        // Not fatal: the folder is then compared every 20 s instead.
        engine.db.log("error", "", None, &format!("inotify: {e}"));
        on_status(Status::Error(format!("inotify: {e}")));
    }
    let watching_ok = watching.is_ok();
    timer(tx.clone(), stop.clone(), WALK_TICK, || Event::Walk);
    let mut last_walk: Option<Instant> = None;

    // Remote edits: the provider's feed, or a periodic full scan.
    let kind = acct.kind.clone();
    let id = acct.id.clone();
    let token_rc = rc.clone();
    let token: delta::Token = Box::new(move || token_rc.token(&id).map(|t| t.0));
    let mut root_id = None;
    match delta::for_kind(&kind, token) {
        Some(feed) => {
            poller(feed, cursor.clone(), tx.clone(), stop.clone());
            if cursor.is_some() {
                let _ = tx.send(Event::Walk);
            }
        }
        None => {
            timer(tx.clone(), stop.clone(), SCAN_EVERY, || Event::Full);
            let _ = tx.send(Event::Full);
        }
    }

    let mut paused = acct.paused;
    // A stopped run waits here for the user, however long and across restarts: nothing below runs until `resolution`.
    let mut held = Held::load(&engine.db);
    let mut resolution: Option<Decision> = None;
    if paused {
        on_status(Status::Paused);
    } else if let Some(h) = &held {
        on_status(h.status());
    }
    // `pending`: paths from inotify; `feed_paths`: paths from the cloud's feed (see `Engine::paths_to_sync`).
    let (mut pending, mut feed_paths) = (BTreeSet::new(), BTreeSet::new());
    let (mut full, mut walk) = (excluded_changed, false);
    // The first loop turn waits for an event: this one makes it run the scan.
    if excluded_changed {
        let _ = tx.send(Event::Full);
    }
    let mut excluded_pending = excluded_changed;
    let mut new_cursor: Option<String> = None;
    // Runs in a row that ended with errors: each one waits longer before the retry.
    let mut retry = Retry::default();
    // Files kept twice since the last alert, and when the alert is due (checked on every turn, so a steady trickle of
    // events cannot hold it back for ever).
    let mut conflicts: Vec<String> = Vec::new();
    let mut alert_at = Instant::now();
    loop {
        if !conflicts.is_empty() && Instant::now() >= alert_at {
            let all = std::mem::take(&mut conflicts);
            on_conflicts(all.len(), all[0].clone());
        }
        let wait = [
            retry.wait(Instant::now()),
            (!conflicts.is_empty()).then(|| alert_at.saturating_duration_since(Instant::now())),
        ]
        .into_iter()
        .flatten()
        .min();
        let first = match wait {
            Some(d) => rx.recv_timeout(d),
            None => rx.recv().map_err(|_| RecvTimeoutError::Disconnected),
        };
        let mut events = match first {
            Ok(e) => vec![e],
            // The alert timer, not the retry one: nothing to scan, the top of the loop announces.
            Err(RecvTimeoutError::Timeout) if !conflicts.is_empty() && Instant::now() >= alert_at => continue,
            Err(RecvTimeoutError::Timeout) => vec![Event::Full],
            Err(RecvTimeoutError::Disconnected) => break,
        };
        // A burst (an editor saving, a folder copied in) is handled as one run: wait until it goes quiet.
        let deadline = Instant::now() + Duration::from_secs(3);
        while Instant::now() < deadline {
            match rx.recv_timeout(Duration::from_millis(300)) {
                Ok(e) => events.push(e),
                Err(_) => break,
            }
        }
        for ev in events {
            match ev {
                Event::Local(p) => pending.extend(p),
                Event::Remote(changes, cur) => {
                    let dirs = engine.db.dirs().unwrap_or_default().into_iter().collect();
                    let db = &engine.db;
                    let i = delta::impact(&changes, &dirs, root_id.as_deref(), |id| {
                        db.path_of_id(id).ok().flatten()
                    });
                    full |= i.full_scan;
                    feed_paths.extend(i.paths);
                    new_cursor = Some(cur);
                }
                Event::Reset(cur) => {
                    full = true;
                    new_cursor = Some(cur);
                }
                Event::Root(id) => root_id = id,
                Event::Walk => {
                    // inotify covers the tree, so the walk is only a safety net, with its own (longer) interval.
                    let covered = watching_ok && !WATCH_LIMIT_HIT.load(Ordering::Relaxed);
                    let minutes = if covered { &WALK_MINUTES } else { &WALK_FALLBACK_MINUTES };
                    if walk_due(last_walk.map(|t| t.elapsed()), minutes.load(Ordering::Relaxed)) {
                        walk = true;
                        last_walk = Some(Instant::now());
                    }
                }
                Event::Full => full = true,
                Event::SyncNow => full = true,
                Event::Resolve(d) => {
                    // Only a held account has anything to resolve, and a mirror cannot put files back in the cloud.
                    if held.is_some() && !(d == Decision::Restore && acct.download_only) {
                        resolution = Some(d);
                        full = true;
                    }
                }
                Event::Pause(p) => {
                    paused = p;
                    on_status(match (&held, p) {
                        (_, true) => Status::Paused,
                        (Some(h), false) => h.status(),
                        (None, false) => syncing(),
                    });
                    // What happened meanwhile is unknown (the feed keeps running, but be safe): compare all.
                    full |= !p;
                }
                Event::Stop => {
                    stop.store(true, Ordering::Relaxed);
                    return Ok(());
                }
                Event::Warn(w) => {
                    engine.db.log("error", "", None, &w);
                    // The stopped card carries the buttons the user needs: a warning must not replace it.
                    if held.is_none() {
                        on_status(Status::Error(w));
                    }
                }
            }
        }
        // Edited here or arrived from the cloud: either way the file on disk is the truth.
        // A missing file means no rules, but a failed read (editor mid-save, permissions) keeps the old ones: empty
        // rules would upload every file they hide.
        let now = match std::fs::read_to_string(root.join(RULES)) {
            Ok(t) => t,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => String::new(),
            Err(_) => rules_text.clone(),
        };
        if now != rules_text {
            engine.ignore = Rules::parse(&now);
            excluded_key = format!("{}\n\0\n{now}", acct.excluded.join("\n"));
            rules_text = now;
            (full, excluded_pending) = (true, true);
        }
        // Taken here, not in the run: a paused or held account must not spin on a retry that is already due (the full
        // scan flag stays set until it can run).
        full |= retry.take_due(Instant::now());
        if paused {
            continue;
        }
        // Held: no walk, no scan, no feed paths, so no "up to date" and no saved cursor for a change nobody
        // approved. Everything that arrives meanwhile is read again by the full scan after the decision.
        if held.is_some() && resolution.is_none() {
            pending.clear();
            feed_paths.clear();
            continue;
        }
        if !(full || walk || !pending.is_empty() || !feed_paths.is_empty()) {
            continue;
        }
        // The count the card showed caps what "Delete them" may delete; a bigger plan stops again.
        engine.confirmed_deletes = resolution
            .filter(|d| *d == Decision::Delete)
            .and(held.as_ref().map(|h| h.deletes));
        engine.restore = resolution == Some(Decision::Restore);
        let progress = engine.progress.clone();
        let over = AtomicBool::new(false);
        let (began, was_full) = (Instant::now(), full);
        let report = std::thread::scope(|s| {
            s.spawn(|| ticker(&rc, &acct.id, &progress, &*on_status, &over));
            let report = if full {
                on_status(syncing());
                engine.full_scan()
            } else {
                let paths = engine.paths_to_sync(
                    std::mem::take(&mut pending).into_iter().collect(),
                    std::mem::take(&mut feed_paths).into_iter().collect(),
                    walk,
                );
                if paths.is_empty() {
                    // The 2-minute walk that finds nothing must not flash "Syncing".
                    Report::default()
                } else {
                    on_status(syncing());
                    let r = engine.sync_paths(&paths);
                    // A folder renamed in the cloud can arrive as its deletions first and the new name in a later
                    // batch: a stop here may be a move only a full scan (which pairs them) can see.
                    if r.aborted.is_some() { engine.full_scan() } else { r }
                }
            };
            over.store(true, Ordering::Relaxed);
            report
        });
        pending.clear();
        feed_paths.clear();
        conflicts.extend(report.conflicts.iter().cloned());
        // Any run while an alert waits (more files arriving) pushes it back: one alert for the whole burst.
        if !conflicts.is_empty() {
            alert_at = Instant::now() + CONFLICT_QUIET;
        }
        // A run that did nothing (the 2-minute walk) leaves no trace, or it would flood the log. The `scan` row holds
        // values only (path = mode, size = milliseconds, msg = "transfers problems"); the dialog words it.
        if report.transferred > 0 || !report.errors.is_empty() {
            engine.db.log(
                "scan",
                if was_full { "full" } else { "paths" },
                Some(began.elapsed().as_millis() as u64),
                &format!("{} {}", report.transferred, report.errors.len()),
            );
        }
        engine.db.trim_log();
        // Only a clean full scan counts: after an error or a stop the next start must scan again.
        if full && excluded_pending && report.errors.is_empty() && report.aborted.is_none() {
            let _ = engine.db.set_meta("excluded", &excluded_key);
            excluded_pending = false;
        }
        (full, walk) = (false, false);
        (engine.confirmed_deletes, engine.restore) = (None, false);
        if let Some((deletes, known, paths)) = &report.aborted {
            // Written before the status goes out, so a quit right after the card shows it still finds it.
            let h = Held {
                deletes: *deletes,
                known: *known,
                paths: paths.clone(),
            };
            h.save(&engine.db);
            (held, resolution) = (Some(h), None);
        } else if report.errors.is_empty() && resolution.take().is_some() {
            // The decision was carried out. After errors it stays, and the retry carries it out again.
            let _ = engine.db.del_meta("held");
            held = None;
        }
        retry.after_run(!report.errors.is_empty(), was_full, Instant::now());
        // Only after a clean run with no retry waiting: a change that failed to sync must be read again after a
        // restart, even when a later run of other paths was clean. The retry (a full scan) covers it while the app
        // stays up, and the cursor is saved once that scan is clean.
        if report.errors.is_empty()
            && report.aborted.is_none()
            && !retry.pending()
            && let Some(c) = new_cursor.take()
        {
            let _ = engine.db.set_meta("cursor", &c);
        }
        on_status(summary(&report));
    }
    Ok(())
}

fn summary(r: &Report) -> Status {
    if let Some((deletes, known, paths)) = &r.aborted {
        return Status::Stopped(*deletes, *known, paths.clone());
    }
    match r.errors.first() {
        Some(e) => Status::Error(format!(
            "{} ({} problems)",
            e.chars().take(140).collect::<String>(),
            r.errors.len()
        )),
        None => Status::Idle(now_secs()),
    }
}

/// `abs` relative to the synced folder, as a `/` path; `None` for the folder itself and for ignored names.
fn rel(root: &Path, abs: &Path) -> Option<String> {
    let r = abs.strip_prefix(root).ok()?;
    let mut parts = Vec::new();
    for c in r.components() {
        let c = c.as_os_str().to_str()?;
        if ignored(c) {
            return None;
        }
        parts.push(c);
    }
    (!parts.is_empty()).then(|| parts.join("/"))
}

/// Sends `make()` every `every` until `stop` is set.
fn syncing() -> Status {
    Status::Syncing(Progress::default(), 0.0)
}

/// While a run works: once a second, tell the UI which file is moving and how fast. Silent when nothing changed
/// (and while the run has not planned anything yet), so an idle account costs no rclone calls.
fn ticker(rc: &Rclone, group: &str, progress: &Mutex<Progress>, on_status: &dyn Fn(Status), over: &AtomicBool) {
    let mut last = None;
    while !over.load(Ordering::Relaxed) {
        // Short naps, so a finished run is not held up for a whole second.
        for _ in 0..10 {
            if over.load(Ordering::Relaxed) {
                return;
            }
            std::thread::sleep(Duration::from_millis(100));
        }
        let p = progress.lock().map(|g| g.clone()).unwrap_or_default();
        if p.total == 0 {
            continue;
        }
        let speed = rc.speed(group).unwrap_or(0.0);
        let now = Some((p.clone(), speed));
        if now != last {
            on_status(Status::Syncing(p, speed));
            last = now;
        }
    }
}

fn timer(tx: Sender<Event>, stop: Arc<AtomicBool>, every: Duration, make: fn() -> Event) {
    std::thread::spawn(move || {
        let mut left = every;
        while !stop.load(Ordering::Relaxed) {
            std::thread::sleep(Duration::from_secs(1));
            left = left.saturating_sub(Duration::from_secs(1));
            if left.is_zero() {
                left = every;
                if tx.send(make()).is_err() {
                    return;
                }
            }
        }
    });
}

/// Runs the provider's feed: long polls (Dropbox) or polls every 30 s, and hands the changes to the worker.
fn poller(mut feed: Box<dyn delta::RemoteChanges>, cursor: Option<String>, tx: Sender<Event>, stop: Arc<AtomicBool>) {
    std::thread::spawn(move || {
        let sleep = |d: Duration| {
            let end = Instant::now() + d;
            while Instant::now() < end && !stop.load(Ordering::Relaxed) {
                std::thread::sleep(Duration::from_millis(500).min(d));
            }
        };
        // Drive and OneDrive report parents by id: the id of the cloud's root tells the synced tree's files apart.
        while !stop.load(Ordering::Relaxed) {
            match feed.root_id() {
                Ok(id) => {
                    let _ = tx.send(Event::Root(id));
                    break;
                }
                Err(e) => {
                    let _ = tx.send(Event::Warn(e));
                    sleep(Duration::from_secs(30));
                }
            }
        }
        let mut cursor = cursor;
        // Consecutive failures so far (`None` = the last step worked).
        let mut failures: Option<u32> = None;
        while !stop.load(Ordering::Relaxed) {
            let step = match &cursor {
                // No cursor yet: take one *before* the first full scan, so nothing that changes during it is missed.
                None => feed.start().map(|c| {
                    cursor = Some(c.clone());
                    Some(Event::Reset(c))
                }),
                Some(c) => feed.poll(c).and_then(|p| match p {
                    Poll::Changes(ch, next) => {
                        cursor = Some(next.clone());
                        Ok((!ch.is_empty()).then_some(Event::Remote(ch, next)))
                    }
                    Poll::Reset => feed.start().map(|c| {
                        cursor = Some(c.clone());
                        Some(Event::Reset(c))
                    }),
                }),
            };
            match step {
                Ok(ev) => {
                    // Back after an error: the card still shows the old message, and a full comparison both
                    // catches what was missed meanwhile and clears it.
                    let ev = ev.or_else(|| failures.take().map(|_| Event::Full));
                    failures = None;
                    if let Some(ev) = ev
                        && tx.send(ev).is_err()
                    {
                        return;
                    }
                    sleep(feed.idle());
                }
                Err(e) => {
                    let _ = tx.send(Event::Warn(e));
                    // Exponential: 30 s, 60 s, ... 10 min. Quota errors (rclone's shared Google client id) get
                    // worse if every client keeps retrying at a fixed pace.
                    let n = failures.map_or(0, |n: u32| n + 1);
                    failures = Some(n);
                    sleep(Duration::from_secs((30u64 << n.min(5)).min(600)));
                }
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_held_run_survives_the_db_and_a_bad_value_still_holds() {
        let db = Db::memory().unwrap();
        assert_eq!(Held::load(&db), None);
        let h = Held {
            deletes: 84,
            known: 92,
            paths: vec!["a/b.txt".into(), "odd\nname".into()],
        };
        h.save(&db);
        assert_eq!(Held::load(&db), Some(h));
        // A value that cannot be read must not release the account.
        db.set_meta("held", "not json").unwrap();
        assert_eq!(Held::load(&db), Some(Held::default()));
        db.del_meta("held").unwrap();
        assert_eq!(Held::load(&db), None);
    }

    #[test]
    fn watch_fix_raises_the_limit() {
        assert!(watch_fix_command(8192).contains("=524288 "));
        assert!(watch_fix_command(1_048_576).contains("=2097152 "));
    }

    #[test]
    fn watcher_paths_are_relative_and_skip_temp_files() {
        let root = Path::new("/home/u/Sync");
        assert_eq!(rel(root, Path::new("/home/u/Sync/a/b.txt")).as_deref(), Some("a/b.txt"));
        assert_eq!(rel(root, root), None);
        assert_eq!(rel(root, Path::new("/home/u/Sync/.fastsync/trash/x")), None);
        assert_eq!(rel(root, Path::new("/home/u/Sync/x.partial")), None);
        assert_eq!(rel(root, Path::new("/elsewhere/x")), None);
    }

    #[test]
    fn retries_back_off_up_to_half_an_hour() {
        let secs = |n| retry_after(n).as_secs();
        assert_eq!((secs(1), secs(2), secs(3), secs(5)), (60, 120, 240, 960));
        assert_eq!((secs(6), secs(7), secs(1000)), (1800, 1800, 1800));
    }

    #[test]
    fn a_clean_run_of_other_paths_does_not_cancel_the_retry_of_a_failed_one() {
        let (mut r, t0) = (Retry::default(), Instant::now());
        r.after_run(true, false, t0);
        assert!(r.pending());
        // Another change arrives and syncs fine before the retry is due.
        r.after_run(false, false, t0 + Duration::from_secs(10));
        assert!(r.pending(), "the failed change would be lost");
        assert!(!r.take_due(t0 + Duration::from_secs(30)));
        // Due: taken once, and a clean full scan ends it.
        assert!(r.take_due(t0 + Duration::from_secs(61)));
        assert!(!r.take_due(t0 + Duration::from_secs(62)));
        r.after_run(true, true, t0 + Duration::from_secs(62));
        assert_eq!(r.wait(t0 + Duration::from_secs(62)), Some(Duration::from_secs(120)));
        r.after_run(false, true, t0 + Duration::from_secs(200));
        assert!(!r.pending());
        assert_eq!(r.fails, 0);
    }

    #[test]
    fn the_walk_runs_first_at_once_then_when_the_setting_says() {
        let secs = Duration::from_secs;
        assert!(walk_due(None, 5), "start-up always walks");
        assert!(walk_due(Some(secs(59)), 1), "a 1-minute setting must not slip to 2");
        assert!(!walk_due(Some(secs(240)), 5));
        assert!(walk_due(Some(secs(300)), 5));
        assert!(
            !walk_due(Some(secs(60)), 2),
            "one tick after a walk is too soon for 2 minutes"
        );
    }

    #[test]
    fn summaries() {
        assert!(matches!(summary(&Report::default()), Status::Idle(_)));
        let aborted = Report {
            aborted: Some((5, 9, vec!["a".into()])),
            ..Default::default()
        };
        assert_eq!(summary(&aborted), Status::Stopped(5, 9, vec!["a".into()]));
    }
}
