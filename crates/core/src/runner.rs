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
    engine::{Engine, Phase, Progress, Report, ignored},
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
    /// A run with several problems: the first message and the count (the UI words the count, so it translates).
    Problems(String, usize),
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
    /// A full comparison asked for (start-up, a rescan, a feed that was reset).
    Full,
    /// A retry came due (`Retry::take_due` says what it runs): only wakes the loop.
    Retry,
    /// The feed answers again after failing. Its cursor did not move meanwhile, so nothing was missed: no scan, just
    /// a walk that clears the old message.
    Recovered,
    /// The `SCAN_EVERY` timer of a provider without a feed. Ticks pile up while a long run works, so one is only
    /// taken when the last full scan ended long enough ago (`scan_due`): else a listing slower than the interval
    /// would start again the moment it ended, for ever.
    Tick,
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
    /// The engine's `cancel`: Pause and Stop set it at once, because the worker reads its events only between runs.
    cancel: Arc<AtomicBool>,
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
    /// Pausing also ends the run in progress (it finishes the transfers under way); the worker clears the flag again
    /// when it reads the resume.
    pub fn pause(&self, on: bool) {
        if on {
            self.cancel.store(true, Ordering::Relaxed);
        }
        let _ = self.tx.send(Event::Pause(on));
    }
    pub fn stop(&self) {
        self.cancel.store(true, Ordering::Relaxed);
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
/// True when a timer tick should start a full scan: none yet, or the last one ended at least half the interval ago
/// (a tick that waited behind a long run reads as "just ended" and is dropped; the next one is due).
fn scan_due(since_end: Option<Duration>) -> bool {
    since_end.is_none_or(|d| d >= SCAN_EVERY / 2)
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

/// More failed paths than this are as good as a full scan: the list is dropped and the retry becomes one.
const RETRY_PATHS_MAX: usize = 5000;

/// What a due retry runs.
#[derive(Debug, PartialEq)]
enum Due {
    /// Something only a whole comparison can retry went wrong (the cloud listing, the state DB).
    Full,
    /// The paths whose own action failed: one lookup and one transfer each, no cloud listing (minutes on a big cloud).
    Paths(Vec<String>),
}

/// What failed and when to try again. A failed cloud change leaves nothing to find it again (the feed's paths are
/// dropped, the local walk only sees local edits), so the failed paths are remembered here, and in the DB (`retry`)
/// so a restart still has them: that is what lets the feed's cursor be saved while a file keeps failing, and what
/// keeps one unreadable file from costing a listing of the whole cloud every time.
#[derive(Default)]
struct Retry {
    fails: u32,
    /// Back-off for files that changed under their transfer (`Report::later`), apart from real failures.
    later_fails: u32,
    at: Option<Instant>,
    /// Only a full scan retries this, until a clean one has run.
    full: bool,
    /// Paths to look at again by themselves (`Report::failed` and `Report::later`).
    paths: BTreeSet<String>,
    /// The text last written to the DB, so an idle loop writes nothing.
    saved: Option<String>,
}

impl Retry {
    /// What the last run left behind. Whatever is there is due at once: it was waiting when the app closed.
    fn load(db: &Db) -> Self {
        let mut r = Self::default();
        let Ok(Some(text)) = db.meta("retry") else {
            return r;
        };
        // A value that cannot be read is a full scan, the safe side.
        match serde_json::from_str::<(bool, BTreeSet<String>)>(&text) {
            Ok((full, paths)) => (r.full, r.paths) = (full, paths),
            Err(_) => r.full = true,
        }
        r.saved = Some(text);
        r.at = Some(Instant::now());
        r
    }
    /// False when the DB could not take it: the caller then keeps the feed's cursor back, or a restart would lose the
    /// failed paths the cursor moved past.
    fn save(&mut self, db: &Db) -> bool {
        if !self.full && self.paths.is_empty() {
            if self.saved.is_none() {
                return true;
            }
            let ok = db.del_meta("retry").is_ok();
            if ok {
                self.saved = None;
            }
            return ok;
        }
        let Ok(text) = serde_json::to_string(&(self.full, &self.paths)) else {
            return false;
        };
        if self.saved.as_deref() == Some(&text) {
            return true;
        }
        let ok = db.set_meta("retry", &text).is_ok();
        if ok {
            self.saved = Some(text);
        }
        ok
    }
    fn wait(&self, now: Instant) -> Option<Duration> {
        self.at.map(|t| t.saturating_duration_since(now))
    }
    /// What is due now, once. The run's `record` sets it again if it fails.
    fn take_due(&mut self, now: Instant) -> Option<Due> {
        if !self.at.is_some_and(|t| now >= t) {
            return None;
        }
        self.at = None;
        Some(if self.full {
            Due::Full
        } else {
            Due::Paths(self.paths.iter().cloned().collect())
        })
    }
    /// Takes in a finished run: `ran` the paths it looked at, `did_full` a whole comparison. A clean full scan ends
    /// everything; a run of some paths only the failures among them, so a retry survives clean runs of other paths.
    fn record(&mut self, r: &Report, ran: &[String], did_full: bool, now: Instant) {
        let global = r.has_global_failure();
        if did_full && !global {
            // Everything was compared again: what failed is now exactly `r.failed`.
            (self.full, self.paths) = (false, BTreeSet::new());
        } else {
            for p in ran {
                self.paths.remove(p);
            }
        }
        self.full |= global;
        self.paths.extend(r.failed.iter().chain(&r.later).cloned());
        if self.paths.len() > RETRY_PATHS_MAX {
            (self.full, self.paths) = (true, BTreeSet::new());
        }
        let failed = global || !r.failed.is_empty();
        if !self.full && self.paths.is_empty() {
            (self.fails, self.later_fails, self.at) = (0, 0, None);
        } else if failed {
            self.fails = self.fails.saturating_add(1);
            self.at = Some(now + retry_after(self.fails));
        } else if !r.later.is_empty() {
            // A file that changed under its transfer is no failure, but one that keeps changing (a database, a VM
            // image) would otherwise be uploaded in part every minute for ever: 1, 2, 4 ... 30 minutes. Never later
            // than a retry already waiting for something that really failed.
            self.later_fails = self.later_fails.saturating_add(1);
            let t = now + retry_after(self.later_fails);
            self.at = Some(self.at.map_or(t, |a| a.min(t)));
        } else if self.at.is_none() {
            self.at = Some(now + RETRY_AFTER);
        }
    }
    /// What the loop does after a run: takes it in, and a stopped run's decision that is still unconfirmed makes the
    /// retry a full scan (the paths that failed in it do not carry the decision out).
    fn finish_run(&mut self, r: &Report, ran: &[String], did_full: bool, decision_pending: bool, now: Instant) {
        self.record(r, ran, did_full, now);
        if decision_pending {
            self.escalate(now);
        }
    }
    /// Makes the retry a full scan: a stopped run's decision (delete or restore) is only carried out and cleared by a
    /// clean whole comparison, not by the paths that failed in it.
    fn escalate(&mut self, now: Instant) {
        (self.full, self.paths) = (true, BTreeSet::new());
        if self.at.is_none() {
            self.fails = self.fails.max(1);
            self.at = Some(now + retry_after(self.fails));
        }
    }
    /// Writes what the run left (the failed paths) and then, only if that worked and the run may confirm it, the feed's
    /// cursor; true when the cursor was written. The order is the point: a failed change is in `retry` (and in the DB),
    /// so the cursor can move past it, and a crash between the two writes replays the feed from the old cursor instead
    /// of losing it. A `retry` the DB could not take keeps the cursor back: the failed paths would be safe nowhere.
    fn persist(&mut self, db: &Db, r: &Report, cursor: Option<&str>) -> bool {
        let remembered = self.save(db);
        match cursor {
            Some(c) if remembered && self.may_confirm(r) => db.set_meta("cursor", c).is_ok(),
            _ => false,
        }
    }
    /// True when the run may confirm the feed's cursor: nothing needs a full scan, and what failed is remembered (and
    /// saved before the cursor), so a restart need not read the whole cloud again to find it.
    fn may_confirm(&self, r: &Report) -> bool {
        r.aborted.is_none() && !r.cancelled && !r.has_global_failure() && !self.full
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
    let cancel = Arc::new(AtomicBool::new(false));
    let handle = std::thread::spawn({
        let (tx, cancel) = (tx.clone(), cancel.clone());
        move || {
            // A panic would end this thread silently and leave the card on its last status for ever.
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                work(acct, rc, tx, rx, cancel, on_status.clone(), &on_conflicts)
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
        cancel,
    }
}

fn work(
    acct: Account,
    rc: Arc<Rclone>,
    tx: Sender<Event>,
    rx: Receiver<Event>,
    cancel: Arc<AtomicBool>,
    on_status: Arc<dyn Fn(Status) + Send + Sync>,
    on_conflicts: &(dyn Fn(usize, String) + Send + Sync),
) -> Result<(), String> {
    let root = PathBuf::from(&acct.local);
    let dir = account_dir(&acct.id).ok_or("no data folder")?;
    crate::files::create_private_dir(&dir).map_err(|e| e.to_string())?;
    let db = Db::open(&dir.join("state.db")).map_err(|e| e.to_string())?;
    // A folder that vanished after files were synced (an unmounted drive) must not be recreated empty: that looks
    // exactly like "the user deleted everything".
    if !root.exists() && db.count().is_ok_and(|n| n > 0) {
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
            dropbox: acct.kind == "dropbox",
        },
        db,
    );
    engine.excluded = acct.excluded.clone();
    engine.download_only = acct.download_only;
    engine.cancel = cancel.clone();
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
    // Also raised when this worker ends by a panic, or the feed's poller would keep polling the provider for good.
    let _stop_feed = StopOnDrop(stop.clone());

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
        // Not fatal: the folder is then compared every `walk_fallback_minutes` instead.
        engine.db.log("error", "", None, &format!("inotify: {e}"));
        on_status(Status::Error(format!("inotify: {e}")));
    }
    let watching_ok = watching.is_ok();
    timer(tx.clone(), WALK_TICK, || Event::Walk);
    let mut last_walk: Option<Instant> = None;
    // When the last full scan ended (any cause), for `scan_due`.
    let mut last_full_end: Option<Instant> = None;

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
            timer(tx.clone(), SCAN_EVERY, || Event::Tick);
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
    // Cloud folders the feed reported without their contents (a new or moved Dropbox folder): listed alone by the run.
    let mut feed_dirs: BTreeSet<String> = BTreeSet::new();
    let (mut full, mut walk) = (excluded_changed, false);
    // Why the next full scan runs (the first cause wins), written to the log as `full:<why>`.
    // The start-up scan (also after Edit, which restarts the runner) is the one `excluded_changed` asks for.
    let mut why: &'static str = "rescan";
    // The first loop turn waits for an event: this one makes it run the scan.
    if excluded_changed {
        let _ = tx.send(Event::Full);
    }
    let mut excluded_pending = excluded_changed;
    let mut new_cursor: Option<String> = None;
    // What failed in the runs so far (and in the last session): retried by itself, each failing run waits longer.
    let mut retry = Retry::load(&engine.db);
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
            Err(RecvTimeoutError::Timeout) => vec![Event::Retry],
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
                    if i.full_scan {
                        mark(&mut full, &mut why, "feed");
                    }
                    feed_paths.extend(i.paths);
                    feed_dirs.extend(i.dirs);
                    new_cursor = Some(cur);
                }
                Event::Reset(cur) => {
                    mark(&mut full, &mut why, "reset");
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
                Event::Full => mark(&mut full, &mut why, "rescan"),
                Event::Retry => {}
                Event::Recovered => walk = true,
                Event::Tick => {
                    if scan_due(last_full_end.map(|t| t.elapsed())) {
                        mark(&mut full, &mut why, "tick");
                    }
                }
                Event::SyncNow => mark(&mut full, &mut why, "now"),
                Event::Resolve(d) => {
                    // Only a held account has anything to resolve, and a mirror cannot put files back in the cloud.
                    if held.is_some() && !(d == Decision::Restore && acct.download_only) {
                        resolution = Some(d);
                        mark(&mut full, &mut why, "decision");
                    }
                }
                Event::Pause(p) => {
                    paused = p;
                    // Only here, never before a run: a Pause pressed just as a run starts must still end it.
                    if !p {
                        cancel.store(false, Ordering::Relaxed);
                    }
                    on_status(match (&held, p) {
                        (_, true) => Status::Paused,
                        (Some(h), false) => h.status(),
                        (None, false) => syncing(),
                    });
                    // What happened meanwhile is already queued: the feed's paths, inotify's, a due retry. A walk (local
                    // only, no cloud call) covers an inotify queue that overflowed. A run Pause cut short set `full`.
                    walk |= !p;
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
            mark(&mut full, &mut why, "rules");
            excluded_pending = true;
        }
        // Taken here, not in the run: a paused or held account must not spin on a retry that is already due (the full
        // scan flag and the queued paths stay until it can run). The paths join the feed's: never dropped as an echo of
        // our own write, which a failed cloud change (the local copy untouched) would be.
        match retry.take_due(Instant::now()) {
            Some(Due::Full) => mark(&mut full, &mut why, "retry"),
            Some(Due::Paths(p)) => feed_paths.extend(p),
            None => {}
        }
        if paused {
            continue;
        }
        // Held: no walk, no scan, no feed paths, so no "up to date" and no saved cursor for a change nobody
        // approved. Everything that arrives meanwhile is read again by the full scan after the decision.
        if held.is_some() && resolution.is_none() {
            pending.clear();
            feed_paths.clear();
            feed_dirs.clear();
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
        let began = Instant::now();
        // The report, the paths and cloud folders the run looked at (none for a full scan) and whether it compared
        // everything.
        let (report, ran, ran_dirs, did_full) = std::thread::scope(|s| {
            s.spawn(|| ticker(&rc, &acct.id, &progress, &*on_status, &over));
            // Raised when this closure ends, however it ends: `scope` joins the ticker before it lets a panic out.
            let _end = EndRun(&over);
            if full {
                on_status(syncing());
                (engine.full_scan(), Vec::new(), Vec::new(), true)
            } else {
                let paths = engine.paths_to_sync(
                    std::mem::take(&mut pending).into_iter().collect(),
                    std::mem::take(&mut feed_paths).into_iter().collect(),
                    walk,
                );
                let dirs: Vec<String> = std::mem::take(&mut feed_dirs).into_iter().collect();
                if paths.is_empty() {
                    // The 2-minute walk that finds nothing must not flash "Syncing".
                    (Report::default(), paths, dirs, false)
                } else {
                    on_status(syncing());
                    let r = engine.sync_paths_in(&paths, &dirs);
                    // A folder renamed in the cloud can arrive as its deletions first and the new name in a later
                    // batch: a stop here may be a move only a full scan (which pairs them) can see.
                    if r.aborted.is_some() || r.wants_full {
                        (engine.full_scan(), paths, dirs, true)
                    } else {
                        (r, paths, dirs, false)
                    }
                }
            }
        });
        pending.clear();
        feed_paths.clear();
        feed_dirs.clear();
        conflicts.extend(report.conflicts.iter().cloned());
        // Any run while an alert waits (more files arriving) pushes it back: one alert for the whole burst.
        if !conflicts.is_empty() {
            alert_at = Instant::now() + CONFLICT_QUIET;
        }
        // A run that did nothing (the 2-minute walk) leaves no trace, or it would flood the log. The `scan` row holds
        // values only (path = mode, size = milliseconds, msg = "transfers problems"); the dialog words it.
        // A full scan is always logged with its cause, except the quiet 5-minute tick of providers without a feed.
        let cause = if full { why } else { "moves" };
        if report.transferred > 0 || !report.errors.is_empty() || (did_full && cause != "tick") {
            engine.db.log(
                "scan",
                &if did_full {
                    format!("full:{cause}")
                } else {
                    "paths".to_string()
                },
                Some(began.elapsed().as_millis() as u64),
                &format!("{} {}", report.transferred, report.errors.len()),
            );
        }
        engine.db.trim_log();
        // Cut short by Pause or Stop: the event that did it is next in the queue. Nothing is confirmed (no cursor, no
        // "up to date", no decision carried out). A full scan is read again in full when the account goes on; a run of
        // some paths only needs those paths again, not a listing of the whole cloud.
        if report.cancelled {
            let (again_full, again) = after_cancel(did_full, ran);
            if again_full {
                mark(&mut full, &mut why, "resume");
            }
            if !again_full {
                feed_paths.extend(again);
                feed_dirs.extend(ran_dirs);
            }
            walk = false;
            (engine.confirmed_deletes, engine.restore) = (None, false);
            continue;
        }
        // Only a whole scan with nothing global wrong counts: after a stop, a cut or a failed listing the next start
        // must scan again. A file that failed does not hold it back: it is retried by its path.
        if full && excluded_pending && settled(&report) {
            let _ = engine.db.set_meta("excluded", &excluded_key);
            excluded_pending = false;
        }
        (full, walk) = (false, false);
        if did_full {
            last_full_end = Some(Instant::now());
        }
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
        } else if did_full && clean(&report) && resolution.take().is_some() {
            // The decision was carried out, by a whole comparison (a run of some paths proves nothing about the rest).
            let _ = engine.db.del_meta("held");
            held = None;
        }
        if report.aborted.is_none() {
            // A stopped run's decision is not carried out by the paths that failed in it: only a clean full scan
            // does that, so with one pending the retry is a full scan.
            retry.finish_run(&report, &ran, did_full, resolution.is_some(), Instant::now());
        }
        if retry.persist(&engine.db, &report, new_cursor.as_deref()) {
            new_cursor = None;
        }
        on_status(summary(&report));
    }
    Ok(())
}

fn summary(r: &Report) -> Status {
    if let Some((deletes, known, paths)) = &r.aborted {
        return Status::Stopped(*deletes, *known, paths.clone());
    }
    let Some(e) = r.errors.first() else {
        return Status::Idle(now_secs());
    };
    let e = e.chars().take(140).collect::<String>();
    if r.errors.len() > 1 {
        Status::Problems(e, r.errors.len())
    } else {
        Status::Error(e)
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

fn syncing() -> Status {
    Status::Syncing(Progress::default(), 0.0)
}

/// While a run works: once a second, tell the UI which file is moving and how fast. Silent when nothing changed
/// (and while the run has not planned anything yet), so an idle account costs no rclone calls.
fn ticker(rc: &Rclone, group: &str, progress: &Mutex<Progress>, on_status: &dyn Fn(Status), over: &AtomicBool) {
    let (mut last, start) = (None, std::time::Instant::now());
    while !over.load(Ordering::Relaxed) {
        // Short naps, so a finished run is not held up for a whole second.
        for _ in 0..10 {
            if over.load(Ordering::Relaxed) {
                return;
            }
            std::thread::sleep(Duration::from_millis(100));
        }
        let mut p = progress.lock().map(|g| g.clone()).unwrap_or_default();
        if p.total == 0 && p.phase == Phase::Transfer {
            continue;
        }
        // Listing and comparing move no bytes: the clock is what shows the card is alive, and no rclone call is needed.
        let speed = if p.phase == Phase::Transfer {
            rc.speed(group).unwrap_or(0.0)
        } else {
            p.secs = start.elapsed().as_secs();
            0.0
        };
        let now = Some((p.clone(), speed));
        if now != last {
            on_status(Status::Syncing(p, speed));
            last = now;
        }
    }
}

/// One sleep per interval, not one per second: an idle account costs no wake-ups in between. The thread ends at its
/// next tick after the worker is gone (the channel is closed), so a stopped account leaves one sleeping thread for at
/// most `every`.
fn timer(tx: Sender<Event>, every: Duration, make: fn() -> Event) {
    std::thread::spawn(move || {
        loop {
            std::thread::sleep(every);
            if tx.send(make()).is_err() {
                return;
            }
        }
    });
}

/// Stops the feed's poller when the worker ends, however it ends.
struct StopOnDrop(Arc<AtomicBool>);

impl Drop for StopOnDrop {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Relaxed);
    }
}

/// Tells the ticker the run is over when dropped, also while unwinding from a panic.
struct EndRun<'a>(&'a AtomicBool);

impl Drop for EndRun<'_> {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Relaxed);
    }
}

/// What a cut-short run leaves to do: `(scan everything, paths to look at again)`. A cut-off full scan starts over; a
/// run of some paths only needs those paths again, not a listing of the whole cloud.
/// Asks for a full scan; the first cause stays the one the log shows.
fn mark(full: &mut bool, why: &mut &'static str, cause: &'static str) {
    if !*full {
        *why = cause;
    }
    *full = true;
}

fn after_cancel(did_full: bool, ran: Vec<String>) -> (bool, Vec<String>) {
    if did_full { (true, Vec::new()) } else { (false, ran) }
}

/// True when a full scan may confirm the unchecked-folders key: it was whole (not stopped or cut short) and nothing
/// that is not about one file went wrong. A file that failed is retried by its path, and the failures that matter to
/// this key (a folder that could not be trashed) are not about one file, so one stuck file cannot keep it open and
/// cost a listing of the whole cloud at every restart.
fn settled(r: &Report) -> bool {
    !r.has_global_failure() && r.aborted.is_none() && !r.cancelled
}

/// True when a run finished with nothing wrong and nothing cut short: only then may it clear a held decision. After an error, a stop or a Pause, the next scan must read it all again. A skipped
/// path (two names that differ only by case) is a state of the tree, not a failure: it is looked at again by every
/// full scan, and counting it here would keep a permanent clash from ever confirming anything.
fn clean(r: &Report) -> bool {
    r.errors.len() <= r.skipped && r.aborted.is_none() && !r.cancelled
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
                    // Back after an error: the card still shows the old message. The cursor did not move while the
                    // feed failed, so nothing was missed and no scan is needed (a listing of the whole cloud takes
                    // minutes after any network blip): a local walk is enough to clear the message.
                    let ev = ev.or_else(|| failures.take().map(|_| Event::Recovered));
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
    fn the_first_cause_of_a_full_scan_is_the_one_logged() {
        let (mut full, mut why) = (false, "rescan");
        mark(&mut full, &mut why, "now");
        mark(&mut full, &mut why, "feed");
        assert!(full);
        assert_eq!(why, "now");
        // A scan that ran resets `full`: the next cause counts again.
        full = false;
        mark(&mut full, &mut why, "feed");
        assert_eq!(why, "feed");
    }

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

    /// A run whose given paths failed, and one that failed in a way only a full scan can retry (a listing).
    fn failed(paths: &[&str]) -> Report {
        let mut r = Report::default();
        for p in paths {
            r.fail(p, "boom");
        }
        r
    }
    fn global() -> Report {
        let mut r = Report::default();
        r.errors.push("listing the cloud failed".into());
        r
    }
    fn s(p: &[&str]) -> Vec<String> {
        p.iter().map(|p| p.to_string()).collect()
    }

    #[test]
    fn a_failed_path_is_retried_alone_and_a_clean_run_of_other_paths_does_not_cancel_it() {
        let (mut r, t0) = (Retry::default(), Instant::now());
        let secs = Duration::from_secs;
        r.record(&failed(&["a"]), &s(&["a"]), false, t0);
        assert!(!r.full, "one failed file is no reason to list the cloud");
        assert!(
            r.may_confirm(&failed(&["a"])),
            "the cursor moves on: the path is remembered"
        );
        // Another change arrives and syncs fine before the retry is due.
        r.record(&Report::default(), &s(&["b"]), false, t0 + secs(10));
        assert_eq!(r.take_due(t0 + secs(30)), None);
        // Due: taken once, and it names just the failed path.
        assert_eq!(r.take_due(t0 + secs(61)), Some(Due::Paths(s(&["a"]))));
        assert_eq!(r.take_due(t0 + secs(62)), None);
        // It fails again: a longer wait, still no full scan.
        r.record(&failed(&["a"]), &s(&["a"]), false, t0 + secs(62));
        assert_eq!(r.wait(t0 + secs(62)), Some(secs(120)));
        assert!(!r.full);
        // It works: nothing is left.
        r.record(&Report::default(), &s(&["a"]), false, t0 + secs(200));
        assert!(r.wait(t0 + secs(200)).is_none() && r.paths.is_empty());
        assert_eq!(r.fails, 0);
    }

    #[test]
    fn what_only_a_full_scan_can_retry_holds_the_cursor_until_one_is_clean() {
        let (mut r, t0) = (Retry::default(), Instant::now());
        let secs = Duration::from_secs;
        r.record(&global(), &[], true, t0);
        assert!(r.full && !r.may_confirm(&global()));
        assert_eq!(r.take_due(t0 + secs(61)), Some(Due::Full));
        // A clean run of some paths in between ends nothing, and may not confirm the cursor.
        r.record(&Report::default(), &s(&["x"]), false, t0 + secs(70));
        assert!(r.full && !r.may_confirm(&Report::default()));
        // A clean full scan does, and forgets the paths it compared again.
        r.paths.insert("old".into());
        r.record(&failed(&["c"]), &[], true, t0 + secs(100));
        assert!(!r.full);
        assert_eq!(
            r.paths.iter().collect::<Vec<_>>(),
            ["c"],
            "what failed is what the scan says failed"
        );
        r.record(&Report::default(), &[], true, t0 + secs(200));
        assert!(r.paths.is_empty() && r.wait(t0 + secs(200)).is_none());
        assert!(r.may_confirm(&Report::default()));
    }

    #[test]
    fn a_file_that_changed_under_its_transfer_is_retried_soon_without_backing_off() {
        let (mut r, t0) = (Retry::default(), Instant::now());
        let mut rep = Report::default();
        rep.later.push("log.txt".into());
        r.record(&rep, &s(&["log.txt"]), false, t0);
        assert_eq!(r.fails, 0, "an edit is no failure");
        assert_eq!(r.wait(t0), Some(RETRY_AFTER));
        assert_eq!(r.take_due(t0 + RETRY_AFTER), Some(Due::Paths(s(&["log.txt"]))));
        // It changed again under the retry: a database or an image never stops, so the wait doubles, up to 30 minutes.
        let t1 = t0 + RETRY_AFTER;
        r.record(&rep, &s(&["log.txt"]), false, t1);
        assert_eq!(r.wait(t1), Some(RETRY_AFTER * 2));
        let mut t = t1;
        for _ in 0..10 {
            t += RETRY_MAX;
            assert!(r.take_due(t).is_some());
            r.record(&rep, &s(&["log.txt"]), false, t);
        }
        assert_eq!(r.wait(t), Some(RETRY_MAX));
        assert_eq!(r.fails, 0);
        // Another file that really failed is not held back by it.
        let mut both = failed(&["a"]);
        both.later.push("log.txt".into());
        r.record(&both, &s(&["a", "log.txt"]), false, t);
        assert_eq!(r.wait(t), Some(retry_after(1)));
        // Once it settles, the back-off starts over.
        r.record(&Report::default(), &s(&["a", "log.txt"]), false, t);
        assert!(r.wait(t).is_none() && r.later_fails == 0);
    }

    #[test]
    fn what_the_loop_does_after_a_run_follows_these_rules() {
        let (mut r, t0) = (Retry::default(), Instant::now());
        // A failed file: retried alone, the cursor may move on (it is remembered).
        r.finish_run(&failed(&["a"]), &s(&["a"]), false, false, t0);
        assert!(!r.full && r.may_confirm(&failed(&["a"])));
        // A decision still unconfirmed after errors: the retry is a full scan.
        r.finish_run(&failed(&["a"]), &[], true, true, t0);
        assert!(r.full && r.paths.is_empty());
        assert_eq!(r.take_due(t0 + Duration::from_secs(121)), Some(Due::Full));
        // A cut-off run: a full scan starts over, a run of some paths only looks at those again.
        assert_eq!(after_cancel(true, s(&["x"])), (true, vec![]));
        assert_eq!(after_cancel(false, s(&["x", "y"])), (false, s(&["x", "y"])));
        // The unchecked-folders key is confirmed by a whole scan whose failures are all about single files...
        assert!(settled(&failed(&["a"])));
        assert!(settled(&Report::default()));
        // ...never by one that stopped, was cut short, or failed in a way that is not about one file.
        assert!(!settled(&global()));
        let cut = Report {
            cancelled: true,
            ..Default::default()
        };
        assert!(!settled(&cut));
        let stopped = Report {
            aborted: Some((1, 10, Vec::new())),
            ..Default::default()
        };
        assert!(!settled(&stopped));
    }

    #[test]
    fn too_many_failed_paths_become_a_full_scan() {
        let (mut r, t0) = (Retry::default(), Instant::now());
        let mut rep = Report::default();
        for i in 0..=RETRY_PATHS_MAX {
            rep.fail(&format!("p{i}"), "x");
        }
        r.record(&rep, &[], false, t0);
        assert!(r.full && r.paths.is_empty());
    }

    #[test]
    fn a_decision_that_failed_is_carried_out_by_a_full_scan_not_by_paths() {
        // Also what a changed unchecked-folders list or rules file needs: its full scan failed on one file, and the key
        // is only confirmed by a clean full scan, so the retry must be one.
        let (mut r, t0) = (Retry::default(), Instant::now());
        r.record(&failed(&["a"]), &[], true, t0);
        assert!(
            !r.full,
            "without a decision or key to confirm a failed file is retried alone"
        );
        r.escalate(t0);
        assert!(r.full && r.paths.is_empty());
        assert_eq!(r.take_due(t0 + RETRY_AFTER), Some(Due::Full));
        r.record(&Report::default(), &[], true, t0 + RETRY_AFTER);
        assert!(!r.full && r.wait(t0 + RETRY_AFTER).is_none());
    }

    #[test]
    fn the_cursor_is_written_after_the_failed_paths_and_never_without_them() {
        let (t0, cur) = (Instant::now(), Some("cursor-2"));
        // A failed file: the list is saved, then the cursor moves on.
        let db = Db::memory().unwrap();
        let mut r = Retry::default();
        r.finish_run(&failed(&["a"]), &s(&["a"]), false, false, t0);
        assert!(r.persist(&db, &failed(&["a"]), cur));
        assert_eq!(db.meta("cursor").unwrap().as_deref(), Some("cursor-2"));
        assert!(db.meta("retry").unwrap().is_some_and(|t| t.contains("\"a\"")));
        // The DB takes the cursor but refuses the failed paths: the cursor must stay where it was.
        let db = Db::memory().unwrap();
        db.set_meta("cursor", "cursor-1").unwrap();
        db.refuse_meta_key("retry");
        let mut r = Retry::default();
        r.finish_run(&failed(&["a"]), &s(&["a"]), false, false, t0);
        assert!(!r.persist(&db, &failed(&["a"]), cur));
        assert_eq!(db.meta("cursor").unwrap().as_deref(), Some("cursor-1"));
        // A failed listing needs a full scan: the cursor stays back whatever was saved.
        let db = Db::memory().unwrap();
        let mut r = Retry::default();
        r.finish_run(&global(), &[], true, false, t0);
        assert!(!r.persist(&db, &global(), cur) && db.meta("cursor").unwrap().is_none());
        // Nothing to remember and a clean run: the cursor moves.
        let db = Db::memory().unwrap();
        let mut r = Retry::default();
        r.finish_run(&Report::default(), &[], false, false, t0);
        assert!(r.persist(&db, &Report::default(), cur));
        // No cursor to confirm (the feed had nothing): nothing is written.
        assert!(!r.persist(&db, &Report::default(), None));
    }

    #[test]
    fn a_db_that_cannot_take_the_failed_paths_keeps_the_cursor_back() {
        let db = Db::memory().unwrap();
        let (mut r, t0) = (Retry::default(), Instant::now());
        r.record(&failed(&["a"]), &[], false, t0);
        assert!(r.save(&db));
        // A DB that refuses the write: the meta table is gone.
        db.break_meta();
        r.record(&failed(&["a", "b"]), &[], false, t0);
        assert!(!r.save(&db), "the caller must not save the cursor then");
    }

    #[test]
    fn the_failed_paths_survive_a_restart_and_an_unreadable_value_means_a_full_scan() {
        let db = Db::memory().unwrap();
        let (mut r, t0) = (Retry::default(), Instant::now());
        r.save(&db);
        assert!(
            db.meta("retry").unwrap().is_none(),
            "nothing to remember, nothing written"
        );
        r.record(&failed(&["a/b.txt", "odd\nname"]), &[], false, t0);
        r.save(&db);
        let mut back = Retry::load(&db);
        assert!(!back.full);
        assert_eq!(
            back.take_due(Instant::now()),
            Some(Due::Paths(s(&["a/b.txt", "odd\nname"])))
        );
        // Healed: the value goes away.
        back.record(&Report::default(), &s(&["a/b.txt", "odd\nname"]), false, t0);
        back.save(&db);
        assert!(db.meta("retry").unwrap().is_none());
        db.set_meta("retry", "not json").unwrap();
        assert_eq!(Retry::load(&db).take_due(Instant::now()), Some(Due::Full));
    }

    #[test]
    fn a_panic_inside_the_run_still_ends_the_ticker() {
        // `scope` joins the ticker before it lets a panic out: without the guard this test would never return.
        let over = AtomicBool::new(false);
        let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            std::thread::scope(|s| {
                s.spawn(|| {
                    while !over.load(Ordering::Relaxed) {
                        std::thread::sleep(Duration::from_millis(10));
                    }
                });
                let _end = EndRun(&over);
                panic!("an engine bug");
            })
        }));
        assert!(r.is_err() && over.load(Ordering::Relaxed));
    }

    #[test]
    fn only_a_clean_run_confirms_anything() {
        assert!(clean(&Report::default()));
        let with = |f: fn(&mut Report)| {
            let mut r = Report::default();
            f(&mut r);
            clean(&r)
        };
        assert!(!with(|r| r.errors.push("x".into())));
        assert!(
            !with(|r| r.fail("a", "boom")),
            "a failed file leaves the key and a decision unconfirmed"
        );
        assert!(!with(|r| r.aborted = Some((1, 10, Vec::new()))));
        assert!(!with(|r| r.cancelled = true), "a Pause must never save the cursor");
        // A permanent name clash must not keep a rules change or a held decision from ever being confirmed.
        assert!(with(|r| r.skip("A.txt", "clash")));
        assert!(!with(|r| {
            r.skip("A.txt", "clash");
            r.errors.push("listing".into());
        }));
        let mut only_skips = Report::default();
        only_skips.skip("A.txt", "clash");
        assert!(Retry::default().may_confirm(&only_skips), "and the cursor moves on");
    }

    #[test]
    fn a_timer_tick_behind_a_long_scan_is_dropped_the_next_one_is_taken() {
        let secs = Duration::from_secs;
        assert!(scan_due(None), "no scan yet");
        // A listing slower than the interval: the ticks queued meanwhile are read right after it ended.
        assert!(!scan_due(Some(secs(0))));
        assert!(!scan_due(Some(secs(149))));
        assert!(scan_due(Some(secs(150))));
        assert!(scan_due(Some(secs(300))));
    }

    #[test]
    fn pause_and_stop_raise_the_cancel_flag_and_only_the_resume_lowers_it() {
        // `spawn` needs rclone and an account: the handle alone shows what the buttons do.
        let (tx, rx) = channel();
        let r = Runner {
            tx,
            handle: None,
            cancel: Arc::new(AtomicBool::new(false)),
        };
        r.pause(false);
        assert!(!r.cancel.load(Ordering::Relaxed), "resume never cancels");
        r.pause(true);
        assert!(r.cancel.load(Ordering::Relaxed));
        r.cancel.store(false, Ordering::Relaxed);
        r.stop();
        assert!(r.cancel.load(Ordering::Relaxed));
        assert_eq!(rx.try_iter().count(), 3);
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
