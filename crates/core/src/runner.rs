//! One running account: a worker thread that owns the engine and the DB, fed by three sources through one channel:
//! inotify (local edits), the provider's delta feed (remote edits) and timers (safety nets, retries).
use std::{
    collections::BTreeSet,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
        mpsc::{Receiver, RecvTimeoutError, Sender, channel},
    },
    time::{Duration, Instant},
};

use notify::{EventKind, RecursiveMode, Watcher};

use crate::{
    accounts::{Account, account_dir},
    db::Db,
    delta::{self, Change, Poll},
    engine::{Engine, Report, ignored},
    rclone::{Rclone, RcloneRemote, exclusions, now_secs},
};

#[derive(Clone, Debug, PartialEq)]
pub enum Status {
    /// Epoch seconds of the last finished run.
    Idle(i64),
    Syncing,
    Paused,
    /// The delete guard stopped a run: (files to delete, files known). "Sync now" confirms it.
    Stopped(usize, usize),
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
    Pause(bool),
    Stop,
    Warn(String),
}

pub struct Runner {
    tx: Sender<Event>,
}

impl Runner {
    /// The user's "Sync now" on one account. After the delete guard stopped a run, it also confirms the deletion.
    pub fn sync_now(&self) {
        let _ = self.tx.send(Event::SyncNow);
    }
    /// A plain full comparison that never confirms anything (the tray's "Sync now" for every account at once).
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
const WALK_EVERY: Duration = Duration::from_secs(120);
const RETRY_AFTER: Duration = Duration::from_secs(60);

pub fn spawn(acct: Account, rc: Arc<Rclone>, on_status: impl Fn(Status) + Send + Sync + 'static) -> Runner {
    let (tx, rx) = channel();
    let runner = Runner { tx: tx.clone() };
    let on_status = Arc::new(on_status);
    std::thread::spawn(move || {
        if let Err(e) = work(acct, rc, tx, rx, on_status.clone()) {
            on_status(Status::Error(e));
        }
    });
    runner
}

fn work(
    acct: Account,
    rc: Arc<Rclone>,
    tx: Sender<Event>,
    rx: Receiver<Event>,
    on_status: Arc<dyn Fn(Status) + Send + Sync>,
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
    let cursor = db.meta("cursor").ok().flatten();
    let folder = acct.remote_path.trim_matches('/').to_string();
    let fs = format!("{}:{folder}", acct.id);
    let mut engine = Engine::new(
        root.clone(),
        RcloneRemote {
            rc: rc.clone(),
            fs,
            exclude: exclusions(&acct.kind),
        },
        db,
    );
    let stop = Arc::new(AtomicBool::new(false));

    // Local edits. If inotify cannot watch the tree (the watch limit), the walk timer runs more often.
    let watcher_tx = tx.clone();
    let watch_root = root.clone();
    let mut watcher = notify::recommended_watcher(move |res: notify::Result<notify::Event>| {
        let Ok(ev) = res else { return };
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
        // Not fatal: the folder is then compared every 20 s instead.
        on_status(Status::Error(format!("inotify: {e}")));
    }
    let walk_every = if watching.is_ok() {
        WALK_EVERY
    } else {
        Duration::from_secs(20)
    };
    timer(tx.clone(), stop.clone(), walk_every, || Event::Walk);

    // Remote edits: the provider's feed, or a periodic full scan.
    let kind = acct.kind.clone();
    let id = acct.id.clone();
    let token_rc = rc.clone();
    let token: delta::Token = Box::new(move || token_rc.token(&id).map(|t| t.0));
    let mut root_id = None;
    match delta::for_kind(&kind, token, &folder) {
        Some(feed) => {
            if !folder.is_empty() {
                // The id of the synced folder, to tell its files from the rest of the drive.
                root_id = rc
                    .stat(&format!("{}:", acct.id), &folder)
                    .ok()
                    .flatten()
                    .map(|i| i.id)
                    .filter(|i| !i.is_empty());
            }
            poller(feed, cursor.clone(), folder.is_empty(), tx.clone(), stop.clone());
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
    if paused {
        on_status(Status::Paused);
    }
    let (mut pending, mut full, mut walk) = (BTreeSet::new(), false, false);
    let mut new_cursor: Option<String> = None;
    let mut retry = false;
    let (mut force, mut stopped) = (false, false);
    loop {
        let first = if retry {
            rx.recv_timeout(RETRY_AFTER)
        } else {
            rx.recv().map_err(|_| RecvTimeoutError::Disconnected)
        };
        let mut events = match first {
            Ok(e) => vec![e],
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
                    pending.extend(i.paths);
                    new_cursor = Some(cur);
                }
                Event::Reset(cur) => {
                    full = true;
                    new_cursor = Some(cur);
                }
                Event::Root(id) => root_id = id,
                Event::Walk => walk = true,
                Event::Full => full = true,
                Event::SyncNow => {
                    full = true;
                    // Only a run that was actually stopped can be confirmed: a routine press must never switch
                    // the guard off (a renamed cloud folder or an unmounted drive would then empty the folder).
                    force = stopped;
                }
                Event::Pause(p) => {
                    paused = p;
                    on_status(if p { Status::Paused } else { Status::Syncing });
                    // What happened meanwhile is unknown (the feed keeps running, but be safe): compare all.
                    full |= !p;
                }
                Event::Stop => {
                    stop.store(true, Ordering::Relaxed);
                    return Ok(());
                }
                Event::Warn(w) => on_status(Status::Error(w)),
            }
        }
        if paused {
            continue;
        }
        if !(full || walk || !pending.is_empty()) {
            continue;
        }
        on_status(Status::Syncing);
        // After the guard stopped a run, "Sync now" is the user confirming the deletion.
        engine.guard_percent = if force { 100 } else { 50 };
        let report = if full {
            engine.full_scan()
        } else {
            let mut paths = engine.filter_local(std::mem::take(&mut pending).into_iter().collect());
            if walk {
                paths.extend(engine.local_changes());
            }
            paths.sort();
            paths.dedup();
            if paths.is_empty() {
                Report::default()
            } else {
                engine.sync_paths(&paths)
            }
        };
        pending.clear();
        (full, walk, force) = (false, false, false);
        stopped = report.aborted.is_some();
        retry = !report.errors.is_empty();
        if let Some(c) = new_cursor.take() {
            let _ = engine.db.set_meta("cursor", &c);
        }
        on_status(summary(&report));
    }
    Ok(())
}

fn summary(r: &Report) -> Status {
    if let Some((deletes, known)) = r.aborted {
        return Status::Stopped(deletes, known);
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
fn poller(
    mut feed: Box<dyn delta::RemoteChanges>,
    cursor: Option<String>,
    need_root: bool,
    tx: Sender<Event>,
    stop: Arc<AtomicBool>,
) {
    std::thread::spawn(move || {
        let sleep = |d: Duration| {
            let end = Instant::now() + d;
            while Instant::now() < end && !stop.load(Ordering::Relaxed) {
                std::thread::sleep(Duration::from_millis(500).min(d));
            }
        };
        if need_root {
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
    fn watcher_paths_are_relative_and_skip_temp_files() {
        let root = Path::new("/home/u/Sync");
        assert_eq!(rel(root, Path::new("/home/u/Sync/a/b.txt")).as_deref(), Some("a/b.txt"));
        assert_eq!(rel(root, root), None);
        assert_eq!(rel(root, Path::new("/home/u/Sync/.fastsync/trash/x")), None);
        assert_eq!(rel(root, Path::new("/home/u/Sync/x.partial")), None);
        assert_eq!(rel(root, Path::new("/elsewhere/x")), None);
    }

    #[test]
    fn summaries() {
        assert!(matches!(summary(&Report::default()), Status::Idle(_)));
        let aborted = Report {
            aborted: Some((5, 9)),
            ..Default::default()
        };
        assert_eq!(summary(&aborted), Status::Stopped(5, 9));
    }
}
