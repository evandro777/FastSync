//! The sync engine: compares local, remote and the DB (see `decide`) and moves the bytes through a `Remote`.
//! Safety rules: nothing is ever removed locally (it goes to `.fastsync/trash`), a local file is only trashed when
//! the DB proves we synced it (a download-only mirror also trashes local-only files, behind the delete guard), a DB row is written only after its transfer succeeded, downloads land in a temp file
//! and are renamed into place, and a run that would delete too much is stopped.
use std::{
    collections::{BTreeMap, BTreeSet, HashMap, VecDeque},
    fs,
    io::ErrorKind,
    path::{Path, PathBuf},
    sync::atomic::{AtomicUsize, Ordering::Relaxed},
};

use crate::{
    db::Db,
    decide::{Action, Base, Sig, case_collisions, conflict_name, decide, decide_down, delete_guard_ok},
    ignore::Rules,
    rclone::{Item, now_secs},
};

pub type Result<T> = std::result::Result<T, String>;

/// The cloud side. Paths are relative to the synced folder and use `/`.
pub trait Remote: Send + Sync {
    /// Every file and folder, recursively.
    fn list(&self) -> Result<Vec<Item>>;
    /// The files and folders directly in `rel` (`""` = the root), paths from the root: one request for many files.
    /// The default errors, which makes the engine ask for each file with `stat` instead.
    fn list_dir(&self, _rel: &str) -> Result<Vec<Item>> {
        Err("list_dir unsupported".into())
    }
    fn stat(&self, rel: &str) -> Result<Option<Item>>;
    fn upload(&self, local_root: &Path, rel: &str) -> Result<()>;
    /// Writes the remote file to exactly `dest` (a temp file; the engine moves it into place).
    fn download(&self, rel: &str, dest: &Path) -> Result<()>;
    fn delete(&self, rel: &str) -> Result<()>;
    /// Moves a cloud file server-side, without overwriting. On an error the engine uploads and deletes instead.
    fn rename(&self, _from: &str, _to: &str) -> Result<()> {
        Err("rename unsupported".into())
    }
    /// Removes the cloud folder `rel` only if it is empty; an error (also "not empty") changes nothing.
    fn remove_empty_dir(&self, _rel: &str) -> Result<()> {
        Err("rmdir unsupported".into())
    }
    /// Drops what the transport remembers about the cloud's folders. Called before a full listing: a folder moved or
    /// renamed in the cloud would otherwise still answer under its old path (rclone keeps the old folder id), and the
    /// engine would download its files again into the old place.
    fn forget_cache(&self) {}
    /// True only when the local file at `local_root/rel` is proven to have the same bytes as `remote` (by the
    /// service's own hash). Unknown means false: the caller then keeps both copies.
    fn same_content(&self, _local_root: &Path, _rel: &str, _remote: &Item) -> bool {
        false
    }
}

/// The engine's own folder inside the synced folder: temp downloads and the trash. Never synced.
pub const HIDDEN: &str = ".fastsync";

#[derive(Default, Debug)]
pub struct Report {
    pub transferred: usize,
    pub errors: Vec<String>,
    /// Set when the delete guard stopped the run: (files to delete, files known, the first `SAMPLE` of those paths).
    pub aborted: Option<(usize, usize, Vec<String>)>,
    /// Files that changed on both sides and were kept twice; the runner turns them into one alert per burst.
    pub conflicts: Vec<String>,
}

/// How many of the paths a stopped run would delete are named to the user (the card says "and N more").
pub const SAMPLE: usize = 5;

#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub enum Kind {
    #[default]
    Up,
    Down,
    Remove,
}

/// Where a run is: the file being handled now and how many of the planned ones are done. `total == 0` = not running.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Progress {
    pub done: usize,
    pub total: usize,
    pub kind: Kind,
    pub path: String,
}

struct Work {
    path: String,
    local: Option<Sig>,
    remote: Option<Item>,
    /// Set when the pairing made this a move: where the file was.
    from: Option<Box<Moved>>,
}

/// The old path of a move, as the plan saw it (`local` for a cloud rename, `remote` for a local one).
struct Moved {
    path: String,
    local: Option<Sig>,
    remote: Option<Item>,
}

type Planned = (Work, Action, Option<Base>);

/// Old paths tried per new path before giving up (each is one hash of a local file).
const PAIR_TRIES: usize = 8;

/// Smaller files are just downloaded: hashing a candidate costs more than the request saves.
const COPY_MIN: u64 = 1 << 20;

pub struct Engine<R: Remote> {
    pub root: PathBuf,
    pub remote: R,
    pub db: Db,
    /// Largest share (percent) of the known files one run may delete.
    pub guard_percent: usize,
    /// The user accepted this many deletes (the count the card showed): the guard lets a run delete no more than that,
    /// whatever the share, so a card that sat for days cannot approve a bigger delete than it named.
    pub confirmed_deletes: Option<usize>,
    /// The user chose "Restore": every delete this run would make is turned into the opposite copy (a file gone from
    /// the cloud goes back up, one gone from here comes back down). Never in a mirror, which cannot upload.
    pub restore: bool,
    /// Unchecked folders (selective sync): never compared, uploaded, downloaded or deleted on either side.
    pub excluded: Vec<String>,
    /// `rules.fastsyncignore`: matching files are never uploaded or downloaded.
    pub ignore: Rules,
    /// One-way mirror: the cloud is never written; local edits and local-only files go to the trash.
    pub download_only: bool,
    /// Read by the runner's ticker while `run` works.
    pub progress: std::sync::Arc<std::sync::Mutex<Progress>>,
}

/// Editor and downloader leftovers that must never be uploaded.
pub fn ignored(name: &str) -> bool {
    name == HIDDEN
        || name.ends_with(".partial")
        || name.ends_with(".part")
        || name.ends_with(".crdownload")
        // Dropbox Paper docs are listed but have no file content ("unsupported_file"): they can never be copied.
        || name.ends_with(".paper")
        || name.ends_with(".swp")
        || name.ends_with('~')
        || name.starts_with(".~lock.")
}

/// Where everything we replace or remove locally goes: `<root>/.fastsync/trash/<time>/<path>`.
pub fn trash_dir(root: &Path) -> PathBuf {
    root.join(HIDDEN).join("trash")
}

/// (files, bytes) in the trash. Never follows a symlink: a link counts as a file of its own size.
pub fn trash_usage(root: &Path) -> (u64, u64) {
    fn go(dir: &Path, total: &mut (u64, u64)) {
        let Ok(entries) = fs::read_dir(dir) else { return };
        for e in entries.flatten() {
            let Ok(m) = fs::symlink_metadata(e.path()) else {
                continue;
            };
            if m.is_dir() {
                go(&e.path(), total);
            } else {
                total.0 += 1;
                total.1 += m.len();
            }
        }
    }
    let mut total = (0, 0);
    go(&trash_dir(root), &mut total);
    total
}

/// Removes the trash for good. A missing folder is fine. `remove_dir_all` does not follow symlinks.
pub fn empty_trash(root: &Path) -> std::io::Result<()> {
    match fs::remove_dir_all(trash_dir(root)) {
        Err(e) if e.kind() != std::io::ErrorKind::NotFound => Err(e),
        _ => Ok(()),
    }
}

/// Deletes the trash batches (`trash/<unix seconds>/…`, named when the files were trashed) at least `days` old.
/// 0 keeps everything. A folder whose name is not a number is not ours and stays.
pub fn purge_trash(root: &Path, days: u32) -> std::io::Result<()> {
    purge_trash_at(root, days, now_secs())
}

fn purge_trash_at(root: &Path, days: u32, now: i64) -> std::io::Result<()> {
    if days == 0 {
        return Ok(());
    }
    let limit = now.saturating_sub(i64::from(days) * 86_400);
    let entries = match fs::read_dir(trash_dir(root)) {
        Ok(e) => e,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(e),
    };
    let mut first_error = None;
    for e in entries.flatten() {
        let Some(stamp) = e.file_name().to_str().and_then(|n| n.parse::<i64>().ok()) else {
            continue;
        };
        if stamp > limit {
            continue;
        }
        // One stuck batch must not keep the others from going.
        match fs::remove_dir_all(e.path()) {
            Err(e) if e.kind() != std::io::ErrorKind::NotFound => first_error = first_error.or(Some(e)),
            _ => {}
        }
    }
    first_error.map_or(Ok(()), Err)
}

/// A remote name must stay inside the folder: a hostile `../../.bashrc` would otherwise write outside it.
pub fn safe_rel(p: &str) -> bool {
    !p.is_empty()
        && !p.contains('\0')
        && p.split('/')
            .all(|c| !c.is_empty() && c != "." && c != ".." && !ignored(c))
}

/// True when `path` is one of the unchecked folders (`list`) or sits inside one.
pub fn excluded(path: &str, list: &[String]) -> bool {
    list.iter()
        .any(|e| path == e || path.strip_prefix(e.as_str()).is_some_and(|r| r.starts_with('/')))
}

/// The unchecked folders after the user ticks or unticks `path`. Unticking a folder makes its own entries redundant.
pub fn toggle_excluded(list: &[String], path: &str, checked: bool) -> Vec<String> {
    let mut out: Vec<String> = list
        .iter()
        .filter(|e| *e != path && !excluded(e, &[path.to_string()]))
        .cloned()
        .collect();
    if !checked {
        out.push(path.to_string());
    }
    out
}

fn sig_of(m: &fs::Metadata) -> Sig {
    let mtime = m
        .modified()
        .ok()
        .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
        .map_or(0, |d| d.as_millis() as i64);
    Sig { size: m.len(), mtime }
}

/// Regular files only: a symlink could point anywhere, and empty folders are not synced.
fn local_sig(root: &Path, rel: &str) -> Option<Sig> {
    let m = fs::symlink_metadata(root.join(rel)).ok()?;
    m.is_file().then(|| sig_of(&m))
}

/// True when a folder on the way to `rel` (below `root`) is a symlink.
fn under_symlink(root: &Path, rel: &str) -> bool {
    let mut p = root.to_path_buf();
    let mut parts = rel.split('/').peekable();
    while let Some(c) = parts.next() {
        if parts.peek().is_none() {
            break;
        }
        p.push(c);
        if fs::symlink_metadata(&p).is_ok_and(|m| m.file_type().is_symlink()) {
            return true;
        }
    }
    false
}

/// Every regular file below `root` with its stamp, plus the places that could not be read (relative; `""` = the root
/// itself). What lives there is unknown, not gone: an unreadable folder must never read as "the user deleted
/// everything in it" (see `blocked`).
enum Lookup {
    One(usize),
    /// A folder's listing for these indexes.
    Dir {
        dir: String,
        idx: Vec<usize>,
    },
}

/// The cloud's answer for each path, as `(index in paths, answer)` in path order. One request per file took ~30-60 s
/// for 84 new files, so a folder with more changed files than its listing has pages (~1000 entries each, estimated
/// from the DB) is listed once instead; the rest is `stat`, `PARALLEL` at a time.
fn lookup<R: Remote>(remote: &R, db: &Db, paths: &[String]) -> Vec<(usize, Result<Option<Item>>)> {
    let mut by_dir: BTreeMap<&str, Vec<usize>> = BTreeMap::new();
    for (i, p) in paths.iter().enumerate() {
        by_dir
            .entry(p.rsplit_once('/').map_or("", |(d, _)| d))
            .or_default()
            .push(i);
    }
    let mut jobs = Vec::new();
    for (dir, idx) in by_dir {
        // A DB error means "unknown size": ask for each file.
        let known = db.count_in(dir).unwrap_or(usize::MAX);
        if idx.len() > known / 1000 + 1 {
            jobs.push(Lookup::Dir { dir: dir.into(), idx });
        } else {
            jobs.extend(idx.into_iter().map(Lookup::One));
        }
    }
    let next = AtomicUsize::new(0);
    let mut got: Vec<(usize, Result<Option<Item>>)> = std::thread::scope(|s| {
        let handles: Vec<_> = (0..PARALLEL.min(jobs.len()))
            .map(|_| {
                s.spawn(|| {
                    let mut got = Vec::new();
                    while let Some(job) = jobs.get(next.fetch_add(1, Relaxed)) {
                        match job {
                            Lookup::One(i) => got.push((*i, remote.stat(&paths[*i]))),
                            Lookup::Dir { dir, idx } => match remote.list_dir(dir) {
                                Ok(items) => {
                                    let mut by_path: HashMap<String, Item> =
                                        items.into_iter().map(|i| (i.path.clone(), i)).collect();
                                    got.extend(idx.iter().map(|&i| (i, Ok(by_path.remove(&paths[i])))));
                                }
                                // "Not found" must not by itself read as "everything in it is gone" (a glitch did once
                                // for the root): the folder's own `stat` has to say it is gone too. One request
                                // instead of one per file, which took a minute for 84 deleted files.
                                Err(e)
                                    if !dir.is_empty()
                                        && e.to_lowercase().contains("not found")
                                        && matches!(remote.stat(dir), Ok(None)) =>
                                {
                                    got.extend(idx.iter().map(|&i| (i, Ok(None))));
                                }
                                Err(_) => got.extend(idx.iter().map(|&i| (i, remote.stat(&paths[i])))),
                            },
                        }
                    }
                    got
                })
            })
            .collect();
        handles
            .into_iter()
            .flat_map(|h| h.join().unwrap_or_else(|e| std::panic::resume_unwind(e)))
            .collect()
    });
    got.sort_by_key(|(i, _)| *i);
    got
}

pub fn scan(root: &Path) -> (HashMap<String, Sig>, Vec<String>) {
    fn go(dir: &Path, prefix: &str, out: &mut HashMap<String, Sig>, bad: &mut Vec<String>) {
        let rd = match fs::read_dir(dir) {
            Ok(rd) => rd,
            Err(_) => {
                bad.push(prefix.to_string());
                return;
            }
        };
        for e in rd {
            let Ok(e) = e else {
                // The listing itself broke half-way: the rest of this folder is unknown.
                bad.push(prefix.to_string());
                break;
            };
            let Some(name) = e.file_name().to_str().map(str::to_string) else {
                // Cannot be named in the DB or the cloud: reported (with a lossy name, which matches no real path, so
                // nothing else is blocked) instead of vanishing from the sync without a word.
                let lossy = e.file_name().to_string_lossy().into_owned();
                bad.push(if prefix.is_empty() {
                    lossy
                } else {
                    format!("{prefix}/{lossy}")
                });
                continue;
            };
            if ignored(&name) {
                continue;
            }
            let rel = if prefix.is_empty() {
                name
            } else {
                format!("{prefix}/{name}")
            };
            let Ok(m) = e.metadata() else {
                bad.push(rel);
                continue;
            };
            if m.is_dir() {
                go(&e.path(), &rel, out, bad);
            } else if m.is_file() {
                out.insert(rel, sig_of(&m));
            }
        }
    }
    let (mut out, mut bad) = (HashMap::new(), Vec::new());
    go(root, "", &mut out, &mut bad);
    (out, bad)
}

/// True when `path` is, or sits inside, a place `scan` could not read.
fn blocked(path: &str, bad: &[String]) -> bool {
    bad.iter()
        .any(|b| b.is_empty() || excluded(path, std::slice::from_ref(b)))
}

fn today() -> String {
    stamp(now_secs())[..10].to_string()
}

/// "2026-11-13 14:05:09" (UTC) for a unix time: the activity log's copyable text.
pub fn stamp(ts: i64) -> String {
    // Civil date from days since 1970 (Howard Hinnant's algorithm).
    let secs = ts.rem_euclid(86_400);
    let z = ts.div_euclid(86_400) + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    format!(
        "{:04}-{m:02}-{d:02} {:02}:{:02}:{:02}",
        yoe + era * 400 + i64::from(m <= 2),
        secs / 3600,
        secs % 3600 / 60,
        secs % 60
    )
}

impl<R: Remote> Engine<R> {
    pub fn new(root: PathBuf, remote: R, db: Db) -> Self {
        Self {
            root,
            remote,
            db,
            guard_percent: 50,
            confirmed_deletes: None,
            restore: false,
            excluded: Vec::new(),
            ignore: Rules::default(),
            download_only: false,
            progress: Default::default(),
        }
    }

    fn set_progress(&self, p: Progress) {
        if let Ok(mut g) = self.progress.lock() {
            *g = p;
        }
    }

    /// An ignored path nobody synced before costs nothing: no stat, no transfer.
    fn skippable(&self, path: &str) -> bool {
        self.ignore.matches(path) && self.db.base(path).ok().flatten().is_none()
    }

    /// A folder the user just unticked: what we synced there leaves the disk (into the trash) and is forgotten, so
    /// the cloud copy stays and nothing is ever deleted remotely. A file edited since the last sync stays where it is.
    fn drop_excluded(&mut self, report: &mut Report) {
        for (path, base) in self.db.all().unwrap_or_default() {
            if !excluded(&path, &self.excluded) {
                continue;
            }
            let untouched =
                !under_symlink(&self.root, &path) && local_sig(&self.root, &path).is_some_and(|l| base.local == l);
            if untouched && let Err(e) = self.trash(&path) {
                self.db.log("error", &path, None, &e.to_string());
                report.errors.push(format!("{path}: {e}"));
                continue;
            }
            if let Err(e) = self.db.forget(&path) {
                report.errors.push(format!("{path}: {e}"));
            }
        }
    }

    /// Lists both sides and compares everything. Used for the first sync, after a delta reset and for providers
    /// without a delta API.
    pub fn full_scan(&mut self) -> Report {
        self.remote.forget_cache();
        let items = match self.remote.list() {
            Ok(i) => i,
            Err(e) => {
                self.db
                    .log("error", "", None, &format!("listing the cloud failed: {e}"));
                return Report {
                    errors: vec![e],
                    ..Default::default()
                };
            }
        };
        let mut report = Report::default();
        self.drop_excluded(&mut report);
        let dirs: Vec<(String, String)> = items
            .iter()
            .filter(|i| i.is_dir && !i.id.is_empty())
            .map(|i| (i.id.clone(), i.path.clone()))
            .collect();
        if let Err(e) = self.db.set_dirs(&dirs) {
            report.errors.push(e.to_string());
        }
        let mut remote: HashMap<String, Item> = HashMap::new();
        // Drive allows two files with one name in a folder: a map would keep either one, so the local copy could flip
        // between them. Such a path is skipped (like a case clash) until the user renames one.
        let mut duplicate: BTreeSet<String> = BTreeSet::new();
        for i in items {
            if i.is_dir || !safe_rel(&i.path) || excluded(&i.path, &self.excluded) {
                continue;
            }
            if let Some(prev) = remote.insert(i.path.clone(), i) {
                duplicate.insert(prev.path);
            }
        }
        let (mut local, bad) = scan(&self.root);
        local.retain(|p, _| !excluded(p, &self.excluded));
        for b in &bad {
            self.db.log(
                "error",
                b,
                None,
                "cannot read this (a folder, or a name that is not valid text), skipped",
            );
        }
        let known: Vec<String> = self.db.all().unwrap_or_default().into_iter().map(|(p, _)| p).collect();
        let paths: BTreeSet<&str> = local
            .keys()
            .chain(remote.keys())
            .chain(known.iter())
            .map(String::as_str)
            .collect();
        let clash = case_collisions(paths.iter().copied());
        let mut work = VecDeque::new();
        for p in paths {
            if blocked(p, &bad) {
                continue;
            }
            if duplicate.contains(p) {
                let why = "two cloud files have this name, skipped";
                self.db.log("error", p, None, why);
                report.errors.push(format!("{p}: {why}"));
                continue;
            }
            if clash.contains(p) {
                let why = "another file differs only by upper/lower case, skipped";
                self.db.log("error", p, None, why);
                report.errors.push(format!("{p}: {why}"));
                continue;
            }
            work.push_back(Work {
                path: p.to_string(),
                local: local.get(p).copied(),
                remote: remote.get(p).cloned(),
                from: None,
            });
        }
        self.run(work, report)
    }

    /// Compares only these paths (from inotify or a remote delta). A folder expands to the files in it.
    pub fn sync_paths(&mut self, paths: &[String]) -> Report {
        let mut report = Report::default();
        let mut all = BTreeSet::new();
        let mut bad: Vec<String> = Vec::new();
        for p in paths {
            if !safe_rel(p) {
                continue;
            }
            // A path behind a symlinked folder is outside what we sync: listing or writing it would leak or
            // overwrite files anywhere the link points.
            if under_symlink(&self.root, p) {
                continue;
            }
            let abs = self.root.join(p);
            // `symlink_metadata`: a link to a folder is not a folder here, `is_dir()` would follow it.
            match fs::symlink_metadata(&abs) {
                Ok(m) if m.is_dir() => {
                    let (files, unreadable) = scan(&abs);
                    all.extend(files.into_keys().map(|f| format!("{p}/{f}")));
                    bad.extend(
                        unreadable
                            .into_iter()
                            .map(|b| if b.is_empty() { p.clone() } else { format!("{p}/{b}") }),
                    );
                }
                // Anything but "not there" (permission denied, I/O error) says nothing about the file. A folder that
                // became a file leaves the paths below it gone, not unreadable.
                Err(e) if !matches!(e.kind(), ErrorKind::NotFound | ErrorKind::NotADirectory) => bad.push(p.clone()),
                _ => {}
            }
            // A folder that is gone locally: everything we knew under it.
            all.extend(self.db.under(p).unwrap_or_default());
            all.insert(p.clone());
        }
        all.retain(|p| !excluded(p, &self.excluded) && !self.skippable(p) && !blocked(p, &bad));
        let all: Vec<String> = all.into_iter().collect();
        let mut work = VecDeque::new();
        for (i, res) in lookup(&self.remote, &self.db, &all) {
            let p = all[i].clone();
            match res {
                Ok(item) => work.push_back(Work {
                    local: local_sig(&self.root, &p),
                    remote: item.filter(|i| !i.is_dir),
                    from: None,
                    path: p,
                }),
                Err(e) => {
                    self.db.log("error", &p, None, &e);
                    report.errors.push(format!("{p}: {e}"));
                }
            }
        }
        self.run(work, report)
    }

    /// The paths of one run: what inotify reported (minus the echo of our own writes) plus what the cloud's feed
    /// reported (never filtered: a file edited or deleted in the cloud has an untouched local copy, which
    /// `filter_local` would read as an echo and drop), and the local walk's finds when `walk`.
    pub fn paths_to_sync(&self, local: Vec<String>, feed: Vec<String>, walk: bool) -> Vec<String> {
        let mut paths = self.filter_local(local);
        paths.extend(feed);
        if walk {
            paths.extend(self.local_changes());
        }
        paths.sort();
        paths.dedup();
        paths
    }

    /// Drops the paths whose local file is exactly what the DB recorded: the echo of our own downloads, which
    /// would otherwise cost a remote call each. Folders and missing files stay.
    fn filter_local(&self, paths: Vec<String>) -> Vec<String> {
        paths
            .into_iter()
            .filter(|p| match (local_sig(&self.root, p), self.db.base(p).ok().flatten()) {
                (Some(l), Some(b)) => b.local != l,
                _ => true,
            })
            .collect()
    }

    /// Paths that changed locally since the DB was written: how edits made while the app was closed are found.
    pub fn local_changes(&self) -> Vec<String> {
        let (mut local, bad) = scan(&self.root);
        local.retain(|p, _| !excluded(p, &self.excluded) && !self.skippable(p));
        // A map, not a scan per file: this runs every couple of minutes on trees of 100k files.
        let known: HashMap<String, Base> = self
            .db
            .all()
            .unwrap_or_default()
            .into_iter()
            .filter(|(p, _)| !excluded(p, &self.excluded))
            .collect();
        let mut out: BTreeSet<String> = local
            .iter()
            .filter(|(p, s)| !known.get(*p).is_some_and(|b| b.local == **s))
            .map(|(p, _)| p.clone())
            .collect();
        out.extend(
            known
                .into_keys()
                .filter(|p| !local.contains_key(p) && !blocked(p, &bad)),
        );
        out.into_iter().collect()
    }

    /// Turns a delete plus a new path holding the same bytes into one move: a folder renamed in the cloud is renamed
    /// here instead of trashed and downloaded again, a folder renamed here is moved in the cloud instead of
    /// uploaded again. The proof is the service's own hash (`same_content`), so a service without one (MEGA) never
    /// pairs and keeps today's copy + delete. A wrong guess is impossible: equal hashes are equal bytes.
    /// shortcut: hashes one candidate at a time on this thread, at most `PAIR_TRIES` per new path; parallelise it
    /// if renaming thousands of files with a slow `local_hash` proves too slow.
    fn pair_moves(&self, plan: Vec<Planned>) -> Vec<Planned> {
        // Old paths by size: only a delete that was safe to make anyway (we synced it, the local copy untouched; the
        // cloud copy present and the local one gone) can become a move.
        let mut olds: HashMap<u64, Vec<usize>> = HashMap::new();
        for (i, (w, a, base)) in plan.iter().enumerate() {
            let size = match (a, w.local, &w.remote, base) {
                (Action::DeleteLocal, Some(l), _, Some(b)) if b.local == l => Some(l.size),
                (Action::DeleteRemote, None, Some(r), _) => Some(r.size),
                _ => None,
            };
            if let Some(s) = size {
                olds.entry(s).or_default().push(i);
            }
        }
        if olds.is_empty() {
            return plan;
        }
        let name = |p: &str| p.rsplit('/').next().unwrap_or(p).to_string();
        let parent = |p: &str| p.rsplit_once('/').map_or(String::new(), |(d, _)| d.to_string());
        let mut taken = vec![false; plan.len()];
        let mut pairs: Vec<(usize, usize)> = Vec::new();
        for (n, (w, a, _)) in plan.iter().enumerate() {
            // The new path, and the size it must have.
            let (size, local_move) = match (a, w.local, &w.remote) {
                (Action::Download, None, Some(r)) => (r.size, true),
                (Action::Upload, Some(l), None) => (l.size, false),
                _ => continue,
            };
            let mut cands: Vec<usize> = olds
                .get(&size)
                .into_iter()
                .flatten()
                .copied()
                .filter(|&o| {
                    let (ow, oa, _) = &plan[o];
                    !taken[o]
                        && (*oa == Action::DeleteLocal) == local_move
                        && (name(&ow.path) == name(&w.path) || parent(&ow.path) == parent(&w.path))
                })
                .collect();
            // The same file name first: a folder rename keeps the names, so that is nearly always the match.
            cands.sort_by_key(|&o| name(&plan[o].0.path) != name(&w.path));
            for o in cands.into_iter().take(PAIR_TRIES) {
                let ow = &plan[o].0;
                let same = if local_move {
                    // The old local file against the cloud's new item.
                    w.remote
                        .as_ref()
                        .is_some_and(|r| self.remote.same_content(&self.root, &ow.path, r))
                } else {
                    // The new local file against the cloud's old item.
                    ow.remote
                        .as_ref()
                        .is_some_and(|r| self.remote.same_content(&self.root, &w.path, r))
                };
                if same {
                    taken[o] = true;
                    pairs.push((o, n));
                    break;
                }
            }
        }
        if pairs.is_empty() {
            return plan;
        }
        let mut plan: Vec<Option<Planned>> = plan.into_iter().map(Some).collect();
        for (o, n) in pairs {
            let Some((old, old_action, _)) = plan[o].take() else {
                continue;
            };
            if let Some((new, action, _)) = plan[n].as_mut() {
                new.from = Some(Box::new(Moved {
                    path: old.path,
                    local: old.local,
                    remote: old.remote,
                }));
                *action = if old_action == Action::DeleteLocal {
                    Action::MoveLocal
                } else {
                    Action::MoveRemote
                };
            }
        }
        plan.into_iter().flatten().collect()
    }

    /// A new cloud file whose bytes already sit in another synced local file (a duplicate made in the cloud) is
    /// copied from that file instead of downloaded. Same proof as a move (`same_content`), but the source stays: it
    /// goes in `from` and `fetch` tries it first, falling back to the download on any doubt.
    /// shortcut: hashes one candidate at a time on this thread, `PAIR_TRIES` per file; parallelise it if many big
    /// duplicates arrive at once.
    fn pair_copies(&self, plan: &mut [Planned]) {
        let name = |p: &str| p.rsplit('/').next().unwrap_or(p).to_string();
        let parent = |p: &str| p.rsplit_once('/').map_or(String::new(), |(d, _)| d.to_string());
        for (w, action, _) in plan.iter_mut() {
            let (Action::Download, None, Some(r)) = (*action, w.local, &w.remote) else {
                continue;
            };
            if r.size < COPY_MIN {
                continue;
            }
            let Ok(mut cands) = self.db.paths_with_size(r.size) else {
                continue;
            };
            // Only a file still exactly as synced, that the rules do not hide, and not through a symlinked folder.
            cands.retain(|(p, s)| {
                *p != w.path
                    && !self.ignore.matches(p)
                    && !under_symlink(&self.root, p)
                    && local_sig(&self.root, p) == Some(*s)
            });
            cands.sort_by_key(|(p, _)| (name(p) != name(&w.path), parent(p) != parent(&w.path)));
            for (p, s) in cands.into_iter().take(PAIR_TRIES) {
                if self.remote.same_content(&self.root, &p, r) {
                    w.from = Some(Box::new(Moved {
                        path: p,
                        local: Some(s),
                        remote: None,
                    }));
                    break;
                }
            }
        }
    }

    fn run(&mut self, mut work: VecDeque<Work>, mut report: Report) -> Report {
        let mut plan = Vec::new();
        // A mirror trashes local-only files, and a new account has no DB rows to measure the guard against: count
        // the local files of this run too, or pointing it at a full folder would trash everything unchecked.
        let local_files = if self.download_only {
            work.iter().filter(|w| w.local.is_some()).count()
        } else {
            0
        };
        let (paths, cloud_seen) = (work.len(), work.iter().filter(|w| w.remote.is_some()).count());
        for w in work.drain(..) {
            let base = match self.db.base(&w.path) {
                Ok(b) => b,
                Err(e) => {
                    report.errors.push(format!("{}: {e}", w.path));
                    continue;
                }
            };
            let remote = w.remote.as_ref().map(sig_item);
            let action = if self.ignore.matches(&w.path) {
                // Dropbox's way: a rule that now matches a synced file removes the cloud copy and keeps the local one.
                // The delete guard still counts it, so a careless `*` stops instead of emptying the cloud.
                match (base, w.remote.is_some()) {
                    (Some(_), true) if !self.download_only => Action::DeleteRemote,
                    (Some(_), _) => Action::Forget,
                    _ => Action::None,
                }
            } else if self.download_only {
                decide_down(w.local, remote, base)
            } else {
                decide(w.local, remote, base)
            };
            // Two different stamps do not make two different files: an upload the app was closed during, or a file
            // put in the cloud by another tool, would otherwise be split into a conflict copy of itself.
            let identical = action == Action::Conflict
                && matches!((w.local, &w.remote), (Some(l), Some(r)) if l.size == r.size)
                && w.remote
                    .as_ref()
                    .is_some_and(|r| self.remote.same_content(&self.root, &w.path, r));
            let action = if identical { Action::Adopt } else { action };
            // Quitting mid-upload leaves an empty item in the cloud (OneDrive lists it, its website hides it). An
            // empty cloud file holds nothing to lose, so the local file goes up instead of being renamed away and
            // replaced by the empty one. Never in a mirror: there the cloud is the truth.
            let unfinished_upload = !self.download_only
                && action == Action::Conflict
                && matches!((w.local, &w.remote), (Some(l), Some(r)) if l.size > 0 && r.size == 0);
            let action = if unfinished_upload { Action::Upload } else { action };
            let action = match action {
                Action::DeleteLocal if self.restore && !self.download_only => Action::Upload,
                // Deleted here: the cloud copy comes back. An ignore rule that removed the cloud copy leaves the
                // local file (`local` is Some): forget it instead of overwriting.
                Action::DeleteRemote if self.restore && !self.download_only => {
                    if w.local.is_some() {
                        Action::Forget
                    } else {
                        Action::Download
                    }
                }
                a => a,
            };
            if action != Action::None {
                plan.push((w, action, base));
            }
        }
        // Before the guard: a rename is not a delete, whatever the folder's size.
        let mut plan = self.pair_moves(plan);
        self.pair_copies(&mut plan);
        let deletes = plan
            .iter()
            .filter(|(_, a, _)| matches!(a, Action::DeleteLocal | Action::DeleteRemote))
            .count();
        // An unreadable DB must not read as "0 known files": that would switch the guard off.
        let known = match self.db.count() {
            Ok(n) => n.max(local_files),
            Err(e) => {
                report.errors.push(format!("state database: {e}"));
                return report;
            }
        };
        let allowed = match self.confirmed_deletes {
            Some(cap) => deletes <= cap,
            None => delete_guard_ok(deletes, known, self.guard_percent),
        };
        if !allowed {
            let sample = plan
                .iter()
                .filter(|(_, a, _)| matches!(a, Action::DeleteLocal | Action::DeleteRemote))
                .take(SAMPLE)
                .map(|(w, ..)| w.path.clone())
                .collect();
            // To tell an empty listing (cloud_seen 0) from a different cloud the next time this trips.
            let remote_deletes = plan.iter().filter(|(_, a, _)| *a == Action::DeleteRemote).count();
            let line = format!(
                "{deletes} deletes ({remote_deletes} in the cloud) of {known} known; {paths} paths in this run, {cloud_seen} seen in the cloud"
            );
            eprintln!("delete guard: {line}");
            // Which files, and which side: without it a trip cannot be diagnosed afterwards.
            let mut listed = line.clone();
            for (i, (w, a, _)) in plan
                .iter()
                .filter(|(_, a, _)| matches!(a, Action::DeleteLocal | Action::DeleteRemote))
                .enumerate()
            {
                let side = if *a == Action::DeleteLocal { "local" } else { "cloud" };
                eprintln!(
                    "delete guard:   {side}: {} (local {}, cloud {})",
                    w.path,
                    w.local.is_some(),
                    w.remote.is_some()
                );
                if i < SAMPLE {
                    listed.push_str(&format!("\n{side}: {}", w.path));
                }
            }
            self.db.log("stopped", "", None, &listed);
            report.aborted = Some((deletes, known, sample));
            return report;
        }
        let mut plan: VecDeque<_> = plan.into();
        let (mut done, mut total) = (0, plan.len());
        let mut transfers = Vec::new();
        // Cloud paths that were deleted or moved away this run: their folders may be empty now.
        let mut gone: Vec<String> = Vec::new();
        while let Some((w, action, base)) = plan.pop_front() {
            // Uploads and downloads wait on the cloud's latency, not on this machine: they run side by side below.
            // Everything else (deletes, conflicts, DB-only rows) stays one at a time, so folders are never removed
            // while a transfer fills them.
            if matches!(
                action,
                Action::Upload | Action::Download | Action::MoveLocal | Action::MoveRemote
            ) {
                transfers.push((w, action));
                continue;
            }
            // Adopt/Forget only touch the DB: they count as done but never name a file.
            let kind = match action {
                Action::Upload => Some(Kind::Up),
                Action::Download | Action::Conflict => Some(Kind::Down),
                Action::DeleteLocal | Action::DeleteRemote => Some(Kind::Remove),
                _ => None,
            };
            if let Some(kind) = kind {
                self.set_progress(Progress {
                    done,
                    total,
                    kind,
                    path: w.path.clone(),
                });
            }
            done += 1;
            match self.exec(&w, action, base) {
                Ok(extra) => {
                    log_done(&self.db, &w, action, &mut report);
                    if action == Action::DeleteRemote {
                        gone.push(w.path.clone());
                    }
                    // A conflict leaves the renamed local copy to be uploaded as a new file.
                    if let Some(e) = extra {
                        plan.push_back((e, Action::Upload, None));
                        total += 1;
                    }
                }
                Err(e) => log_failed(&self.db, &w, &e, &mut report),
            }
        }
        self.run_transfers(transfers, done, total, &mut report, &mut gone);
        self.prune_cloud_dirs(&gone);
        self.set_progress(Progress::default());
        report
    }

    /// Removes the cloud folders that deleting or moving files out left empty. Sync has no folders of its own, so
    /// without this the old folder of a moved or emptied one stays in the cloud for ever. Deepest first, so a chain of
    /// empty folders goes in one pass. `rmdir` only ever removes an empty folder (a full one is an error, ignored
    /// here), never the synced root, never an unchecked folder.
    fn prune_cloud_dirs(&self, gone: &[String]) {
        let mut dirs: Vec<&str> = gone
            .iter()
            .flat_map(|p| p.rmatch_indices('/').map(move |(i, _)| &p[..i]))
            .filter(|d| !excluded(d, &self.excluded))
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        dirs.sort_by_key(|d| std::cmp::Reverse(d.matches('/').count()));
        for d in dirs {
            let _ = self.remote.remove_empty_dir(d);
        }
    }

    /// The uploads and downloads of a run, `PARALLEL` at a time. Only the cloud calls and file moves run in the
    /// workers; every DB write, log line and counter stays on this thread (the DB has a single connection).
    /// `gone`: gets the cloud path a finished move left (for `prune_cloud_dirs`).
    fn run_transfers(
        &mut self,
        jobs: Vec<(Work, Action)>,
        done: usize,
        total: usize,
        report: &mut Report,
        gone: &mut Vec<String>,
    ) {
        if jobs.is_empty() {
            return;
        }
        let workers = PARALLEL.min(jobs.len());
        let queue = std::sync::Mutex::new(VecDeque::from(jobs));
        let finished = AtomicUsize::new(done);
        let (remote, root, download_only) = (&self.remote, &self.root, self.download_only);
        let (progress, db) = (&self.progress, &self.db);
        let (tx, rx) = std::sync::mpsc::channel();
        std::thread::scope(|s| {
            for _ in 0..workers {
                let tx = tx.clone();
                let (queue, finished) = (&queue, &finished);
                s.spawn(move || {
                    while let Some((w, action)) = queue.lock().ok().and_then(|mut q| q.pop_front()) {
                        // Shows the file that started last; `done` counts the finished ones.
                        if let Ok(mut g) = progress.lock() {
                            *g = Progress {
                                done: finished.load(Relaxed),
                                total,
                                kind: if matches!(action, Action::Upload | Action::MoveRemote) {
                                    Kind::Up
                                } else {
                                    Kind::Down
                                },
                                path: w.path.clone(),
                            };
                        }
                        let result = transfer(remote, root, download_only, &w, action);
                        if tx.send((w, action, result)).is_err() {
                            break;
                        }
                    }
                });
            }
            drop(tx);
            for (w, action, result) in rx {
                // The new row first: a crash in between leaves two rows (the old one reads as gone on both sides and
                // is forgotten next run), never none.
                let saved = result.and_then(|(b, id)| {
                    db.set(&w.path, &b, &id).map_err(|e| e.to_string())?;
                    // A download's `from` is the file it was copied from: that one stays.
                    match &w.from {
                        Some(f) if action != Action::Download => db.forget(&f.path).map_err(|e| e.to_string()),
                        _ => Ok(()),
                    }
                });
                match saved {
                    Ok(()) => {
                        log_done(db, &w, action, report);
                        if let (Action::MoveRemote, Some(f)) = (action, &w.from) {
                            gone.push(f.path.clone());
                        }
                    }
                    Err(e) => log_failed(db, &w, &e, report),
                }
                finished.fetch_add(1, Relaxed);
            }
        });
    }

    fn exec(&mut self, w: &Work, action: Action, base: Option<Base>) -> Result<Option<Work>> {
        let db = |e: rusqlite::Error| e.to_string();
        let (path, rid) = (w.path.as_str(), w.remote.as_ref().map_or("", |r| r.id.as_str()));
        refuse(&self.root, self.download_only, path, action)?;
        match action {
            Action::None => {}
            Action::Forget => self.db.forget(path).map_err(db)?,
            Action::Adopt => {
                let (l, r) = (
                    w.local.ok_or("file vanished")?,
                    sig_item(w.remote.as_ref().ok_or("file vanished")?),
                );
                self.db.set(path, &Base { local: l, remote: r }, rid).map_err(db)?;
            }
            Action::Upload | Action::Download | Action::MoveLocal | Action::MoveRemote => {
                return Err("handled by run_transfers".into());
            }
            Action::DeleteRemote => {
                if let Some(old) = &w.remote {
                    cloud_unchanged(&self.remote, path, old)?;
                }
                self.remote.delete(path)?;
                self.db.forget(path).map_err(db)?;
            }
            Action::DeleteLocal => {
                // Only with a base (decide guarantees it) and an unchanged local file: we synced it, so it may go.
                debug_assert!(base.is_some() || self.download_only);
                // The plan may be hours old: an edit made since then must not be moved away.
                if local_sig(&self.root, path) != w.local {
                    return Err("changed locally, will retry".into());
                }
                self.trash(path).map_err(|e| e.to_string())?;
                self.db.forget(path).map_err(db)?;
            }
            Action::Conflict if self.download_only => {
                // The cloud wins: the local copy goes to the trash, nothing is uploaded.
                let r = w.remote.as_ref().ok_or("file vanished")?;
                self.trash(path).map_err(|e| e.to_string())?;
                self.download(path, None, r)?;
            }
            Action::Conflict => {
                let r = w.remote.as_ref().ok_or("file vanished")?;
                let date = today();
                let aside = (1..)
                    .map(|n| conflict_name(path, &date, n))
                    .find(|c| !self.root.join(c).exists())
                    .unwrap_or_default();
                fs::rename(self.root.join(path), self.root.join(&aside)).map_err(|e| e.to_string())?;
                self.download(path, None, r)?;
                return Ok(Some(Work {
                    local: local_sig(&self.root, &aside),
                    remote: None,
                    from: None,
                    path: aside,
                }));
            }
        }
        Ok(None)
    }

    fn download(&mut self, path: &str, expect_local: Option<Sig>, r: &Item) -> Result<()> {
        let (b, id) = fetch(&self.remote, &self.root, path, expect_local, r, None)?;
        self.db.set(path, &b, &id).map_err(|e| e.to_string())
    }

    /// Moves a file to `.fastsync/trash/<time>/<path>` (same filesystem, so a rename), then removes the empty
    /// folders it leaves behind.
    fn trash(&self, rel: &str) -> std::io::Result<()> {
        let batch = trash_dir(&self.root).join(now_secs().to_string());
        let mut dest = batch.join(rel);
        // `rename` replaces a file: the same path trashed twice in one second must not eat the first copy.
        for n in 1.. {
            if fs::symlink_metadata(&dest).is_err() {
                break;
            }
            dest = batch.join(format!("{rel}.{n}"));
        }
        fs::create_dir_all(dest.parent().unwrap_or(&self.root))?;
        fs::rename(self.root.join(rel), dest)?;
        prune_empty(&self.root, rel);
        Ok(())
    }
}

fn sig_item(i: &Item) -> Sig {
    Sig {
        size: i.size,
        mtime: i.mtime,
    }
}

/// Uploads and downloads running at once. The cloud's per-call latency, not bandwidth, bounds a sync of many small
/// files; four keeps well inside the providers' rate limits (rclone retries a 429 by itself).
const PARALLEL: usize = 4;

/// The refusals every action shares: nothing is read or written through a symlinked folder, and a mirror never
/// changes the cloud.
fn refuse(root: &Path, download_only: bool, path: &str, action: Action) -> Result<()> {
    if action != Action::None && action != Action::Forget && under_symlink(root, path) {
        return Err("inside a symlinked folder, skipped".into());
    }
    if download_only && matches!(action, Action::Upload | Action::DeleteRemote | Action::MoveRemote) {
        return Err("download-only: the cloud is never changed".into());
    }
    Ok(())
}

/// Errors unless the cloud copy still looks like `planned`, the one the run's plan was made from.
fn cloud_unchanged<R: Remote>(remote: &R, path: &str, planned: &Item) -> Result<()> {
    match remote.stat(path)? {
        Some(now) if sig_item(&now).same_as(&sig_item(planned)) => Ok(()),
        _ => Err("changed in the cloud, will retry".into()),
    }
}

/// One upload or download, without touching the DB: the row to write (how both sides look now, the remote id).
/// Runs on the workers, so it takes the pieces of the engine it needs instead of `&self`.
fn transfer<R: Remote>(
    remote: &R,
    root: &Path,
    download_only: bool,
    w: &Work,
    action: Action,
) -> Result<(Base, String)> {
    refuse(root, download_only, &w.path, action)?;
    match action {
        Action::Upload => {
            let l = w.local.ok_or("file vanished")?;
            // Overwriting a cloud copy: it must still be the one this run saw.
            match &w.remote {
                Some(old) => cloud_unchanged(remote, &w.path, old)?,
                // A new file: another device may have put one there since the listing (a first sync takes hours).
                None if remote.stat(&w.path)?.is_some() => return Err("appeared in the cloud, will retry".into()),
                None => {}
            }
            remote.upload(root, &w.path)?;
            // What the remote reports now is the base, so its own echo in a delta compares equal.
            let r = remote.stat(&w.path)?.ok_or("uploaded file not found")?;
            Ok((
                Base {
                    local: l,
                    remote: sig_item(&r),
                },
                r.id,
            ))
        }
        Action::Download => fetch(
            remote,
            root,
            &w.path,
            w.local,
            w.remote.as_ref().ok_or("file vanished")?,
            w.from.as_deref(),
        ),
        Action::MoveLocal => {
            let from = w.from.as_deref().ok_or("move without a source")?;
            refuse(root, download_only, &from.path, action)?;
            let r = w.remote.as_ref().ok_or("file vanished")?;
            // The plan may be old: neither the file to move nor the place it goes to may have changed since.
            if local_sig(root, &from.path) != from.local || local_sig(root, &w.path).is_some() {
                return Err("changed locally, will retry".into());
            }
            let dest = root.join(&w.path);
            if let Some(parent) = dest.parent() {
                fs::create_dir_all(parent).map_err(|e| e.to_string())?;
            }
            fs::rename(root.join(&from.path), &dest).map_err(|e| e.to_string())?;
            prune_empty(root, &from.path);
            let l = local_sig(root, &w.path).ok_or("moved file not found")?;
            Ok((
                Base {
                    local: l,
                    remote: sig_item(r),
                },
                r.id.clone(),
            ))
        }
        Action::MoveRemote => {
            let from = w.from.as_deref().ok_or("move without a source")?;
            refuse(root, download_only, &from.path, action)?;
            let (l, old) = (
                w.local.ok_or("file vanished")?,
                from.remote.as_ref().ok_or("file vanished")?,
            );
            if local_sig(root, &w.path) != w.local {
                return Err("changed locally, will retry".into());
            }
            cloud_unchanged(remote, &from.path, old)?;
            // A move over an existing cloud file would replace it.
            if remote.stat(&w.path)?.is_some() {
                return Err("appeared in the cloud, will retry".into());
            }
            // A service that cannot move gets what the unpaired plan would have done: upload, then delete the old.
            if remote.rename(&from.path, &w.path).is_err() {
                remote.upload(root, &w.path)?;
                remote.delete(&from.path)?;
            }
            let r = remote.stat(&w.path)?.ok_or("moved file not found")?;
            Ok((
                Base {
                    local: l,
                    remote: sig_item(&r),
                },
                r.id,
            ))
        }
        _ => Err("not a transfer".into()),
    }
}

/// Removes the empty folders a file left behind, up to (not including) the synced folder.
fn prune_empty(root: &Path, rel: &str) {
    let mut dir = root.join(rel);
    while dir.pop() && dir != root && fs::remove_dir(&dir).is_ok() {}
}

/// Downloads `r` to a temp file and moves it into place, unless the local file changed meanwhile.
fn fetch<R: Remote>(
    remote: &R,
    root: &Path,
    path: &str,
    expect_local: Option<Sig>,
    r: &Item,
    copy_of: Option<&Moved>,
) -> Result<(Base, String)> {
    static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let tmp_dir = root.join(HIDDEN).join("tmp");
    fs::create_dir_all(&tmp_dir).map_err(|e| e.to_string())?;
    // The counter keeps parallel downloads (and two runs in one second) from sharing a temp file.
    let n = NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let tmp = tmp_dir.join(format!("{}-{}-{n}", std::process::id(), now_secs()));
    // A failed local copy is no error: the download below does the job.
    let copied = copy_of.is_some_and(|f| copy_local(root, f, &tmp, r.mtime).is_ok());
    if !copied {
        remote.download(path, &tmp)?;
    }
    // The user edited it while it downloaded: keep their edit, the next run sorts it out.
    if local_sig(root, path) != expect_local {
        let _ = fs::remove_file(&tmp);
        return Err("changed while downloading, will retry".into());
    }
    let dest = root.join(path);
    if let Some(parent) = dest.parent() {
        fs::create_dir_all(parent).map_err(|e| e.to_string())?;
    }
    fs::rename(&tmp, &dest).map_err(|e| e.to_string())?;
    let l = local_sig(root, path).ok_or("downloaded file not found")?;
    Ok((
        Base {
            local: l,
            remote: sig_item(r),
        },
        r.id.clone(),
    ))
}

/// Copies the synced local file `from` to `tmp` with the cloud file's time. The source must be unchanged before and
/// after the copy, or the bytes are not the ones the hash vouched for; on any doubt `tmp` is removed.
fn copy_local(root: &Path, from: &Moved, tmp: &Path, mtime_ms: i64) -> Result<()> {
    let src = root.join(&from.path);
    let r = (|| {
        if local_sig(root, &from.path) != from.local || under_symlink(root, &from.path) {
            return Err("source changed".to_string());
        }
        fs::copy(&src, tmp).map_err(|e| e.to_string())?;
        if local_sig(root, &from.path) != from.local {
            return Err("source changed while copying".to_string());
        }
        let t = std::time::UNIX_EPOCH + std::time::Duration::from_millis(mtime_ms.max(0) as u64);
        fs::File::options()
            .write(true)
            .open(tmp)
            .and_then(|f| f.set_modified(t))
            .map_err(|e| e.to_string())
    })();
    if r.is_err() {
        let _ = fs::remove_file(tmp);
    }
    r
}

/// The activity-log row and the counter of an action that succeeded.
fn log_done(db: &Db, w: &Work, action: Action, report: &mut Report) {
    let (log, size) = match action {
        Action::Upload => ("up", w.local.map(|s| s.size)),
        Action::Download => ("down", w.remote.as_ref().map(|r| r.size)),
        Action::Conflict => ("conflict", w.remote.as_ref().map(|r| r.size)),
        Action::DeleteLocal => ("trash", w.local.map(|s| s.size)),
        Action::DeleteRemote => ("delcloud", w.remote.as_ref().map(|r| r.size)),
        Action::MoveLocal => ("move", w.remote.as_ref().map(|r| r.size)),
        Action::MoveRemote => ("move", w.local.map(|s| s.size)),
        _ => ("", None),
    };
    if !log.is_empty() {
        // A move's message is where the file was.
        let from = w.from.as_ref().map_or("", |f| f.path.as_str());
        db.log(log, &w.path, size, from);
    }
    if action == Action::Conflict {
        report.conflicts.push(w.path.clone());
    }
    report.transferred += usize::from(matches!(
        action,
        Action::Upload | Action::Download | Action::Conflict | Action::MoveLocal | Action::MoveRemote
    ));
}

fn log_failed(db: &Db, w: &Work, e: &str, report: &mut Report) {
    db.log("error", &w.path, None, e);
    report.errors.push(format!("{}: {e}", w.path));
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A folder standing in for the cloud, so the whole engine runs without rclone or a network.
    struct DirRemote(PathBuf);

    impl DirRemote {
        fn item(&self, rel: &str, m: &fs::Metadata) -> Item {
            let s = sig_of(m);
            Item {
                path: rel.into(),
                size: s.size,
                mtime: s.mtime,
                is_dir: m.is_dir(),
                id: String::new(),
                hash: None,
            }
        }
    }

    impl Remote for DirRemote {
        fn list(&self) -> Result<Vec<Item>> {
            Ok(scan(&self.0)
                .0
                .into_iter()
                .map(|(p, s)| Item {
                    path: p,
                    size: s.size,
                    mtime: s.mtime,
                    is_dir: false,
                    id: String::new(),
                    hash: None,
                })
                .collect())
        }
        fn list_dir(&self, rel: &str) -> Result<Vec<Item>> {
            // rclone's wording for a folder that is not there.
            let rd = fs::read_dir(self.0.join(rel)).map_err(|_| "operations/list: directory not found".to_string())?;
            Ok(rd
                .flatten()
                .filter_map(|e| {
                    let p = if rel.is_empty() {
                        e.file_name().to_string_lossy().into()
                    } else {
                        format!("{rel}/{}", e.file_name().to_string_lossy())
                    };
                    e.metadata().ok().map(|m| self.item(&p, &m))
                })
                .collect())
        }
        fn stat(&self, rel: &str) -> Result<Option<Item>> {
            Ok(fs::metadata(self.0.join(rel)).ok().map(|m| self.item(rel, &m)))
        }
        fn upload(&self, root: &Path, rel: &str) -> Result<()> {
            copy_keep_time(&root.join(rel), &self.0.join(rel))
        }
        fn download(&self, rel: &str, dest: &Path) -> Result<()> {
            copy_keep_time(&self.0.join(rel), dest)
        }
        fn delete(&self, rel: &str) -> Result<()> {
            fs::remove_file(self.0.join(rel)).map_err(|e| e.to_string())
        }
        fn rename(&self, from: &str, to: &str) -> Result<()> {
            fs::create_dir_all(self.0.join(to).parent().unwrap()).map_err(|e| e.to_string())?;
            fs::rename(self.0.join(from), self.0.join(to)).map_err(|e| e.to_string())
        }
        fn remove_empty_dir(&self, rel: &str) -> Result<()> {
            // Fails on a folder with anything in it, like rclone's rmdir.
            fs::remove_dir(self.0.join(rel)).map_err(|e| e.to_string())
        }
        fn same_content(&self, root: &Path, rel: &str, remote: &Item) -> bool {
            matches!((fs::read(root.join(rel)), fs::read(self.0.join(&remote.path))), (Ok(a), Ok(b)) if a == b)
        }
    }

    fn copy_keep_time(from: &Path, to: &Path) -> Result<()> {
        fs::create_dir_all(to.parent().unwrap()).map_err(|e| e.to_string())?;
        fs::copy(from, to).map_err(|e| e.to_string())?;
        let t = fs::metadata(from)
            .and_then(|m| m.modified())
            .map_err(|e| e.to_string())?;
        fs::File::options()
            .write(true)
            .open(to)
            .and_then(|f| f.set_modified(t))
            .map_err(|e| e.to_string())
    }

    struct Fixture {
        base: PathBuf,
        engine: Engine<DirRemote>,
    }

    impl Fixture {
        fn new(name: &str) -> Self {
            let base = std::env::temp_dir().join(format!("fastsync-test-{}-{name}", std::process::id()));
            let _ = fs::remove_dir_all(&base);
            let (local, remote) = (base.join("local"), base.join("remote"));
            fs::create_dir_all(&local).unwrap();
            fs::create_dir_all(&remote).unwrap();
            Self {
                engine: Engine::new(local, DirRemote(remote), Db::memory().unwrap()),
                base,
            }
        }
        fn local(&self, p: &str) -> PathBuf {
            self.engine.root.join(p)
        }
        fn remote(&self, p: &str) -> PathBuf {
            self.engine.remote.0.join(p)
        }
    }

    impl Drop for Fixture {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.base);
        }
    }

    fn write(p: PathBuf, text: &str, secs_ago: u64) {
        fs::create_dir_all(p.parent().unwrap()).unwrap();
        fs::write(&p, text).unwrap();
        let t = std::time::SystemTime::now() - std::time::Duration::from_secs(secs_ago);
        fs::File::options()
            .write(true)
            .open(&p)
            .unwrap()
            .set_modified(t)
            .unwrap();
    }

    /// Counts the lookups of `sync_paths`; `list_err` makes every listing fail with that text.
    struct Calls {
        inner: DirRemote,
        list_err: Option<&'static str>,
        lists: AtomicUsize,
        stats: AtomicUsize,
    }

    impl Calls {
        fn new(inner: DirRemote, list_err: Option<&'static str>) -> Self {
            Self {
                inner,
                list_err,
                lists: AtomicUsize::new(0),
                stats: AtomicUsize::new(0),
            }
        }
    }

    impl Remote for Calls {
        fn list(&self) -> Result<Vec<Item>> {
            self.inner.list()
        }
        fn list_dir(&self, rel: &str) -> Result<Vec<Item>> {
            self.lists.fetch_add(1, Relaxed);
            if let Some(e) = self.list_err {
                return Err(e.into());
            }
            self.inner.list_dir(rel)
        }
        fn stat(&self, rel: &str) -> Result<Option<Item>> {
            self.stats.fetch_add(1, Relaxed);
            self.inner.stat(rel)
        }
        fn upload(&self, root: &Path, rel: &str) -> Result<()> {
            self.inner.upload(root, rel)
        }
        fn download(&self, rel: &str, dest: &Path) -> Result<()> {
            self.inner.download(rel, dest)
        }
        fn delete(&self, rel: &str) -> Result<()> {
            self.inner.delete(rel)
        }
    }

    /// A cloud that lists `dup.txt` twice, as Drive can.
    struct Twice(DirRemote);
    impl Remote for Twice {
        fn list(&self) -> Result<Vec<Item>> {
            let mut all = self.0.list()?;
            if let Some(i) = all.iter().find(|i| i.path == "dup.txt").cloned() {
                all.push(Item { size: i.size + 1, ..i });
            }
            Ok(all)
        }
        fn stat(&self, rel: &str) -> Result<Option<Item>> {
            self.0.stat(rel)
        }
        fn upload(&self, root: &Path, rel: &str) -> Result<()> {
            self.0.upload(root, rel)
        }
        fn download(&self, rel: &str, dest: &Path) -> Result<()> {
            self.0.download(rel, dest)
        }
        fn delete(&self, rel: &str) -> Result<()> {
            self.0.delete(rel)
        }
    }

    #[test]
    fn two_cloud_files_with_one_name_are_skipped_and_the_rest_still_syncs() {
        let f = Fixture::new("dupname");
        write(f.remote("dup.txt"), "cloud", 100);
        write(f.local("dup.txt"), "mine", 50);
        write(f.remote("ok.txt"), "fine", 100);
        let mut e = Engine::new(
            f.engine.root.clone(),
            Twice(DirRemote(f.remote(""))),
            Db::memory().unwrap(),
        );
        let r = e.full_scan();
        assert!(r.errors.len() == 1 && r.errors[0].contains("dup.txt"), "{r:?}");
        assert_eq!(fs::read_to_string(f.local("dup.txt")).unwrap(), "mine");
        assert_eq!(fs::read_to_string(f.local("ok.txt")).unwrap(), "fine");
        assert!(e.db.base("dup.txt").unwrap().is_none());
    }

    fn many_new_files(name: &str, list_err: Option<&'static str>) -> (Fixture, Engine<Calls>, Vec<String>) {
        let f = Fixture::new(name);
        let e = Engine::new(
            f.engine.root.clone(),
            Calls::new(DirRemote(f.engine.remote.0.clone()), list_err),
            Db::memory().unwrap(),
        );
        let mut paths: Vec<String> = (0..20).map(|i| format!("a/f{i}.txt")).collect();
        paths.push("b/x.txt".into());
        for p in &paths {
            write(f.local(p), "x", 100);
        }
        (f, e, paths)
    }

    #[test]
    fn many_new_files_in_a_folder_cost_one_listing() {
        let (f, mut e, paths) = many_new_files("list-many", None);
        let r = e.sync_paths(&paths);
        assert!(r.errors.is_empty() && r.transferred == 21, "{r:?}");
        // `a` (20 files, new to the DB and to the cloud) is listed once, `b/x.txt` alone is a plain stat. The
        // other stats are the uploads' own check afterwards.
        assert_eq!(e.remote.lists.load(Relaxed), 1);
        assert!(f.remote("a/f19.txt").exists() && f.remote("b/x.txt").exists());
    }

    #[test]
    fn a_failed_listing_falls_back_to_one_stat_per_file() {
        let (f, mut e, paths) = many_new_files("list-fails", Some("boom"));
        let r = e.sync_paths(&paths);
        assert!(r.errors.is_empty() && r.transferred == 21, "{r:?}");
        assert_eq!(e.remote.lists.load(Relaxed), 1);
        assert!(e.remote.stats.load(Relaxed) >= 21);
        assert!(f.remote("a/f0.txt").exists());
    }

    #[test]
    fn a_folder_gone_from_the_cloud_costs_one_listing_and_one_stat() {
        let (f, mut e, paths) = many_new_files("list-gone", None);
        assert_eq!(e.sync_paths(&paths).transferred, 21);
        fs::remove_dir_all(f.remote("a")).unwrap();
        let (lists, stats) = (e.remote.lists.load(Relaxed), e.remote.stats.load(Relaxed));
        let own: Vec<String> = paths[..3].to_vec();
        let r = e.sync_paths(&own);
        assert!(r.errors.is_empty(), "{r:?}");
        assert_eq!(e.remote.lists.load(Relaxed), lists + 1);
        assert_eq!(
            e.remote.stats.load(Relaxed),
            stats + 1,
            "the folder itself, not each file"
        );
        // Same outcome as a stat per file: the cloud copy is gone, so the local one goes to the trash.
        assert!(!f.local(&paths[0]).exists() && f.local(&paths[3]).exists());
        assert_eq!(e.db.base(&paths[0]).unwrap(), None);
    }

    #[test]
    fn not_found_while_the_folder_still_exists_is_not_read_as_gone() {
        let (f, mut e, paths) = many_new_files("list-glitch", None);
        assert_eq!(e.sync_paths(&paths).transferred, 21);
        // The listing says "not found" but the folder and its files are there: a glitch, nothing may be deleted.
        e.remote.list_err = Some("operations/list: directory not found");
        let stats = e.remote.stats.load(Relaxed);
        let own: Vec<String> = paths[..3].to_vec();
        let r = e.sync_paths(&own);
        assert!(r.errors.is_empty() && r.transferred == 0, "{r:?}");
        assert!(
            e.remote.stats.load(Relaxed) >= stats + 1 + 3,
            "folder stat, then one per file"
        );
        assert!(f.local(&paths[0]).exists() && f.remote(&paths[0]).exists());
        assert!(e.db.base(&paths[0]).unwrap().is_some());
    }

    /// Records what `Engine.progress` says at the moment each upload starts.
    struct Spy(
        DirRemote,
        std::sync::Arc<std::sync::Mutex<Progress>>,
        std::sync::Arc<std::sync::Mutex<Vec<Progress>>>,
    );

    impl Remote for Spy {
        fn list(&self) -> Result<Vec<Item>> {
            self.0.list()
        }
        fn stat(&self, rel: &str) -> Result<Option<Item>> {
            self.0.stat(rel)
        }
        fn upload(&self, root: &Path, rel: &str) -> Result<()> {
            self.2.lock().unwrap().push(self.1.lock().unwrap().clone());
            self.0.upload(root, rel)
        }
        fn download(&self, rel: &str, dest: &Path) -> Result<()> {
            self.0.download(rel, dest)
        }
        fn delete(&self, rel: &str) -> Result<()> {
            self.0.delete(rel)
        }
    }

    /// A cloud that runs a hook once, at the first upload or delete: something changes while the run is under way.
    struct Racy(DirRemote, std::sync::Mutex<Option<Box<dyn FnOnce() + Send>>>);

    impl Remote for Racy {
        fn list(&self) -> Result<Vec<Item>> {
            self.0.list()
        }
        fn stat(&self, rel: &str) -> Result<Option<Item>> {
            self.0.stat(rel)
        }
        fn upload(&self, root: &Path, rel: &str) -> Result<()> {
            if let Some(hook) = self.1.lock().unwrap().take() {
                hook();
            }
            self.0.upload(root, rel)
        }
        fn download(&self, rel: &str, dest: &Path) -> Result<()> {
            self.0.download(rel, dest)
        }
        fn delete(&self, rel: &str) -> Result<()> {
            if let Some(hook) = self.1.lock().unwrap().take() {
                hook();
            }
            self.0.delete(rel)
        }
    }

    fn racy(f: &Fixture) -> Engine<Racy> {
        let remote = Racy(DirRemote(f.engine.remote.0.clone()), Default::default());
        Engine::new(f.engine.root.clone(), remote, Db::memory().unwrap())
    }

    /// Counts how many uploads are inside `upload` at the same moment.
    struct Slow(
        DirRemote,
        std::sync::atomic::AtomicUsize,
        std::sync::atomic::AtomicUsize,
    );

    impl Remote for Slow {
        fn list(&self) -> Result<Vec<Item>> {
            self.0.list()
        }
        fn stat(&self, rel: &str) -> Result<Option<Item>> {
            self.0.stat(rel)
        }
        fn upload(&self, root: &Path, rel: &str) -> Result<()> {
            use std::sync::atomic::Ordering::SeqCst;
            let now = self.1.fetch_add(1, SeqCst) + 1;
            self.2.fetch_max(now, SeqCst);
            std::thread::sleep(std::time::Duration::from_millis(150));
            self.1.fetch_sub(1, SeqCst);
            self.0.upload(root, rel)
        }
        fn download(&self, rel: &str, dest: &Path) -> Result<()> {
            self.0.download(rel, dest)
        }
        fn delete(&self, rel: &str) -> Result<()> {
            self.0.delete(rel)
        }
    }

    #[test]
    fn transfers_run_side_by_side_and_every_row_is_written() {
        let f = Fixture::new("parallel");
        let remote = Slow(
            DirRemote(f.engine.remote.0.clone()),
            Default::default(),
            Default::default(),
        );
        let mut e = Engine::new(f.engine.root.clone(), remote, Db::memory().unwrap());
        for i in 0..12 {
            write(f.local(&format!("up{i}.txt")), "x", 100);
            write(f.remote(&format!("down{i}.txt")), "y", 100);
        }
        let r = e.full_scan();
        assert!(r.errors.is_empty() && r.transferred == 24, "{r:?}");
        let peak = e.remote.2.load(std::sync::atomic::Ordering::SeqCst);
        assert!((2..=PARALLEL).contains(&peak), "peak {peak}");
        assert_eq!(e.db.count().unwrap(), 24);
        assert!((0..12).all(|i| f.remote(&format!("up{i}.txt")).exists() && f.local(&format!("down{i}.txt")).exists()));
        assert_eq!(e.full_scan().transferred, 0, "the rows describe what is on disk");
    }

    #[test]
    fn a_cloud_edit_made_during_the_run_is_not_overwritten() {
        let f = Fixture::new("race-cloud");
        let mut e = racy(&f);
        write(f.local("b.txt"), "one", 100);
        write(f.local("c.txt"), "one", 100);
        e.full_scan();
        write(f.local("b.txt"), "two!!", 50);
        // c deleted here: its cloud delete is a one-at-a-time step that always runs before the uploads start, so
        // the cloud edit of b lands after the plan was made and before b's upload, with no thread timing involved.
        fs::remove_file(f.local("c.txt")).unwrap();
        let cloud_b = f.remote("b.txt");
        *e.remote.1.lock().unwrap() = Some(Box::new(move || fs::write(cloud_b, "cloud edit").unwrap()));
        let r = e.full_scan();
        assert_eq!(r.errors.len(), 1, "{:?}", r.errors);
        assert_eq!(fs::read_to_string(f.remote("b.txt")).unwrap(), "cloud edit");
        assert!(!f.remote("c.txt").exists());
    }

    #[test]
    fn a_new_file_never_overwrites_one_that_appeared_in_the_cloud_since_the_listing() {
        let f = Fixture::new("race-new");
        let mut e = racy(&f);
        write(f.local("c.txt"), "one", 100);
        e.full_scan();
        fs::remove_file(f.local("c.txt")).unwrap();
        write(f.local("n.txt"), "mine", 50);
        // The delete of c runs before the upload of n, after the plan: another device creates n meanwhile.
        let cloud_n = f.remote("n.txt");
        *e.remote.1.lock().unwrap() = Some(Box::new(move || fs::write(cloud_n, "theirs").unwrap()));
        let r = e.full_scan();
        assert_eq!(r.errors.len(), 1, "{:?}", r.errors);
        assert_eq!(fs::read_to_string(f.remote("n.txt")).unwrap(), "theirs");
    }

    #[test]
    fn a_local_edit_made_during_the_run_is_not_trashed() {
        let f = Fixture::new("race-local");
        let mut e = racy(&f);
        write(f.local("a.txt"), "one", 100);
        write(f.local("b.txt"), "one", 100);
        e.full_scan();
        // b deleted in the cloud: the plan says "trash the local copy". a deleted here comes first in the plan, and
        // its cloud call is where the user edits b.
        fs::remove_file(f.remote("b.txt")).unwrap();
        fs::remove_file(f.local("a.txt")).unwrap();
        let local_b = f.local("b.txt");
        *e.remote.1.lock().unwrap() = Some(Box::new(move || fs::write(local_b, "user edit").unwrap()));
        let r = e.full_scan();
        assert_eq!(r.errors.len(), 1, "{:?}", r.errors);
        assert_eq!(fs::read_to_string(f.local("b.txt")).unwrap(), "user edit");
    }

    #[test]
    fn a_folder_replaced_by_a_file_deletes_what_was_below_it() {
        let mut f = Fixture::new("folder-to-file");
        write(f.local("docs/x.txt"), "x", 100);
        f.engine.full_scan();
        assert!(f.remote("docs/x.txt").exists());
        fs::remove_dir_all(f.local("docs")).unwrap();
        fs::write(f.local("docs"), "now a file").unwrap();
        let r = f.engine.sync_paths(&["docs/x.txt".to_string()]);
        assert!(r.errors.is_empty(), "{r:?}");
        assert!(!f.remote("docs/x.txt").exists(), "not read as unreadable");
    }

    #[test]
    fn an_unreadable_folder_is_not_read_as_deleted() {
        use std::os::unix::fs::PermissionsExt;
        let mut f = Fixture::new("unreadable");
        write(f.local("keep.txt"), "k", 100);
        write(f.local("locked/a.txt"), "a", 100);
        f.engine.full_scan();
        assert!(f.remote("locked/a.txt").exists());
        let locked = f.local("locked");
        fs::set_permissions(&locked, fs::Permissions::from_mode(0o000)).unwrap();
        // Running as root reads it anyway: nothing to prove then.
        if fs::read_dir(&locked).is_ok() {
            fs::set_permissions(&locked, fs::Permissions::from_mode(0o755)).unwrap();
            return;
        }
        let full = f.engine.full_scan();
        let paths = f.engine.sync_paths(&["locked/a.txt".to_string(), "locked".to_string()]);
        let walked = f.engine.local_changes();
        fs::set_permissions(&locked, fs::Permissions::from_mode(0o755)).unwrap();
        assert!(f.remote("locked/a.txt").exists(), "the cloud copy must stay");
        assert!(full.errors.is_empty() && paths.errors.is_empty() && paths.transferred == 0);
        assert!(!walked.iter().any(|p| p.starts_with("locked")), "{walked:?}");
    }

    #[test]
    fn stamp_is_a_utc_date_and_time() {
        assert_eq!(stamp(0), "1970-01-01 00:00:00");
        assert_eq!(stamp(1_794_578_709), "2026-11-13 14:05:09");
    }

    #[test]
    fn transfers_and_failures_leave_rows_in_the_activity_log() {
        let mut f = Fixture::new("log");
        let file = f.base.join("state.db");
        f.engine.db = Db::open(&file).unwrap();
        write(f.local("up.txt"), "mine", 100);
        write(f.remote("down.txt"), "theirs!", 100);
        f.engine.full_scan();
        // A cloud that cannot be listed is a logged problem, not an empty cloud.
        struct Broken;
        impl Remote for Broken {
            fn list(&self) -> Result<Vec<Item>> {
                Err("boom".into())
            }
            fn stat(&self, _: &str) -> Result<Option<Item>> {
                Ok(None)
            }
            fn upload(&self, _: &Path, _: &str) -> Result<()> {
                Ok(())
            }
            fn download(&self, _: &str, _: &Path) -> Result<()> {
                Ok(())
            }
            fn delete(&self, _: &str) -> Result<()> {
                Ok(())
            }
            fn same_content(&self, _: &Path, _: &str, _: &Item) -> bool {
                false
            }
        }
        let mut broken = Engine::new(f.engine.root.clone(), Broken, Db::open(&file).unwrap());
        broken.full_scan();
        let rows = crate::db::read_log(&file, 10).unwrap();
        let find = |k: &str, p: &str| rows.iter().find(|r| r.kind == k && r.path == p);
        assert_eq!(find("up", "up.txt").and_then(|r| r.size), Some(4));
        assert_eq!(find("down", "down.txt").and_then(|r| r.size), Some(7));
        assert!(find("error", "").is_some_and(|r| r.msg.contains("boom")));
    }

    #[test]
    fn an_empty_cloud_file_left_by_an_interrupted_upload_is_filled_not_split_into_a_conflict() {
        let mut f = Fixture::new("empty-cloud");
        write(f.local("big.bin"), "the real data", 5000);
        write(f.remote("big.bin"), "", 100);
        let r = f.engine.full_scan();
        assert!(r.errors.is_empty(), "{r:?}");
        assert_eq!(r.transferred, 1);
        assert_eq!(fs::read_to_string(f.remote("big.bin")).unwrap(), "the real data");
        assert_eq!(fs::read_to_string(f.local("big.bin")).unwrap(), "the real data");
        let names: Vec<_> = fs::read_dir(&f.engine.root)
            .unwrap()
            .flatten()
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .collect();
        assert!(!names.iter().any(|n| n.contains("conflict")), "{names:?}");
        assert_eq!(f.engine.full_scan().transferred, 0);
        // A mirror keeps the cloud as the truth: its local copy is replaced, never uploaded.
        let mut m = Fixture::new("empty-cloud-mirror");
        m.engine.download_only = true;
        write(m.local("big.bin"), "the real data", 5000);
        write(m.remote("big.bin"), "", 100);
        m.engine.full_scan();
        assert_eq!(fs::read_to_string(m.remote("big.bin")).unwrap(), "");
    }

    #[test]
    fn the_same_bytes_with_different_times_are_adopted_not_split_into_a_conflict() {
        let mut f = Fixture::new("same-bytes");
        write(f.local("a.pdf"), "same bytes", 5000);
        write(f.remote("a.pdf"), "same bytes", 100);
        write(f.local("b.pdf"), "local text", 5000);
        write(f.remote("b.pdf"), "other text", 100);
        let r = f.engine.full_scan();
        assert!(r.errors.is_empty(), "{r:?}");
        assert!(f.engine.db.base("a.pdf").unwrap().is_some(), "recorded as synced");
        let names: Vec<String> = fs::read_dir(&f.engine.root)
            .unwrap()
            .flatten()
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .collect();
        assert!(!names.iter().any(|n| n.starts_with("a (conflict")), "{names:?}");
        // Different bytes of the same size still split, so no edit is ever lost.
        assert!(names.iter().any(|n| n.starts_with("b (conflict")), "{names:?}");
        assert_eq!(f.engine.full_scan().transferred, 0, "and the next scan finds nothing");
    }

    #[test]
    fn progress_counts_the_planned_files_and_clears_when_the_run_ends() {
        let base = std::env::temp_dir().join(format!("fastsync-test-{}-progress", std::process::id()));
        let _ = fs::remove_dir_all(&base);
        let (local, remote) = (base.join("local"), base.join("remote"));
        fs::create_dir_all(&remote).unwrap();
        write(local.join("a.txt"), "a", 100);
        write(local.join("b.txt"), "b", 100);
        let (progress, seen) = (
            Default::default(),
            std::sync::Arc::new(std::sync::Mutex::new(Vec::new())),
        );
        let spy = Spy(DirRemote(remote), std::sync::Arc::clone(&progress), seen.clone());
        let mut e = Engine::new(local, spy, Db::memory().unwrap());
        e.progress = progress;
        assert_eq!(e.full_scan().transferred, 2);
        let seen = seen.lock().unwrap();
        assert_eq!(seen.len(), 2);
        assert!(seen.iter().all(|p| p.total == 2 && p.kind == Kind::Up));
        // Side by side, so the second may start before the first has finished: `done` only counts finished ones.
        assert!(seen.iter().any(|p| p.done == 0) && seen.iter().all(|p| p.done < 2));
        assert_eq!(*e.progress.lock().unwrap(), Progress::default());
        let _ = fs::remove_dir_all(&base);
    }

    #[test]
    fn first_sync_merges_both_sides_then_edits_flow_each_way() {
        let mut f = Fixture::new("merge");
        write(f.local("a.txt"), "local", 100);
        write(f.remote("dir/b.txt"), "remote", 100);
        let r = f.engine.full_scan();
        assert!(r.errors.is_empty() && r.aborted.is_none(), "{r:?}");
        assert_eq!(fs::read_to_string(f.remote("a.txt")).unwrap(), "local");
        assert_eq!(fs::read_to_string(f.local("dir/b.txt")).unwrap(), "remote");
        // A second scan finds nothing to do (no echo).
        assert_eq!(f.engine.full_scan().transferred, 0);
        // Local edit goes up, through the incremental path.
        write(f.local("a.txt"), "local v2", 10);
        assert_eq!(f.engine.sync_paths(&["a.txt".into()]).transferred, 1);
        assert_eq!(fs::read_to_string(f.remote("a.txt")).unwrap(), "local v2");
        // Remote edit comes down.
        write(f.remote("dir/b.txt"), "remote v2!", 5);
        assert_eq!(f.engine.sync_paths(&["dir/b.txt".into()]).transferred, 1);
        assert_eq!(fs::read_to_string(f.local("dir/b.txt")).unwrap(), "remote v2!");
        assert_eq!(
            f.engine.sync_paths(&["a.txt".into(), "dir/b.txt".into()]).transferred,
            0
        );
    }

    #[test]
    fn a_path_from_the_cloud_feed_is_not_taken_for_the_echo_of_our_own_write() {
        let mut f = Fixture::new("feed");
        write(f.local("a.txt"), "one", 100);
        assert!(f.engine.full_scan().errors.is_empty());
        write(f.remote("a.txt"), "edited in the cloud", 5);
        // The same path from inotify is an echo (local file == DB row); from the feed it is a real change.
        assert!(f.engine.paths_to_sync(vec!["a.txt".into()], vec![], false).is_empty());
        let paths = f.engine.paths_to_sync(vec![], vec!["a.txt".into()], false);
        assert_eq!(paths, ["a.txt"]);
        assert_eq!(f.engine.sync_paths(&paths).transferred, 1);
        assert_eq!(fs::read_to_string(f.local("a.txt")).unwrap(), "edited in the cloud");
        // A delete in the cloud arrives the same way.
        fs::remove_file(f.remote("a.txt")).unwrap();
        let paths = f.engine.paths_to_sync(vec![], vec!["a.txt".into()], false);
        assert!(f.engine.sync_paths(&paths).errors.is_empty());
        assert!(!f.local("a.txt").exists());
    }

    #[test]
    fn a_remote_delete_trashes_the_local_file_instead_of_removing_it() {
        let mut f = Fixture::new("trash");
        write(f.local("gone/x.txt"), "x", 100);
        f.engine.full_scan();
        fs::remove_file(f.remote("gone/x.txt")).unwrap();
        f.engine.sync_paths(&["gone/x.txt".into()]);
        assert!(!f.local("gone/x.txt").exists());
        assert!(!f.local("gone").exists(), "the empty folder is cleaned up");
        let trash = f.local(".fastsync/trash");
        let stamp = fs::read_dir(&trash).unwrap().next().unwrap().unwrap().path();
        assert_eq!(fs::read_to_string(stamp.join("gone/x.txt")).unwrap(), "x");
        assert_eq!(f.engine.db.base("gone/x.txt").unwrap(), None);
    }

    #[test]
    fn a_local_delete_deletes_remotely_and_a_file_never_synced_is_not_deleted() {
        let mut f = Fixture::new("del");
        write(f.local("a.txt"), "a", 100);
        f.engine.full_scan();
        fs::remove_file(f.local("a.txt")).unwrap();
        f.engine.sync_paths(&["a.txt".into()]);
        assert!(!f.remote("a.txt").exists());
        // New on the remote and unknown to the DB: it must come down, not be treated as a deletion.
        write(f.remote("new.txt"), "n", 100);
        f.engine.full_scan();
        assert!(f.local("new.txt").exists());
    }

    /// `n` synced files of different sizes in `d/`, so each one has a single possible partner.
    fn synced_folder(f: &mut Fixture, n: usize) {
        for i in 0..n {
            write(f.local(&format!("d/f{i}.txt")), &"x".repeat(i + 1), 100);
        }
        let r = f.engine.full_scan();
        assert!(r.errors.is_empty() && r.aborted.is_none(), "{r:?}");
    }

    #[test]
    fn a_folder_renamed_in_the_cloud_is_renamed_here_not_downloaded_again() {
        let mut f = Fixture::new("move-local");
        synced_folder(&mut f, 12);
        fs::rename(f.remote("d"), f.remote("e")).unwrap();
        // 12 of 12 known files: unpaired, this is 12 deletes and the guard stops the run.
        let r = f.engine.full_scan();
        assert!(r.errors.is_empty() && r.aborted.is_none(), "{r:?}");
        assert_eq!(r.transferred, 12);
        for i in 0..12 {
            assert_eq!(
                fs::read_to_string(f.local(&format!("e/f{i}.txt"))).unwrap(),
                "x".repeat(i + 1)
            );
            assert!(!f.local(&format!("d/f{i}.txt")).exists());
            assert_eq!(f.engine.db.base(&format!("d/f{i}.txt")).unwrap(), None);
            assert!(f.engine.db.base(&format!("e/f{i}.txt")).unwrap().is_some());
        }
        assert!(!f.local("d").exists(), "the empty folder is cleaned up");
        // No trash. (The first sync was a local-only folder uploaded, so nothing was ever downloaded here: the temp
        // folder a download creates must not exist.)
        assert!(!f.local(".fastsync").exists());
        assert_eq!(f.engine.full_scan().transferred, 0);
    }

    #[test]
    fn a_folder_renamed_here_is_moved_in_the_cloud_not_uploaded_again() {
        use std::os::unix::fs::MetadataExt;
        let mut f = Fixture::new("move-remote");
        synced_folder(&mut f, 12);
        let ino = fs::metadata(f.remote("d/f3.txt")).unwrap().ino();
        fs::rename(f.local("d"), f.local("e")).unwrap();
        let r = f.engine.full_scan();
        assert!(r.errors.is_empty() && r.aborted.is_none(), "{r:?}");
        assert_eq!(r.transferred, 12);
        // The same cloud file, not a copy.
        assert_eq!(fs::metadata(f.remote("e/f3.txt")).unwrap().ino(), ino);
        assert!(!f.remote("d/f3.txt").exists());
        assert_eq!(f.engine.db.base("d/f3.txt").unwrap(), None);
        assert!(f.engine.db.base("e/f3.txt").unwrap().is_some());
        assert_eq!(f.engine.full_scan().transferred, 0);
    }

    #[test]
    fn the_same_size_with_other_bytes_is_not_a_move() {
        let mut f = Fixture::new("move-other");
        write(f.local("a.txt"), "aaaa", 100);
        f.engine.full_scan();
        // Renamed in the cloud, but the new file only has the same size.
        fs::remove_file(f.remote("a.txt")).unwrap();
        write(f.remote("b.txt"), "bbbb", 100);
        let r = f.engine.full_scan();
        assert!(r.errors.is_empty(), "{r:?}");
        assert_eq!(fs::read_to_string(f.local("b.txt")).unwrap(), "bbbb");
        let trash = fs::read_dir(f.local(".fastsync/trash"))
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
            .path();
        assert_eq!(fs::read_to_string(trash.join("a.txt")).unwrap(), "aaaa");
    }

    /// A cloud that refuses every download: a copy made from a local file must not need one.
    struct NoDownload(DirRemote);

    impl Remote for NoDownload {
        fn list(&self) -> Result<Vec<Item>> {
            self.0.list()
        }
        fn stat(&self, rel: &str) -> Result<Option<Item>> {
            self.0.stat(rel)
        }
        fn upload(&self, root: &Path, rel: &str) -> Result<()> {
            self.0.upload(root, rel)
        }
        fn download(&self, _: &str, _: &Path) -> Result<()> {
            Err("downloaded".into())
        }
        fn delete(&self, rel: &str) -> Result<()> {
            self.0.delete(rel)
        }
        fn same_content(&self, root: &Path, rel: &str, remote: &Item) -> bool {
            self.0.same_content(root, rel, remote)
        }
    }

    #[test]
    fn a_file_duplicated_in_the_cloud_is_copied_locally_not_downloaded() {
        let f = Fixture::new("copy-local");
        let big = "x".repeat(COPY_MIN as usize + 10);
        let mut e = Engine::new(
            f.engine.root.clone(),
            NoDownload(DirRemote(f.engine.remote.0.clone())),
            Db::memory().unwrap(),
        );
        write(f.local("a.bin"), &big, 100);
        assert!(e.full_scan().errors.is_empty());
        fs::copy(f.remote("a.bin"), f.remote("b.bin")).unwrap();
        let r = e.full_scan();
        assert!(r.errors.is_empty(), "{r:?}");
        assert_eq!(fs::read_to_string(f.local("b.bin")).unwrap(), big);
        assert!(e.db.base("a.bin").unwrap().is_some(), "the source keeps its row");
        assert!(f.local("a.bin").exists());
        assert_eq!(e.full_scan().transferred, 0);
    }

    #[test]
    fn a_cloud_file_with_the_size_of_another_but_other_bytes_is_downloaded() {
        let mut f = Fixture::new("copy-other");
        write(f.local("a.bin"), &"a".repeat(COPY_MIN as usize), 100);
        f.engine.full_scan();
        write(f.remote("b.bin"), &"b".repeat(COPY_MIN as usize), 100);
        let r = f.engine.full_scan();
        assert!(r.errors.is_empty(), "{r:?}");
        assert_eq!(
            fs::read_to_string(f.local("b.bin")).unwrap(),
            "b".repeat(COPY_MIN as usize)
        );
        assert_eq!(
            fs::read_to_string(f.local("a.bin")).unwrap(),
            "a".repeat(COPY_MIN as usize)
        );
    }

    #[test]
    fn a_mirror_follows_a_cloud_rename_but_never_moves_the_cloud() {
        let mut f = Fixture::new("move-mirror");
        f.engine.download_only = true;
        write(f.remote("a/x.txt"), "xx", 100);
        f.engine.full_scan();
        let ino = std::os::unix::fs::MetadataExt::ino(&fs::metadata(f.local("a/x.txt")).unwrap());
        fs::rename(f.remote("a"), f.remote("b")).unwrap();
        f.engine.full_scan();
        assert!(!f.local("a/x.txt").exists());
        // The same local file, not a fresh download.
        assert_eq!(
            std::os::unix::fs::MetadataExt::ino(&fs::metadata(f.local("b/x.txt")).unwrap()),
            ino
        );
        // A local rename is a local-only file: trashed, and the cloud file comes back.
        fs::rename(f.local("b/x.txt"), f.local("b/y.txt")).unwrap();
        f.engine.full_scan();
        assert!(f.remote("b/x.txt").exists() && !f.remote("b/y.txt").exists());
        assert!(f.local("b/x.txt").exists() && !f.local("b/y.txt").exists());
    }

    #[test]
    fn a_move_never_overwrites_a_file_changed_since_the_plan() {
        let f = Fixture::new("move-stale");
        write(f.local("old.txt"), "one", 100);
        let stale = local_sig(&f.engine.root, "old.txt");
        write(f.local("old.txt"), "edited!", 10);
        let item = Item {
            path: "new.txt".into(),
            size: 3,
            mtime: 0,
            is_dir: false,
            id: String::new(),
            hash: None,
        };
        let w = Work {
            path: "new.txt".into(),
            local: None,
            remote: Some(item),
            from: Some(Box::new(Moved {
                path: "old.txt".into(),
                local: stale,
                remote: None,
            })),
        };
        let e = transfer(&f.engine.remote, &f.engine.root, false, &w, Action::MoveLocal).unwrap_err();
        assert!(e.contains("changed locally"), "{e}");
        assert_eq!(fs::read_to_string(f.local("old.txt")).unwrap(), "edited!");
        assert!(!f.local("new.txt").exists());
    }

    /// rclone's folder cache: after a folder moved in the cloud, its old path still answers (with the files now at
    /// the new path) until `forget_cache`.
    struct Cached {
        inner: DirRemote,
        /// (old folder, new folder) while the cache is stale.
        stale: std::sync::Mutex<Option<(String, String)>>,
        downloads: AtomicUsize,
    }

    impl Cached {
        /// The real path behind `rel` and whether the answer comes from the stale cache.
        fn real(&self, rel: &str) -> String {
            match &*self.stale.lock().unwrap() {
                Some((old, new)) if rel == old || rel.starts_with(&format!("{old}/")) => {
                    format!("{new}{}", &rel[old.len()..])
                }
                _ => rel.to_string(),
            }
        }
    }

    impl Remote for Cached {
        fn list(&self) -> Result<Vec<Item>> {
            self.inner.list()
        }
        fn list_dir(&self, rel: &str) -> Result<Vec<Item>> {
            let real = self.real(rel);
            Ok(self
                .inner
                .list_dir(&real)?
                .into_iter()
                .map(|mut i| {
                    i.path = format!("{rel}{}", &i.path[real.len()..]);
                    i
                })
                .collect())
        }
        fn stat(&self, rel: &str) -> Result<Option<Item>> {
            Ok(self.inner.stat(&self.real(rel))?.map(|mut i| {
                i.path = rel.into();
                i
            }))
        }
        fn upload(&self, root: &Path, rel: &str) -> Result<()> {
            self.inner.upload(root, rel)
        }
        fn download(&self, rel: &str, dest: &Path) -> Result<()> {
            self.downloads.fetch_add(1, Relaxed);
            self.inner.download(&self.real(rel), dest)
        }
        fn delete(&self, rel: &str) -> Result<()> {
            self.inner.delete(rel)
        }
        fn forget_cache(&self) {
            *self.stale.lock().unwrap() = None;
        }
        fn same_content(&self, root: &Path, rel: &str, remote: &Item) -> bool {
            self.inner.same_content(root, rel, remote)
        }
    }

    #[test]
    fn a_folder_moved_in_the_cloud_is_not_downloaded_again_through_its_old_path() {
        let f = Fixture::new("stale-cache");
        for i in 0..12 {
            write(f.local(&format!("d/f{i}.txt")), &"x".repeat(i + 1), 100);
        }
        let mut e = Engine::new(
            f.engine.root.clone(),
            Cached {
                inner: DirRemote(f.engine.remote.0.clone()),
                stale: Default::default(),
                downloads: AtomicUsize::new(0),
            },
            Db::memory().unwrap(),
        );
        assert!(e.full_scan().errors.is_empty());
        // Moved in the cloud, and the transport still answers for `d`.
        fs::create_dir_all(f.remote("b")).unwrap();
        fs::rename(f.remote("d"), f.remote("b/d")).unwrap();
        *e.remote.stale.lock().unwrap() = Some(("d".into(), "b/d".into()));
        let r = e.full_scan();
        assert!(r.errors.is_empty() && r.aborted.is_none(), "{r:?}");
        assert!(f.local("b/d/f3.txt").exists());
        // The echo of our own rename: the old paths, asked about one by one or as a folder.
        let old: Vec<String> = (0..12).map(|i| format!("d/f{i}.txt")).collect();
        let r = e.sync_paths(&old);
        assert!(r.errors.is_empty(), "{r:?}");
        assert!(!f.local("d").exists(), "the old folder came back");
        assert_eq!(e.remote.downloads.load(Relaxed), 0);
    }

    #[test]
    fn a_folder_emptied_by_a_move_or_a_delete_is_removed_in_the_cloud() {
        let mut f = Fixture::new("empty-dirs");
        write(f.local("a/b/one.txt"), "one", 100);
        write(f.local("a/b/two!.txt"), "two", 100);
        write(f.local("a/keep.txt"), "keep!", 100);
        write(f.local("z/last.txt"), "last", 100);
        for i in 0..4 {
            write(f.local(&format!("pad{i}.txt")), &"p".repeat(i + 10), 100);
        }
        assert!(f.engine.full_scan().errors.is_empty());
        // A local rename of a folder: its files move, and the old cloud folder must not stay behind.
        fs::rename(f.local("a/b"), f.local("c")).unwrap();
        // A delete of the only file of a folder.
        fs::remove_file(f.local("z/last.txt")).unwrap();
        let r = f.engine.full_scan();
        assert!(r.errors.is_empty() && r.aborted.is_none(), "{r:?}");
        assert!(f.remote("c/one.txt").exists());
        assert!(!f.remote("a/b").exists(), "the emptied folder stays in the cloud");
        assert!(!f.remote("z").exists(), "the emptied folder stays in the cloud");
        // `a` still holds a file: it must stay.
        assert!(f.remote("a/keep.txt").exists());
    }

    #[test]
    fn both_edited_keeps_both_copies() {
        let mut f = Fixture::new("conflict");
        write(f.local("r.txt"), "base", 1000);
        f.engine.full_scan();
        write(f.local("r.txt"), "mine", 20);
        write(f.remote("r.txt"), "theirs!", 10);
        let r = f.engine.full_scan();
        assert!(r.errors.is_empty(), "{r:?}");
        assert_eq!(r.conflicts, ["r.txt"]);
        assert_eq!(fs::read_to_string(f.local("r.txt")).unwrap(), "theirs!");
        let name = conflict_name("r.txt", &today(), 1);
        assert_eq!(fs::read_to_string(f.local(&name)).unwrap(), "mine");
        assert_eq!(
            fs::read_to_string(f.remote(&name)).unwrap(),
            "mine",
            "the local copy is uploaded too"
        );
    }

    fn trashed(f: &Fixture, rel: &str) -> Option<String> {
        let stamps = fs::read_dir(f.local(".fastsync/trash")).ok()?;
        stamps
            .flatten()
            .find_map(|s| fs::read_to_string(s.path().join(rel)).ok())
    }

    #[test]
    fn a_name_that_is_not_valid_text_is_reported_not_silently_skipped() {
        use std::os::unix::ffi::OsStrExt;
        let f = Fixture::new("badname");
        write(f.local("ok.txt"), "x", 10);
        fs::write(f.engine.root.join(std::ffi::OsStr::from_bytes(b"caf\xe9.txt")), "y").unwrap();
        let (files, bad) = scan(&f.engine.root);
        assert!(files.contains_key("ok.txt") && files.len() == 1);
        assert_eq!(bad.len(), 1, "{bad:?}");
        assert!(bad[0].starts_with("caf"));
        // It blocks nothing real: the other file still syncs.
        let mut f = f;
        let r = f.engine.full_scan();
        assert!(f.remote("ok.txt").exists(), "{r:?}");
    }

    #[test]
    fn the_same_path_trashed_twice_in_a_second_keeps_both_copies() {
        let f = Fixture::new("trash-twice");
        for text in ["first", "second"] {
            write(f.local("a.txt"), text, 10);
            f.engine.trash("a.txt").unwrap();
        }
        // Same batch (renamed copy) or two batches if the clock ticked: two files either way.
        assert_eq!(trash_usage(&f.engine.root), (2, 11));
    }

    #[test]
    fn the_trash_is_counted_and_emptied() {
        let mut f = Fixture::new("trash-usage");
        assert_eq!(trash_usage(&f.engine.root), (0, 0));
        assert!(empty_trash(&f.engine.root).is_ok(), "no trash yet is fine");
        write(f.local("gone/x.txt"), "12345", 100);
        f.engine.full_scan();
        fs::remove_file(f.remote("gone/x.txt")).unwrap();
        f.engine.sync_paths(&["gone/x.txt".into()]);
        assert_eq!(trash_usage(&f.engine.root), (1, 5));
        empty_trash(&f.engine.root).unwrap();
        assert_eq!(trash_usage(&f.engine.root), (0, 0));
        assert!(!f.local(".fastsync/trash").exists());
        assert!(empty_trash(&f.engine.root).is_ok());
    }

    #[test]
    fn old_trash_batches_are_purged_and_the_rest_stays() {
        let f = Fixture::new("purge");
        let (root, now) = (f.engine.root.clone(), now_secs());
        let day = 86_400;
        write(
            f.local(&format!(".fastsync/trash/{}/old.txt", now - 91 * day)),
            "old",
            100,
        );
        write(
            f.local(&format!(".fastsync/trash/{}/edge.txt", now - 90 * day)),
            "edge",
            100,
        );
        write(f.local(&format!(".fastsync/trash/{}/new.txt", now - day)), "new", 100);
        write(f.local(".fastsync/trash/junk/keep.txt"), "not ours", 100);
        assert!(purge_trash(&root, 0).is_ok());
        assert_eq!(trash_usage(&root).0, 4, "0 keeps everything");
        purge_trash_at(&root, 90, now).unwrap();
        // 91 and exactly 90 days old go; a day old and a folder with a non-numeric name stay.
        assert_eq!(trash_usage(&root).0, 2);
        assert!(f.local(".fastsync/trash/junk/keep.txt").exists());
        assert!(purge_trash(&root.join("missing"), 90).is_ok());
    }

    #[test]
    fn download_only_mirrors_the_cloud_and_never_writes_to_it() {
        let mut f = Fixture::new("mirror");
        f.engine.download_only = true;
        write(f.remote("a.txt"), "cloud a", 100);
        write(f.remote("b.txt"), "cloud b", 100);
        write(f.local("only-here.txt"), "mine", 100);
        let r = f.engine.full_scan();
        assert!(r.errors.is_empty() && r.aborted.is_none(), "{r:?}");
        assert_eq!(fs::read_to_string(f.local("a.txt")).unwrap(), "cloud a");
        // A local-only file is not uploaded: it goes to the trash.
        assert!(!f.remote("only-here.txt").exists());
        assert!(!f.local("only-here.txt").exists());
        assert_eq!(trashed(&f, "only-here.txt").as_deref(), Some("mine"));
        // A local edit loses to the cloud copy and is kept in the trash.
        write(f.local("a.txt"), "edited here", 10);
        let r = f.engine.sync_paths(&["a.txt".into()]);
        assert!(r.errors.is_empty(), "{r:?}");
        assert_eq!(fs::read_to_string(f.local("a.txt")).unwrap(), "cloud a");
        assert_eq!(fs::read_to_string(f.remote("a.txt")).unwrap(), "cloud a");
        assert_eq!(trashed(&f, "a.txt").as_deref(), Some("edited here"));
        // A file deleted here comes back.
        fs::remove_file(f.local("b.txt")).unwrap();
        f.engine.sync_paths(&["b.txt".into()]);
        assert_eq!(fs::read_to_string(f.local("b.txt")).unwrap(), "cloud b");
        assert!(f.remote("b.txt").exists());
        // The cloud's own changes and deletes reach the disk.
        write(f.remote("a.txt"), "cloud a v2", 5);
        fs::remove_file(f.remote("b.txt")).unwrap();
        f.engine.full_scan();
        assert_eq!(fs::read_to_string(f.local("a.txt")).unwrap(), "cloud a v2");
        assert!(!f.local("b.txt").exists());
        assert_eq!(trashed(&f, "b.txt").as_deref(), Some("cloud b"));
    }

    #[test]
    fn download_only_guard_stops_a_new_account_pointed_at_a_full_folder() {
        let mut f = Fixture::new("mirror-guard");
        f.engine.download_only = true;
        write(f.remote("c.txt"), "c", 100);
        for i in 0..12 {
            write(f.local(&format!("mine{i}.txt")), "x", 100);
        }
        let r = f.engine.full_scan();
        assert!(r.aborted.is_some(), "{r:?}");
        assert!(f.local("mine0.txt").exists());
        assert!(!f.local("c.txt").exists());
    }

    #[test]
    fn the_delete_guard_stops_a_run_that_empties_the_folder() {
        let mut f = Fixture::new("guard");
        for i in 0..12 {
            write(f.local(&format!("f{i}.txt")), "x", 100);
        }
        f.engine.full_scan();
        // An unmounted remote looks like "everything was deleted".
        for i in 0..12 {
            fs::remove_file(f.remote(&format!("f{i}.txt"))).unwrap();
        }
        let r = f.engine.full_scan();
        assert!(r.aborted.is_some());
        assert!(f.local("f0.txt").exists());
        assert_eq!(f.engine.db.all().unwrap().len(), 12);
    }

    /// 12 synced files, then `gone` of them removed from the cloud (the guard trips, nothing is deleted yet).
    fn cloud_loses(name: &str, gone: usize) -> Fixture {
        let mut f = Fixture::new(name);
        for i in 0..12 {
            write(f.local(&format!("f{i}.txt")), "x", 100);
        }
        f.engine.full_scan();
        for i in 0..gone {
            fs::remove_file(f.remote(&format!("f{i}.txt"))).unwrap();
        }
        assert!(f.engine.full_scan().aborted.is_some());
        f
    }

    #[test]
    fn restore_puts_files_the_cloud_lost_back_up() {
        let mut f = cloud_loses("restore-up", 12);
        f.engine.restore = true;
        let r = f.engine.full_scan();
        assert!(
            r.aborted.is_none() && r.errors.is_empty() && r.transferred == 12,
            "{r:?}"
        );
        assert!((0..12).all(|i| f.remote(&format!("f{i}.txt")).exists() && f.local(&format!("f{i}.txt")).exists()));
        assert_eq!(f.engine.db.count().unwrap(), 12);
    }

    #[test]
    fn restore_brings_files_deleted_here_back_down() {
        let mut f = Fixture::new("restore-down");
        for i in 0..12 {
            write(f.local(&format!("f{i}.txt")), "x", 100);
        }
        f.engine.full_scan();
        for i in 0..12 {
            fs::remove_file(f.local(&format!("f{i}.txt"))).unwrap();
        }
        assert!(f.engine.full_scan().aborted.is_some());
        f.engine.restore = true;
        let r = f.engine.full_scan();
        assert!(r.aborted.is_none() && r.transferred == 12, "{r:?}");
        assert!((0..12).all(|i| f.local(&format!("f{i}.txt")).exists() && f.remote(&format!("f{i}.txt")).exists()));
    }

    #[test]
    fn restore_never_uploads_from_a_mirror() {
        let mut f = Fixture::new("restore-mirror");
        f.engine.download_only = true;
        write(f.remote("c.txt"), "c", 100);
        for i in 0..12 {
            write(f.local(&format!("mine{i}.txt")), "x", 100);
        }
        f.engine.restore = true;
        let r = f.engine.full_scan();
        assert!(r.aborted.is_some(), "restore must not change what a mirror does: {r:?}");
        assert!(!f.remote("mine0.txt").exists());
    }

    #[test]
    fn confirmed_deletes_are_capped_at_the_count_the_card_showed() {
        let mut f = cloud_loses("confirm-cap", 8);
        // The card said 8; by the time the user presses Delete, 2 more are gone from the cloud.
        fs::remove_file(f.remote("f8.txt")).unwrap();
        fs::remove_file(f.remote("f9.txt")).unwrap();
        f.engine.confirmed_deletes = Some(8);
        let r = f.engine.full_scan();
        assert_eq!(r.aborted.as_ref().map(|a| a.0), Some(10), "{r:?}");
        assert!(f.local("f0.txt").exists(), "nothing was deleted");
        // The same count passes, whatever share of the known files it is.
        f.engine.confirmed_deletes = Some(10);
        let r = f.engine.full_scan();
        assert!(r.aborted.is_none() && r.errors.is_empty(), "{r:?}");
        assert!(!f.local("f0.txt").exists() && f.local("f10.txt").exists());
    }

    #[test]
    fn offline_edits_are_found_by_the_local_walk() {
        let mut f = Fixture::new("walk");
        write(f.local("a.txt"), "a", 100);
        write(f.local("b.txt"), "b", 100);
        f.engine.full_scan();
        assert!(f.engine.local_changes().is_empty());
        write(f.local("a.txt"), "changed", 5);
        fs::remove_file(f.local("b.txt")).unwrap();
        write(f.local("c.txt"), "c", 5);
        assert_eq!(f.engine.local_changes(), ["a.txt", "b.txt", "c.txt"]);
    }

    #[test]
    fn a_symlinked_folder_is_never_followed_in_either_direction() {
        let mut f = Fixture::new("symlink");
        let outside = f.base.join("outside");
        write(outside.join("secret.txt"), "secret", 100);
        std::os::unix::fs::symlink(&outside, f.local("link")).unwrap();
        write(f.remote("link/from-cloud.txt"), "cloud", 100);
        // Up: neither the link nor a path behind it may reach the cloud.
        let r = f.engine.sync_paths(&["link".into(), "link/secret.txt".into()]);
        assert_eq!(r.transferred, 0);
        assert!(!f.remote("link/secret.txt").exists());
        // Down: a remote file under the link must not be written through it.
        let r = f.engine.full_scan();
        assert!(r.errors.iter().any(|e| e.contains("symlink")), "{r:?}");
        assert!(!outside.join("from-cloud.txt").exists());
        assert_eq!(fs::read_to_string(outside.join("secret.txt")).unwrap(), "secret");
    }

    #[test]
    fn unchecked_folders_match_their_whole_subtree_only() {
        let list = vec!["Photos".to_string(), "a/b".to_string()];
        assert!(excluded("Photos", &list) && excluded("Photos/2020/x.jpg", &list) && excluded("a/b/c", &list));
        assert!(!excluded("Photos2/x", &list) && !excluded("a/bc", &list) && !excluded("a", &list));
        // Unchecking a parent absorbs its children; checking a folder brings back only that entry.
        assert_eq!(toggle_excluded(&list, "a", false), ["Photos", "a"]);
        assert_eq!(toggle_excluded(&list, "a/b", true), ["Photos"]);
        assert_eq!(toggle_excluded(&list, "Photos", false), ["a/b", "Photos"]);
    }

    #[test]
    fn unchecking_a_synced_folder_trashes_the_local_copy_and_keeps_the_cloud_one() {
        let mut f = Fixture::new("uncheck");
        write(f.local("keep.txt"), "k", 100);
        write(f.local("Photos/a.jpg"), "a", 100);
        write(f.local("Photos/edited.jpg"), "old", 100);
        f.engine.full_scan();
        write(f.local("Photos/edited.jpg"), "my edit", 5);
        f.engine.excluded = vec!["Photos".into()];
        let r = f.engine.full_scan();
        assert!(r.errors.is_empty() && r.aborted.is_none(), "{r:?}");
        assert!(!f.local("Photos/a.jpg").exists());
        let stamp = fs::read_dir(f.local(".fastsync/trash"))
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
            .path();
        assert_eq!(fs::read_to_string(stamp.join("Photos/a.jpg")).unwrap(), "a");
        assert!(f.remote("Photos/a.jpg").exists(), "the cloud copy is never deleted");
        assert_eq!(fs::read_to_string(f.local("Photos/edited.jpg")).unwrap(), "my edit");
        assert_eq!(f.engine.db.base("Photos/a.jpg").unwrap(), None);
        assert!(f.engine.db.base("keep.txt").unwrap().is_some());
    }

    #[test]
    fn unchecked_folders_are_neither_uploaded_nor_downloaded_and_come_back_when_checked() {
        let mut f = Fixture::new("excluded");
        f.engine.excluded = vec!["skip".into()];
        write(f.local("skip/up.txt"), "u", 100);
        write(f.remote("skip/down.txt"), "d", 100);
        write(f.remote("ok.txt"), "o", 100);
        f.engine.full_scan();
        assert!(!f.remote("skip/up.txt").exists() && !f.local("skip/down.txt").exists());
        assert!(f.local("ok.txt").exists());
        let r = f.engine.sync_paths(&["skip/up.txt".into(), "skip".into()]);
        assert_eq!(r.transferred, 0);
        assert!(f.engine.local_changes().iter().all(|p| !p.starts_with("skip")));
        f.engine.excluded.clear();
        f.engine.full_scan();
        assert!(f.remote("skip/up.txt").exists() && f.local("skip/down.txt").exists());
    }

    #[test]
    fn ignored_files_are_neither_uploaded_nor_downloaded() {
        let mut f = Fixture::new("ignore-new");
        f.engine.ignore = Rules::parse("*.log\nbuild/");
        write(f.local("a.log"), "l", 100);
        write(f.local("build/o.bin"), "b", 100);
        write(f.local("ok.txt"), "o", 100);
        write(f.remote("down.log"), "d", 100);
        f.engine.full_scan();
        assert!(f.remote("ok.txt").exists());
        assert!(!f.remote("a.log").exists() && !f.remote("build/o.bin").exists() && !f.local("down.log").exists());
        assert!(f.engine.local_changes().is_empty());
        assert_eq!(f.engine.sync_paths(&["a.log".into()]).transferred, 0);
    }

    #[test]
    fn a_new_rule_removes_the_cloud_copy_and_keeps_the_local_one() {
        let mut f = Fixture::new("ignore-synced");
        write(f.local("a.log"), "l", 100);
        f.engine.full_scan();
        assert!(f.remote("a.log").exists());
        f.engine.ignore = Rules::parse("*.log");
        let r = f.engine.full_scan();
        assert!(r.errors.is_empty(), "{:?}", r.errors);
        assert!(!f.remote("a.log").exists() && f.local("a.log").exists());
        assert!(f.engine.db.base("a.log").unwrap().is_none());
        // Gone from the rules: it is a new local file again and goes back up.
        f.engine.ignore = Rules::default();
        f.engine.full_scan();
        assert!(f.remote("a.log").exists());
    }

    #[test]
    fn download_only_forgets_an_ignored_file_without_touching_either_side() {
        let mut f = Fixture::new("ignore-down");
        f.engine.download_only = true;
        write(f.remote("a.log"), "l", 100);
        f.engine.full_scan();
        f.engine.ignore = Rules::parse("*.log");
        f.engine.full_scan();
        assert!(f.remote("a.log").exists() && f.local("a.log").exists());
        assert!(f.engine.db.base("a.log").unwrap().is_none());
    }

    #[test]
    fn a_rule_matching_everything_trips_the_delete_guard() {
        let mut f = Fixture::new("ignore-guard");
        for i in 0..12 {
            write(f.local(&format!("f{i}.txt")), "x", 100);
        }
        f.engine.full_scan();
        f.engine.ignore = Rules::parse("*");
        let r = f.engine.full_scan();
        let (deletes, _, sample) = r.aborted.expect("stopped");
        assert!(
            deletes == 12 && sample.len() == SAMPLE && sample[0] == "f0.txt",
            "{sample:?}"
        );
        assert!(f.remote("f0.txt").exists());
    }

    #[test]
    fn names_that_escape_the_folder_or_are_temporary_are_refused() {
        for bad in [
            "../x",
            "a/../../x",
            "/etc/passwd",
            "",
            "a//b",
            "x.partial",
            ".fastsync/tmp",
            "Um teste/My Paper doc.paper",
        ] {
            assert!(!safe_rel(bad), "{bad}");
        }
        assert!(safe_rel("dir/file.txt") && safe_rel("..hidden"));
    }
}
