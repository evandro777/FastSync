//! The sync engine: compares local, remote and the DB (see `decide`) and moves the bytes through a `Remote`.
//! Safety rules: nothing is ever removed locally (it goes to `.fastsync/trash`), a local file is only trashed when
//! the DB proves we synced it (a download-only mirror also trashes local-only files, behind the delete guard), a DB row is written only after its transfer succeeded, downloads land in a temp file
//! and are renamed into place, and a run that would delete too much is stopped.
use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque},
    fs,
    io::ErrorKind,
    path::{Path, PathBuf},
    sync::atomic::{AtomicUsize, Ordering::Relaxed},
};

use crate::{
    db::Db,
    decide::{
        Action, Base, DirAction, MIN_FILES, Sig, conflict_name, decide, decide_dir, decide_down, delete_guard_ok,
    },
    ignore::{RULES, Rules},
    rclone::{Item, now_secs},
};

mod case;
mod pair;
use case::{CASE_TMP, CLASH_WHY, case_siblings, is_old_file, move_dir_by_case, rename_by_case, spelling_kept};

pub type Result<T> = std::result::Result<T, String>;

/// The cloud side. Paths are relative to the synced folder and use `/`.
pub trait Remote: Send + Sync {
    /// Every file and folder, recursively.
    fn list(&self) -> Result<Vec<Item>>;
    /// `list`, telling `on_progress(folders done, folders total)` as it goes. A remote that cannot split its listing
    /// just lists, and the card then shows the phase without a count.
    /// `cancelled` is asked between folders: when it says true the listing fails (never a shorter list that reads as
    /// "deleted in the cloud"), and the caller tells a cancel from a real error by its own flag.
    fn list_progress(
        &self,
        _on_progress: &(dyn Fn(usize, usize) + Sync),
        _cancelled: &(dyn Fn() -> bool + Sync),
    ) -> Result<Vec<Item>> {
        self.list()
    }
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
    /// Every file and folder under the cloud folder `dir`, recursively and unfiltered (nothing ignored or excluded
    /// is left out), paths from the root. The default errors, which keeps a folder rename file by file.
    fn list_under(&self, _dir: &str) -> Result<Vec<Item>> {
        Err("list_under unsupported".into())
    }
    /// Renames the cloud folder `from` to `to` as a whole, server-side. `to` does not exist. An error leaves the
    /// engine moving the files one by one.
    fn move_dir(&self, _from: &str, _to: &str) -> Result<()> {
        Err("move_dir unsupported".into())
    }
    /// Copies a cloud file to a new name inside the cloud (no bytes cross the network). On an error the engine
    /// uploads instead.
    fn copy(&self, _from: &str, _to: &str) -> Result<()> {
        Err("copy unsupported".into())
    }
    /// The `kind` hash of the local file `local_root/rel`; `None` when the service has none to compare with.
    /// Asked once per file by `pair_uploads`, then compared with each candidate's cloud hash.
    fn local_hash(&self, _local_root: &Path, _rel: &str, _kind: &str) -> Result<Option<String>> {
        Ok(None)
    }
    /// Removes the cloud folder `rel` only if it is empty; an error (also "not empty") changes nothing.
    fn remove_empty_dir(&self, _rel: &str) -> Result<()> {
        Err("rmdir unsupported".into())
    }
    /// Creates the cloud folder `rel` (with its parents); fine when it exists. An empty folder has no file to carry it.
    fn make_dir(&self, _rel: &str) -> Result<()> {
        Err("mkdir unsupported".into())
    }
    /// Drops what the transport remembers about the cloud's folders. Called before a full listing: a folder moved or
    /// renamed in the cloud would otherwise still answer under its old path (rclone keeps the old folder id), and the
    /// engine would download its files again into the old place.
    fn forget_cache(&self) {}
    /// Drops the folders of an unfinished listing that the cloud's feed says changed (`None` = all of them), so a
    /// retry after a failed or paused scan never re-uses a folder that is out of date.
    fn forget_listed(&self, _paths: Option<&[String]>) {}
    /// True only when the local file at `local_root/rel` is proven to have the same bytes as `remote` (by the
    /// service's own hash). Unknown means false: the caller then keeps both copies.
    fn same_content(&self, _local_root: &Path, _rel: &str, _remote: &Item) -> bool {
        false
    }
    /// Where the cloud entry `rel` points when it is a symlink (Dropbox keeps the target in its metadata and the file
    /// itself is empty), `None` for a plain file. An `Err` is "could not find out": the caller retries later.
    fn link_target(&self, _rel: &str) -> Result<Option<String>> {
        Ok(None)
    }
    /// `same_content` that can say "could not find out" (`Err`: the check itself failed) apart from "different".
    /// Where a wrong "different" has a price (a conflict copy of identical bytes) the engine asks this one.
    fn compare(&self, local_root: &Path, rel: &str, remote: &Item) -> Result<bool> {
        Ok(self.same_content(local_root, rel, remote))
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
    /// Pause or Stop ended the run early: what is left was not looked at, so this is neither "up to date" nor a
    /// clean run (no feed cursor saved, nothing confirmed). The runner scans in full when it goes on.
    pub cancelled: bool,
    /// Paths whose action failed (`fail`): the runner retries just these, not the whole cloud.
    pub failed: Vec<String>,
    /// Cloud folders the feed reported that could not be listed: the runner retries just these (`fail_dir`).
    pub failed_dirs: Vec<String>,
    /// The run did nothing and asks for a full scan instead (too many folders reported at once to list one by one).
    pub wants_full: bool,
    /// Paths left for later without an error: the file changed under the transfer (an editor saved it). The edit
    /// already queued them; the runner keeps them too, in case that event was missed.
    pub later: Vec<String>,
    /// Errors about one path that retrying cannot change (a name clash) or that `fail` counted: `errors` holds all of
    /// them, and whatever is not counted here is not about one path (a listing, the state DB).
    pub(crate) accounted: usize,
    /// How many of `errors` are `skip`s: states, not failures. A run with only these is still a clean comparison.
    pub skipped: usize,
}

impl Report {
    /// One action on `path` failed: on the card, and retried by itself (a stat and the transfer, no listing).
    pub fn fail(&mut self, path: &str, why: &str) {
        self.errors.push(format!("{path}: {why}"));
        self.failed.push(path.to_string());
        self.accounted += 1;
    }

    /// The cloud folder `dir` could not be listed: on the card, and retried by itself (one listing of that folder).
    pub fn fail_dir(&mut self, dir: &str, why: &str) {
        self.errors.push(format!("{dir}: {why}"));
        self.failed_dirs.push(dir.to_string());
        self.accounted += 1;
    }

    /// A state the path is in (two names that differ only by case) that retrying changes nothing about: on the card,
    /// never retried. The next full scan looks again, as does any change to the files.
    pub fn skip(&mut self, path: &str, why: &str) {
        self.errors.push(format!("{path}: {why}"));
        self.accounted += 1;
        self.skipped += 1;
    }

    /// True when an error is not about one path (the cloud listing failed, the state DB): only a full scan retries it.
    /// Anything pushed straight into `errors` counts as this, the safe default.
    pub fn has_global_failure(&self) -> bool {
        self.errors.len() > self.accounted
    }
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

/// What a run is doing before and between transfers. A big first sync spends minutes outside `Transfer`, and the card
/// used to say only "Checking for changes…" for all of it.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub enum Phase {
    /// Moving files (or idle when `total == 0`).
    #[default]
    Transfer,
    /// Listing the cloud: `done` of `total` top-level folders.
    ListCloud,
    /// Walking the local folder (no count).
    ScanLocal,
    /// Comparing both sides: `done` of `total` files.
    Compare,
}

/// Where a run is: the file being handled now and how many of the planned ones are done. `total == 0` and `phase ==
/// Transfer` = not running.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Progress {
    pub done: usize,
    pub total: usize,
    pub kind: Kind,
    pub path: String,
    pub phase: Phase,
    /// Seconds since the run started, filled in by the runner's ticker outside `Transfer` so the card keeps moving
    /// while one slow call (a huge folder's listing) reports nothing.
    pub secs: u64,
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

/// A folder the plan looked at and which sides have it (see `decide_dir`).
struct DirWork {
    path: String,
    local: bool,
    cloud: bool,
    /// A rule now hides this synced folder: its cloud copy goes (empty-only), the local one stays. `local`/`cloud`
    /// are not looked at.
    hidden: bool,
}

type Planned = (Work, Action, Option<Base>);

/// Old paths tried per new path before giving up (each is one hash of a local file).
const PAIR_TRIES: usize = 8;

/// Smaller files are just downloaded: hashing a candidate costs more than the request saves.
const COPY_MIN: u64 = 1 << 20;

/// A conflict's local file counts as the newer one only by more than this many milliseconds (providers keep whole
/// seconds): closer than that, the cloud's version keeps the name, as it always did.
const NEWER_BY_MS: i64 = 2000;

/// Adopted files per database transaction (see `run`).
const ADOPT_BATCH: usize = 1000;

pub struct Engine<R: Remote> {
    pub root: PathBuf,
    pub remote: R,
    pub db: Db,
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
    /// The cloud takes names that differ only by case for one file (Dropbox, OneDrive). Drive and MEGA keep both, so
    /// there such names are plain files and nothing is renamed or skipped for them.
    pub case_blind: bool,
    /// Read by the runner's ticker while `run` works.
    pub progress: std::sync::Arc<std::sync::Mutex<Progress>>,
    /// Set by Pause and Stop (from the UI thread) to end the run in progress: it finishes the transfers under way,
    /// starts no new one and returns `Report.cancelled`. The runner clears it before every run.
    pub cancel: std::sync::Arc<std::sync::atomic::AtomicBool>,
    /// `(old, new, sig)`: new local files that must get a `Case Conflict` name. Planned with the new name (the plan
    /// already counts the file there), renamed by `run` only after the delete guard and the cancel checks have passed,
    /// so a stopped or cut-short run never leaves a user's file renamed.
    case_renames: Vec<(String, String, Sig)>,
    /// When the walk last reported each folder (`local_changes`): a folder that never gets settled (its cloud `mkdir`
    /// keeps failing, a case twin, a file holding the name) is reported again only after `DIR_REPORT_EVERY`, so it
    /// does not start a run at every walk.
    dir_reported: std::sync::Mutex<HashMap<String, std::time::Instant>>,
}

const DIR_REPORT_EVERY: std::time::Duration = std::time::Duration::from_secs(30 * 60);

/// Editor and downloader leftovers, and the native clients' own state, that must never be synced. The native
/// folders (Dropbox's `.dropbox.cache`, ...) are one machine's private cache: sharing them would corrupt the client
/// that owns the folder, so this is built in and not a rule the user could delete.
pub fn ignored(name: &str) -> bool {
    // Windows and macOS write these in mixed case (`Thumbs.db`, `.DS_Store`, `Desktop.ini`).
    let lower = name.to_ascii_lowercase();
    matches!(
        lower.as_str(),
        ".dropbox"
            | ".dropbox.cache"
            | ".dropbox.attr"
            | ".tmp.drivedownload"
            | ".tmp.driveupload"
            | ".debris"
            | ".ds_store"
            | "thumbs.db"
            | "desktop.ini"
            | "icon\r"
    ) || lower.starts_with("~$")
        || name == HIDDEN
        || name.ends_with(".partial")
        || name.ends_with(".part")
        || name.ends_with(".crdownload")
        // Dropbox Paper docs are listed but have no file content ("unsupported_file"): they can never be copied.
        || name.ends_with(".paper")
        || name.ends_with(".swp")
        || name.ends_with('~')
        || name.starts_with(".~lock.")
}

/// Where the trash lives and how it is counted, emptied and aged (`trash.rs`); re-exported so `engine::` paths keep
/// working.
pub use crate::trash::{empty_trash, purge_trash, trash_dir, trash_usage};

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

/// Regular files only: a symlink could point anywhere, and folders have their own rows (`decide_dir`).
fn local_sig(root: &Path, rel: &str) -> Option<Sig> {
    let m = fs::symlink_metadata(root.join(rel)).ok()?;
    m.is_file().then(|| sig_of(&m))
}

/// More reported folders than this are listed by their parents instead (`feed_dirs_to_list`); only when that still
/// leaves more (they are all at the top level) the run asks for a full scan.
const FEED_DIRS_MAX: usize = 30;

/// The folders of `dirs` that are not inside another one of them, sorted and without repeats. Compared with
/// `ancestor/`, not by sorting alone: `a b` sorts between `a` and `a/b`.
fn top_dirs(mut dirs: Vec<String>) -> Vec<String> {
    dirs.sort();
    dirs.dedup();
    let mut top: Vec<String> = Vec::new();
    for d in dirs {
        if !top
            .iter()
            .any(|a| d.strip_prefix(a.as_str()).is_some_and(|r| r.starts_with('/')))
        {
            top.push(d);
        }
    }
    top
}

/// True when a failed listing of the cloud folder `dir` really means it is gone. "Not found" alone is not enough (a
/// glitch once passed as an empty root): the folder's own `stat` has to say so too, and the root is never gone.
fn folder_gone<R: Remote>(remote: &R, dir: &str, err: &str) -> bool {
    !dir.is_empty() && err.to_lowercase().contains("not found") && matches!(remote.stat(dir), Ok(None))
}

/// `Remote::list_under` for each folder, `PARALLEL` at a time, in the order given. A cancel stops before the next
/// folder; the answers then miss folders, which only matters to a run that is being cancelled anyway.
fn list_folders<R: Remote>(
    remote: &R,
    dirs: &[String],
    cancel: &std::sync::atomic::AtomicBool,
) -> Vec<(String, Result<Vec<Item>>)> {
    let next = AtomicUsize::new(0);
    let mut got: Vec<(usize, Result<Vec<Item>>)> = std::thread::scope(|s| {
        let workers: Vec<_> = (0..PARALLEL.min(dirs.len()))
            .map(|_| {
                s.spawn(|| {
                    let mut got = Vec::new();
                    while !cancel.load(Relaxed) {
                        let i = next.fetch_add(1, Relaxed);
                        let Some(d) = dirs.get(i) else { break };
                        got.push((i, remote.list_under(d)));
                    }
                    got
                })
            })
            .collect();
        workers
            .into_iter()
            .flat_map(|h| h.join().unwrap_or_else(|e| std::panic::resume_unwind(e)))
            .collect()
    });
    got.sort_by_key(|(i, _)| *i);
    got.into_iter().map(|(i, r)| (dirs[i].clone(), r)).collect()
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
/// `cancel` stops the workers before the next job: the answers then miss paths, which only matters to a run that is
/// being cancelled anyway (`run` returns before it acts on a plan).
fn lookup<R: Remote>(
    remote: &R,
    db: &Db,
    paths: &[String],
    cancel: &std::sync::atomic::AtomicBool,
    case_blind: bool,
) -> Vec<(usize, Result<Option<Item>>)> {
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
                    while !cancel.load(Relaxed)
                        && let Some(job) = jobs.get(next.fetch_add(1, Relaxed))
                    {
                        match job {
                            Lookup::One(i) => {
                                // A case-insensitive cloud answers for `Report.txt` when asked for `report.txt`: if it
                                // says the name it holds, that is a clash, not the same file.
                                let got_one = match remote.stat(&paths[*i]) {
                                    Ok(Some(it))
                                        if case_blind
                                            && it.path != paths[*i]
                                            && it.path.to_lowercase() == paths[*i].to_lowercase() =>
                                    {
                                        Err(CLASH_WHY.to_string())
                                    }
                                    other => other,
                                };
                                got.push((*i, got_one));
                            }
                            Lookup::Dir { dir, idx } => match remote.list_dir(dir) {
                                Ok(items) => {
                                    let mut by_path: HashMap<String, Item> =
                                        items.into_iter().map(|i| (i.path.clone(), i)).collect();
                                    let lower: HashSet<String> = by_path.keys().map(|k| k.to_lowercase()).collect();
                                    got.extend(idx.iter().map(|&i| match by_path.remove(&paths[i]) {
                                        Some(it) => (i, Ok(Some(it))),
                                        // Only another spelling of the name is there: the cloud treats it as one file.
                                        None if case_blind && lower.contains(&paths[i].to_lowercase()) => {
                                            (i, Err(CLASH_WHY.to_string()))
                                        }
                                        None => (i, Ok(None)),
                                    }));
                                }
                                // "Not found" must not by itself read as "everything in it is gone" (a glitch did once
                                // for the root): the folder's own `stat` has to say it is gone too. One request
                                // instead of one per file, which took a minute for 84 deleted files.
                                Err(e) if folder_gone(remote, dir, &e) => {
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

/// `skip`: the unchecked folders (paths from `root`). They are not entered at all: walking a 50k-file folder only to
/// drop what it holds cost every full scan and every walk. `ignore`: a folder the rules hide is not entered either
/// (a `node_modules/` of 100k files would otherwise be read and matched file by file on every walk); the callers
/// treat a path missing from the result like an ignored one, because `run` applies the rules before anything else.
/// It returns the files, the folders it entered (every one that is not unchecked or hidden by the rules, empty or
/// not) and the unreadable places. A folder missing from the result is "not looked at" as much as "not there":
/// callers judge it with the same filters as for files.
pub fn scan(root: &Path, skip: &[String], ignore: &Rules) -> (HashMap<String, Sig>, BTreeSet<String>, Vec<String>) {
    scan_from(root, "", skip, ignore)
}

/// `scan` of the folder `dir`, which sits at `prefix` below the synced root: the returned paths start with `prefix`,
/// so `skip` and `ignore` judge them like a walk from the root would.
fn scan_from(
    dir: &Path,
    prefix: &str,
    skip: &[String],
    ignore: &Rules,
) -> (HashMap<String, Sig>, BTreeSet<String>, Vec<String>) {
    type Out<'a> = (&'a mut HashMap<String, Sig>, &'a mut BTreeSet<String>);
    fn go(dir: &Path, prefix: &str, (skip, ignore): (&[String], &Rules), (out, dirs): Out, bad: &mut Vec<String>) {
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
                if !excluded(&rel, skip) && !ignore.matches_dir(&rel) {
                    dirs.insert(rel.clone());
                    go(&e.path(), &rel, (skip, ignore), (&mut *out, &mut *dirs), bad);
                }
            } else if m.is_file() && !excluded(&rel, skip) {
                out.insert(rel, sig_of(&m));
            }
        }
    }
    let (mut out, mut dirs, mut bad) = (HashMap::new(), BTreeSet::new(), Vec::new());
    go(dir, prefix, (skip, ignore), (&mut out, &mut dirs), &mut bad);
    (out, dirs, bad)
}

/// True when `path` is, or sits inside, a place `scan` could not read.
fn blocked(path: &str, bad: &[String]) -> bool {
    bad.iter()
        .any(|b| b.is_empty() || excluded(path, std::slice::from_ref(b)))
}

/// The local date, so an evening conflict is not named after tomorrow (UTC is only the fallback).
fn today() -> String {
    crate::db::local_date().unwrap_or_else(|| stamp(now_secs())[..10].to_string())
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
            confirmed_deletes: None,
            restore: false,
            excluded: Vec::new(),
            ignore: Rules::default(),
            download_only: false,
            case_blind: true,
            progress: Default::default(),
            cancel: Default::default(),
            case_renames: Vec::new(),
            dir_reported: Default::default(),
        }
    }

    fn cancelled(&self) -> bool {
        self.cancel.load(Relaxed)
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
            if untouched {
                if let Err(e) = self.trash(&path) {
                    self.db.log("error", &path, None, &e.to_string());
                    report.errors.push(format!("{path}: {e}"));
                    continue;
                }
                prune_empty(&self.root, &path);
            }
            if let Err(e) = self.db.forget(&path) {
                report.errors.push(format!("{path}: {e}"));
            }
        }
        // Its synced folders leave the disk too (empty ones: `remove_dir` refuses anything with a file in it), deepest
        // first. The cloud's folders stay.
        let mut gone: Vec<String> = self
            .db
            .all_dir_rows()
            .unwrap_or_default()
            .into_iter()
            .filter(|d| excluded(d, &self.excluded))
            .collect();
        gone.sort_by_key(|d| std::cmp::Reverse(d.matches('/').count()));
        for dir in gone {
            if !under_symlink(&self.root, &dir) {
                let _ = fs::remove_dir(self.root.join(&dir));
            }
            let _ = self.db.forget_dir_row(&dir);
        }
    }

    /// Lists both sides and compares everything. Used for the first sync, after a delta reset and for providers
    /// without a delta API.
    pub fn full_scan(&mut self) -> Report {
        crate::debug::log(|| "scan     full scan".into());
        self.case_renames.clear();
        self.remote.forget_cache();
        let phase = |phase, done, total| {
            if let Ok(mut g) = self.progress.lock() {
                *g = Progress {
                    done,
                    total,
                    phase,
                    ..Default::default()
                };
            }
        };
        phase(Phase::ListCloud, 0, 0);
        let cancel = &self.cancel;
        let listed = self
            .remote
            .list_progress(&|d, t| phase(Phase::ListCloud, d, t), &|| cancel.load(Relaxed));
        // Checked before anything is trashed or planned: a cancelled listing is neither an error nor a result.
        if self.cancelled() {
            self.set_progress(Progress::default());
            return Report {
                cancelled: true,
                ..Default::default()
            };
        }
        let items = match listed {
            Ok(i) => i,
            Err(e) => {
                self.set_progress(Progress::default());
                self.db
                    .log("error", "", None, &format!("listing the cloud failed: {e}"));
                return Report {
                    errors: vec![e],
                    ..Default::default()
                };
            }
        };
        phase(Phase::ScanLocal, 0, 0);
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
        let mut cloud_dirs: HashSet<String> = HashSet::new();
        for i in items {
            if i.is_dir {
                cloud_dirs.insert(i.path);
                continue;
            }
            if !safe_rel(&i.path) || excluded(&i.path, &self.excluded) {
                continue;
            }
            if let Some(prev) = remote.insert(i.path.clone(), i) {
                duplicate.insert(prev.path);
            }
        }
        let (local, local_dirs, bad) = scan(&self.root, &self.excluded, &self.ignore);
        if self.cancelled() {
            self.set_progress(Progress::default());
            report.cancelled = true;
            return report;
        }
        // Both listings are whole, so a folder missing from one side really is missing there.
        let dir_cands: BTreeSet<String> = local_dirs
            .iter()
            .chain(&cloud_dirs)
            .cloned()
            .chain(self.db.all_dir_rows().unwrap_or_default())
            .collect();
        let dir_work = self.plan_dirs(
            dir_cands,
            &bad,
            &|p| {
                if local_dirs.contains(p) {
                    Some(true)
                } else {
                    self.local_dir(p)
                }
            },
            &|p| Some(cloud_dirs.contains(p)),
        );
        drop((local_dirs, cloud_dirs));
        for b in &bad {
            self.db.log(
                "error",
                b,
                None,
                "cannot read this (a folder, or a name that is not valid text), skipped",
            );
        }
        let known: Vec<String> = self.db.all().unwrap_or_default().into_iter().map(|(p, _)| p).collect();
        let mut local = local;
        // Names that differ only by case: a rename by case alone is a rename, a second file gets its `Case Conflict`
        // name, and what is neither is left alone (see `resolve_case_group`).
        let settled = self.settle_case_clashes(
            local
                .keys()
                .chain(remote.keys())
                .chain(known.iter())
                .map(String::as_str),
            &|p| remote.contains_key(p),
            &mut report,
        );
        // Planned under the new name; `run` renames the files once the delete guard and the cancel checks have passed.
        for (old, new) in &settled.moved {
            if let Some(s) = local.remove(old) {
                local.insert(new.clone(), s);
                self.case_renames.push((old.clone(), new.clone(), s));
            }
        }
        // Owned names, so the cloud items can be moved out of `remote` into the work (a clone of every item doubled
        // the memory of a scan of a big cloud).
        let paths: BTreeSet<String> = local.keys().chain(remote.keys()).chain(known.iter()).cloned().collect();
        drop(known);
        // A new name that is the old one with other case has no cloud copy of its own, whatever the listing says.
        let renamed_to: HashSet<&str> = settled.renames.iter().map(|(_, n)| n.as_str()).collect();
        let mut work = VecDeque::new();
        for p in paths {
            if blocked(&p, &bad) {
                continue;
            }
            if duplicate.contains(&p) {
                let why = "two cloud files have this name, skipped";
                self.db.log("error", &p, None, why);
                report.skip(&p, why);
                continue;
            }
            if settled.skip.contains(&p) {
                self.db.log("error", &p, None, CLASH_WHY);
                report.skip(&p, CLASH_WHY);
                continue;
            }
            let cloud = remote.remove(&p);
            work.push_back(Work {
                local: local.get(&p).copied(),
                remote: if renamed_to.contains(p.as_str()) { None } else { cloud },
                path: p,
                from: None,
            });
        }
        self.run(work, dir_work, report)
    }

    /// Which side has this folder, as `plan_dirs` wants it: `None` when it cannot be told (a link or file holds the
    /// name, a symlinked parent, an I/O error), never read as "not there".
    fn local_dir(&self, p: &str) -> Option<bool> {
        if under_symlink(&self.root, p) {
            return None;
        }
        match fs::symlink_metadata(self.root.join(p)) {
            Ok(m) => m.is_dir().then_some(true),
            Err(e) if matches!(e.kind(), ErrorKind::NotFound | ErrorKind::NotADirectory) => Some(false),
            Err(_) => None,
        }
    }

    /// The folders to compare among `cands`, with what each side shows (`None` = could not tell: left alone). Also left
    /// alone: unchecked folders, unreadable places (`blocked`), folders with a case-only twin among the candidates
    /// (Dropbox and OneDrive take them for one folder; the files' own rules sort the names out), and folders the
    /// rules hide, which `scan` never entered so they read as missing; their rows are dropped.
    fn plan_dirs(
        &self,
        cands: BTreeSet<String>,
        bad: &[String],
        local: &dyn Fn(&str) -> Option<bool>,
        cloud: &dyn Fn(&str) -> Option<bool>,
    ) -> Vec<DirWork> {
        let mut out = Vec::new();
        let mut seen: Vec<DirWork> = Vec::new();
        for p in cands {
            if !safe_rel(&p) || excluded(&p, &self.excluded) || blocked(&p, bad) {
                continue;
            }
            if self.ignore.matches_dir(&p) {
                // A rule now hides a folder we synced: the files in it left the cloud (`DeleteRemote`), and the empty
                // folders go with them, as the rule is the user's wish. The local folder stays. A mirror or a restore
                // never writes the cloud: only the row goes.
                // Only a folder with a row is touched at all (a read): a hidden tree of thousands of cloud folders
                // would otherwise cost a write for each at every full scan.
                if self.db.dir_synced(&p).unwrap_or(false) {
                    if self.download_only || self.restore {
                        let _ = self.db.forget_dir_row(&p);
                    } else {
                        out.push(DirWork {
                            path: p,
                            local: false,
                            cloud: true,
                            hidden: true,
                        });
                    }
                }
                continue;
            }
            if let (Some(local), Some(cloud)) = (local(&p), cloud(&p)) {
                seen.push(DirWork {
                    path: p,
                    local,
                    cloud,
                    hidden: false,
                });
            }
        }
        // Twins by case only count when a side really has them: the old spelling of a folder renamed by case is a row
        // with nothing behind it and must be forgotten, not read as a clash (it would be reported at every walk).
        let mut spellings: HashMap<String, usize> = HashMap::new();
        for d in seen.iter().filter(|d| d.local || d.cloud) {
            *spellings.entry(d.path.to_lowercase()).or_default() += 1;
        }
        out.extend(
            seen.into_iter()
                .filter(|d| spellings.get(&d.path.to_lowercase()).is_none_or(|n| *n <= 1)),
        );
        out
    }

    /// The folders among `dirs` that get a listing: the top ones only (copying a tree in makes the feed report every new
    /// subfolder, and each listing is recursive), and not the ones that are unchecked, hidden by the rules, behind a
    /// symlink or not a clean relative path. Compared with `ancestor/`, not by sorting: `a b` sorts between `a` and
    /// `a/b`.
    fn feed_dirs_to_list(&self, dirs: &[String]) -> Vec<String> {
        let mut top = top_dirs(dirs.to_vec());
        // Too many to list one by one (an archive unpacked in the cloud): list their parents instead, one level up at a
        // time from the deepest, which is still a part of the cloud, never more than the full scan would read. Only a
        // set that reaches the top level stays long, and `sync_paths_in` then asks for the full scan.
        while top.len() > FEED_DIRS_MAX {
            let deepest = top.iter().map(|d| d.matches('/').count()).max().unwrap_or(0);
            if deepest == 0 {
                break;
            }
            top = top_dirs(
                top.into_iter()
                    .map(|d| match d.matches('/').count() == deepest {
                        true => d.rsplit_once('/').map_or(String::new(), |(p, _)| p.to_string()),
                        false => d,
                    })
                    .collect(),
            );
        }
        top.into_iter()
            .filter(|d| {
                safe_rel(d)
                    && !excluded(d, &self.excluded)
                    && !under_symlink(&self.root, d)
                    && !self.ignore.matches_dir(d)
            })
            .collect()
    }

    /// Compares only these paths (from inotify or a remote delta). A folder expands to the files in it.
    pub fn sync_paths(&mut self, paths: &[String]) -> Report {
        self.sync_paths_in(paths, &[])
    }

    /// `sync_paths`, also listing the cloud folders `dirs` (a new or moved folder whose contents the feed did not
    /// report): one recursive request for each instead of a scan of the whole cloud. A folder that cannot be listed is
    /// a failure of its own (`Report::failed_dirs`): the runner lists just that folder again and the cursor may move on.
    pub fn sync_paths_in(&mut self, paths: &[String], dirs: &[String]) -> Report {
        crate::debug::log(|| format!("scan     {} changed path(s), {} folder(s)", paths.len(), dirs.len()));
        self.case_renames.clear();
        let mut report = Report::default();
        let to_list = self.feed_dirs_to_list(dirs);
        // Still dozens of folders after climbing to their parents (all at the top level): they cost as many listings
        // as the cloud has folders to read at all, which one full scan does with progress and in parallel. Nothing has
        // been touched yet.
        if to_list.len() > FEED_DIRS_MAX {
            report.wants_full = true;
            return report;
        }
        let mut all = BTreeSet::new();
        let mut bad: Vec<String> = Vec::new();
        // Folders to compare: a reported folder and the ones below it, and the synced folders under every reported path
        // (a folder deleted here, or in the cloud, is reported as itself).
        let mut dir_cands: BTreeSet<String> = BTreeSet::new();
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
                    // A folder the rules hide is not walked (`npm install` in an ignored `node_modules/` reports
                    // a folder event per directory); what was synced there before the rule comes from `db.under`.
                    if !self.ignore.matches_dir(p) {
                        // Walked with the prefix `p`, so the unchecked folders and the rules hide whole folders
                        // (a `node_modules/` of 200k files copied in is not read); `skippable` still filters files.
                        let (files, dirs, unreadable) = scan_from(&abs, p, &self.excluded, &self.ignore);
                        dir_cands.insert(p.clone());
                        dir_cands.extend(dirs);
                        all.extend(files.into_keys());
                        bad.extend(unreadable);
                    }
                }
                // Anything but "not there" (permission denied, I/O error) says nothing about the file. A folder that
                // became a file leaves the paths below it gone, not unreadable.
                Err(e) if !matches!(e.kind(), ErrorKind::NotFound | ErrorKind::NotADirectory) => bad.push(p.clone()),
                _ => {}
            }
            // A folder that is gone locally: everything we knew under it.
            all.extend(self.db.under(p).unwrap_or_default());
            all.insert(p.clone());
            dir_cands.extend(self.db.dir_rows_under(p).unwrap_or_default());
            // A new rule hides a folder: its files are queued as paths, and its (now empty) cloud folders must follow.
            // The folders under it are not reported (an empty one has no file): the rows know them.
            for a in p
                .match_indices('/')
                .map(|(i, _)| &p[..i])
                .chain([p.as_str()])
                .filter(|a| self.ignore.matches_dir(a))
            {
                dir_cands.insert(a.to_string());
                dir_cands.extend(self.db.dir_rows_under(a).unwrap_or_default());
            }
        }
        // What the cloud holds in the folders it said changed, listed in one request each, `PARALLEL` at a time. The
        // cache goes first: a folder moved or renamed in the cloud would otherwise still answer under its old path
        // (see `forget_cache`).
        let mut listed: HashMap<String, Item> = HashMap::new();
        // The folders those listings hold, and the listed ones themselves: a recursive listing is complete, so a
        // folder under a listed one that it does not show is gone from the cloud.
        let (mut listed_dirs, mut listed_roots): (HashSet<String>, Vec<String>) = (HashSet::new(), Vec::new());
        if !to_list.is_empty() {
            self.remote.forget_cache();
        }
        for d in &to_list {
            dir_cands.extend(self.db.dir_rows_under(d).unwrap_or_default());
        }
        for (d, res) in list_folders(&self.remote, &to_list, &self.cancel) {
            match res {
                Ok(items) => {
                    listed_roots.push(d.clone());
                    listed_dirs.insert(d.clone());
                    dir_cands.insert(d.clone());
                    for it in items {
                        if it.is_dir && safe_rel(&it.path) {
                            dir_cands.insert(it.path.clone());
                            listed_dirs.insert(it.path);
                        } else if !it.is_dir && safe_rel(&it.path) {
                            listed.insert(it.path.clone(), it);
                        }
                    }
                }
                // Created and deleted again before we looked (`.git` temp folders, tool caches): the feed reported
                // it, the cloud no longer has it. Same proof as in `lookup`: the folder's own `stat` must say gone.
                Err(e) if folder_gone(&self.remote, &d, &e) => listed_roots.push(d),
                Err(e) => {
                    // Not knowing what is in it is not "nothing". Only this folder is listed again by the retry, not
                    // the whole cloud (a full scan is minutes on a big one).
                    self.db
                        .log("error", &d, None, &format!("listing the folder failed: {e}"));
                    report.fail_dir(&d, &e);
                }
            }
        }
        all.extend(listed.keys().cloned());
        all.retain(|p| !excluded(p, &self.excluded) && !self.skippable(p) && !blocked(p, &bad));
        let mut all: Vec<String> = all.into_iter().collect();
        // The name check `full_scan` makes over the whole tree, for these paths and their namesakes on disk or in a
        // folder listing: a watcher event for `report.txt` next to a synced `Report.txt` must not reach the cloud, where
        // they are one file. A rename by case alone is a rename; a second file gets its `Case Conflict` name.
        let siblings = case_siblings(&self.root, &self.db, &all);
        let settled = self.settle_case_clashes(
            siblings
                .iter()
                .map(String::as_str)
                .chain(listed.keys().map(String::as_str)),
            &|p| listed.contains_key(p),
            &mut report,
        );
        let renamed_away: HashSet<&str> = settled.moved.iter().map(|(old, _)| old.as_str()).collect();
        all.retain(|p| {
            let skip = settled.skip.contains(p);
            if skip {
                self.db.log("error", p, None, CLASH_WHY);
                report.skip(p, CLASH_WHY);
            }
            !skip && !renamed_away.contains(p.as_str())
        });
        all.extend(settled.moved.iter().map(|(_, new)| new.clone()));
        // Planned under the new name; `run` renames the files once the delete guard and the cancel checks have passed.
        for (old, new) in &settled.moved {
            if let Some(s) = local_sig(&self.root, old) {
                self.case_renames.push((old.clone(), new.clone(), s));
            }
        }
        // The old name of a rename by case alone is looked at too, even when only the new one was reported.
        for (old, _) in &settled.renames {
            if !all.contains(old) {
                all.push(old.clone());
            }
        }
        // The listing already answered for these; ask the cloud only about the rest.
        let (known, ask): (Vec<String>, Vec<String>) = all.into_iter().partition(|p| listed.contains_key(p));
        let mut work = VecDeque::new();
        for p in known {
            work.push_back(Work {
                local: self.planned_sig(&p),
                remote: listed.remove(&p),
                from: None,
                path: p,
            });
        }
        let mut answers: HashMap<String, Result<Option<Item>>> =
            lookup(&self.remote, &self.db, &ask, &self.cancel, self.case_blind)
                .into_iter()
                .map(|(i, r)| (ask[i].clone(), r))
                .collect();
        // A case-insensitive cloud answers for the new name of a rename by case alone with the old file itself: the new
        // name has no cloud copy of its own, or it would be adopted and the old name deleted with it. Only when the
        // answer really is the old file: a cloud that allows both names may hold another file under the new one, and
        // that one must be left alone (it is then an ordinary conflict).
        for (old, new) in &settled.renames {
            let same = match (answers.get(old), answers.get(new)) {
                (Some(Ok(Some(o))), Some(Ok(Some(found)))) => is_old_file(found, o, new),
                (Some(Ok(Some(_))), Some(Err(e))) => e == CLASH_WHY,
                _ => false,
            };
            if same {
                answers.insert(new.clone(), Ok(None));
            }
        }
        for p in &ask {
            let p = p.clone();
            let Some(res) = answers.remove(&p) else {
                continue;
            };
            match res {
                Ok(item) => work.push_back(Work {
                    local: self.planned_sig(&p),
                    remote: item.filter(|i| !i.is_dir),
                    from: None,
                    path: p,
                }),
                // The cloud holds only another spelling of this name. A new local file gets its `Case Conflict` name
                // and goes up as a file of its own; anything else is left alone.
                Err(e) if e == CLASH_WHY => {
                    let sig = local_sig(&self.root, &p);
                    let new_file = !self.download_only && sig.is_some() && self.db.base(&p).ok().flatten().is_none();
                    let planned: Vec<String> = self.case_renames.iter().map(|(_, n, _)| n.clone()).collect();
                    match new_file.then(|| self.plan_case_conflict(&p, &planned)) {
                        Some(Ok(new)) => {
                            work.push_back(Work {
                                local: sig,
                                remote: None,
                                from: None,
                                path: new.clone(),
                            });
                            if let Some(s) = sig {
                                self.case_renames.push((p.clone(), new, s));
                            }
                        }
                        Some(Err(e)) => {
                            self.db.log("error", &p, None, &e);
                            report.fail(&p, &e);
                        }
                        None => {
                            self.db.log("error", &p, None, CLASH_WHY);
                            report.skip(&p, CLASH_WHY);
                        }
                    }
                }
                Err(e) => {
                    self.db.log("error", &p, None, &e);
                    report.fail(&p, &e);
                }
            }
        }
        let covered = |p: &str| {
            listed_roots
                .iter()
                .any(|d| p == d || p.strip_prefix(d.as_str()).is_some_and(|r| r.starts_with('/')))
        };
        // A folder no listing covered is asked for like a file (`lookup`: one listing per parent where that is cheaper).
        let ask: Vec<String> = dir_cands
            .iter()
            .filter(|p| {
                !covered(p)
                    && safe_rel(p)
                    && !excluded(p, &self.excluded)
                    && !self.ignore.matches_dir(p)
                    && !blocked(p, &bad)
                    && self.local_dir(p).is_some()
            })
            .cloned()
            .collect();
        let mut in_cloud: HashMap<&str, bool> = HashMap::new();
        for (i, res) in lookup(&self.remote, &self.db, &ask, &self.cancel, self.case_blind) {
            match res {
                Ok(Some(it)) if it.is_dir => {
                    in_cloud.insert(&ask[i], true);
                }
                Ok(None) => {
                    in_cloud.insert(&ask[i], false);
                }
                // A file holds the name, or only another spelling of it: not ours to judge.
                Ok(Some(_)) => {}
                // Only another spelling of the name is there (a folder renamed by case alone, seen from its old
                // name): a name we had and no longer have here is simply not in the cloud under that spelling.
                Err(e) if e == CLASH_WHY => {
                    if self.local_dir(&ask[i]) == Some(false) {
                        in_cloud.insert(&ask[i], false);
                    }
                }
                Err(e) => report.fail(&ask[i], &e),
            }
        }
        let dir_work = self.plan_dirs(dir_cands, &bad, &|p| self.local_dir(p), &|p| {
            if covered(p) {
                Some(listed_dirs.contains(p))
            } else {
                in_cloud.get(p).copied()
            }
        });
        self.run(work, dir_work, report)
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
        let (mut local, local_dirs, bad) = scan(&self.root, &self.excluded, &self.ignore);
        local.retain(|p, _| !self.skippable(p));
        // A map, not a scan per file: this runs every couple of minutes on trees of 100k files.
        let known: HashMap<String, Base> = self
            .db
            .all()
            .unwrap_or_default()
            .into_iter()
            // Not walked any more, so not "missing": a file under a hidden folder was never edited or deleted here.
            .filter(|(p, _)| !excluded(p, &self.excluded) && !self.ignore.matches(p))
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
        // Folders made or removed while the app was closed (an empty one has no file to show it).
        let rows: HashSet<String> = self.db.all_dir_rows().unwrap_or_default().into_iter().collect();
        let mut dirs: Vec<String> = local_dirs.iter().filter(|d| !rows.contains(*d)).cloned().collect();
        dirs.extend(rows.into_iter().filter(|d| {
            !local_dirs.contains(d) && !excluded(d, &self.excluded) && !self.ignore.matches_dir(d) && !blocked(d, &bad)
        }));
        if let Ok(mut seen) = self.dir_reported.lock() {
            let now = std::time::Instant::now();
            // Only folders still unsettled are remembered, and the time is set when one first shows up (never
            // refreshed), so a stuck folder is reported again after the interval and a settled one is forgotten.
            seen.retain(|k, t| dirs.contains(k) && now.duration_since(*t) < DIR_REPORT_EVERY);
            out.extend(dirs.into_iter().filter(|d| {
                let new = !seen.contains_key(d);
                seen.entry(d.clone()).or_insert(now);
                new
            }));
        } else {
            out.extend(dirs);
        }
        out.into_iter().collect()
    }

    fn run(&mut self, mut work: VecDeque<Work>, dirs: Vec<DirWork>, mut report: Report) -> Report {
        let mut plan = Vec::new();
        // A mirror trashes local-only files, and a new account has no DB rows to measure the guard against: count
        // the local files of this run too, or pointing it at a full folder would trash everything unchecked.
        let local_files = if self.download_only {
            work.iter().filter(|w| w.local.is_some()).count()
        } else {
            0
        };
        let (paths, cloud_seen) = (work.len(), work.iter().filter(|w| w.remote.is_some()).count());
        for (n, w) in work.drain(..).enumerate() {
            if n % 256 == 0 {
                // A plan cut off here is dropped whole: the guard and the pairing never see half of one.
                if self.cancelled() {
                    self.set_progress(Progress::default());
                    report.cancelled = true;
                    return report;
                }
                self.set_progress(Progress {
                    done: n,
                    total: paths,
                    phase: Phase::Compare,
                    ..Default::default()
                });
            }
            let base = match self.db.base(&w.path) {
                Ok(b) => b,
                Err(e) => {
                    // One path's row could not be read: retried by that path, not by a scan of the whole cloud.
                    report.fail(&w.path, &e.to_string());
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
            } else if w.local.is_none() && fs::symlink_metadata(self.root.join(&w.path)).is_ok() {
                // `scan` never lists links, so "no local file" here may be a link holding the name (the official
                // Dropbox client leaves an empty file in the cloud for each). Links are never uploaded, so there is
                // nothing to download, upload or delete: planning it only made the download fail on every retry.
                // (`make_link` is what creates a link from a cloud one.)
                Action::None
            } else if self.download_only {
                decide_down(w.local, remote, base)
            } else {
                decide(w.local, remote, base)
            };
            // (Same size, other stamp: `same_content` below, after the loop, so the hashing runs in parallel.)
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
                crate::debug::log(|| {
                    format!(
                        "plan     {}: {action:?} (local {:?}, cloud {remote:?}, synced {base:?})",
                        w.path, w.local
                    )
                });
                plan.push((w, action, base));
            }
        }
        self.adopt_identical(&mut plan, &mut report);
        // Before the guard: a rename is not a delete, whatever the folder's size.
        let mut plan = self.pair_moves(plan);
        self.pair_copies(&mut plan);
        self.pair_uploads(&mut plan);
        // A cloud folder that could not be listed may hold the new name of a folder renamed in the cloud: its old files
        // read as deleted, with nothing to pair them to, and the guard cannot judge a small share of a big account. They
        // wait, in the retry with the folder, instead of being trashed and downloaded again.
        if !report.failed_dirs.is_empty() {
            plan.retain(|(w, a, _)| {
                let keep = *a != Action::DeleteLocal;
                if !keep {
                    report.fail(&w.path, "a cloud folder could not be listed, so the delete waits");
                }
                keep
            });
        }
        // The rules file travels like any file, and a rule that hides synced files deletes their cloud copies. Another
        // machine that sees those deletes before the new rules reads them as ordinary deletes and trashes its files.
        // So the rules go up before the deletes, and a run that brings new rules in waits with its local deletes: the
        // next run reads the rules and makes them `Forget`.
        let rules_in = plan
            .iter()
            .any(|(w, a, _)| w.path == RULES && matches!(a, Action::Download | Action::Conflict));
        if rules_in {
            // `later`, not `fail`: a normal event on the other machines, no red card; the retry still picks it up.
            plan.retain(|(w, a, _)| {
                let keep = *a != Action::DeleteLocal;
                if !keep {
                    report.later.push(w.path.clone());
                }
                keep
            });
        }
        // Nothing was done yet: a cancel here costs nothing, and the guard must not judge a plan that was cut short.
        if self.cancelled() {
            self.set_progress(Progress::default());
            report.cancelled = true;
            return report;
        }
        let deletes = plan
            .iter()
            .filter(|(_, a, _)| matches!(a, Action::DeleteLocal | Action::DeleteRemote))
            .count();
        // An unreadable DB must not read as "0 known files": that would switch the guard off.
        let known = match self.db.count() {
            Ok(n) => n.max(local_files),
            Err(e) => {
                report.errors.push(format!("state database: {e}"));
                self.set_progress(Progress::default());
                return report;
            }
        };
        let allowed = match self.confirmed_deletes {
            Some(cap) => deletes <= cap,
            None => delete_guard_ok(deletes, known, GUARD_PERCENT) && !self.folder_wiped(&plan),
        };
        if !allowed {
            self.report_stop(&plan, known, (paths, cloud_seen), &mut report);
            self.set_progress(Progress::default());
            return report;
        }
        // The plan counted the new local files under their `Case Conflict` names: rename them now that nothing stops the
        // run. One that changed meanwhile stays where it is, and its upload is dropped.
        let dropped = self.apply_case_renames(&mut report);
        plan.retain(|(w, ..)| !dropped.contains(&w.path));
        // After the guard (a stopped run must not publish its rules), before the deletes they cause: see above.
        let hides = plan
            .iter()
            .any(|(w, a, _)| *a == Action::DeleteRemote && self.ignore.matches(&w.path));
        if hides
            && let Some(i) = plan
                .iter()
                .position(|(w, a, _)| w.path == RULES && *a == Action::Upload)
        {
            let (w, a, _) = plan.remove(i);
            let total = plan.len() + 1;
            self.run_transfers(vec![(w, a)], 0, total, &mut report);
            // Not up: the deletes it explains wait for the retry too.
            if report.failed.iter().chain(&report.later).any(|p| p == RULES) {
                plan.retain(|(w, a, _)| {
                    let keep = !(*a == Action::DeleteRemote && self.ignore.matches(&w.path));
                    if !keep {
                        report.later.push(w.path.clone());
                    }
                    keep
                });
            }
        }
        let mut plan: VecDeque<_> = plan.into();
        let (mut done, mut total) = (0, plan.len());
        let mut transfers = Vec::new();
        // Adopt only writes a row: a first sync onto a folder that is already there makes one per file, and a commit
        // each cost ~20 KB of journal. They share a transaction of `ADOPT_BATCH` rows; a crash loses at most a batch,
        // whose files are then adopted again.
        let mut open_rows = 0usize;
        while let Some((w, action, base)) = plan.pop_front() {
            if self.cancelled() {
                break;
            }
            if action != Action::Adopt && open_rows > 0 {
                self.commit_rows(&mut report);
                open_rows = 0;
            }
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
                    ..Default::default()
                });
            } else if done % 256 == 0 {
                // Adopting thousands of files already on disk names no file, but is not a frozen card either.
                self.set_progress(Progress {
                    done,
                    total,
                    phase: Phase::Compare,
                    ..Default::default()
                });
            }
            done += 1;
            if action == Action::Adopt && open_rows == 0 && self.db.begin() {
                open_rows = 1;
            } else if open_rows > 0 {
                open_rows += 1;
            }
            if open_rows >= ADOPT_BATCH {
                self.commit_rows(&mut report);
                open_rows = 0;
            }
            match self.exec(&w, action, base) {
                Ok(extra) => {
                    log_done(&self.db, &w, action, &mut report);
                    // A conflict leaves its copies to be uploaded.
                    for e in extra {
                        plan.push_back((e, Action::Upload, None));
                        total += 1;
                    }
                }
                Err(e) => log_failed(&self.db, &w, &e, &mut report),
            }
        }
        if open_rows > 0 {
            self.commit_rows(&mut report);
        }
        // A folder renamed here goes as one cloud call; what that cannot prove stays one move per file.
        let uploads: Vec<String> = transfers
            .iter()
            .filter(|(_, a)| matches!(a, Action::Upload | Action::MoveRemote))
            .map(|(w, _)| w.path.clone())
            .collect();
        done += self.move_dirs(&mut transfers, (done, total), &mut report);
        self.run_transfers(transfers, done, total, &mut report);
        // After the files: a folder the user deleted is empty by now, and a new one may already exist from a move.
        self.run_dirs(dirs, &uploads, &mut report);
        self.set_progress(Progress::default());
        report.cancelled = self.cancelled();
        report
    }

    /// Ends the transaction of adopted rows. A failure rolls the batch back (`Db::commit`): those files are adopted
    /// again by the next scan, and the run says so instead of losing later writes silently.
    fn commit_rows(&self, report: &mut Report) {
        if let Err(e) = self.db.commit() {
            report.errors.push(format!("state database: {e}"));
        }
    }

    /// Records a run the delete guard stopped: stderr, the activity log and `Report.aborted`.
    /// `seen` is (paths in the run, files seen in the cloud).
    fn report_stop(
        &self,
        plan: &[(Work, Action, Option<Base>)],
        known: usize,
        seen: (usize, usize),
        report: &mut Report,
    ) {
        let (paths, cloud_seen) = seen;
        let dels = || {
            plan.iter()
                .filter(|(_, a, _)| matches!(a, Action::DeleteLocal | Action::DeleteRemote))
        };
        let deletes = dels().count();
        let sample = dels().take(SAMPLE).map(|(w, ..)| w.path.clone()).collect();
        // To tell an empty listing (cloud_seen 0) from a different cloud the next time this trips.
        let remote_deletes = plan.iter().filter(|(_, a, _)| *a == Action::DeleteRemote).count();
        let line = format!(
            "{deletes} deletes ({remote_deletes} in the cloud) of {known} known; {paths} paths in this run, {cloud_seen} seen in the cloud"
        );
        eprintln!("delete guard: {line}");
        // Which files, and which side: without it a trip cannot be diagnosed afterwards.
        let mut listed = line.clone();
        for (i, (w, a, _)) in dels().enumerate() {
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
    }

    /// True when the plan deletes every known file of one top-level folder of `MIN_FILES` or more: a disk mounted
    /// inside the synced folder that went away looks exactly like that, and stays under the account-wide share. A rule
    /// that hides a folder (a delete with the local file still there) is not counted.
    fn folder_wiped(&self, plan: &[(Work, Action, Option<Base>)]) -> bool {
        let mut gone: HashMap<&str, usize> = HashMap::new();
        for (w, a, _) in plan {
            let wiped = match a {
                Action::DeleteLocal => true,
                Action::DeleteRemote => w.local.is_none(),
                _ => false,
            };
            if let (true, Some((top, _))) = (wiped, w.path.split_once('/')) {
                *gone.entry(top).or_default() += 1;
            }
        }
        gone.into_iter()
            // A mirror's local-only files have no row: only folders the DB knows are judged (the share rule covers it).
            .any(|(top, n)| n >= MIN_FILES && self.db.under(top).is_ok_and(|k| !k.is_empty() && n >= k.len()))
    }

    /// Folders, after the files (see `decide_dir`). A removal only ever takes an **empty** folder (`remove_dir`,
    /// `rmdir`): what is inside went through the file plan and the delete guard first, and a folder that still holds
    /// something (a file saved meanwhile, a `.DS_Store`) stays and is just forgotten, so nothing retries for ever and
    /// whatever is in it syncs as files. Removals go deepest first, creations shallowest first. A folder a run
    /// emptied is *not* removed: only a folder the user deleted is (the official clients behave the same).
    fn run_dirs(&mut self, dirs: Vec<DirWork>, uploads: &[String], report: &mut Report) {
        let depth = |p: &str| p.matches('/').count();
        // A folder with a file that went up this run is in the cloud already (an upload makes its folders): a first sync
        // of 10k folders must not cost 10k more requests. A failed or postponed upload proves nothing.
        let unproven: HashSet<&String> = report.failed.iter().chain(&report.later).collect();
        let made: HashSet<String> = uploads
            .iter()
            .filter(|u| !unproven.contains(u))
            .flat_map(|u| u.match_indices('/').map(|(i, _)| u[..i].to_string()))
            .collect();
        let mut removals = Vec::new();
        let mut creations = Vec::new();
        for d in dirs {
            let synced = match self.db.dir_synced(&d.path) {
                Ok(s) => s,
                Err(e) => {
                    report.fail(&d.path, &e.to_string());
                    continue;
                }
            };
            let action = if d.hidden {
                DirAction::RemoveCloud
            } else {
                decide_dir(d.local, d.cloud, synced, self.download_only, self.restore)
            };
            let res = match action {
                DirAction::None => Ok(()),
                DirAction::Adopt => self.db.add_dir_row(&d.path),
                DirAction::Forget => self.db.forget_dir_row(&d.path),
                a @ (DirAction::RemoveLocal | DirAction::RemoveCloud) => {
                    removals.push((d.path, a));
                    continue;
                }
                a => {
                    creations.push((d.path, a));
                    continue;
                }
            };
            if let Err(e) = res {
                report.fail(&d.path, &e.to_string());
            }
        }
        removals.sort_by_key(|(p, _)| std::cmp::Reverse(depth(p)));
        creations.sort_by_key(|(p, _)| depth(p));
        for (path, action) in removals.into_iter().chain(creations) {
            if self.cancelled() {
                return;
            }
            let res = if action == DirAction::MakeCloud && made.contains(&path) {
                self.db.add_dir_row(&path).map_err(|e| e.to_string())
            } else {
                self.dir_action(&path, action)
            };
            if let Err(e) = res {
                self.db.log("error", &path, None, &e);
                report.fail(&path, &e);
            }
        }
    }

    fn dir_action(&self, path: &str, action: DirAction) -> Result<()> {
        let abs = self.root.join(path);
        let row = |r: rusqlite::Result<()>| r.map_err(|e| e.to_string());
        match action {
            DirAction::RemoveLocal => {
                // Behind a symlinked parent the path is outside what we sync. Not empty, already gone or taken by a
                // file: it stays or is gone, and is forgotten (see `run_dirs`). Any other failure (no permission) is
                // retried: a forgotten row would make the folder go back up to the cloud it was deleted from.
                if !under_symlink(&self.root, path)
                    && let Err(e) = fs::remove_dir(&abs)
                    && !matches!(
                        e.kind(),
                        ErrorKind::NotFound | ErrorKind::NotADirectory | ErrorKind::DirectoryNotEmpty
                    )
                {
                    return Err(e.to_string());
                }
                row(self.db.forget_dir_row(path))
            }
            DirAction::RemoveCloud => {
                // "Not empty" is the answer for a folder that holds something we cannot see (or a file added meanwhile):
                // it stays and is just forgotten. Any other failure with the folder still there is retried, or the
                // folder the user deleted would come back at the next scan.
                // The wording differs by provider, so a failure that does not say it is checked by looking inside.
                if let Err(e) = self.remote.remove_empty_dir(path)
                    && !e.to_lowercase().contains("not empty")
                    && self.remote.stat(path)?.is_some()
                    && self.remote.list_dir(path)?.is_empty()
                {
                    return Err(e);
                }
                row(self.db.forget_dir_row(path))
            }
            DirAction::MakeLocal => {
                if under_symlink(&self.root, path) {
                    return Ok(());
                }
                fs::create_dir_all(&abs).map_err(|e| e.to_string())?;
                row(self.db.add_dir_row(path))
            }
            DirAction::MakeCloud => {
                self.remote.make_dir(path)?;
                row(self.db.add_dir_row(path))
            }
            _ => Ok(()),
        }
    }

    /// The uploads and downloads of a run, `PARALLEL` at a time. Only the cloud calls and file moves run in the
    /// workers; every DB write, log line and counter stays on this thread (the DB has a single connection).
    fn run_transfers(&mut self, jobs: Vec<(Work, Action)>, done: usize, total: usize, report: &mut Report) {
        if jobs.is_empty() {
            return;
        }
        let workers = PARALLEL.min(jobs.len());
        let queue = std::sync::Mutex::new(VecDeque::from(jobs));
        let finished = AtomicUsize::new(done);
        let (remote, root, download_only) = (&self.remote, &self.root, self.download_only);
        let (progress, db, cancel) = (&self.progress, &self.db, &self.cancel);
        let (tx, rx) = std::sync::mpsc::channel();
        std::thread::scope(|s| {
            for _ in 0..workers {
                let tx = tx.clone();
                let (queue, finished) = (&queue, &finished);
                s.spawn(move || {
                    // A cancel lets the transfers under way finish (a half-written file is worse) and takes no more.
                    while !cancel.load(Relaxed)
                        && let Some((w, action)) = queue.lock().ok().and_then(|mut q| q.pop_front())
                    {
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
                                ..Default::default()
                            };
                        }
                        let result = match make_link(remote, root, download_only, &w, action) {
                            Ok(Some(done)) => Ok(done),
                            Ok(None) => {
                                transfer(remote, root, download_only, &w, action).map(|(b, id)| Done::Row(b, id))
                            }
                            Err(e) => Err(e),
                        };
                        if tx.send((w, action, result)).is_err() {
                            break;
                        }
                    }
                });
            }
            drop(tx);
            for (w, action, result) in rx {
                if matches!(&result, Err(e) if e == CHANGED) {
                    report.later.push(w.path.clone());
                    finished.fetch_add(1, Relaxed);
                    continue;
                }
                // The new row first: a crash in between leaves two rows (the old one reads as gone on both sides and
                // is forgotten next run), never none.
                // `Ok(false)`: nothing was done and nothing is logged.
                let saved = result.and_then(|done| match done {
                    Done::Row(b, id) => {
                        db.set(&w.path, &b, &id).map_err(|e| e.to_string())?;
                        // A download's or upload's `from` is the file it was copied from: that one stays.
                        match &w.from {
                            Some(f) if matches!(action, Action::MoveLocal | Action::MoveRemote) => {
                                db.forget(&f.path).map_err(|e| e.to_string()).map(|()| true)
                            }
                            _ => Ok(true),
                        }
                    }
                    Done::Link => Ok(true),
                    Done::Skipped => Ok(false),
                });
                match saved {
                    Ok(true) => {
                        log_done(db, &w, action, report);
                    }
                    Ok(false) => {}
                    Err(e) => log_failed(db, &w, &e, report),
                }
                finished.fetch_add(1, Relaxed);
            }
        });
    }

    /// What else must go up after the action: a conflict leaves its copies (see `Action::Conflict`).
    fn exec(&mut self, w: &Work, action: Action, base: Option<Base>) -> Result<Vec<Work>> {
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
                // The older of the two gets the `(conflict)` name: the newer one is most likely what the user wants
                // under the real name. Within `NEWER_BY_MS` (a provider's whole seconds) the cloud keeps it.
                if w.local.is_some_and(|l| l.mtime > r.mtime + NEWER_BY_MS) {
                    // The cloud's older version goes beside it; the local one stays and goes up over the cloud's.
                    fetch_to(&self.remote, &self.root, path, &aside, None, r, None)?;
                    // The row reads "local still as the cloud's, then edited": whatever stops the upload below, the
                    // next scan sees an ordinary local edit, not another conflict with another copy of the old file.
                    let base = Base {
                        local: sig_item(r),
                        remote: sig_item(r),
                    };
                    self.db.set(path, &base, rid).map_err(db)?;
                    return Ok(vec![
                        Work {
                            local: local_sig(&self.root, path),
                            remote: Some(r.clone()),
                            from: None,
                            path: path.to_string(),
                        },
                        Work {
                            local: local_sig(&self.root, &aside),
                            remote: None,
                            from: None,
                            path: aside,
                        },
                    ]);
                }
                fs::rename(self.root.join(path), self.root.join(&aside)).map_err(|e| e.to_string())?;
                self.download(path, None, r)?;
                return Ok(vec![Work {
                    local: local_sig(&self.root, &aside),
                    remote: None,
                    from: None,
                    path: aside,
                }]);
            }
        }
        Ok(Vec::new())
    }

    fn download(&mut self, path: &str, expect_local: Option<Sig>, r: &Item) -> Result<()> {
        let (b, id) = fetch(&self.remote, &self.root, path, expect_local, r, None)?;
        self.db.set(path, &b, &id).map_err(|e| e.to_string())
    }

    /// Moves a file to `.fastsync/trash/<time>/<path>`. Its folder stays, empty or not (folders are synced).
    fn trash(&self, rel: &str) -> std::io::Result<()> {
        trash_move(&self.root, &self.root.join(rel), rel)
    }
}

/// Moves the file `src` to `.fastsync/trash/<time>/<rel>`: a rename, or copy + remove when `src` is on another file
/// system (a disk mounted inside the synced folder).
fn trash_move(root: &Path, src: &Path, rel: &str) -> std::io::Result<()> {
    let batch = trash_dir(root).join(now_secs().to_string());
    let mut dest = batch.join(rel);
    // `rename` replaces a file: the same path trashed twice in one second must not eat the first copy.
    for n in 1.. {
        if fs::symlink_metadata(&dest).is_err() {
            break;
        }
        dest = batch.join(format!("{rel}.{n}"));
    }
    fs::create_dir_all(dest.parent().unwrap_or(root))?;
    match fs::rename(src, &dest) {
        Err(e) if e.kind() == ErrorKind::CrossesDevices => {
            copy_with_time(src, &dest)?;
            // The copy is only the trash copy once the original is gone.
            fs::remove_file(src).inspect_err(|_| {
                let _ = fs::remove_file(&dest);
            })
        }
        r => r,
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
pub(crate) const PARALLEL: usize = 4;

/// Largest share (percent) of the known files one run may delete.
const GUARD_PERCENT: usize = 50;

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
            // A local copy of a synced file: let the service copy it. Only a result whose hash is the proven one
            // counts (the source may have changed since the plan); anything else is uploaded below.
            let copied = w.from.as_deref().and_then(|f| {
                let want = f.remote.as_ref()?.hash.as_ref()?;
                remote.copy(&f.path, &w.path).ok()?;
                let got = remote.stat(&w.path).ok()??;
                (got.size == l.size && got.hash.as_ref() == Some(want)).then_some(got)
            });
            let r = match copied {
                Some(r) => r,
                None => {
                    if let Err(e) = remote.upload(root, &w.path) {
                        // A file saved while it uploads fails rclone's hash check ("corrupted on transfer") about
                        // bytes that were fine when read: not an error, the new version goes up on its own.
                        return Err(if local_sig(root, &w.path) != w.local {
                            CHANGED.into()
                        } else {
                            e
                        });
                    }
                    // What the remote reports now is the base, so its own echo in a delta compares equal.
                    remote.stat(&w.path)?.ok_or("uploaded file not found")?
                }
            };
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
            // A rename by case alone (`MyFile.txt` -> `myfile.txt`): a case-insensitive cloud answers for the new name
            // with the old file itself, which is not "something that appeared". Another file under the new name is.
            let case_only = from.path.to_lowercase() == w.path.to_lowercase();
            // A move over an existing cloud file would replace it.
            if let Some(found) = remote.stat(&w.path)?
                && !(case_only && is_old_file(&found, old, &w.path))
            {
                return Err("appeared in the cloud, will retry".into());
            }
            // A service that cannot move gets what the unpaired plan would have done: upload, then delete the old.
            // Never for a case-only rename: on a case-insensitive cloud the upload lands on the old file and the
            // delete would then remove the only copy.
            if case_only {
                // Only a folder part differs (`Docs/a.txt` -> `docs/a.txt`): moving the file cannot change the folder's
                // name, only renaming the whole folder can (`move_dirs`, which needs every file of it to move).
                let parent = |p: &str| p.rsplit_once('/').map_or(String::new(), |(d, _)| d.to_string());
                if parent(&from.path) != parent(&w.path) {
                    return Err(
                        "its folder was renamed by case alone: only the whole folder can be renamed in the \
                                cloud, and something else is in it; will retry"
                            .into(),
                    );
                }
                // Checked after the rename, whatever it answered: "ok" is not "renamed" for a cloud that ignores case.
                let direct = remote
                    .rename(&from.path, &w.path)
                    .and_then(|()| spelling_kept(remote, &w.path));
                if let Err(e) = direct {
                    rename_by_case(remote, &from.path, &w.path)
                        .and_then(|()| spelling_kept(remote, &w.path))
                        .map_err(|e2| format!("could not rename the cloud file, will retry: {e}; {e2}"))?;
                }
            } else if remote.rename(&from.path, &w.path).is_err() {
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

/// What `transfer` and `fetch` say when the local file changed under them. `run_transfers` does not show it as an
/// error (an edit is no failure, and the edit's own event syncs the new version): the path goes to `Report.later`.
const CHANGED: &str = "changed while transferring, will retry";

/// What a finished transfer leaves to record.
enum Done {
    /// The row to write: how both sides look now, the remote id.
    Row(Base, String),
    /// A symlink was made. No row: `local_sig` does not see links, so a row would read as "deleted here" later.
    Link,
    /// A cloud link we refuse to make (see `safe_link_target`): nothing done, nothing wrong.
    Skipped,
}

/// True when a link at `rel` pointing at `target` stays inside the synced folder, judged by the names alone: relative,
/// never above the root, no NUL. Anything else (an absolute path, `../..` out of the folder) is not made, so a cloud
/// entry cannot aim a later write at `~/.ssh`. A folder on the way that is itself a link is `refuse`'s to catch.
fn safe_link_target(rel: &str, target: &str) -> bool {
    if target.is_empty() || target.len() > 4096 || target.contains('\0') || target.starts_with('/') {
        return false;
    }
    // Folders between the root and the link.
    let mut depth = rel.split('/').count() as i64 - 1;
    for part in target.split('/') {
        match part {
            "" | "." => {}
            ".." => {
                depth -= 1;
                if depth < 0 {
                    return false;
                }
            }
            _ => depth += 1,
        }
    }
    true
}

/// A new empty cloud file may be a symlink the official Dropbox client stored: its target is in the metadata, not the
/// bytes. Makes the link instead of an empty file, never over a name that is taken. `Ok(None)`: a plain file, download
/// it as usual. An `Err` (the metadata call failed) is retried, so a hiccup never turns a link into an empty file.
fn make_link<R: Remote>(
    remote: &R,
    root: &Path,
    download_only: bool,
    w: &Work,
    action: Action,
) -> Result<Option<Done>> {
    let Some(r) = &w.remote else { return Ok(None) };
    if action != Action::Download || w.local.is_some() || w.from.is_some() || r.size != 0 || r.is_dir {
        return Ok(None);
    }
    refuse(root, download_only, &w.path, action)?;
    let Some(target) = remote.link_target(&w.path)? else {
        return Ok(None);
    };
    if !safe_link_target(&w.path, &target) {
        crate::debug::log(|| format!("link     {}: target {target:?} not made (outside the folder)", w.path));
        return Ok(Some(Done::Skipped));
    }
    let dest = root.join(&w.path);
    if let Some(parent) = dest.parent() {
        fs::create_dir_all(parent).map_err(|e| e.to_string())?;
    }
    // Fails when the name exists, so a file or link that appeared since the plan is never replaced.
    match std::os::unix::fs::symlink(&target, &dest) {
        Ok(()) => Ok(Some(Done::Link)),
        Err(e) if e.kind() == ErrorKind::AlreadyExists => {
            Err("a file or link with this name appeared here, not replaced".into())
        }
        Err(e) => Err(e.to_string()),
    }
}

/// Removes the empty folders a file left behind, up to (not including) the synced folder. Only for a folder the user
/// unticked (it leaves the disk); elsewhere an emptied folder stays, like the official clients keep it.
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
    fetch_to(remote, root, path, path, expect_local, r, copy_of)
}

/// `fetch` of the cloud file `path` into the local file `local` (another name only for a conflict copy of it).
fn fetch_to<R: Remote>(
    remote: &R,
    root: &Path,
    path: &str,
    local: &str,
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
    let placed = (|| -> Result<()> {
        // A failed local copy is no error: the download below does the job.
        let copied = copy_of.is_some_and(|f| copy_local(root, f, &tmp, r.mtime).is_ok());
        if !copied {
            remote.download(path, &tmp)?;
        }
        // The user edited it while it downloaded: keep their edit, the next run sorts it out.
        if local_sig(root, local) != expect_local {
            return Err(CHANGED.into());
        }
        let dest = root.join(local);
        if let Some(parent) = dest.parent() {
            fs::create_dir_all(parent).map_err(|e| e.to_string())?;
        }
        if expect_local.is_none() {
            // Nothing was here a moment ago: a file that appeared since (saved by the user between the check above
            // and here) must not be replaced, and `rename` would. A hard link fails when the name exists.
            match fs::hard_link(&tmp, &dest) {
                Ok(()) => Ok(()),
                Err(e) if e.kind() == ErrorKind::AlreadyExists => {
                    Err("a file or link with this name appeared here, not replaced".into())
                }
                // A file system without hard links, or another one than `.fastsync/tmp` (a disk mounted inside the
                // folder): the plain rename (the old, tiny window).
                Err(_) => rename_over(&tmp, &dest),
            }
        } else {
            // Overwriting a file we synced: take it out of the way first and look at it, so an edit saved between the
            // check above and now is put back (or trashed), never replaced.
            // In the trash, not in `tmp`: the next start empties `tmp`, so a quit or crash while the user's file sat
            // there would lose it for good. Same file system as `tmp`.
            let aside = trash_dir(root).join("set-aside");
            let old = aside.join(format!("{}-{}-{n}.old", std::process::id(), now_secs()));
            if fs::create_dir_all(&aside).is_err() || fs::rename(&dest, &old).is_err() {
                // Cannot be set aside (another file system): the plain rename, with its tiny window.
                return rename_over(&tmp, &dest);
            }
            let kept = fs::symlink_metadata(&old).is_ok_and(|m| m.is_file() && Some(sig_of(&m)) == expect_local);
            let result = if kept {
                let put = match fs::hard_link(&tmp, &dest) {
                    Ok(()) => Ok(()),
                    Err(e) if e.kind() == ErrorKind::AlreadyExists => {
                        Err("a file with this name appeared here, not replaced".into())
                    }
                    Err(_) => rename_over(&tmp, &dest),
                };
                // Nothing new went in: the old file goes back (unless the name is taken again), so a failure never
                // leaves the folder without it.
                if put.is_err() {
                    let _ = fs::hard_link(&old, &dest);
                }
                put
            } else {
                // Edited meanwhile: the edit goes back, or to the trash when the name is taken again.
                if fs::hard_link(&old, &dest).is_err() {
                    trash_move(root, &old, local).map_err(|e| e.to_string())?;
                }
                Err(CHANGED.into())
            };
            let _ = fs::remove_file(&old);
            result
        }
    })();
    // Whatever happened, no temp copy stays behind: a retried failure would otherwise leave one full-size file per
    // try until the next start. (After a rename there is nothing to remove.)
    let _ = fs::remove_file(&tmp);
    placed?;
    let l = local_sig(root, local).ok_or("downloaded file not found")?;
    Ok((
        Base {
            local: l,
            remote: sig_item(r),
        },
        r.id.clone(),
    ))
}

/// `fs::rename`, or, when `dest` is on another file system than `tmp` (a disk mounted inside the folder), a copy to a
/// hidden name beside `dest` (ignored by `scan`: `.part`) and a rename from there.
fn rename_over(tmp: &Path, dest: &Path) -> Result<()> {
    match fs::rename(tmp, dest) {
        Err(e) if e.kind() == ErrorKind::CrossesDevices => {
            let mut name = dest.file_name().unwrap_or_default().to_os_string();
            name.push(".fastsync.part");
            let part = dest.with_file_name(name);
            copy_with_time(tmp, &part)
                .and_then(|()| fs::rename(&part, dest))
                .inspect_err(|_| {
                    let _ = fs::remove_file(&part);
                })
        }
        r => r,
    }
    .map_err(|e| e.to_string())
}

/// `fs::copy` that also keeps the modified time (the sync compares it).
fn copy_with_time(src: &Path, dst: &Path) -> std::io::Result<()> {
    fs::copy(src, dst)?;
    if let Ok(t) = fs::metadata(src).and_then(|m| m.modified()) {
        let _ = fs::File::options()
            .write(true)
            .open(dst)
            .and_then(|f| f.set_modified(t));
    }
    Ok(())
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
    report.fail(&w.path, e);
    // A move's old path is part of the same job: it is looked at again too, or its delete would wait for a full scan.
    if let Some(f) = &w.from {
        report.failed.push(f.path.clone());
    }
}

#[cfg(test)]
mod tests;
