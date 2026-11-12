//! The sync engine: compares local, remote and the DB (see `decide`) and moves the bytes through a `Remote`.
//! Safety rules: nothing is ever removed locally (it goes to `.fastsync/trash`), a local file is only trashed when
//! the DB proves we synced it, a DB row is written only after its transfer succeeded, downloads land in a temp file
//! and are renamed into place, and a run that would delete too much is stopped.
use std::{
    collections::{BTreeSet, HashMap, VecDeque},
    fs,
    path::{Path, PathBuf},
};

use crate::{
    db::Db,
    decide::{Action, Base, Sig, case_collisions, conflict_name, decide, delete_guard_ok},
    rclone::{Item, now_secs},
};

pub type Result<T> = std::result::Result<T, String>;

/// The cloud side. Paths are relative to the synced folder and use `/`.
pub trait Remote: Send {
    /// Every file and folder, recursively.
    fn list(&self) -> Result<Vec<Item>>;
    fn stat(&self, rel: &str) -> Result<Option<Item>>;
    fn upload(&self, local_root: &Path, rel: &str) -> Result<()>;
    /// Writes the remote file to exactly `dest` (a temp file; the engine moves it into place).
    fn download(&self, rel: &str, dest: &Path) -> Result<()>;
    fn delete(&self, rel: &str) -> Result<()>;
}

/// The engine's own folder inside the synced folder: temp downloads and the trash. Never synced.
pub const HIDDEN: &str = ".fastsync";

#[derive(Default, Debug)]
pub struct Report {
    pub transferred: usize,
    pub errors: Vec<String>,
    /// Set when the delete guard stopped the run.
    pub aborted: Option<(usize, usize)>,
}

struct Work {
    path: String,
    local: Option<Sig>,
    remote: Option<Item>,
}

pub struct Engine<R: Remote> {
    pub root: PathBuf,
    pub remote: R,
    pub db: Db,
    /// Largest share (percent) of the known files one run may delete.
    pub guard_percent: usize,
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

/// A remote name must stay inside the folder: a hostile `../../.bashrc` would otherwise write outside it.
pub fn safe_rel(p: &str) -> bool {
    !p.is_empty()
        && !p.contains('\0')
        && p.split('/')
            .all(|c| !c.is_empty() && c != "." && c != ".." && !ignored(c))
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

pub fn walk(root: &Path) -> HashMap<String, Sig> {
    fn go(dir: &Path, prefix: &str, out: &mut HashMap<String, Sig>) {
        let Ok(rd) = fs::read_dir(dir) else { return };
        for e in rd.flatten() {
            let (Some(name), Ok(m)) = (e.file_name().to_str().map(str::to_string), e.metadata()) else {
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
            if m.is_dir() {
                go(&e.path(), &rel, out);
            } else if m.is_file() {
                out.insert(rel, sig_of(&m));
            }
        }
    }
    let mut out = HashMap::new();
    go(root, "", &mut out);
    out
}

fn today() -> String {
    // Civil date from days since 1970 (Howard Hinnant's algorithm).
    let z = now_secs().div_euclid(86_400) + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    format!("{:04}-{m:02}-{d:02}", yoe + era * 400 + i64::from(m <= 2))
}

impl<R: Remote> Engine<R> {
    pub fn new(root: PathBuf, remote: R, db: Db) -> Self {
        Self {
            root,
            remote,
            db,
            guard_percent: 50,
        }
    }

    /// Lists both sides and compares everything. Used for the first sync, after a delta reset and for providers
    /// without a delta API.
    pub fn full_scan(&mut self) -> Report {
        let items = match self.remote.list() {
            Ok(i) => i,
            Err(e) => {
                return Report {
                    errors: vec![e],
                    ..Default::default()
                };
            }
        };
        let mut report = Report::default();
        let dirs: Vec<(String, String)> = items
            .iter()
            .filter(|i| i.is_dir && !i.id.is_empty())
            .map(|i| (i.id.clone(), i.path.clone()))
            .collect();
        if let Err(e) = self.db.set_dirs(&dirs) {
            report.errors.push(e.to_string());
        }
        let remote: HashMap<String, Item> = items
            .into_iter()
            .filter(|i| !i.is_dir && safe_rel(&i.path))
            .map(|i| (i.path.clone(), i))
            .collect();
        let local = walk(&self.root);
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
            if clash.contains(p) {
                report
                    .errors
                    .push(format!("{p}: another file differs only by upper/lower case, skipped"));
                continue;
            }
            work.push_back(Work {
                path: p.to_string(),
                local: local.get(p).copied(),
                remote: remote.get(p).cloned(),
            });
        }
        self.run(work, report)
    }

    /// Compares only these paths (from inotify or a remote delta). A folder expands to the files in it.
    pub fn sync_paths(&mut self, paths: &[String]) -> Report {
        let mut report = Report::default();
        let mut all = BTreeSet::new();
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
            if fs::symlink_metadata(&abs).is_ok_and(|m| m.is_dir()) {
                all.extend(walk(&abs).into_keys().map(|f| format!("{p}/{f}")));
            }
            // A folder that is gone locally: everything we knew under it.
            all.extend(self.db.under(p).unwrap_or_default());
            all.insert(p.clone());
        }
        let mut work = VecDeque::new();
        for p in all {
            match self.remote.stat(&p) {
                Ok(item) => work.push_back(Work {
                    local: local_sig(&self.root, &p),
                    remote: item.filter(|i| !i.is_dir),
                    path: p,
                }),
                Err(e) => report.errors.push(format!("{p}: {e}")),
            }
        }
        self.run(work, report)
    }

    /// Drops the paths whose local file is exactly what the DB recorded: the echo of our own downloads, which
    /// would otherwise cost a remote call each. Folders and missing files stay.
    pub fn filter_local(&self, paths: Vec<String>) -> Vec<String> {
        paths
            .into_iter()
            .filter(|p| match (local_sig(&self.root, p), self.db.base(p).ok().flatten()) {
                (Some(l), Some(b)) => !b.local.same_as(&l),
                _ => true,
            })
            .collect()
    }

    /// Paths that changed locally since the DB was written: how edits made while the app was closed are found.
    pub fn local_changes(&self) -> Vec<String> {
        let local = walk(&self.root);
        // A map, not a scan per file: this runs every couple of minutes on trees of 100k files.
        let known: HashMap<String, Base> = self.db.all().unwrap_or_default().into_iter().collect();
        let mut out: BTreeSet<String> = local
            .iter()
            .filter(|(p, s)| !known.get(*p).is_some_and(|b| b.local.same_as(s)))
            .map(|(p, _)| p.clone())
            .collect();
        out.extend(known.into_keys().filter(|p| !local.contains_key(p)));
        out.into_iter().collect()
    }

    fn run(&mut self, mut work: VecDeque<Work>, mut report: Report) -> Report {
        let mut plan = Vec::new();
        let mut deletes = 0;
        for w in work.drain(..) {
            let base = match self.db.base(&w.path) {
                Ok(b) => b,
                Err(e) => {
                    report.errors.push(format!("{}: {e}", w.path));
                    continue;
                }
            };
            let action = decide(w.local, w.remote.as_ref().map(sig_item), base);
            deletes += usize::from(matches!(action, Action::DeleteLocal | Action::DeleteRemote));
            if action != Action::None {
                plan.push((w, action, base));
            }
        }
        let known = self.db.all().map_or(0, |v| v.len());
        if !delete_guard_ok(deletes, known, self.guard_percent) {
            report.aborted = Some((deletes, known));
            return report;
        }
        let mut plan: VecDeque<_> = plan.into();
        while let Some((w, action, base)) = plan.pop_front() {
            match self.exec(&w, action, base) {
                Ok(extra) => {
                    report.transferred +=
                        usize::from(matches!(action, Action::Upload | Action::Download | Action::Conflict));
                    // A conflict leaves the renamed local copy to be uploaded as a new file.
                    plan.extend(extra.map(|e| (e, Action::Upload, None)));
                }
                Err(e) => report.errors.push(format!("{}: {e}", w.path)),
            }
        }
        report
    }

    fn exec(&mut self, w: &Work, action: Action, base: Option<Base>) -> Result<Option<Work>> {
        let db = |e: rusqlite::Error| e.to_string();
        let (path, rid) = (w.path.as_str(), w.remote.as_ref().map_or("", |r| r.id.as_str()));
        // Every read and write below goes through `root/path`: never through a symlinked folder.
        if action != Action::None && action != Action::Forget && under_symlink(&self.root, path) {
            return Err("inside a symlinked folder, skipped".into());
        }
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
            Action::Upload => {
                let l = w.local.ok_or("file vanished")?;
                self.remote.upload(&self.root, path)?;
                // What the remote reports now is the base, so its own echo in a delta compares equal.
                let r = self.remote.stat(path)?.ok_or("uploaded file not found")?;
                self.db
                    .set(
                        path,
                        &Base {
                            local: l,
                            remote: sig_item(&r),
                        },
                        &r.id,
                    )
                    .map_err(db)?;
            }
            Action::Download => {
                let r = w.remote.as_ref().ok_or("file vanished")?;
                self.download(path, w.local, r)?;
            }
            Action::DeleteRemote => {
                self.remote.delete(path)?;
                self.db.forget(path).map_err(db)?;
            }
            Action::DeleteLocal => {
                // Only with a base (decide guarantees it) and an unchanged local file: we synced it, so it may go.
                debug_assert!(base.is_some());
                self.trash(path).map_err(|e| e.to_string())?;
                self.db.forget(path).map_err(db)?;
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
                    path: aside,
                }));
            }
        }
        Ok(None)
    }

    fn download(&mut self, path: &str, expect_local: Option<Sig>, r: &Item) -> Result<()> {
        let tmp_dir = self.root.join(HIDDEN).join("tmp");
        fs::create_dir_all(&tmp_dir).map_err(|e| e.to_string())?;
        let tmp = tmp_dir.join(format!("{}-{}", std::process::id(), now_secs()));
        self.remote.download(path, &tmp)?;
        // The user edited it while it downloaded: keep their edit, the next run sorts it out.
        if local_sig(&self.root, path) != expect_local {
            let _ = fs::remove_file(&tmp);
            return Err("changed while downloading, will retry".into());
        }
        let dest = self.root.join(path);
        if let Some(parent) = dest.parent() {
            fs::create_dir_all(parent).map_err(|e| e.to_string())?;
        }
        fs::rename(&tmp, &dest).map_err(|e| e.to_string())?;
        let l = local_sig(&self.root, path).ok_or("downloaded file not found")?;
        self.db
            .set(
                path,
                &Base {
                    local: l,
                    remote: sig_item(r),
                },
                &r.id,
            )
            .map_err(|e| e.to_string())
    }

    /// Moves a file to `.fastsync/trash/<time>/<path>` (same filesystem, so a rename), then removes the empty
    /// folders it leaves behind.
    fn trash(&self, rel: &str) -> std::io::Result<()> {
        let dest = self
            .root
            .join(HIDDEN)
            .join("trash")
            .join(now_secs().to_string())
            .join(rel);
        fs::create_dir_all(dest.parent().unwrap_or(&self.root))?;
        fs::rename(self.root.join(rel), dest)?;
        let mut dir = self.root.join(rel);
        while dir.pop() && dir != self.root && fs::remove_dir(&dir).is_ok() {}
        Ok(())
    }
}

fn sig_item(i: &Item) -> Sig {
    Sig {
        size: i.size,
        mtime: i.mtime,
    }
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
            }
        }
    }

    impl Remote for DirRemote {
        fn list(&self) -> Result<Vec<Item>> {
            Ok(walk(&self.0)
                .into_iter()
                .map(|(p, s)| Item {
                    path: p,
                    size: s.size,
                    mtime: s.mtime,
                    is_dir: false,
                    id: String::new(),
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

    #[test]
    fn both_edited_keeps_both_copies() {
        let mut f = Fixture::new("conflict");
        write(f.local("r.txt"), "base", 1000);
        f.engine.full_scan();
        write(f.local("r.txt"), "mine", 20);
        write(f.remote("r.txt"), "theirs!", 10);
        let r = f.engine.full_scan();
        assert!(r.errors.is_empty(), "{r:?}");
        assert_eq!(fs::read_to_string(f.local("r.txt")).unwrap(), "theirs!");
        let name = conflict_name("r.txt", &today(), 1);
        assert_eq!(fs::read_to_string(f.local(&name)).unwrap(), "mine");
        assert_eq!(
            fs::read_to_string(f.remote(&name)).unwrap(),
            "mine",
            "the local copy is uploaded too"
        );
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
