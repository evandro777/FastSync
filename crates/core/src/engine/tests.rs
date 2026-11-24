use super::*;

/// A folder standing in for the cloud, so the whole engine runs without rclone or a network.
struct DirRemote(PathBuf);

impl DirRemote {
    /// Every file and folder under `base`, as paths below `prefix` (`""` = the cloud's root).
    fn all(&self, base: &Path, prefix: &str) -> Vec<Item> {
        let join = |p: String| if prefix.is_empty() { p } else { format!("{prefix}/{p}") };
        let (files, dirs, _) = scan(base, &[], &Rules::default());
        let dirs = dirs.into_iter().map(|p| Item {
            path: join(p),
            size: 0,
            mtime: 0,
            is_dir: true,
            id: String::new(),
            hash: None,
        });
        let files = files.into_iter().map(|(p, s)| Item {
            path: join(p),
            size: s.size,
            mtime: s.mtime,
            is_dir: false,
            id: String::new(),
            hash: None,
        });
        dirs.chain(files).collect()
    }

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
        Ok(self.all(&self.0, ""))
    }
    fn make_dir(&self, rel: &str) -> Result<()> {
        fs::create_dir_all(self.0.join(rel)).map_err(|e| e.to_string())
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
    fn list_under(&self, dir: &str) -> Result<Vec<Item>> {
        if !self.0.join(dir).is_dir() {
            return Err("directory not found".into());
        }
        Ok(self.all(&self.0.join(dir), dir))
    }
    fn move_dir(&self, from: &str, to: &str) -> Result<()> {
        fs::create_dir_all(self.0.join(to).parent().unwrap()).map_err(|e| e.to_string())?;
        fs::rename(self.0.join(from), self.0.join(to)).map_err(|e| e.to_string())
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
    fn list_under(&self, dir: &str) -> Result<Vec<Item>> {
        self.inner.list_under(dir)
    }
    fn upload(&self, root: &Path, rel: &str) -> Result<()> {
        self.inner.upload(root, rel)
    }
    fn make_dir(&self, rel: &str) -> Result<()> {
        self.inner.make_dir(rel)
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
    fn make_dir(&self, rel: &str) -> Result<()> {
        self.0.make_dir(rel)
    }
    fn download(&self, rel: &str, dest: &Path) -> Result<()> {
        self.0.download(rel, dest)
    }
    fn delete(&self, rel: &str) -> Result<()> {
        self.0.delete(rel)
    }
}

/// A cloud whose empty files `links` says are symlinks (as Dropbox's metadata does); `fail`: the lookup errors.
struct Links {
    inner: DirRemote,
    links: Vec<(&'static str, &'static str)>,
    fail: bool,
}
impl Remote for Links {
    fn list(&self) -> Result<Vec<Item>> {
        self.inner.list()
    }
    fn stat(&self, rel: &str) -> Result<Option<Item>> {
        self.inner.stat(rel)
    }
    fn upload(&self, root: &Path, rel: &str) -> Result<()> {
        self.inner.upload(root, rel)
    }
    fn make_dir(&self, rel: &str) -> Result<()> {
        self.inner.make_dir(rel)
    }
    fn download(&self, rel: &str, dest: &Path) -> Result<()> {
        self.inner.download(rel, dest)
    }
    fn delete(&self, rel: &str) -> Result<()> {
        self.inner.delete(rel)
    }
    fn link_target(&self, rel: &str) -> Result<Option<String>> {
        if self.fail {
            return Err("dropbox metadata: HTTP 429".into());
        }
        Ok(self.links.iter().find(|(p, _)| *p == rel).map(|(_, t)| t.to_string()))
    }
}

fn with_links(f: &Fixture, links: Vec<(&'static str, &'static str)>, fail: bool) -> Engine<Links> {
    Engine::new(
        f.engine.root.clone(),
        Links {
            inner: DirRemote(f.remote("")),
            links,
            fail,
        },
        Db::memory().unwrap(),
    )
}

#[test]
fn a_cloud_symlink_becomes_a_link_here_and_stays_one() {
    let f = Fixture::new("link-made");
    write(f.remote("bin/vite"), "", 100);
    let mut e = with_links(&f, vec![("bin/vite", "../vite/bin/vite.js")], false);
    let r = e.full_scan();
    assert!(r.errors.is_empty() && r.transferred == 1, "{r:?}");
    let link = f.local("bin/vite");
    assert!(fs::symlink_metadata(&link).unwrap().file_type().is_symlink());
    assert_eq!(fs::read_link(&link).unwrap(), Path::new("../vite/bin/vite.js"));
    // No row (it could not describe a link), and the next scan leaves everything alone: nothing deleted, nothing
    // downloaded over the link.
    assert!(e.db.base("bin/vite").unwrap().is_none());
    let r = e.full_scan();
    assert!(
        r.errors.is_empty() && r.transferred == 0 && r.aborted.is_none(),
        "{r:?}"
    );
    assert!(fs::symlink_metadata(&link).unwrap().file_type().is_symlink());
    assert!(f.remote("bin/vite").exists(), "the cloud entry stays");
}

#[test]
fn a_cloud_symlink_that_leaves_the_folder_is_not_made_and_is_not_an_error() {
    let f = Fixture::new("link-unsafe");
    for p in ["abs", "up", "deep/up"] {
        write(f.remote(p), "", 100);
    }
    let mut e = with_links(
        &f,
        vec![("abs", "/home/other/Dropbox/x"), ("up", "../x"), ("deep/up", "../../x")],
        false,
    );
    let r = e.full_scan();
    assert!(r.errors.is_empty() && r.transferred == 0, "{r:?}");
    for p in ["abs", "up", "deep/up"] {
        assert!(fs::symlink_metadata(f.local(p)).is_err(), "{p} must not exist");
    }
}

#[test]
fn a_failed_link_lookup_is_retried_and_never_leaves_an_empty_file() {
    let f = Fixture::new("link-retry");
    write(f.remote("bin/vite"), "", 100);
    let mut e = with_links(&f, vec![("bin/vite", "x")], true);
    let r = e.full_scan();
    assert_eq!(r.errors.len(), 1, "{r:?}");
    assert!(fs::symlink_metadata(f.local("bin/vite")).is_err());
}

#[test]
fn an_empty_cloud_file_that_is_no_link_is_still_downloaded() {
    let f = Fixture::new("link-none");
    write(f.remote("pkg/__init__.py"), "", 100);
    let mut e = with_links(&f, vec![], false);
    let r = e.full_scan();
    assert!(r.errors.is_empty() && r.transferred == 1, "{r:?}");
    let m = fs::symlink_metadata(f.local("pkg/__init__.py")).unwrap();
    assert!(m.is_file() && m.len() == 0);
}

#[test]
fn link_targets_must_stay_inside_the_folder() {
    for (rel, target, ok) in [
        ("a", "b", true),
        ("a", "./b/../c", true),
        ("d/a", "../b", true),
        ("d/a", "../../b", false),
        ("a", "../b", false),
        ("d/e/a", "../../x/../y", true),
        ("a", "/etc/passwd", false),
        ("a", "", false),
        ("a", "b\0c", false),
    ] {
        assert_eq!(safe_link_target(rel, target), ok, "{rel} -> {target}");
    }
    assert!(!safe_link_target("a", &"b/".repeat(2100)));
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
fn a_gone_folder_answers_for_everything_below_it_with_one_call() {
    let f = Fixture::new("lookup-gone-tree");
    let remote = Calls::new(DirRemote(f.engine.remote.0.clone()), None);
    let db = Db::memory().unwrap();
    // The folder, two subfolders and two files in the deepest one (two files make that a listing job): the cloud has
    // none of it. Only the stat of `d` is needed.
    let paths: Vec<String> = ["d", "d/x", "d/x/y", "d/x/y/a.txt", "d/x/y/b.txt"]
        .map(String::from)
        .into();
    let got = lookup(&remote, &db, &paths, &std::sync::atomic::AtomicBool::new(false), false);
    assert_eq!(got.len(), paths.len());
    assert!(got.iter().all(|(_, r)| matches!(r, Ok(None))));
    assert_eq!(remote.stats.load(Relaxed), 1);
    assert_eq!(remote.lists.load(Relaxed), 0);
}

#[test]
fn a_folder_that_exists_does_not_hide_what_is_below_it() {
    let f = Fixture::new("lookup-kept-tree");
    write(f.remote("d/x/a.txt"), "one", 100);
    write(f.remote("d/x/b.txt"), "two", 100);
    let remote = Calls::new(DirRemote(f.engine.remote.0.clone()), None);
    let db = Db::memory().unwrap();
    let paths: Vec<String> = ["d", "d/x/a.txt", "d/x/b.txt", "d/x/c.txt"].map(String::from).into();
    let got = lookup(&remote, &db, &paths, &std::sync::atomic::AtomicBool::new(false), false);
    let found: Vec<bool> = got.iter().map(|(_, r)| matches!(r, Ok(Some(_)))).collect();
    assert_eq!(found, [true, true, true, false]);
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

/// A cloud whose recursive folder listing (`list_under`) says "not found" for a missing folder, as rclone does, or
/// fails with `err` whatever the folder.
struct Under(DirRemote, Option<&'static str>);
impl Remote for Under {
    fn list(&self) -> Result<Vec<Item>> {
        self.0.list()
    }
    fn list_under(&self, dir: &str) -> Result<Vec<Item>> {
        if let Some(e) = self.1 {
            return Err(e.into());
        }
        if !self.0.0.join(dir).is_dir() {
            return Err("error in ListJSON: directory not found".into());
        }
        self.0.list_under(dir)
    }
    fn stat(&self, rel: &str) -> Result<Option<Item>> {
        self.0.stat(rel)
    }
    fn upload(&self, root: &Path, rel: &str) -> Result<()> {
        self.0.upload(root, rel)
    }
    fn make_dir(&self, rel: &str) -> Result<()> {
        self.0.make_dir(rel)
    }
    fn download(&self, rel: &str, dest: &Path) -> Result<()> {
        self.0.download(rel, dest)
    }
    fn delete(&self, rel: &str) -> Result<()> {
        self.0.delete(rel)
    }
}

fn under_engine(name: &str, err: Option<&'static str>) -> (Fixture, Engine<Under>) {
    let f = Fixture::new(name);
    let e = Engine::new(
        f.engine.root.clone(),
        Under(DirRemote(f.engine.remote.0.clone()), err),
        Db::memory().unwrap(),
    );
    (f, e)
}

#[test]
fn a_reported_folder_already_gone_from_the_cloud_is_no_problem() {
    let (_f, mut e) = under_engine("feed-gone", None);
    let r = e.sync_paths_in(&["tmp".into()], &["tmp".into()]);
    assert!(r.errors.is_empty() && r.failed_dirs.is_empty(), "{r:?}");
}

#[test]
fn a_reported_folder_that_fails_to_list_is_retried_alone_not_by_a_full_scan() {
    let (f, mut e) = under_engine("feed-boom", Some("network down"));
    write(f.remote("d/x.txt"), "x", 100);
    let r = e.sync_paths_in(&["d".into()], &["d".into()]);
    assert_eq!(r.failed_dirs, ["d"], "{r:?}");
    assert!(!r.has_global_failure() && r.errors.len() == 1, "{r:?}");
    assert!(!f.local("d/x.txt").exists(), "nothing was read as an empty cloud");
}

#[test]
fn not_found_while_the_reported_folder_exists_is_a_failure_not_a_delete() {
    // The listing says "not found" but `stat` finds the folder: a glitch, never "gone".
    let (f, mut e) = under_engine("feed-glitch", Some("error in ListJSON: directory not found"));
    write(f.remote("d/x.txt"), "x", 100);
    let r = e.sync_paths_in(&["d".into()], &["d".into()]);
    assert_eq!(r.failed_dirs, ["d"], "{r:?}");
    assert!(f.remote("d/x.txt").exists());
}

#[test]
fn a_retry_of_a_folder_alone_downloads_what_is_in_it() {
    // No paths, only the folder: what the runner hands over when a failed folder listing is retried.
    let (f, mut e) = under_engine("feed-dir-only", None);
    write(f.remote("d/x.txt"), "x", 100);
    let r = e.sync_paths_in(&[], &["d".into()]);
    assert!(r.errors.is_empty() && r.transferred == 1, "{r:?}");
    assert_eq!(fs::read_to_string(f.local("d/x.txt")).unwrap(), "x");
}

#[test]
fn the_root_is_never_gone() {
    let (_f, e) = under_engine("feed-root", None);
    assert!(!folder_gone(&e.remote, "", "directory not found"));
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
    fn make_dir(&self, rel: &str) -> Result<()> {
        self.0.make_dir(rel)
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
    fn make_dir(&self, rel: &str) -> Result<()> {
        self.0.make_dir(rel)
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
    fn make_dir(&self, rel: &str) -> Result<()> {
        self.0.make_dir(rel)
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
fn a_run_cancelled_before_it_acts_does_nothing_and_says_so() {
    let mut f = Fixture::new("cancel-early");
    write(f.local("up.txt"), "u", 100);
    write(f.remote("down.txt"), "d", 100);
    f.engine.cancel.store(true, Relaxed);
    let r = f.engine.full_scan();
    assert!(r.cancelled && r.transferred == 0 && r.errors.is_empty(), "{r:?}");
    assert!(!f.remote("up.txt").exists() && !f.local("down.txt").exists());
    f.engine.cancel.store(false, Relaxed);
    let r = f.engine.full_scan();
    assert!(!r.cancelled && f.remote("up.txt").exists() && f.local("down.txt").exists());
}

#[test]
fn a_cancel_during_a_run_lets_the_action_under_way_finish_and_starts_no_transfer() {
    let f = Fixture::new("cancel-mid");
    let mut e = racy(&f);
    write(f.local("c.txt"), "one", 100);
    e.full_scan();
    fs::remove_file(f.local("c.txt")).unwrap();
    write(f.local("n.txt"), "mine", 50);
    // The cloud delete of c runs first; the user presses Pause right then, before the upload of n starts.
    let cancel = e.cancel.clone();
    *e.remote.1.lock().unwrap() = Some(Box::new(move || cancel.store(true, Relaxed)));
    let r = e.full_scan();
    assert!(r.cancelled && r.errors.is_empty(), "{r:?}");
    assert!(!f.remote("c.txt").exists(), "the delete under way finished");
    assert!(!f.remote("n.txt").exists(), "no new transfer started");
}

#[test]
fn a_pause_during_a_big_copy_in_skips_the_cloud_lookups_and_acts_on_nothing() {
    let (f, mut e, paths) = many_new_files("cancel-lookup", None);
    e.cancel.store(true, Relaxed);
    let r = e.sync_paths(&paths);
    assert!(r.cancelled && r.transferred == 0 && r.errors.is_empty(), "{r:?}");
    assert_eq!(
        e.remote.lists.load(Relaxed) + e.remote.stats.load(Relaxed),
        0,
        "no cloud call at all"
    );
    assert!(!f.remote("a/f0.txt").exists());
}

#[test]
fn a_symlink_holding_a_name_is_skipped_not_an_error() {
    let mut f = Fixture::new("no-replace");
    // A symlink is not a regular file, so `scan` sees "nothing here"; the name is still taken. Planning a
    // download over it failed on every retry, which also kept the run from ever counting as clean.
    std::os::unix::fs::symlink("/nowhere", f.local("s.txt")).unwrap();
    write(f.remote("s.txt"), "cloud", 100);
    let r = f.engine.full_scan();
    assert!(r.errors.is_empty() && r.transferred == 0, "{r:?}");
    assert!(fs::symlink_metadata(f.local("s.txt")).unwrap().file_type().is_symlink());
    assert!(f.remote("s.txt").exists(), "the cloud copy stays");
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
fn a_touched_file_with_the_same_bytes_is_not_uploaded_again_but_an_edit_of_the_same_size_is() {
    let mut f = Fixture::new("touched");
    write(f.local("a.txt"), "same bytes", 5000);
    write(f.local("b.txt"), "old text!!", 5000);
    assert!(f.engine.full_scan().errors.is_empty());
    // Only the stamp changes (a `touch`, another instance writing the same bytes): nothing to send.
    write(f.local("a.txt"), "same bytes", 9000);
    // Same size, other bytes: a real edit still goes up.
    write(f.local("b.txt"), "new text!!", 9000);
    let r = f.engine.full_scan();
    assert!(r.errors.is_empty(), "{r:?}");
    assert_eq!(r.transferred, 1, "only b.txt");
    assert_eq!(fs::read_to_string(f.remote("b.txt")).unwrap(), "new text!!");
    assert_eq!(
        f.engine.full_scan().transferred,
        0,
        "a.txt's row was updated, so the next scan is quiet"
    );
}

/// A cloud whose byte check always fails, as when rclone cannot read the local file.
struct CannotCompare(DirRemote);

impl Remote for CannotCompare {
    fn list(&self) -> Result<Vec<Item>> {
        self.0.list()
    }
    fn stat(&self, rel: &str) -> Result<Option<Item>> {
        self.0.stat(rel)
    }
    fn upload(&self, root: &Path, rel: &str) -> Result<()> {
        self.0.upload(root, rel)
    }
    fn make_dir(&self, rel: &str) -> Result<()> {
        self.0.make_dir(rel)
    }
    fn download(&self, rel: &str, dest: &Path) -> Result<()> {
        self.0.download(rel, dest)
    }
    fn delete(&self, rel: &str) -> Result<()> {
        self.0.delete(rel)
    }
    fn compare(&self, _: &Path, _: &str, _: &Item) -> Result<bool> {
        Err("permission denied".into())
    }
}

#[test]
fn a_failed_byte_check_skips_the_file_with_an_error_instead_of_splitting_it() {
    let f = Fixture::new("cannot-compare");
    let remote = CannotCompare(DirRemote(f.engine.remote.0.clone()));
    let mut e = Engine::new(f.engine.root.clone(), remote, Db::memory().unwrap());
    write(f.local("a.pdf"), "same bytes", 5000);
    write(f.remote("a.pdf"), "same bytes", 100);
    write(f.remote("new.txt"), "unrelated", 100);
    let r = e.full_scan();
    assert_eq!(r.errors.len(), 1, "{r:?}");
    assert!(
        r.errors[0].contains("a.pdf") && r.errors[0].contains("could not compare"),
        "{r:?}"
    );
    let names: Vec<String> = fs::read_dir(&f.engine.root)
        .unwrap()
        .flatten()
        .map(|d| d.file_name().to_string_lossy().into_owned())
        .collect();
    assert!(!names.iter().any(|n| n.contains("conflict")), "{names:?}");
    assert_eq!(fs::read_to_string(f.local("a.pdf")).unwrap(), "same bytes");
    assert!(e.db.base("a.pdf").unwrap().is_none(), "not recorded as synced either");
    assert!(f.local("new.txt").exists(), "the rest of the run goes on");
}

#[test]
fn a_failed_byte_check_of_a_plain_upload_still_uploads_without_an_error() {
    let f = Fixture::new("upload-cannot-compare");
    let remote = CannotCompare(DirRemote(f.engine.remote.0.clone()));
    let mut e = Engine::new(f.engine.root.clone(), remote, Db::memory().unwrap());
    write(f.local("a.txt"), "old text!!", 5000);
    assert!(e.full_scan().errors.is_empty());
    write(f.local("a.txt"), "new text!!", 9000);
    let r = e.full_scan();
    assert!(r.errors.is_empty(), "{r:?}");
    assert_eq!(r.transferred, 1, "{r:?}");
    assert_eq!(fs::read_to_string(f.remote("a.txt")).unwrap(), "new text!!");
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
    assert!(
        f.local("gone").is_dir(),
        "the emptied folder stays, as in the official clients"
    );
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
fn a_rename_the_watcher_saw_is_a_move_even_when_the_name_changed() {
    // A service with no hash (`blind`): a new name proves nothing without the watcher's word.
    let (f, mut e) = counting_fixture("move-hinted", true);
    for i in 0..3 {
        write(f.local(&format!("d/f{i}.txt")), &"x".repeat(i + 1), 100);
    }
    assert!(e.full_scan().errors.is_empty());
    fs::rename(f.local("d/f1.txt"), f.local("d/renamed.txt")).unwrap();
    e.local_moves = vec![("d/f1.txt".into(), "d/renamed.txt".into())];
    let r = e.sync_paths(&["d/f1.txt".into(), "d/renamed.txt".into()]);
    assert!(r.errors.is_empty(), "{r:?}");
    assert_eq!(Counting::n(&e.remote.renames), 1, "moved in the cloud");
    assert_eq!(Counting::n(&e.remote.uploads), 3, "only the first sync uploaded");
    assert!(f.remote("d/renamed.txt").exists() && !f.remote("d/f1.txt").exists());
}

#[test]
fn without_the_hint_a_renamed_file_with_a_new_name_and_no_hash_is_uploaded() {
    let (f, mut e) = counting_fixture("move-unhinted", true);
    for i in 0..3 {
        write(f.local(&format!("d/f{i}.txt")), &"x".repeat(i + 1), 100);
    }
    assert!(e.full_scan().errors.is_empty());
    fs::rename(f.local("d/f1.txt"), f.local("d/renamed.txt")).unwrap();
    let r = e.sync_paths(&["d/f1.txt".into(), "d/renamed.txt".into()]);
    assert!(r.errors.is_empty(), "{r:?}");
    assert_eq!(Counting::n(&e.remote.renames), 0);
    assert_eq!(Counting::n(&e.remote.uploads), 4);
}

#[test]
fn a_hinted_rename_of_a_file_edited_since_is_still_uploaded() {
    let (f, mut e) = counting_fixture("move-hinted-edited", true);
    for i in 0..3 {
        write(f.local(&format!("d/f{i}.txt")), &"x".repeat(i + 1), 100);
    }
    assert!(e.full_scan().errors.is_empty());
    fs::rename(f.local("d/f1.txt"), f.local("d/renamed.txt")).unwrap();
    // Same size, other bytes and time: not the file that was synced.
    write(f.local("d/renamed.txt"), "yy", 5);
    e.local_moves = vec![("d/f1.txt".into(), "d/renamed.txt".into())];
    let r = e.sync_paths(&["d/f1.txt".into(), "d/renamed.txt".into()]);
    assert!(r.errors.is_empty(), "{r:?}");
    assert_eq!(Counting::n(&e.remote.renames), 0);
    assert_eq!(fs::read_to_string(f.remote("d/renamed.txt")).unwrap(), "yy");
}

#[test]
fn a_path_under_a_folder_listed_whole_costs_no_stat_when_the_listing_lacks_it() {
    let f = Fixture::new("covered-path");
    write(f.remote("d/a.txt"), "a", 100);
    let mut e = Engine::new(
        f.engine.root.clone(),
        Calls::new(DirRemote(f.engine.remote.0.clone()), None),
        Db::memory().unwrap(),
    );
    let r = e.sync_paths_in(&["d/ghost.txt".into()], &["d".into()]);
    assert!(r.errors.is_empty(), "{r:?}");
    assert_eq!(e.remote.stats.load(Relaxed), 0, "the listing of `d` answered");
}

#[test]
fn many_renamed_folders_do_not_ask_for_a_full_scan_and_a_move_from_nowhere_changes_nothing() {
    let mut f = Fixture::new("moves-many");
    for i in 0..31 {
        write(f.local(&format!("top{i}/a.txt")), "x", 100);
    }
    assert!(f.engine.full_scan().errors.is_empty());
    let (mut paths, mut moves) = (Vec::new(), vec![("never-synced".to_string(), "top0x".to_string())]);
    for i in 0..31 {
        fs::rename(f.local(&format!("top{i}")), f.local(&format!("new{i}"))).unwrap();
        moves.push((format!("top{i}"), format!("new{i}")));
        paths.extend([format!("top{i}/a.txt"), format!("new{i}/a.txt")]);
    }
    f.engine.local_moves = moves;
    let r = f.engine.sync_paths(&paths);
    assert!(!r.wants_full && r.errors.is_empty(), "{r:?}");
    assert!(f.remote("new30/a.txt").exists() && !f.remote("top30/a.txt").exists());
}

#[test]
fn a_folder_the_watcher_saw_renamed_is_moved_whole_by_a_run_of_paths() {
    let (f, mut e) = counting_fixture("moved-dir-paths", true);
    write(f.local("a/one.txt"), "first", 100);
    write(f.local("a/deep/two.txt"), "second one", 100);
    assert!(e.full_scan().errors.is_empty());
    fs::rename(f.local("a"), f.local("b")).unwrap();
    e.local_moves = vec![("a".into(), "b".into())];
    let r = e.sync_paths(&["a".into(), "b".into()]);
    assert!(r.errors.is_empty(), "{r:?}");
    assert_eq!(Counting::n(&e.remote.dir_moves), 1);
    assert_eq!(Counting::n(&e.remote.uploads), 2, "only the first sync uploaded");
    assert!(f.remote("b/deep/two.txt").exists() && !f.remote("a").exists());
}

#[test]
fn the_feeds_echo_of_what_we_synced_is_dropped_and_anything_else_is_kept() {
    use crate::delta::Change;
    let mut f = Fixture::new("feed-echo");
    synced_folder(&mut f, 3);
    let base = f.engine.db.base("d/f1.txt").unwrap().unwrap();
    let file = |path: &str, size: u64, mtime: i64| Change {
        path: Some(path.into()),
        size: Some(size),
        mtime: Some(mtime),
        ..Default::default()
    };
    let gone = |path: &str| Change {
        path: Some(path.into()),
        removed: true,
        ..Default::default()
    };
    let e = &f.engine;
    // Our own file coming back, exactly as recorded.
    assert!(e.feed_echo(&file("d/f1.txt", base.remote.size, base.remote.mtime)));
    // Another size or time, or a file we know nothing of: something changed in the cloud.
    assert!(!e.feed_echo(&file("d/f1.txt", base.remote.size + 1, base.remote.mtime)));
    assert!(!e.feed_echo(&file("d/f1.txt", base.remote.size, base.remote.mtime + 1000)));
    assert!(!e.feed_echo(&file("d/new.txt", 1, 1000)));
    // The same entry while the local file was edited since: the edit must be seen.
    write(f.local("d/f1.txt"), "edited!", 1);
    assert!(
        !f.engine
            .feed_echo(&file("d/f1.txt", base.remote.size, base.remote.mtime))
    );
    let e = &f.engine;
    // A delete of a name nothing knows (the old name of a folder we moved) is an echo; one we have rows for, in any
    // case, or with a file on disk, is not.
    assert!(e.feed_echo(&gone("old/name.txt")));
    assert!(!e.feed_echo(&gone("d/f2.txt")));
    assert!(!e.feed_echo(&gone("D/F2.TXT")));
    assert!(!e.feed_echo(&gone("d")));
    write(f.local("local-only.txt"), "mine", 5);
    assert!(!f.engine.feed_echo(&gone("local-only.txt")));
}

#[test]
fn an_id_feeds_echo_needs_the_same_file_in_the_same_place_with_the_same_hash() {
    use crate::delta::Change;
    use std::collections::HashMap;
    let mut f = Fixture::new("feed-echo-id");
    synced_folder(&mut f, 3);
    let base = f.engine.db.base("d/f1.txt").unwrap().unwrap();
    let hash = ("md5".to_string(), "abc".to_string());
    f.engine.db.set_hashed("d/f1.txt", &base, "ID1", Some(&hash)).unwrap();
    f.engine
        .db
        .set("d/f2.txt", &f.engine.db.base("d/f2.txt").unwrap().unwrap(), "ID2")
        .unwrap();
    let dirs: HashMap<String, String> = [("DIR".to_string(), "d".to_string())].into();
    let echo = Change {
        id: "ID1".into(),
        name: "f1.txt".into(),
        parent: "DIR".into(),
        size: Some(base.remote.size),
        mtime: Some(base.remote.mtime),
        hashes: vec![("sha1".into(), "zzz".into()), hash.clone()],
        ..Default::default()
    };
    let is = |e: &Engine<DirRemote>, c: &Change| e.feed_echo_id(c, &dirs, Some("ROOT"));
    assert!(is(&f.engine, &echo));
    // No time in the feed: the hash and the size still prove it.
    assert!(is(
        &f.engine,
        &Change {
            mtime: None,
            ..echo.clone()
        }
    ));
    // Anything that differs is a real change, or cannot be told: kept.
    let kept = |c: Change| assert!(!is(&f.engine, &c), "{c:?}");
    kept(Change {
        size: Some(base.remote.size + 1),
        ..echo.clone()
    });
    kept(Change {
        mtime: Some(base.remote.mtime + 5000),
        ..echo.clone()
    });
    kept(Change {
        hashes: vec![("md5".into(), "other".into())],
        ..echo.clone()
    });
    kept(Change {
        hashes: vec![("sha1".into(), "abc".into())],
        ..echo.clone()
    });
    kept(Change {
        hashes: vec![],
        ..echo.clone()
    });
    kept(Change {
        name: "renamed.txt".into(),
        ..echo.clone()
    });
    kept(Change {
        parent: "ELSEWHERE".into(),
        ..echo.clone()
    });
    kept(Change {
        removed: true,
        ..echo.clone()
    });
    kept(Change {
        is_dir: true,
        ..echo.clone()
    });
    kept(Change {
        id: "unknown".into(),
        ..echo.clone()
    });
    // A row with no hash is never an echo (its own file, same everything else).
    kept(Change {
        id: "ID2".into(),
        name: "f2.txt".into(),
        ..echo.clone()
    });
    // A file in the root is told by the root id.
    write(f.local("top.txt"), "x", 1);
    let s = local_sig(&f.engine.root, "top.txt").unwrap();
    f.engine
        .db
        .set_hashed("top.txt", &Base { local: s, remote: s }, "ID3", Some(&hash))
        .unwrap();
    let top = Change {
        id: "ID3".into(),
        name: "top.txt".into(),
        parent: "ROOT".into(),
        size: Some(s.size),
        mtime: Some(s.mtime),
        hashes: vec![hash.clone()],
        ..Default::default()
    };
    assert!(is(&f.engine, &top));
    // The same entry reported under a folder that is not the row's: a move.
    assert!(!is(
        &f.engine,
        &Change {
            parent: "DIR".into(),
            ..top
        }
    ));
    // The local file edited since: the edit must be seen.
    write(f.local("d/f1.txt"), "edited!", 1);
    assert!(!is(&f.engine, &echo));
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
    fn make_dir(&self, rel: &str) -> Result<()> {
        self.0.make_dir(rel)
    }
    fn download(&self, _: &str, _: &Path) -> Result<()> {
        Err("downloaded".into())
    }
    fn delete(&self, rel: &str) -> Result<()> {
        self.0.delete(rel)
    }
    fn list_under(&self, dir: &str) -> Result<Vec<Item>> {
        self.0.list_under(dir)
    }
    fn same_content(&self, root: &Path, rel: &str, remote: &Item) -> bool {
        self.0.same_content(root, rel, remote)
    }
}

#[test]
fn a_folder_renamed_in_the_cloud_is_renamed_here_through_the_feed_path_too() {
    // What Dropbox's feed says for it: the old folder deleted, the new one created. Fewer than `SPLIT_MOVE_MIN`
    // deletions, so no full scan pairs them: the paths run must, or the folder is trashed and downloaded again.
    use std::os::unix::fs::MetadataExt;
    let f = Fixture::new("feed-rename");
    let mut e = Engine::new(
        f.engine.root.clone(),
        NoDownload(DirRemote(f.engine.remote.0.clone())),
        Db::memory().unwrap(),
    );
    for i in 0..10 {
        write(f.local(&format!("a/deep/f{i}.txt")), &format!("content {i}"), 100);
    }
    assert!(e.full_scan().errors.is_empty());
    let ino = fs::metadata(f.local("a/deep/f3.txt")).unwrap().ino();
    fs::rename(f.remote("a"), f.remote("b")).unwrap();
    let r = e.sync_paths_in(&["a".to_string(), "b".to_string()], &["b".to_string()]);
    assert!(r.errors.is_empty() && r.aborted.is_none(), "{r:?}");
    // The same files, renamed: a download would have failed (this cloud refuses them) and left nothing.
    assert!(!f.local("a").exists());
    assert_eq!(fs::metadata(f.local("b/deep/f3.txt")).unwrap().ino(), ino);
    assert_eq!(fs::read_to_string(f.local("b/deep/f9.txt")).unwrap(), "content 9");
    assert!(
        !f.local(".fastsync/trash").exists() || fs::read_dir(f.local(".fastsync/trash")).unwrap().next().is_none(),
        "nothing was trashed"
    );
    assert_eq!(e.full_scan().transferred, 0);
}

#[test]
fn sibling_folders_are_listed_side_by_side_and_too_many_ask_for_a_full_scan() {
    let f = Fixture::new("feed-many");
    let mut e = Engine::new(
        f.engine.root.clone(),
        OnlyFolders {
            inner: DirRemote(f.engine.remote.0.clone()),
            folders_fail: false,
            listings: Default::default(),
        },
        Db::memory().unwrap(),
    );
    let names = |n: usize| -> Vec<String> { (0..n).map(|i| format!("unzipped/dir{i}")).collect() };
    for d in names(FEED_DIRS_MAX + 1) {
        write(f.remote(&format!("{d}/file.txt")), &d, 100);
    }
    // A handful: each listed once, all their files come down.
    let few = names(6);
    let r = e.sync_paths_in(&few, &few);
    assert!(r.errors.is_empty() && !r.wants_full, "{r:?}");
    assert_eq!(e.remote.listings.load(Relaxed), 6);
    for d in &few {
        assert_eq!(fs::read_to_string(f.local(&format!("{d}/file.txt"))).unwrap(), *d);
    }
    // More than the limit under one parent: that parent is listed once, no full scan.
    let many = names(FEED_DIRS_MAX + 1);
    let before = e.remote.listings.load(Relaxed);
    let r = e.sync_paths_in(&many, &many);
    assert!(!r.wants_full && r.errors.is_empty(), "{r:?}");
    assert_eq!(e.remote.listings.load(Relaxed), before + 1);
    assert_eq!(
        fs::read_to_string(f.local(&format!("{}/file.txt", many[FEED_DIRS_MAX]))).unwrap(),
        many[FEED_DIRS_MAX]
    );
    // More than the limit at the top level: nothing is listed or touched, the run asks for a full scan.
    let top: Vec<String> = (0..=FEED_DIRS_MAX).map(|i| format!("top{i}")).collect();
    for d in &top {
        write(f.remote(&format!("{d}/file.txt")), d, 100);
    }
    let before = e.remote.listings.load(Relaxed);
    let r = e.sync_paths_in(&top, &top);
    assert!(r.wants_full && r.errors.is_empty() && r.transferred == 0, "{r:?}");
    assert_eq!(e.remote.listings.load(Relaxed), before);
    assert!(!f.local("top0/file.txt").exists());
    // Exactly the limit still goes by listing; subfolders of a reported folder do not count.
    let at_limit = names(FEED_DIRS_MAX);
    let mut with_subs = at_limit.clone();
    with_subs.extend(at_limit.iter().map(|d| format!("{d}/sub")));
    assert!(!e.sync_paths_in(&with_subs, &with_subs).wants_full);
}

#[test]
fn nested_reported_folders_are_listed_once_from_the_top() {
    let f = Fixture::new("feed-nested");
    write(f.remote("new/a.txt"), "first", 100);
    write(f.remote("new/sub/b.txt"), "second", 100);
    write(f.remote("a b/c.txt"), "a name between a folder and its child", 100);
    let mut e = Engine::new(
        f.engine.root.clone(),
        OnlyFolders {
            inner: DirRemote(f.engine.remote.0.clone()),
            folders_fail: false,
            listings: Default::default(),
        },
        Db::memory().unwrap(),
    );
    let dirs: Vec<String> = ["new/sub", "new", "a b", "new/sub/deeper"].map(String::from).into();
    let r = e.sync_paths_in(&dirs, &dirs);
    assert!(r.errors.is_empty(), "{r:?}");
    assert_eq!(
        e.remote.listings.load(Relaxed),
        2,
        "`new` and `a b`, not their subfolders"
    );
    assert_eq!(fs::read_to_string(f.local("new/sub/b.txt")).unwrap(), "second");
    assert_eq!(fs::read_to_string(f.local("new/a.txt")).unwrap(), "first");
    assert!(f.local("a b/c.txt").exists());
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
fn a_cloud_edit_keeps_the_local_files_mode() {
    use std::os::unix::fs::PermissionsExt;
    let mut f = Fixture::new("keep-mode");
    write(f.local("s.sh"), "one", 100);
    f.engine.full_scan();
    fs::set_permissions(f.local("s.sh"), fs::Permissions::from_mode(0o755)).unwrap();
    write(f.remote("s.sh"), "two!!", 200);
    let r = f.engine.full_scan();
    assert!(r.errors.is_empty(), "{r:?}");
    assert_eq!(fs::read_to_string(f.local("s.sh")).unwrap(), "two!!");
    assert_eq!(
        fs::metadata(f.local("s.sh")).unwrap().permissions().mode() & 0o777,
        0o755
    );
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

/// A cloud that reports a content hash and can copy server-side, counting both uploads and copies.
/// `bad_copy`: the copy lands with other bytes (the source changed between the check and the copy).
struct Hashing {
    inner: DirRemote,
    uploads: std::sync::atomic::AtomicUsize,
    copies: std::sync::atomic::AtomicUsize,
    bad_copy: bool,
    dir_copies: std::sync::atomic::AtomicUsize,
    /// A folder copy that stops after this many files and then fails.
    dir_copy_limit: Option<usize>,
    /// Answers `list_dir` (with hashes) and counts it; off, it errors like a remote with no such call.
    listing: bool,
    lists: std::sync::atomic::AtomicUsize,
}

impl Hashing {
    fn new(dir: &Path, bad_copy: bool) -> Self {
        Self {
            inner: DirRemote(dir.to_path_buf()),
            uploads: Default::default(),
            copies: Default::default(),
            bad_copy,
            dir_copies: Default::default(),
            dir_copy_limit: None,
            listing: false,
            lists: Default::default(),
        }
    }
    fn sum(p: &Path) -> Option<String> {
        use std::hash::{Hash, Hasher};
        let mut h = std::collections::hash_map::DefaultHasher::new();
        fs::read(p).ok()?.hash(&mut h);
        Some(format!("{:x}", h.finish()))
    }
    fn count(c: &std::sync::atomic::AtomicUsize) -> usize {
        c.load(Relaxed)
    }
}

impl Remote for Hashing {
    fn list(&self) -> Result<Vec<Item>> {
        self.inner.list()
    }
    fn list_dir(&self, rel: &str) -> Result<Vec<Item>> {
        if !self.listing {
            return Err("list_dir unsupported".into());
        }
        self.lists.fetch_add(1, Relaxed);
        Ok(self
            .inner
            .list_dir(rel)?
            .into_iter()
            .map(|mut i| {
                i.hash = Self::sum(&self.inner.0.join(&i.path)).map(|h| ("sum".to_string(), h));
                i
            })
            .collect())
    }
    fn stat(&self, rel: &str) -> Result<Option<Item>> {
        Ok(self.inner.stat(rel)?.map(|mut i| {
            i.hash = Self::sum(&self.inner.0.join(rel)).map(|h| ("sum".to_string(), h));
            i.id = format!("id:{rel}");
            i
        }))
    }
    fn upload(&self, root: &Path, rel: &str) -> Result<()> {
        self.uploads.fetch_add(1, Relaxed);
        self.inner.upload(root, rel)
    }
    fn make_dir(&self, rel: &str) -> Result<()> {
        self.inner.make_dir(rel)
    }
    fn download(&self, rel: &str, dest: &Path) -> Result<()> {
        self.inner.download(rel, dest)
    }
    fn delete(&self, rel: &str) -> Result<()> {
        self.inner.delete(rel)
    }
    fn copy(&self, from: &str, to: &str) -> Result<()> {
        self.copies.fetch_add(1, Relaxed);
        fs::create_dir_all(self.inner.0.join(to).parent().unwrap()).map_err(|e| e.to_string())?;
        if self.bad_copy {
            return fs::write(self.inner.0.join(to), "not the same bytes").map_err(|e| e.to_string());
        }
        fs::copy(self.inner.0.join(from), self.inner.0.join(to))
            .map(drop)
            .map_err(|e| e.to_string())
    }
    fn list_under(&self, dir: &str) -> Result<Vec<Item>> {
        Ok(self
            .inner
            .list_under(dir)?
            .into_iter()
            .map(|mut i| {
                i.hash = Self::sum(&self.inner.0.join(&i.path)).map(|h| ("sum".to_string(), h));
                i
            })
            .collect())
    }
    fn copy_dir(&self, from: &str, to: &str) -> Result<()> {
        self.dir_copies.fetch_add(1, Relaxed);
        let mut files: Vec<_> = self.inner.list_under(from)?.into_iter().filter(|i| !i.is_dir).collect();
        files.sort_by(|x, y| x.path.cmp(&y.path));
        for (n, i) in files.iter().enumerate() {
            if self.dir_copy_limit == Some(n) {
                return Err("copy stopped".into());
            }
            let dest = format!("{to}/{}", &i.path[from.len() + 1..]);
            copy_keep_time(&self.inner.0.join(&i.path), &self.inner.0.join(dest))?;
        }
        Ok(())
    }
    fn local_hash(&self, root: &Path, rel: &str, _kind: &str) -> Result<Option<String>> {
        Ok(Self::sum(&root.join(rel)))
    }
}

/// A synced folder `a` of two big and two small files, a local copy of it as `b`; the engine has seen only `a`.
fn folder_copy_fixture(name: &str, limit: Option<usize>) -> (Fixture, Engine<Hashing>) {
    let f = Fixture::new(name);
    let mut remote = Hashing::new(&f.engine.remote.0, false);
    remote.dir_copy_limit = limit;
    let mut e = Engine::new(f.engine.root.clone(), remote, Db::memory().unwrap());
    let big = |c: &str| c.repeat(COPY_MIN as usize + 10);
    write(f.local("a/one.bin"), &big("1"), 100);
    write(f.local("a/deep/two.bin"), &big("2"), 100);
    write(f.local("a/small.txt"), "small", 100);
    write(f.local("a/deep/tiny.txt"), "tiny", 100);
    assert!(e.full_scan().errors.is_empty());
    for p in ["one.bin", "deep/two.bin", "small.txt", "deep/tiny.txt"] {
        fs::create_dir_all(f.local(&format!("b/{p}")).parent().unwrap()).unwrap();
        fs::copy(f.local(&format!("a/{p}")), f.local(&format!("b/{p}"))).unwrap();
    }
    (f, e)
}

#[test]
fn a_copied_folder_is_one_cloud_folder_copy_small_files_included() {
    let (f, mut e) = folder_copy_fixture("dir-copy", None);
    let r = e.full_scan();
    assert!(r.errors.is_empty(), "{r:?}");
    assert_eq!(Hashing::count(&e.remote.dir_copies), 1);
    assert_eq!(Hashing::count(&e.remote.copies), 0);
    assert_eq!(Hashing::count(&e.remote.uploads), 4, "only the first sync uploaded");
    assert_eq!(fs::read_to_string(f.remote("b/deep/tiny.txt")).unwrap(), "tiny");
    for p in ["b/one.bin", "b/deep/two.bin", "b/small.txt", "b/deep/tiny.txt"] {
        assert!(e.db.base(p).unwrap().is_some(), "{p}");
    }
    let again = e.full_scan();
    assert!(again.errors.is_empty(), "{again:?}");
    assert_eq!(again.transferred, 0);
}

#[test]
fn a_folder_copy_is_not_used_when_the_copy_lacks_a_file_of_the_original() {
    let (f, mut e) = folder_copy_fixture("dir-copy-missing", None);
    fs::remove_file(f.local("b/small.txt")).unwrap();
    let r = e.full_scan();
    assert!(r.errors.is_empty(), "{r:?}");
    assert_eq!(
        Hashing::count(&e.remote.dir_copies),
        0,
        "the copy would resurrect small.txt"
    );
    assert!(!f.remote("b/small.txt").exists());
}

#[test]
fn a_folder_copy_is_not_used_when_a_copied_file_was_edited() {
    let (f, mut e) = folder_copy_fixture("dir-copy-edited", None);
    write(f.local("b/small.txt"), "other", 100);
    let r = e.full_scan();
    assert!(r.errors.is_empty(), "{r:?}");
    assert_eq!(Hashing::count(&e.remote.dir_copies), 0);
    assert_eq!(fs::read_to_string(f.remote("b/small.txt")).unwrap(), "other");
}

#[test]
fn a_folder_copy_is_not_used_onto_a_cloud_folder_that_has_files() {
    let (f, mut e) = folder_copy_fixture("dir-copy-taken", None);
    write(f.remote("b/elsewhere.txt"), "x", 100);
    e.full_scan();
    assert_eq!(Hashing::count(&e.remote.dir_copies), 0);
}

#[test]
fn a_folder_copy_that_stops_halfway_keeps_rows_only_for_what_landed() {
    let (f, mut e) = folder_copy_fixture("dir-copy-half", Some(2));
    // First pass: rows for the 2 copied files, the rest by the one-by-one path (which may need a second run).
    e.full_scan();
    let r = e.full_scan();
    assert!(r.errors.is_empty(), "{r:?}");
    assert_eq!(Hashing::count(&e.remote.dir_copies), 1);
    for p in ["b/one.bin", "b/deep/two.bin", "b/small.txt", "b/deep/tiny.txt"] {
        assert!(e.db.base(p).unwrap().is_some(), "{p}");
        assert!(f.remote(p).exists(), "{p}");
    }
    assert_eq!(e.full_scan().transferred, 0);
}

#[test]
fn a_local_copy_of_a_synced_file_is_copied_in_the_cloud_not_uploaded() {
    let f = Fixture::new("copy-up");
    let big = "x".repeat(COPY_MIN as usize + 10);
    let mut e = Engine::new(
        f.engine.root.clone(),
        Hashing::new(&f.engine.remote.0, false),
        Db::memory().unwrap(),
    );
    write(f.local("a.bin"), &big, 100);
    assert!(e.full_scan().errors.is_empty());
    assert_eq!(Hashing::count(&e.remote.uploads), 1);
    // The row of an upload keeps the cloud's hash, which is what lets the feed's echo be told from an edit.
    let (_, _, hash) = e.db.by_id("id:a.bin").unwrap().unwrap();
    assert_eq!(hash.map(|(k, _)| k).as_deref(), Some("sum"));
    fs::create_dir_all(f.local("sub")).unwrap();
    fs::copy(f.local("a.bin"), f.local("sub/b.bin")).unwrap();
    let r = e.full_scan();
    assert!(r.errors.is_empty(), "{r:?}");
    assert_eq!(Hashing::count(&e.remote.uploads), 1, "nothing was uploaded");
    assert_eq!(Hashing::count(&e.remote.copies), 1);
    assert_eq!(fs::read_to_string(f.remote("sub/b.bin")).unwrap(), big);
    assert!(e.db.base("a.bin").unwrap().is_some(), "the source keeps its row");
    assert!(e.db.base("sub/b.bin").unwrap().is_some());
    assert_eq!(e.full_scan().transferred, 0);
}

#[test]
fn many_local_copies_of_one_file_look_the_source_up_with_one_listing() {
    let f = Fixture::new("copy-up-listing");
    let big = "x".repeat(COPY_MIN as usize + 10);
    let mut remote = Hashing::new(&f.engine.remote.0, false);
    remote.listing = true;
    let mut e = Engine::new(f.engine.root.clone(), remote, Db::memory().unwrap());
    write(f.local("a.bin"), &big, 100);
    assert!(e.full_scan().errors.is_empty());
    fs::create_dir_all(f.local("sub")).unwrap();
    for i in 0..3 {
        fs::copy(f.local("a.bin"), f.local(&format!("sub/b{i}.bin"))).unwrap();
    }
    let r = e.full_scan();
    assert!(r.errors.is_empty(), "{r:?}");
    assert_eq!(Hashing::count(&e.remote.uploads), 1, "only the first file went up");
    assert_eq!(Hashing::count(&e.remote.copies), 3);
    assert_eq!(Hashing::count(&e.remote.lists), 1, "one listing of the source's folder");
}

#[test]
fn a_cloud_copy_with_other_bytes_is_replaced_by_the_real_upload() {
    let f = Fixture::new("copy-up-bad");
    let big = "x".repeat(COPY_MIN as usize + 10);
    let mut e = Engine::new(
        f.engine.root.clone(),
        Hashing::new(&f.engine.remote.0, true),
        Db::memory().unwrap(),
    );
    write(f.local("a.bin"), &big, 100);
    e.full_scan();
    fs::copy(f.local("a.bin"), f.local("b.bin")).unwrap();
    let r = e.full_scan();
    assert!(r.errors.is_empty(), "{r:?}");
    assert_eq!(Hashing::count(&e.remote.copies), 1);
    assert_eq!(Hashing::count(&e.remote.uploads), 2, "the copy failed its check");
    assert_eq!(fs::read_to_string(f.remote("b.bin")).unwrap(), big);
}

#[test]
fn a_new_file_with_the_size_of_a_synced_one_but_other_bytes_is_uploaded() {
    let f = Fixture::new("copy-up-other");
    let mut e = Engine::new(
        f.engine.root.clone(),
        Hashing::new(&f.engine.remote.0, false),
        Db::memory().unwrap(),
    );
    write(f.local("a.bin"), &"a".repeat(COPY_MIN as usize), 100);
    e.full_scan();
    write(f.local("b.bin"), &"b".repeat(COPY_MIN as usize), 100);
    let r = e.full_scan();
    assert!(r.errors.is_empty(), "{r:?}");
    assert_eq!(Hashing::count(&e.remote.copies), 0);
    assert_eq!(Hashing::count(&e.remote.uploads), 2);
}

/// A cloud that counts uploads, file renames and folder renames. `blind`: no way to prove equal bytes (MEGA).
struct Counting {
    inner: DirRemote,
    uploads: std::sync::atomic::AtomicUsize,
    renames: std::sync::atomic::AtomicUsize,
    dir_moves: std::sync::atomic::AtomicUsize,
    deletes: std::sync::atomic::AtomicUsize,
    purges: std::sync::atomic::AtomicUsize,
    blind: bool,
    /// Every listing of a folder fails once a folder was moved.
    list_fails_after_move: bool,
}

impl Counting {
    fn new(dir: &Path, blind: bool) -> Self {
        Self {
            inner: DirRemote(dir.to_path_buf()),
            uploads: Default::default(),
            renames: Default::default(),
            dir_moves: Default::default(),
            deletes: Default::default(),
            purges: Default::default(),
            blind,
            list_fails_after_move: false,
        }
    }
    fn n(c: &std::sync::atomic::AtomicUsize) -> usize {
        c.load(Relaxed)
    }
}

impl Remote for Counting {
    fn list(&self) -> Result<Vec<Item>> {
        self.inner.list()
    }
    fn stat(&self, rel: &str) -> Result<Option<Item>> {
        self.inner.stat(rel)
    }
    fn upload(&self, root: &Path, rel: &str) -> Result<()> {
        self.uploads.fetch_add(1, Relaxed);
        self.inner.upload(root, rel)
    }
    fn make_dir(&self, rel: &str) -> Result<()> {
        self.inner.make_dir(rel)
    }
    fn download(&self, rel: &str, dest: &Path) -> Result<()> {
        self.inner.download(rel, dest)
    }
    fn delete(&self, rel: &str) -> Result<()> {
        self.deletes.fetch_add(1, Relaxed);
        self.inner.delete(rel)
    }
    fn purge_dir(&self, rel: &str) -> Result<()> {
        self.purges.fetch_add(1, Relaxed);
        fs::remove_dir_all(self.inner.0.join(rel)).map_err(|e| e.to_string())
    }
    fn rename(&self, from: &str, to: &str) -> Result<()> {
        self.renames.fetch_add(1, Relaxed);
        self.inner.rename(from, to)
    }
    fn remove_empty_dir(&self, rel: &str) -> Result<()> {
        self.inner.remove_empty_dir(rel)
    }
    fn list_under(&self, dir: &str) -> Result<Vec<Item>> {
        if self.list_fails_after_move && Self::n(&self.dir_moves) > 0 {
            return Err("listing failed".into());
        }
        self.inner.list_under(dir)
    }
    fn move_dir(&self, from: &str, to: &str) -> Result<()> {
        self.dir_moves.fetch_add(1, Relaxed);
        self.inner.move_dir(from, to)
    }
    fn same_content(&self, root: &Path, rel: &str, remote: &Item) -> bool {
        !self.blind && self.inner.same_content(root, rel, remote)
    }
}

fn counting_fixture(name: &str, blind: bool) -> (Fixture, Engine<Counting>) {
    let f = Fixture::new(name);
    let e = Engine::new(
        f.engine.root.clone(),
        Counting::new(&f.engine.remote.0, blind),
        Db::memory().unwrap(),
    );
    (f, e)
}

/// Root files so one deleted folder is far from the delete guard's limits.
fn delete_dir_fixture(name: &str) -> (Fixture, Engine<Counting>) {
    let (f, mut e) = counting_fixture(name, false);
    for i in 0..8 {
        write(f.local(&format!("keep{i}.txt")), "keep", 100);
    }
    write(f.local("top/stay.txt"), "stay", 100);
    write(f.local("top/gone/one.txt"), "first", 100);
    write(f.local("top/gone/deep/two.txt"), "second one", 100);
    write(f.local("top/gone/three.txt"), "third", 100);
    assert!(e.full_scan().errors.is_empty());
    (f, e)
}

#[test]
fn a_folder_deleted_here_is_one_cloud_folder_delete() {
    let (f, mut e) = delete_dir_fixture("dir-purge");
    fs::remove_dir_all(f.local("top/gone")).unwrap();
    let r = e.full_scan();
    assert!(r.errors.is_empty() && r.aborted.is_none(), "{r:?}");
    assert_eq!(Counting::n(&e.remote.purges), 1);
    assert_eq!(Counting::n(&e.remote.deletes), 0);
    assert!(!f.remote("top/gone").exists());
    assert!(f.remote("top/stay.txt").exists());
    assert!(e.db.base("top/gone/deep/two.txt").unwrap().is_none());
    let again = e.full_scan();
    assert!(again.errors.is_empty(), "{again:?}");
    assert_eq!(Counting::n(&e.remote.purges), 1);
}

#[test]
fn a_folder_delete_is_not_used_when_the_cloud_folder_holds_another_file() {
    let (f, mut e) = delete_dir_fixture("dir-purge-extra");
    write(f.remote("top/gone/new-from-elsewhere.txt"), "mine", 100);
    fs::remove_dir_all(f.local("top/gone")).unwrap();
    let r = e.full_scan();
    assert!(r.errors.is_empty(), "{r:?}");
    assert_eq!(Counting::n(&e.remote.purges), 0);
    assert_eq!(
        fs::read_to_string(f.remote("top/gone/new-from-elsewhere.txt")).unwrap(),
        "mine"
    );
    assert!(!f.remote("top/gone/one.txt").exists());
}

#[test]
fn a_folder_delete_is_not_used_when_something_new_sits_in_the_local_folder() {
    let (f, mut e) = delete_dir_fixture("dir-purge-local");
    fs::remove_file(f.local("top/gone/one.txt")).unwrap();
    fs::remove_file(f.local("top/gone/three.txt")).unwrap();
    fs::remove_file(f.local("top/gone/deep/two.txt")).unwrap();
    write(f.local("top/gone/fresh.txt"), "new", 100);
    let r = e.full_scan();
    assert!(r.errors.is_empty(), "{r:?}");
    assert_eq!(Counting::n(&e.remote.purges), 0);
    assert_eq!(fs::read_to_string(f.remote("top/gone/fresh.txt")).unwrap(), "new");
    assert!(!f.remote("top/gone/one.txt").exists());
}

#[test]
fn a_renamed_folder_is_one_cloud_folder_move() {
    let (f, mut e) = counting_fixture("dir-move", false);
    write(f.local("a/one.txt"), "first", 100);
    write(f.local("a/deep/two.txt"), "second one", 100);
    write(f.local("a/deep/er/three.txt"), "third", 100);
    assert!(e.full_scan().errors.is_empty());
    fs::rename(f.local("a"), f.local("b")).unwrap();
    let r = e.full_scan();
    assert!(r.errors.is_empty(), "{r:?}");
    assert_eq!(Counting::n(&e.remote.dir_moves), 1);
    assert_eq!(Counting::n(&e.remote.renames), 0);
    assert_eq!(Counting::n(&e.remote.uploads), 3, "only the first sync uploaded");
    assert_eq!(fs::read_to_string(f.remote("b/deep/er/three.txt")).unwrap(), "third");
    assert!(!f.remote("a").exists());
    assert!(e.db.under("a").unwrap().is_empty());
    assert_eq!(e.db.under("b").unwrap().len(), 3);
    assert_eq!(r.transferred, 3);
    assert_eq!(e.full_scan().transferred, 0);
}

#[test]
fn a_folder_with_a_cloud_file_nobody_synced_yet_is_moved_file_by_file() {
    let (f, mut e) = counting_fixture("dir-move-extra", false);
    write(f.local("a/one.txt"), "first", 100);
    write(f.local("a/two.txt"), "second one", 100);
    assert!(e.full_scan().errors.is_empty());
    // Another device added a file to the folder, and it was not downloaded yet.
    write(f.remote("a/theirs.txt"), "from elsewhere", 100);
    fs::rename(f.local("a"), f.local("b")).unwrap();
    let r = e.full_scan();
    assert!(r.errors.is_empty(), "{r:?}");
    // Moving the folder would have taken their file along: the files go one by one.
    assert_eq!(Counting::n(&e.remote.dir_moves), 0);
    assert_eq!(Counting::n(&e.remote.renames), 2);
    assert_eq!(Counting::n(&e.remote.uploads), 2);
    assert!(f.remote("b/one.txt").exists() && f.remote("b/two.txt").exists());
    assert!(f.remote("a/theirs.txt").exists(), "their file stayed where it was");
}

#[test]
fn a_folder_rename_onto_an_existing_cloud_folder_is_never_a_folder_move() {
    let (f, mut e) = counting_fixture("dir-move-taken", false);
    write(f.local("a/one.txt"), "first", 100);
    write(f.local("a/two.txt"), "second one", 100);
    assert!(e.full_scan().errors.is_empty());
    write(f.remote("b/other.txt"), "already there", 100);
    fs::rename(f.local("a"), f.local("b")).unwrap();
    e.full_scan();
    assert_eq!(Counting::n(&e.remote.dir_moves), 0);
    assert_eq!(fs::read_to_string(f.remote("b/other.txt")).unwrap(), "already there");
}

#[test]
fn a_service_with_no_hash_moves_a_file_that_kept_its_name_size_and_time_and_nothing_else() {
    let (f, mut e) = counting_fixture("blind-move", true);
    write(f.local("a/x.bin"), "same", 100);
    write(f.local("a/y.bin"), "other", 100);
    assert!(e.full_scan().errors.is_empty());
    // Another name with the same size and time proves nothing (an archive's files share both): upload + delete.
    fs::rename(f.local("a/x.bin"), f.local("a/z.bin")).unwrap();
    let r = e.full_scan();
    assert!(r.errors.is_empty(), "{r:?}");
    assert_eq!(Counting::n(&e.remote.renames), 0);
    assert_eq!(Counting::n(&e.remote.uploads), 3);
    assert_eq!(fs::read_to_string(f.remote("a/z.bin")).unwrap(), "same");
    assert!(!f.remote("a/x.bin").exists());
    // The same name in another folder, with the size and time of the synced file, is the same file moved.
    fs::create_dir_all(f.local("d")).unwrap();
    fs::rename(f.local("a/y.bin"), f.local("d/y.bin")).unwrap();
    let r = e.full_scan();
    assert!(r.errors.is_empty(), "{r:?}");
    assert_eq!(Counting::n(&e.remote.renames), 1);
    assert_eq!(Counting::n(&e.remote.uploads), 3);
    assert_eq!(fs::read_to_string(f.remote("d/y.bin")).unwrap(), "other");
}

#[test]
fn a_folder_rename_seen_by_the_watcher_is_one_cloud_folder_move() {
    let (f, mut e) = counting_fixture("dir-move-paths", false);
    write(f.local("a/one.txt"), "first", 100);
    write(f.local("a/deep/two.txt"), "second one", 100);
    assert!(e.full_scan().errors.is_empty());
    fs::rename(f.local("a"), f.local("b")).unwrap();
    // What inotify reports for a folder rename: the old and the new folder.
    let r = e.sync_paths(&["a".to_string(), "b".to_string()]);
    assert!(r.errors.is_empty(), "{r:?}");
    assert_eq!(Counting::n(&e.remote.dir_moves), 1);
    assert_eq!(Counting::n(&e.remote.renames), 0);
    assert_eq!(Counting::n(&e.remote.uploads), 2, "only the first sync uploaded");
    assert_eq!(fs::read_to_string(f.remote("b/deep/two.txt")).unwrap(), "second one");
    assert_eq!(e.full_scan().transferred, 0);
}

#[test]
fn a_failed_listing_after_the_folder_moved_still_leaves_the_rows_right() {
    let (f, mut e) = counting_fixture("dir-move-blind-after", false);
    write(f.local("a/one.txt"), "first", 100);
    write(f.local("a/two.txt"), "second one", 100);
    assert!(e.full_scan().errors.is_empty());
    e.remote.list_fails_after_move = true;
    fs::rename(f.local("a"), f.local("b")).unwrap();
    let r = e.full_scan();
    assert!(r.errors.is_empty(), "{r:?}");
    assert_eq!(Counting::n(&e.remote.dir_moves), 1);
    assert!(e.db.under("a").unwrap().is_empty());
    assert_eq!(e.db.under("b").unwrap().len(), 2);
    assert_eq!(Counting::n(&e.remote.uploads), 2);
    // The old folder is forgotten and the new one known: the walk has nothing left to report.
    assert!(e.paths_to_sync(vec![], vec![], true).is_empty());
}

#[test]
fn dir_pair_cuts_the_common_ending_off() {
    use super::pair::dir_pair;
    let s = |a: &str, b: &str| Some((a.to_string(), b.to_string()));
    assert_eq!(
        dir_pair("Teste/x/f.bin", "Teste with apps/x/f.bin"),
        s("Teste", "Teste with apps")
    );
    assert_eq!(dir_pair("a/b/f", "a/c/f"), s("a/b", "a/c"));
    assert_eq!(dir_pair("a/f", "a/g"), None, "the name changed too");
    assert_eq!(dir_pair("f", "d/f"), None, "moved out of the root");
    assert_eq!(dir_pair("a/f", "a/b/f"), None, "one folder inside the other");
    assert_eq!(dir_pair("a/b/f", "x/a/b/f"), None, "nothing left on the old side");
    assert_eq!(
        dir_pair("T/P/f", "T/S/P/f"),
        s("T/P", "T/S/P"),
        "a folder moved into a sibling"
    );
    assert_eq!(
        dir_pair("T/P/q/f", "T/S/P/q/f"),
        s("T/P", "T/S/P"),
        "the same with a subfolder"
    );
}

/// A cloud whose upload always fails the way rclone's hash check does for a file saved meanwhile. `edit`: the
/// file is saved with this text first, as an editor would while the upload runs.
struct FailUpload {
    inner: DirRemote,
    edit: Option<(PathBuf, &'static str)>,
}

impl Remote for FailUpload {
    fn list(&self) -> Result<Vec<Item>> {
        self.inner.list()
    }
    fn stat(&self, rel: &str) -> Result<Option<Item>> {
        self.inner.stat(rel)
    }
    fn upload(&self, _: &Path, _: &str) -> Result<()> {
        if let Some((p, text)) = &self.edit {
            fs::write(p, text).map_err(|e| e.to_string())?;
        }
        Err("operations/copyfile: corrupted on transfer: dropbox hashes differ".into())
    }
    fn download(&self, rel: &str, dest: &Path) -> Result<()> {
        self.inner.download(rel, dest)
    }
    fn delete(&self, rel: &str) -> Result<()> {
        self.inner.delete(rel)
    }
}

#[test]
fn a_file_saved_during_its_upload_is_no_error_and_goes_up_as_the_new_version() {
    let f = Fixture::new("later");
    write(f.local("a.txt"), "one", 100);
    let mut e = Engine::new(
        f.engine.root.clone(),
        FailUpload {
            inner: DirRemote(f.engine.remote.0.clone()),
            edit: Some((f.local("a.txt"), "edited while it uploads")),
        },
        Db::memory().unwrap(),
    );
    let r = e.full_scan();
    assert!(r.errors.is_empty(), "{r:?}");
    assert_eq!(r.later, ["a.txt"]);
    assert!(r.failed.is_empty() && !r.has_global_failure());
    assert!(e.db.base("a.txt").unwrap().is_none(), "nothing was recorded as synced");
    // The next pass (the edit's own event) uploads what is there now.
    let r = f.engine.remote.0.join("a.txt");
    let mut ok = Engine::new(
        f.engine.root.clone(),
        DirRemote(f.engine.remote.0.clone()),
        Db::memory().unwrap(),
    );
    assert!(ok.full_scan().errors.is_empty());
    assert_eq!(fs::read_to_string(r).unwrap(), "edited while it uploads");
}

#[test]
fn the_same_upload_failure_on_an_untouched_file_is_an_error_retried_by_its_path() {
    let f = Fixture::new("not-later");
    write(f.local("a.txt"), "one", 100);
    let mut e = Engine::new(
        f.engine.root.clone(),
        FailUpload {
            inner: DirRemote(f.engine.remote.0.clone()),
            edit: None,
        },
        Db::memory().unwrap(),
    );
    let r = e.full_scan();
    assert_eq!(r.errors.len(), 1, "{r:?}");
    assert_eq!(r.failed, ["a.txt"]);
    assert!(r.later.is_empty());
    assert!(
        !r.has_global_failure(),
        "one file's failure is not a reason to list the cloud again"
    );
}

/// A cloud that cannot list everything (minutes on a big one): only `list_under` answers.
struct OnlyFolders {
    inner: DirRemote,
    folders_fail: bool,
    /// Folder listings asked for.
    listings: std::sync::atomic::AtomicUsize,
}

impl Remote for OnlyFolders {
    fn list(&self) -> Result<Vec<Item>> {
        Err("the whole cloud was listed".into())
    }
    fn stat(&self, rel: &str) -> Result<Option<Item>> {
        self.inner.stat(rel)
    }
    fn upload(&self, root: &Path, rel: &str) -> Result<()> {
        self.inner.upload(root, rel)
    }
    fn make_dir(&self, rel: &str) -> Result<()> {
        self.inner.make_dir(rel)
    }
    fn download(&self, rel: &str, dest: &Path) -> Result<()> {
        self.inner.download(rel, dest)
    }
    fn delete(&self, rel: &str) -> Result<()> {
        self.inner.delete(rel)
    }
    fn list_under(&self, dir: &str) -> Result<Vec<Item>> {
        self.listings.fetch_add(1, Relaxed);
        if self.folders_fail {
            return Err("listing the folder failed".into());
        }
        self.inner.list_under(dir)
    }
}

#[test]
fn a_folder_the_feed_reported_is_listed_alone_and_its_files_come_down() {
    let f = Fixture::new("feed-dir");
    write(f.remote("new/a.txt"), "first", 100);
    write(f.remote("new/sub/b.txt"), "second", 100);
    write(f.remote("elsewhere/c.txt"), "not asked about", 100);
    let mut e = Engine::new(
        f.engine.root.clone(),
        OnlyFolders {
            inner: DirRemote(f.engine.remote.0.clone()),
            folders_fail: false,
            listings: Default::default(),
        },
        Db::memory().unwrap(),
    );
    let r = e.sync_paths_in(&["new".to_string()], &["new".to_string()]);
    assert!(r.errors.is_empty(), "{r:?}");
    assert_eq!(fs::read_to_string(f.local("new/a.txt")).unwrap(), "first");
    assert_eq!(fs::read_to_string(f.local("new/sub/b.txt")).unwrap(), "second");
    assert!(!f.local("elsewhere").exists(), "only the reported folder was looked at");
    // The echo of our own upload: the same folder reported again costs nothing.
    assert_eq!(
        e.sync_paths_in(&["new".to_string()], &["new".to_string()]).transferred,
        0
    );
}

#[test]
fn a_folder_that_cannot_be_listed_is_a_failure_of_its_own_never_an_empty_folder() {
    let f = Fixture::new("feed-dir-fail");
    write(f.local("new/a.txt"), "mine", 100);
    let mut e = Engine::new(
        f.engine.root.clone(),
        OnlyFolders {
            inner: DirRemote(f.engine.remote.0.clone()),
            folders_fail: true,
            listings: Default::default(),
        },
        Db::memory().unwrap(),
    );
    let r = e.sync_paths_in(&["new".to_string()], &["new".to_string()]);
    // Shown as an error and listed again by itself; no full scan, and the cursor may move on.
    assert_eq!(r.failed_dirs, ["new"], "{r:?}");
    assert!(!r.has_global_failure() && r.errors.len() == 1, "{r:?}");
}

#[test]
fn a_folder_renamed_in_the_cloud_whose_new_name_cannot_be_listed_keeps_the_old_files_until_it_can() {
    let f = Fixture::new("rename-listing-fails");
    write(f.remote("a/x.txt"), "synced", 100);
    let mut e = Engine::new(
        f.engine.root.clone(),
        OnlyFolders {
            inner: DirRemote(f.engine.remote.0.clone()),
            folders_fail: false,
            listings: Default::default(),
        },
        Db::memory().unwrap(),
    );
    assert!(
        e.sync_paths_in(&["a".to_string()], &["a".to_string()])
            .errors
            .is_empty()
    );
    assert_eq!(fs::read_to_string(f.local("a/x.txt")).unwrap(), "synced");
    // Renamed in the cloud, and the listing of the new name fails: the old file reads as deleted there.
    fs::rename(f.remote("a"), f.remote("b")).unwrap();
    e.remote.folders_fail = true;
    let paths = ["a/x.txt".to_string(), "b".to_string()];
    let r = e.sync_paths_in(&paths, &["b".to_string()]);
    assert_eq!(r.failed_dirs, ["b"], "{r:?}");
    assert!(
        r.failed.contains(&"a/x.txt".to_string()),
        "retried with the folder: {r:?}"
    );
    assert!(
        f.local("a/x.txt").exists() && trashed(&f, "a/x.txt").is_none(),
        "nothing was trashed"
    );
}

#[test]
fn a_watcher_event_for_a_second_file_differing_only_by_case_gets_the_case_conflict_name() {
    let mut f = Fixture::new("clash-path");
    write(f.local("Report.txt"), "the synced one", 100);
    assert!(f.engine.full_scan().errors.is_empty());
    write(f.local("report.txt"), "another file", 50);
    let r = f.engine.sync_paths(&["report.txt".to_string()]);
    assert!(r.errors.is_empty(), "{r:?}");
    // As Dropbox's own client does: the new file is renamed, both exist in the cloud under names it tells apart.
    assert_eq!(r.conflicts, ["report (Case Conflict).txt"]);
    assert!(!f.local("report.txt").exists() && !f.remote("report.txt").exists());
    assert_eq!(
        fs::read_to_string(f.remote("report (Case Conflict).txt")).unwrap(),
        "another file"
    );
    assert_eq!(fs::read_to_string(f.remote("Report.txt")).unwrap(), "the synced one");
    assert_eq!(fs::read_to_string(f.local("Report.txt")).unwrap(), "the synced one");
    assert_eq!(f.engine.full_scan().transferred, 0);
}

#[test]
fn a_cloud_folder_listing_with_only_another_spelling_of_the_name_is_a_clash() {
    // `lookup` through the folder listing (many changed files in one folder): the cloud holds `Report.txt`,
    // here is a new `report.txt`.
    let f = Fixture::new("clash-cloud");
    write(f.remote("d/Report.txt"), "theirs", 100);
    let names: Vec<String> = (0..5).map(|i| format!("d/new{i}.txt")).collect();
    write(f.local("d/report.txt"), "mine", 100);
    for n in &names {
        write(f.local(n), "x", 100);
    }
    let mut all = names.clone();
    all.push("d/report.txt".into());
    let answers = lookup(
        &DirRemote(f.engine.remote.0.clone()),
        &Db::memory().unwrap(),
        &all,
        &std::sync::atomic::AtomicBool::new(false),
        true,
    );
    let clash = answers.iter().find(|(i, _)| all[*i] == "d/report.txt").unwrap();
    assert_eq!(clash.1, Err(CLASH_WHY.to_string()));
    assert!(answers.iter().filter(|(_, r)| r == &Ok(None)).count() == 5);
}

/// A cloud that, like Dropbox and OneDrive, takes `report.txt` and `Report.txt` for one file: every name is
/// matched ignoring case and answered with the spelling it is stored under. Counts what it is asked to change.
struct Folding {
    inner: DirRemote,
    uploads: std::sync::atomic::AtomicUsize,
    deletes: std::sync::atomic::AtomicUsize,
    renames: std::sync::atomic::AtomicUsize,
    /// The cloud cannot rename a file by case alone (what rclone's `movefile` might do on a real one).
    no_rename: bool,
    /// `stat` answers with the spelling it was asked for, not the stored one (rclone might do either); the item's
    /// `id` is the stored path, so the same file still has the same id.
    echo: bool,
    /// A rename or folder move to a name that differs only by case is refused (a rename through another name works).
    direct_case_fails: bool,
    /// The nth call of `rename` (1 = the first) fails; 0 = none.
    fail_rename_number: usize,
    /// Every rename and folder move answers "ok" and changes nothing (a cloud that ignores a change of case).
    noop_renames: bool,
    dir_moves: std::sync::atomic::AtomicUsize,
}

impl Folding {
    fn new(dir: &Path) -> Self {
        Self {
            inner: DirRemote(dir.to_path_buf()),
            uploads: Default::default(),
            deletes: Default::default(),
            renames: Default::default(),
            no_rename: false,
            echo: false,
            direct_case_fails: false,
            fail_rename_number: 0,
            noop_renames: false,
            dir_moves: Default::default(),
        }
    }
    /// Where a move to `to` lands: the folder part as stored (the cloud finds it ignoring case), the name as given.
    fn target(&self, to: &str) -> String {
        match to.rsplit_once('/') {
            Some((d, n)) => format!("{}/{n}", self.real(d)),
            None => to.to_string(),
        }
    }
    /// `rel` with every part spelled as stored, or as given where nothing matches.
    fn real(&self, rel: &str) -> String {
        let mut cur = self.inner.0.clone();
        let mut parts = Vec::new();
        for part in rel.split('/') {
            let stored = fs::read_dir(&cur).ok().and_then(|rd| {
                rd.flatten()
                    .map(|e| e.file_name().to_string_lossy().into_owned())
                    .find(|n| n.eq_ignore_ascii_case(part))
            });
            let name = stored.unwrap_or_else(|| part.to_string());
            cur.push(&name);
            parts.push(name);
        }
        parts.join("/")
    }
    fn n(c: &std::sync::atomic::AtomicUsize) -> usize {
        c.load(Relaxed)
    }
}

impl Remote for Folding {
    fn list(&self) -> Result<Vec<Item>> {
        self.inner.list()
    }
    fn stat(&self, rel: &str) -> Result<Option<Item>> {
        let real = self.real(rel);
        Ok(self.inner.stat(&real)?.map(|mut i| {
            i.id = real.clone();
            if self.echo {
                i.path = rel.to_string();
            }
            i
        }))
    }
    fn upload(&self, root: &Path, rel: &str) -> Result<()> {
        self.uploads.fetch_add(1, Relaxed);
        self.inner.upload(root, &self.real(rel))
    }
    fn make_dir(&self, rel: &str) -> Result<()> {
        self.inner.make_dir(&self.real(rel))
    }
    fn download(&self, rel: &str, dest: &Path) -> Result<()> {
        self.inner.download(&self.real(rel), dest)
    }
    fn delete(&self, rel: &str) -> Result<()> {
        self.deletes.fetch_add(1, Relaxed);
        self.inner.delete(&self.real(rel))
    }
    fn rename(&self, from: &str, to: &str) -> Result<()> {
        let n = self.renames.fetch_add(1, Relaxed) + 1;
        if self.no_rename || n == self.fail_rename_number {
            return Err("refused".into());
        }
        if self.noop_renames {
            return Ok(());
        }
        let (f, t) = (self.real(from), self.target(to));
        // Onto itself: what a case-insensitive cloud says for `Docs/a.txt` -> `docs/a.txt`.
        if f == t || (self.direct_case_fails && f.to_lowercase() == t.to_lowercase()) {
            return Err("cannot rename to the same name".into());
        }
        self.inner.rename(&f, &t)
    }
    fn move_dir(&self, from: &str, to: &str) -> Result<()> {
        self.dir_moves.fetch_add(1, Relaxed);
        if self.noop_renames {
            return Ok(());
        }
        let (f, t) = (self.real(from), self.target(to));
        if f == t || (self.direct_case_fails && f.to_lowercase() == t.to_lowercase()) {
            return Err("cannot rename to the same name".into());
        }
        self.inner.move_dir(&f, &t)
    }
    fn list_under(&self, dir: &str) -> Result<Vec<Item>> {
        self.inner.list_under(&self.real(dir))
    }
    /// The stored names, as a real listing shows them (unlike `stat`, which may echo the asked-for spelling).
    fn list_dir(&self, rel: &str) -> Result<Vec<Item>> {
        self.inner.list_dir(&self.real(rel))
    }
    fn same_content(&self, root: &Path, rel: &str, remote: &Item) -> bool {
        self.inner.same_content(root, rel, remote)
    }
}

fn folding_fixture(name: &str) -> (Fixture, Engine<Folding>) {
    let f = Fixture::new(name);
    let e = Engine::new(
        f.engine.root.clone(),
        Folding::new(&f.engine.remote.0),
        Db::memory().unwrap(),
    );
    (f, e)
}

#[test]
fn a_rename_by_case_alone_is_a_cloud_rename_never_an_adopt_plus_a_delete() {
    // A full scan; a watcher run with both names; one with only the new name (the old one comes from the DB); and
    // each of them against a cloud that echoes the spelling it was asked for.
    for (n, (via_watcher, only_new, echo)) in [
        (false, false, false),
        (true, false, false),
        (true, true, false),
        (true, false, true),
        (true, true, true),
    ]
    .into_iter()
    .enumerate()
    {
        let (f, mut e) = folding_fixture(&format!("case-rename-{n}"));
        e.remote.echo = echo;
        write(f.local("docs/MyFile.txt"), "the only copy", 100);
        assert!(e.full_scan().errors.is_empty());
        fs::rename(f.local("docs/MyFile.txt"), f.local("docs/myfile.txt")).unwrap();
        let r = if via_watcher {
            let mut paths = vec!["docs/myfile.txt".to_string()];
            if !only_new {
                paths.insert(0, "docs/MyFile.txt".to_string());
            }
            e.sync_paths(&paths)
        } else {
            e.full_scan()
        };
        assert!(
            r.errors.is_empty() && r.conflicts.is_empty() && r.aborted.is_none(),
            "{r:?}"
        );
        assert_eq!(
            Folding::n(&e.remote.deletes),
            0,
            "the only cloud copy was never deleted"
        );
        assert_eq!(Folding::n(&e.remote.renames), 1);
        assert_eq!(Folding::n(&e.remote.uploads), 1, "only the first sync uploaded");
        assert_eq!(
            fs::read_to_string(f.remote("docs/myfile.txt")).unwrap(),
            "the only copy"
        );
        assert!(
            !fs::read_dir(f.remote("docs"))
                .unwrap()
                .any(|e| e.unwrap().file_name() == "MyFile.txt")
        );
        assert!(e.db.base("docs/myfile.txt").unwrap().is_some() && e.db.base("docs/MyFile.txt").unwrap().is_none());
        assert_eq!(e.full_scan().transferred, 0);
    }
}

#[test]
fn a_run_the_delete_guard_stops_leaves_a_new_second_file_unrenamed() {
    let mut f = Fixture::new("case-guard");
    for n in ["k1.txt", "k2.txt", "k3.txt"] {
        write(f.local(n), n, 100);
    }
    assert!(f.engine.full_scan().errors.is_empty());
    // Everything known is gone here: deleting all of it in the cloud is what the guard stops.
    for n in ["k1.txt", "k2.txt", "k3.txt"] {
        fs::remove_file(f.local(n)).unwrap();
    }
    write(f.local("Dup.txt"), "older", 100);
    write(f.local("dup.txt"), "newer", 50);
    let r = f.engine.full_scan();
    assert!(r.aborted.is_some(), "{r:?}");
    // Nothing was done, so the user's files are as they left them: no rename for a plan that never ran.
    assert!(f.local("Dup.txt").exists() && f.local("dup.txt").exists());
    assert!(!f.local("dup (Case Conflict).txt").exists() && r.conflicts.is_empty());
    // Once the guard is answered (here: the deletes are no longer pending) the same files are renamed.
    write(f.local("k1.txt"), "k1.txt", 100);
    write(f.local("k2.txt"), "k2.txt", 100);
    write(f.local("k3.txt"), "k3.txt", 100);
    let r = f.engine.full_scan();
    assert!(r.aborted.is_none() && r.errors.is_empty(), "{r:?}");
    assert_eq!(r.conflicts, ["dup (Case Conflict).txt"]);
    assert!(!f.local("dup.txt").exists() && f.local("Dup.txt").exists());
}

#[test]
fn a_whole_folder_vanishing_stops_the_run_even_under_the_account_wide_share() {
    let mut f = Fixture::new("folder-wiped");
    for i in 0..100 {
        write(f.local(&format!("keep/k{i}.txt")), &"k".repeat(i + 1), 100);
    }
    for i in 0..12 {
        write(f.local(&format!("disk/f{i}.txt")), &"x".repeat(i + 1), 100);
    }
    assert!(f.engine.full_scan().errors.is_empty());
    // An unmounted disk inside the folder: 12 of 112 files, far below 50%, but all of `disk/`. Its mount point stays.
    fs::remove_dir_all(f.local("disk")).unwrap();
    fs::create_dir(f.local("disk")).unwrap();
    let r = f.engine.full_scan();
    assert_eq!(r.aborted.as_ref().map(|a| a.0), Some(12), "{r:?}");
    assert!(f.remote("disk/f0.txt").exists(), "nothing was deleted in the cloud");
}

#[test]
fn a_nested_folder_vanishing_stops_the_run_too() {
    let mut f = Fixture::new("folder-wiped-nested");
    // `media` stays well above the share: 40 of 100 files go, all of `media/disk2`.
    for i in 0..60 {
        write(f.local(&format!("media/k{i}.txt")), &"k".repeat(i + 1), 100);
    }
    for i in 0..40 {
        write(f.local(&format!("media/disk2/f{i}.txt")), &"x".repeat(i + 1), 100);
    }
    assert!(f.engine.full_scan().errors.is_empty());
    fs::remove_dir_all(f.local("media/disk2")).unwrap();
    fs::create_dir(f.local("media/disk2")).unwrap();
    let r = f.engine.full_scan();
    assert_eq!(r.aborted.as_ref().map(|a| a.0), Some(40), "{r:?}");
    assert!(
        f.remote("media/disk2/f0.txt").exists(),
        "nothing was deleted in the cloud"
    );
}

#[test]
fn a_mirror_trashing_a_folder_of_local_only_files_is_not_stopped_by_the_folder_rule() {
    let mut f = Fixture::new("mirror-folder");
    f.engine.download_only = true;
    for i in 0..30 {
        let n = format!("keep/k{i}.txt");
        write(f.local(&n), &"k".repeat(i + 1), 100);
        write(f.remote(&n), &"k".repeat(i + 1), 100);
    }
    // No DB row for these (a new account): 12 of 42 local files, under the share, and not a synced folder wiped.
    for i in 0..12 {
        write(f.local(&format!("mine/f{i}.txt")), "x", 100);
    }
    let r = f.engine.full_scan();
    assert!(r.aborted.is_none() && r.errors.is_empty(), "{r:?}");
    assert!(!f.local("mine/f0.txt").exists());
}

#[test]
fn a_download_that_cannot_be_placed_leaves_no_temp_copy_behind() {
    let mut f = Fixture::new("tmp-clean");
    write(f.remote("sub/a.txt"), "data", 100);
    // A file where the folder must go: the download succeeds, placing it cannot.
    write(f.local("sub"), "in the way", 100);
    let r = f.engine.full_scan();
    assert!(!r.errors.is_empty(), "{r:?}");
    let left = fs::read_dir(f.local(".fastsync/tmp")).map_or(0, |d| d.count());
    assert_eq!(left, 0, "the temp copy is removed on every failure");
}

#[test]
fn a_part_of_a_folder_vanishing_is_not_stopped_below_the_share() {
    let mut f = Fixture::new("folder-part");
    for i in 0..100 {
        write(f.local(&format!("keep/k{i}.txt")), &"k".repeat(i + 1), 100);
    }
    for i in 0..12 {
        write(f.local(&format!("disk/f{i}.txt")), &"x".repeat(i + 1), 100);
    }
    assert!(f.engine.full_scan().errors.is_empty());
    for i in 0..11 {
        fs::remove_file(f.local(&format!("disk/f{i}.txt"))).unwrap();
    }
    let r = f.engine.full_scan();
    assert!(r.aborted.is_none() && r.errors.is_empty(), "{r:?}");
    assert!(!f.remote("disk/f0.txt").exists());
}

/// The names directly in the cloud's root folder.
fn cloud_root_names(f: &Fixture) -> Vec<String> {
    let mut v: Vec<String> = fs::read_dir(f.remote(""))
        .unwrap()
        .flatten()
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .collect();
    v.sort();
    v
}

#[test]
fn a_folder_renamed_by_case_alone_is_renamed_in_the_cloud_through_a_temporary_name() {
    // `mv Docs docs`: a case-insensitive cloud answers for `docs/a.txt` with the file in `Docs`, so moving the
    // files can never change the folder's name; the folder's own rename can, in two steps.
    for files in [1usize, 3] {
        let (f, mut e) = folding_fixture(&format!("case-folder-{files}"));
        e.remote.direct_case_fails = true;
        let names = ["Docs/a.txt", "Docs/sub/b.txt", "Docs/sub/c.txt"];
        for (i, p) in names.iter().take(files).enumerate() {
            write(f.local(p), &format!("content {i}"), 100);
        }
        assert!(e.full_scan().errors.is_empty());
        fs::rename(f.local("Docs"), f.local("docs")).unwrap();
        let r = e.full_scan();
        assert!(r.errors.is_empty() && r.aborted.is_none(), "{r:?}");
        assert_eq!(cloud_root_names(&f), ["docs"], "no `Docs`, no temporary name left");
        for (i, p) in names.iter().take(files).enumerate() {
            let new = p.replace("Docs", "docs");
            assert_eq!(fs::read_to_string(f.remote(&new)).unwrap(), format!("content {i}"));
            assert!(e.db.base(&new).unwrap().is_some() && e.db.base(p).unwrap().is_none());
        }
        assert_eq!(Folding::n(&e.remote.deletes), 0);
        assert_eq!(
            Folding::n(&e.remote.dir_moves),
            2,
            "the folder went there and back to the new name"
        );
        assert_eq!(e.full_scan().transferred, 0);
    }
}

#[test]
fn a_file_the_cloud_will_not_rename_by_case_directly_goes_through_a_temporary_name() {
    let (f, mut e) = folding_fixture("case-file-tmp");
    e.remote.direct_case_fails = true;
    write(f.local("MyFile.txt"), "the only copy", 100);
    assert!(e.full_scan().errors.is_empty());
    fs::rename(f.local("MyFile.txt"), f.local("myfile.txt")).unwrap();
    let r = e.full_scan();
    assert!(r.errors.is_empty(), "{r:?}");
    assert_eq!(cloud_root_names(&f), ["myfile.txt"]);
    assert_eq!(fs::read_to_string(f.remote("myfile.txt")).unwrap(), "the only copy");
    assert_eq!(Folding::n(&e.remote.deletes), 0);
    assert_eq!(e.full_scan().transferred, 0);
}

#[test]
fn a_case_rename_the_cloud_answers_ok_to_but_does_not_do_writes_no_row() {
    // The outcome that would leave the DB and the cloud disagreeing for ever: "ok", and the old spelling stays.
    let (f, mut e) = folding_fixture("case-noop");
    write(f.local("MyFile.txt"), "the only copy", 100);
    assert!(e.full_scan().errors.is_empty());
    e.remote.noop_renames = true;
    fs::rename(f.local("MyFile.txt"), f.local("myfile.txt")).unwrap();
    let r = e.full_scan();
    assert!(r.errors.iter().any(|m| m.contains("kept the old spelling")), "{r:?}");
    assert!(r.skipped == 0, "not a clash: {r:?}");
    assert_eq!(cloud_root_names(&f), ["MyFile.txt"]);
    assert_eq!(Folding::n(&e.remote.deletes), 0);
    // The old row stays, so the pair is looked at again (and not skipped) at the next scan.
    assert!(e.db.base("MyFile.txt").unwrap().is_some() && e.db.base("myfile.txt").unwrap().is_none());
    // The cloud then does it: the same pair now finishes.
    e.remote.noop_renames = false;
    let r = e.full_scan();
    assert!(r.errors.is_empty() && r.skipped == 0, "{r:?}");
    assert_eq!(cloud_root_names(&f), ["myfile.txt"]);
    assert_eq!(e.full_scan().transferred, 0);
}

#[test]
fn a_folder_renamed_by_case_with_something_else_in_it_in_the_cloud_is_a_clear_error_not_a_row() {
    // `Docs` also holds a file we do not know yet: the folder cannot be renamed as a whole, and moving `a.txt`
    // alone cannot change the folder's name.
    let (f, mut e) = folding_fixture("case-folder-extra");
    write(f.local("Docs/a.txt"), "mine", 100);
    assert!(e.full_scan().errors.is_empty());
    write(f.remote("Docs/extra.txt"), "added elsewhere", 100);
    fs::rename(f.local("Docs"), f.local("docs")).unwrap();
    let r = e.full_scan();
    assert!(r.errors.iter().any(|m| m.contains("renamed by case alone")), "{r:?}");
    assert!(e.db.base("Docs/a.txt").unwrap().is_some() && e.db.base("docs/a.txt").unwrap().is_none());
    assert_eq!(fs::read_to_string(f.remote("Docs/a.txt")).unwrap(), "mine");
    assert_eq!(Folding::n(&e.remote.deletes), 0);
}

#[test]
fn a_rename_through_a_temporary_name_that_fails_halfway_is_undone() {
    let (f, mut e) = folding_fixture("case-file-undo");
    e.remote.direct_case_fails = true;
    write(f.local("MyFile.txt"), "the only copy", 100);
    assert!(e.full_scan().errors.is_empty());
    fs::rename(f.local("MyFile.txt"), f.local("myfile.txt")).unwrap();
    // Calls: 1 direct (refused), 2 to the temporary name, 3 on to the new name (fails), 4 back (the undo).
    e.remote.fail_rename_number = 3;
    let r = e.full_scan();
    assert!(!r.errors.is_empty(), "{r:?}");
    assert_eq!(
        cloud_root_names(&f),
        ["MyFile.txt"],
        "back under its own name, nothing left behind"
    );
    assert_eq!(fs::read_to_string(f.remote("MyFile.txt")).unwrap(), "the only copy");
    assert_eq!(Folding::n(&e.remote.deletes), 0);
}

#[test]
fn a_case_rename_never_overwrites_another_file_already_holding_the_new_name() {
    // A cloud that allows both names (Drive): another device put a different `docs/myfile.txt` there.
    let f = Fixture::new("case-rename-theirs");
    let mut e = Engine::new(
        f.engine.root.clone(),
        Counting::new(&f.engine.remote.0, false),
        Db::memory().unwrap(),
    );
    write(f.local("docs/MyFile.txt"), "mine", 100);
    assert!(e.full_scan().errors.is_empty());
    write(f.remote("docs/myfile.txt"), "theirs, longer", 50);
    fs::rename(f.local("docs/MyFile.txt"), f.local("docs/myfile.txt")).unwrap();
    let r = e.sync_paths(&["docs/MyFile.txt".to_string(), "docs/myfile.txt".to_string()]);
    assert!(r.aborted.is_none(), "{r:?}");
    assert_eq!(Counting::n(&e.remote.renames), 0, "nothing was moved onto their file");
    assert_eq!(
        fs::read_to_string(f.remote("docs/myfile.txt")).unwrap(),
        "theirs, longer"
    );
    // Mine is kept too, as an ordinary conflict copy.
    let all: Vec<String> = fs::read_dir(f.remote("docs"))
        .unwrap()
        .flatten()
        .map(|e| fs::read_to_string(e.path()).unwrap())
        .collect();
    assert!(all.iter().any(|t| t == "mine"), "{all:?}");
}

#[test]
fn a_case_rename_the_cloud_cannot_do_is_an_error_that_keeps_the_only_copy() {
    let (f, mut e) = folding_fixture("case-rename-fail");
    write(f.local("MyFile.txt"), "the only copy", 100);
    assert!(e.full_scan().errors.is_empty());
    e.remote.no_rename = true;
    fs::rename(f.local("MyFile.txt"), f.local("myfile.txt")).unwrap();
    let r = e.full_scan();
    assert!(!r.errors.is_empty(), "shown, retried: {r:?}");
    // No upload-then-delete fallback: that would have removed the only copy.
    assert_eq!(Folding::n(&e.remote.deletes), 0);
    assert_eq!(fs::read_to_string(f.remote("MyFile.txt")).unwrap(), "the only copy");
}

#[test]
fn a_second_file_differing_only_by_case_becomes_a_case_conflict_in_both_places() {
    let (f, mut e) = folding_fixture("case-conflict");
    write(f.local("MyFile.txt"), "first", 100);
    assert!(e.full_scan().errors.is_empty());
    write(f.local("myfile.txt"), "second", 50);
    let r = e.full_scan();
    assert!(r.errors.is_empty(), "{r:?}");
    assert_eq!(r.conflicts, ["myfile (Case Conflict).txt"]);
    assert_eq!(fs::read_to_string(f.local("MyFile.txt")).unwrap(), "first");
    assert_eq!(
        fs::read_to_string(f.local("myfile (Case Conflict).txt")).unwrap(),
        "second"
    );
    assert!(!f.local("myfile.txt").exists());
    assert_eq!(fs::read_to_string(f.remote("MyFile.txt")).unwrap(), "first");
    assert_eq!(
        fs::read_to_string(f.remote("myfile (Case Conflict).txt")).unwrap(),
        "second"
    );
    assert_eq!(Folding::n(&e.remote.deletes), 0);
    assert_eq!(e.full_scan().transferred, 0);
}

#[test]
fn a_cloud_that_keeps_both_spellings_gets_both_files_as_they_are() {
    // Drive and MEGA: `Makefile` and `makefile` are two files, so nothing is renamed or skipped.
    let mut f = Fixture::new("case-sensitive-cloud");
    f.engine.case_blind = false;
    write(f.local("Makefile"), "one", 100);
    write(f.local("makefile"), "two", 100);
    let r = f.engine.full_scan();
    assert!(r.errors.is_empty() && r.conflicts.is_empty(), "{r:?}");
    assert_eq!(fs::read_to_string(f.remote("Makefile")).unwrap(), "one");
    assert_eq!(fs::read_to_string(f.remote("makefile")).unwrap(), "two");
    assert_eq!(f.engine.full_scan().transferred, 0);
}

#[test]
fn a_new_file_beside_a_cloud_only_namesake_is_renamed_before_it_can_overwrite_it() {
    // The cloud has `Report.txt` (not downloaded yet, no row); here `report.txt` appears. A watcher run only
    // sees the new name: the cloud's answer for it is the other spelling.
    let (f, mut e) = folding_fixture("case-cloud-only");
    write(f.remote("Report.txt"), "theirs", 100);
    write(f.local("report.txt"), "mine", 50);
    let r = e.sync_paths(&["report.txt".to_string()]);
    assert!(r.errors.is_empty(), "{r:?}");
    assert_eq!(r.conflicts, ["report (Case Conflict).txt"]);
    assert_eq!(fs::read_to_string(f.remote("Report.txt")).unwrap(), "theirs");
    assert_eq!(
        fs::read_to_string(f.remote("report (Case Conflict).txt")).unwrap(),
        "mine"
    );
}

#[test]
fn a_clash_nothing_can_settle_is_shown_but_never_retried_and_an_unattributed_error_needs_a_full_scan() {
    // Two files of a mirror: the cloud is the truth, nothing is renamed there, the names are left alone.
    let mut f = Fixture::new("skips");
    f.engine.download_only = true;
    write(f.local("Dup.txt"), "one", 100);
    write(f.local("dup.txt"), "two", 100);
    let r = f.engine.full_scan();
    assert!(!r.errors.is_empty(), "{r:?}");
    assert!(r.failed.is_empty() && !r.has_global_failure(), "{r:?}");
    let mut r = Report::default();
    r.errors.push("listing the cloud failed".into());
    assert!(r.has_global_failure(), "anything not counted is not about one path");
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
    fn make_dir(&self, rel: &str) -> Result<()> {
        self.inner.make_dir(rel)
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
fn a_renamed_folder_leaves_no_old_cloud_folder_but_one_emptied_by_a_delete_stays() {
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
    assert!(
        !f.remote("a/b").exists(),
        "the folder the user renamed is gone from the cloud"
    );
    // Only a deleted folder is removed: one that just lost its last file stays, as in the official clients.
    assert!(f.remote("z").is_dir(), "the emptied folder stays in the cloud");
    assert!(f.local("z").is_dir());
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

#[test]
fn in_a_conflict_the_older_copy_gets_the_conflict_name() {
    // The local edit is newer: it keeps the name and goes up over the cloud's; the cloud's version is kept beside it.
    let mut f = Fixture::new("conflict-local-newer");
    write(f.local("r.txt"), "mine", 10);
    write(f.remote("r.txt"), "theirs!", 20);
    let r = f.engine.full_scan();
    assert!(r.errors.is_empty(), "{r:?}");
    assert_eq!(r.conflicts, ["r.txt"]);
    let name = conflict_name("r.txt", &today(), 1);
    assert_eq!(fs::read_to_string(f.local("r.txt")).unwrap(), "mine");
    assert_eq!(fs::read_to_string(f.local(&name)).unwrap(), "theirs!");
    assert_eq!(
        fs::read_to_string(f.remote("r.txt")).unwrap(),
        "mine",
        "the newer one goes up"
    );
    assert_eq!(
        fs::read_to_string(f.remote(&name)).unwrap(),
        "theirs!",
        "the older one is kept in the cloud too"
    );
    // Settled: the next scan finds nothing to do and makes no second copy.
    let r = f.engine.full_scan();
    assert!(
        r.errors.is_empty() && r.conflicts.is_empty() && r.transferred == 0,
        "{r:?}"
    );
    assert!(!f.local(&conflict_name("r.txt", &today(), 2)).exists());
}

#[test]
fn a_conflict_within_the_providers_whole_seconds_keeps_the_cloud_name() {
    let mut f = Fixture::new("conflict-tie");
    write(f.local("r.txt"), "mine", 10);
    write(f.remote("r.txt"), "theirs!", 11);
    let r = f.engine.full_scan();
    assert!(r.errors.is_empty(), "{r:?}");
    assert_eq!(fs::read_to_string(f.local("r.txt")).unwrap(), "theirs!");
    assert_eq!(
        fs::read_to_string(f.local(&conflict_name("r.txt", &today(), 1))).unwrap(),
        "mine"
    );
}

#[test]
fn the_cloud_copy_that_takes_the_name_in_a_conflict_keeps_the_local_mode() {
    use std::os::unix::fs::PermissionsExt;
    let mut f = Fixture::new("conflict-mode");
    write(f.local("s.sh"), "mine", 10);
    fs::set_permissions(f.local("s.sh"), fs::Permissions::from_mode(0o755)).unwrap();
    write(f.remote("s.sh"), "theirs!", 11);
    let r = f.engine.full_scan();
    assert!(r.errors.is_empty(), "{r:?}");
    assert_eq!(fs::read_to_string(f.local("s.sh")).unwrap(), "theirs!");
    assert_eq!(
        fs::metadata(f.local("s.sh")).unwrap().permissions().mode() & 0o777,
        0o755
    );
}

/// A cloud whose uploads fail while `fail` is set.
struct FailUp(DirRemote, std::sync::Arc<std::sync::atomic::AtomicBool>);

impl Remote for FailUp {
    fn list(&self) -> Result<Vec<Item>> {
        self.0.list()
    }
    fn stat(&self, rel: &str) -> Result<Option<Item>> {
        self.0.stat(rel)
    }
    fn upload(&self, root: &Path, rel: &str) -> Result<()> {
        if self.1.load(Relaxed) {
            return Err("connection lost".into());
        }
        self.0.upload(root, rel)
    }
    fn make_dir(&self, rel: &str) -> Result<()> {
        self.0.make_dir(rel)
    }
    fn download(&self, rel: &str, dest: &Path) -> Result<()> {
        self.0.download(rel, dest)
    }
    fn delete(&self, rel: &str) -> Result<()> {
        self.0.delete(rel)
    }
}

#[test]
fn a_newer_local_file_whose_upload_failed_is_an_ordinary_edit_next_time_not_a_second_conflict() {
    let f = Fixture::new("conflict-upload-fails");
    write(f.local("r.txt"), "mine", 10);
    write(f.remote("r.txt"), "theirs!", 20);
    let fail = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(true));
    let mut e = Engine::new(
        f.engine.root.clone(),
        FailUp(DirRemote(f.engine.remote.0.clone()), fail.clone()),
        Db::memory().unwrap(),
    );
    let r = e.full_scan();
    assert!(!r.errors.is_empty(), "the failed upload is reported");
    assert_eq!(
        fs::read_to_string(f.local("r.txt")).unwrap(),
        "mine",
        "the newer file is never touched"
    );
    assert_eq!(fs::read_to_string(f.remote("r.txt")).unwrap(), "theirs!");
    fail.store(false, Relaxed);
    let r = e.full_scan();
    assert!(r.errors.is_empty() && r.conflicts.is_empty(), "{r:?}");
    assert_eq!(
        fs::read_to_string(f.remote("r.txt")).unwrap(),
        "mine",
        "the retry is a plain upload"
    );
    let again = conflict_name("r.txt", &today(), 2);
    assert!(
        !f.local(&again).exists() && !f.remote(&again).exists(),
        "no second copy of the old version"
    );
}

/// A cloud whose downloads all fail.
struct NoDown(DirRemote);

impl Remote for NoDown {
    fn list(&self) -> Result<Vec<Item>> {
        self.0.list()
    }
    fn stat(&self, rel: &str) -> Result<Option<Item>> {
        self.0.stat(rel)
    }
    fn upload(&self, root: &Path, rel: &str) -> Result<()> {
        self.0.upload(root, rel)
    }
    fn make_dir(&self, rel: &str) -> Result<()> {
        self.0.make_dir(rel)
    }
    fn download(&self, _rel: &str, _dest: &Path) -> Result<()> {
        Err("connection lost".into())
    }
    fn delete(&self, rel: &str) -> Result<()> {
        self.0.delete(rel)
    }
}

#[test]
fn a_conflict_whose_download_fails_still_keeps_the_local_edit() {
    let f = Fixture::new("conflict-fail");
    write(f.local("r.txt"), "mine", 20);
    write(f.remote("r.txt"), "theirs!", 10);
    let mut e = Engine::new(
        f.engine.root.clone(),
        NoDown(DirRemote(f.engine.remote.0.clone())),
        Db::memory().unwrap(),
    );
    let r = e.full_scan();
    assert!(!r.errors.is_empty(), "the failed download is reported");
    let aside = conflict_name("r.txt", &today(), 1);
    assert_eq!(
        fs::read_to_string(f.local(&aside)).unwrap(),
        "mine",
        "the edit is only renamed"
    );
    assert_eq!(fs::read_to_string(f.remote("r.txt")).unwrap(), "theirs!");
    assert!(e.db.base("r.txt").unwrap().is_none(), "nothing was recorded as synced");
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
    let (files, _, bad) = scan(&f.engine.root, &[], &Rules::default());
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
    crate::trash::purge_trash_at(&root, 90, now).unwrap();
    // 91 and exactly 90 days old go; a day old and a folder with a non-numeric name stay.
    assert_eq!(trash_usage(&root).0, 2);
    assert!(f.local(".fastsync/trash/junk/keep.txt").exists());
    assert!(purge_trash(&root.join("missing"), 90).is_ok());
}

#[test]
fn a_file_left_in_set_aside_by_a_crash_goes_into_a_dated_trash_batch() {
    let f = Fixture::new("set-aside");
    let root = f.engine.root.clone();
    write(f.local(".fastsync/trash/set-aside/1-2-3.old"), "the user's file", 100);
    assert_eq!(crate::trash::recover_set_aside(&root), 1);
    assert!(!f.local(".fastsync/trash/set-aside").exists());
    // Counted and aged like any trashed file now.
    assert_eq!(trash_usage(&root).0, 1);
    assert_eq!(crate::trash::recover_set_aside(&root), 0);
}

#[test]
fn a_failed_commit_is_rolled_back_so_later_writes_are_not_swallowed() {
    let db = Db::memory().unwrap();
    assert!(db.begin());
    db.set_meta("a", "1").unwrap();
    assert!(db.commit().is_ok());
    // Nothing open: COMMIT fails, and the connection stays in autocommit, so this write is kept.
    assert!(db.commit().is_err());
    db.set_meta("b", "2").unwrap();
    assert_eq!(db.meta("b").unwrap().as_deref(), Some("2"));
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

#[test]
fn only_a_stop_over_local_deletes_asks_for_a_full_scan_to_find_cloud_moves() {
    // Files lost in the cloud (maybe a folder renamed there): a full scan can pair them.
    let mut f = cloud_loses("stop-cloud", 12);
    let paths: Vec<String> = (0..12).map(|i| format!("f{i}.txt")).collect();
    let r = f.engine.sync_paths(&paths);
    assert!(r.aborted.is_some() && r.aborted_local_deletes, "{r:?}");
    // The user's own deletes: nothing a full scan could turn into a move.
    let mut f = Fixture::new("stop-local");
    for i in 0..12 {
        write(f.local(&format!("f{i}.txt")), "x", 100);
    }
    f.engine.full_scan();
    for i in 0..12 {
        fs::remove_file(f.local(&format!("f{i}.txt"))).unwrap();
    }
    let r = f.engine.sync_paths(&paths);
    assert!(r.aborted.is_some() && !r.aborted_local_deletes, "{r:?}");
}

/// 30 files at the top and 12 in `d/`, all synced.
fn folder_synced(name: &str) -> Fixture {
    let mut f = Fixture::new(name);
    // On disk, so a test can read the activity log back.
    f.engine.db = Db::open(&f.base.join("state.db")).unwrap();
    for i in 0..30 {
        write(f.local(&format!("top{i}.txt")), "x", 100);
    }
    fs::create_dir(f.local("d")).unwrap();
    for i in 0..12 {
        write(f.local(&format!("d/f{i}.txt")), "x", 100);
    }
    f.engine.full_scan();
    f
}

#[test]
fn a_folder_the_user_deleted_is_not_a_wiped_folder() {
    let mut f = folder_synced("wipe-deleted");
    fs::remove_dir_all(f.local("d")).unwrap();
    let paths: Vec<String> = (0..12).map(|i| format!("d/f{i}.txt")).collect();
    let r = f.engine.sync_paths(&paths);
    assert!(r.aborted.is_none() && r.errors.is_empty(), "{r:?}");
    assert!(!f.remote("d/f0.txt").exists());
}

#[test]
fn a_folder_left_empty_looks_like_an_unmounted_drive_and_stops_with_its_name() {
    let mut f = folder_synced("wipe-empty");
    for i in 0..12 {
        fs::remove_file(f.local(&format!("d/f{i}.txt"))).unwrap();
    }
    let paths: Vec<String> = (0..12).map(|i| format!("d/f{i}.txt")).collect();
    let r = f.engine.sync_paths(&paths);
    assert!(r.aborted.is_some(), "{r:?}");
    assert!(f.remote("d/f0.txt").exists());
    let rows = crate::db::read_log(&f.base.join("state.db"), 10).unwrap();
    assert_eq!(
        rows.iter().find(|e| e.kind == "stopped").map(|e| e.path.as_str()),
        Some("folder:d")
    );
}

#[test]
fn a_folder_wiped_in_the_cloud_still_stops_though_it_exists_here() {
    let mut f = folder_synced("wipe-cloud");
    for i in 0..12 {
        fs::remove_file(f.remote(&format!("d/f{i}.txt"))).unwrap();
    }
    let paths: Vec<String> = (0..12).map(|i| format!("d/f{i}.txt")).collect();
    let r = f.engine.sync_paths(&paths);
    assert!(r.aborted.is_some() && r.aborted_local_deletes, "{r:?}");
    assert!(f.local("d/f0.txt").exists());
    let rows = crate::db::read_log(&f.base.join("state.db"), 10).unwrap();
    assert_eq!(
        rows.iter().find(|e| e.kind == "stopped").map(|e| e.path.as_str()),
        Some("folder:d")
    );
}

#[test]
fn a_folder_deleted_in_the_cloud_is_trashed_without_asking() {
    let mut f = folder_synced("wipe-cloud-gone");
    fs::remove_dir_all(f.remote("d")).unwrap();
    let paths: Vec<String> = (0..12).map(|i| format!("d/f{i}.txt")).collect();
    let r = f.engine.sync_paths(&paths);
    assert!(r.aborted.is_none() && r.errors.is_empty(), "{r:?}");
    assert!(!f.local("d/f0.txt").exists());
    assert!(f.local(".fastsync/trash").exists());
}

#[test]
fn a_folder_renamed_in_the_cloud_without_its_new_name_does_not_stop() {
    let mut f = folder_synced("wipe-cloud-renamed");
    fs::rename(f.remote("d"), f.remote("e")).unwrap();
    // Only the old names reach the run: the feed has not brought the new ones yet.
    let paths: Vec<String> = (0..12).map(|i| format!("d/f{i}.txt")).collect();
    let r = f.engine.sync_paths(&paths);
    assert!(r.aborted.is_none(), "{r:?}");
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
fn confirmed_deletes_are_limited_to_the_paths_the_stopped_run_held() {
    let mut f = cloud_loses("confirm-paths", 8);
    let set = |names: &[usize]| names.iter().map(|i| format!("f{i}.txt")).collect();
    // Same count as the card showed (8), but f7 was not on it and f9 was: another file went missing meanwhile.
    f.engine.confirmed_deletes = Some(8);
    f.engine.confirmed_paths = Some(set(&[0, 1, 2, 3, 4, 5, 6, 9]));
    let r = f.engine.full_scan();
    assert!(r.aborted.is_some(), "{r:?}");
    assert!(f.local("f7.txt").exists(), "nothing was deleted");
    f.engine.confirmed_paths = Some(set(&[0, 1, 2, 3, 4, 5, 6, 7]));
    let r = f.engine.full_scan();
    assert!(r.aborted.is_none() && !f.local("f7.txt").exists(), "{r:?}");
}

/// A cloud that records every `stat` and fails it for the path `fail`.
struct Probe {
    inner: DirRemote,
    fail: &'static str,
    seen: std::sync::Mutex<Vec<String>>,
}

impl Remote for Probe {
    fn list(&self) -> Result<Vec<Item>> {
        self.inner.list()
    }
    fn stat(&self, rel: &str) -> Result<Option<Item>> {
        self.seen.lock().unwrap().push(rel.to_string());
        if rel == self.fail {
            return Err("boom".into());
        }
        self.inner.stat(rel)
    }
    fn upload(&self, root: &Path, rel: &str) -> Result<()> {
        self.inner.upload(root, rel)
    }
    fn make_dir(&self, rel: &str) -> Result<()> {
        self.inner.make_dir(rel)
    }
    fn download(&self, rel: &str, dest: &Path) -> Result<()> {
        self.inner.download(rel, dest)
    }
    fn delete(&self, rel: &str) -> Result<()> {
        self.inner.delete(rel)
    }
}

/// The synced fixture's engine, with a `Probe` for a cloud (same folders, same DB).
fn probed(f: &mut Fixture, fail: &'static str) -> Engine<Probe> {
    let db = std::mem::replace(&mut f.engine.db, Db::memory().unwrap());
    let remote = Probe {
        inner: DirRemote(f.base.join("remote")),
        fail,
        seen: Default::default(),
    };
    Engine::new(f.engine.root.clone(), remote, db)
}

#[test]
fn a_cloud_that_cannot_say_whether_the_folder_is_gone_still_stops() {
    let mut f = folder_synced("wipe-stat-err");
    fs::remove_dir_all(f.remote("d")).unwrap();
    let mut e = probed(&mut f, "d");
    let paths: Vec<String> = (0..12).map(|i| format!("d/f{i}.txt")).collect();
    let r = e.sync_paths(&paths);
    assert!(r.aborted.is_some(), "{r:?}");
    assert!(f.local("d/f0.txt").exists());
}

#[test]
fn a_subfolder_of_a_folder_gone_from_the_cloud_is_not_asked_about_again() {
    let mut f = folder_synced("wipe-one-lookup");
    for i in 0..12 {
        write(f.local(&format!("d/s/g{i}.txt")), "x", 100);
    }
    f.engine.full_scan();
    fs::remove_dir_all(f.remote("d")).unwrap();
    let mut e = probed(&mut f, "");
    let paths: Vec<String> = (0..12)
        .map(|i| format!("d/f{i}.txt"))
        .chain((0..12).map(|i| format!("d/s/g{i}.txt")))
        .collect();
    let r = e.sync_paths(&paths);
    assert!(r.aborted.is_none(), "{r:?}");
    let seen = e.remote.seen.lock().unwrap();
    assert!(seen.iter().any(|p| p == "d"), "{seen:?}");
    assert!(!seen.iter().any(|p| p == "d/s"), "{seen:?}");
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
fn thousands_of_adopted_files_all_get_their_row_across_several_transactions() {
    let mut f = Fixture::new("adopt-batches");
    for i in 0..ADOPT_BATCH * 2 + 100 {
        write(f.local(&format!("d/{i}.txt")), "x", 100);
    }
    f.engine.full_scan();
    // A new database, as for an account whose folder already matches the cloud: every file is adopted.
    f.engine.db = Db::memory().unwrap();
    f.engine.files.take_dirty();
    let r = f.engine.full_scan();
    assert!(r.errors.is_empty() && r.transferred == 0, "{r:?}");
    assert_eq!(f.engine.db.count().unwrap(), ADOPT_BATCH * 2 + 100);
    // The file manager hears about every one of them, the last, still-open batch included (and only after its commit).
    let (dirty, all) = f.engine.files.take_dirty();
    // (+1: their folder `d`, which `touch` marks too)
    assert!(all || dirty.len() == ADOPT_BATCH * 2 + 100 + 1, "{}", dirty.len());
}

#[test]
fn a_walk_from_a_subfolder_applies_the_rules_and_unchecked_folders_to_root_paths() {
    let f = Fixture::new("scan-from");
    write(f.local("d/keep.txt"), "k", 100);
    write(f.local("d/build/o.bin"), "b", 100);
    write(f.local("d/off/x.txt"), "x", 100);
    let (files, dirs, bad) = scan_from(&f.local("d"), "d", &["d/off".to_string()], &Rules::parse("build/"));
    assert_eq!(files.keys().collect::<Vec<_>>(), ["d/keep.txt"]);
    assert_eq!(dirs.into_iter().collect::<Vec<_>>(), Vec::<String>::new());
    assert!(bad.is_empty());
}

/// Writes down the order of uploads and deletes.
struct Ordered(DirRemote, std::sync::Mutex<Vec<String>>);
impl Remote for Ordered {
    fn list(&self) -> Result<Vec<Item>> {
        self.0.list()
    }
    fn stat(&self, rel: &str) -> Result<Option<Item>> {
        self.0.stat(rel)
    }
    fn upload(&self, root: &Path, rel: &str) -> Result<()> {
        self.1.lock().unwrap().push(format!("up {rel}"));
        self.0.upload(root, rel)
    }
    fn make_dir(&self, rel: &str) -> Result<()> {
        self.0.make_dir(rel)
    }
    fn download(&self, rel: &str, dest: &Path) -> Result<()> {
        self.0.download(rel, dest)
    }
    fn delete(&self, rel: &str) -> Result<()> {
        self.1.lock().unwrap().push(format!("del {rel}"));
        self.0.delete(rel)
    }
}

#[test]
fn new_rules_go_up_before_the_cloud_deletes_they_cause() {
    let f = Fixture::new("ignore-order");
    let remote = DirRemote(f.engine.remote.0.clone());
    let mut e = Engine::new(
        f.engine.root.clone(),
        Ordered(remote, Default::default()),
        Db::memory().unwrap(),
    );
    write(f.local("a.log"), "l", 100);
    e.full_scan();
    e.remote.1.lock().unwrap().clear();
    // Another machine that sees the delete before the rules would trash its own copy.
    write(f.local(RULES), "*.log", 50);
    e.ignore = Rules::parse("*.log");
    let r = e.full_scan();
    assert!(r.errors.is_empty(), "{r:?}");
    assert_eq!(
        *e.remote.1.lock().unwrap(),
        [format!("up {RULES}"), "del a.log".to_string()]
    );
}

#[test]
fn a_run_that_brings_new_rules_in_leaves_its_local_deletes_for_the_next_run() {
    let mut f = Fixture::new("ignore-arrives");
    write(f.local("a.log"), "l", 100);
    f.engine.full_scan();
    // Another machine added the rule: its run deleted the cloud copy and uploaded the rules.
    fs::remove_file(f.remote("a.log")).unwrap();
    write(f.remote(RULES), "*.log", 50);
    let r = f.engine.full_scan();
    assert!(f.local("a.log").exists(), "not trashed: {r:?}");
    assert_eq!(r.later, ["a.log"]);
    assert!(r.errors.is_empty(), "no red card: {r:?}");
    // The runner reads the new rules before the next run: the file is then just forgotten, still here.
    f.engine.ignore = Rules::parse("*.log");
    let r = f.engine.full_scan();
    assert!(f.local("a.log").exists() && r.errors.is_empty(), "{r:?}");
    assert!(f.engine.db.base("a.log").unwrap().is_none());
}

#[test]
fn a_folder_hidden_by_a_rule_is_not_walked_but_still_handled_like_ignored_files() {
    let mut f = Fixture::new("ignore-dir");
    write(f.local("build/o.bin"), "l", 100);
    write(f.local("src/a.rs"), "s", 100);
    let rules = Rules::parse("build/");
    let (files, _, _) = scan(&f.engine.root, &[], &rules);
    assert_eq!(files.keys().collect::<Vec<_>>(), ["src/a.rs"], "build/ is not entered");
    // Synced before the rule: the cloud copy goes, the local file stays, and the walk does not call it deleted.
    f.engine.full_scan();
    assert!(f.remote("build/o.bin").exists());
    f.engine.ignore = rules;
    let r = f.engine.full_scan();
    assert!(r.errors.is_empty(), "{:?}", r.errors);
    assert!(!f.remote("build/o.bin").exists() && f.local("build/o.bin").exists());
    assert!(f.remote("src/a.rs").exists());
    assert!(f.engine.local_changes().is_empty());
    // A folder event inside the hidden folder (an `npm install`) neither walks it nor uploads anything.
    write(f.local("build/new/x.bin"), "n", 100);
    let r = f.engine.sync_paths(&["build/new".to_string()]);
    assert!(r.errors.is_empty() && r.transferred == 0, "{r:?}");
    assert!(!f.remote("build/new/x.bin").exists());
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

#[test]
fn native_client_files_are_ignored() {
    for name in [
        ".dropbox",
        ".dropbox.cache",
        ".dropbox.attr",
        ".tmp.drivedownload",
        ".tmp.driveupload",
        ".debris",
        ".DS_Store",
        "Thumbs.db",
        "Desktop.ini",
        "Icon\r",
        "~$report.docx",
    ] {
        assert!(
            ignored(name) && !safe_rel(name) && !safe_rel(&format!("a/{name}/x")),
            "{name}"
        );
    }
    for name in ["rules.dropboxignore", ".megaignore", "a~$b.txt", "dropbox.txt"] {
        assert!(!ignored(name), "{name}");
    }
}

#[test]
fn trashing_the_same_path_twice_keeps_both_copies() {
    let f = Fixture::new("trash-twice");
    let root = f.engine.root.clone();
    for text in ["one", "two"] {
        write(f.local("a.txt"), text, 10);
        trash_move(&root, &f.local("a.txt"), "a.txt").unwrap();
    }
    assert!(!f.local("a.txt").exists());
    assert_eq!(trash_usage(&root).0, 2);
}

#[test]
fn an_overwriting_download_replaces_the_file_and_leaves_no_set_aside_copy() {
    let f = Fixture::new("overwrite-clean");
    write(f.local("a.txt"), "old", 100);
    write(f.remote("a.txt"), "new content", 10);
    let item = f.engine.remote.stat("a.txt").unwrap().unwrap();
    let expect = local_sig(&f.engine.root, "a.txt");
    fetch(&f.engine.remote, &f.engine.root, "a.txt", expect, &item, None).unwrap();
    assert_eq!(fs::read_to_string(f.local("a.txt")).unwrap(), "new content");
    let tmp = f.engine.root.join(HIDDEN).join("tmp");
    assert_eq!(fs::read_dir(tmp).unwrap().count(), 0);
}

#[test]
fn an_empty_folder_goes_up_and_an_empty_cloud_folder_comes_down() {
    let mut f = Fixture::new("dirs-empty");
    fs::create_dir_all(f.local("mine/inner")).unwrap();
    fs::create_dir_all(f.remote("theirs")).unwrap();
    let r = f.engine.full_scan();
    assert!(r.errors.is_empty(), "{r:?}");
    assert!(f.remote("mine/inner").is_dir() && f.local("theirs").is_dir());
    // Both are synced now: a second scan changes nothing.
    assert!(f.engine.full_scan().errors.is_empty());
    assert!(f.local("mine/inner").is_dir() && f.remote("theirs").is_dir());
}

#[test]
fn a_folder_the_user_deletes_is_deleted_on_the_other_side_and_one_emptied_is_not() {
    let mut f = Fixture::new("dirs-delete");
    for d in ["here", "there", "kept"] {
        fs::create_dir_all(f.local(d)).unwrap();
    }
    write(f.local("kept/a.txt"), "a", 100);
    assert!(f.engine.full_scan().errors.is_empty());
    fs::remove_dir(f.local("here")).unwrap();
    fs::remove_dir(f.remote("there")).unwrap();
    // The last file of `kept` goes: the folder stays on both sides.
    fs::remove_file(f.local("kept/a.txt")).unwrap();
    let r = f.engine.full_scan();
    assert!(r.errors.is_empty(), "{r:?}");
    assert!(!f.remote("here").exists() && !f.local("there").exists());
    assert!(f.local("kept").is_dir() && f.remote("kept").is_dir());
    // And through a path run (the watcher's way).
    fs::create_dir_all(f.local("w")).unwrap();
    assert!(f.engine.sync_paths(&["w".into()]).errors.is_empty());
    assert!(f.remote("w").is_dir());
    fs::remove_dir(f.local("w")).unwrap();
    assert!(f.engine.sync_paths(&["w".into()]).errors.is_empty());
    assert!(!f.remote("w").exists());
}

#[test]
fn a_folder_removed_in_the_cloud_that_still_holds_a_new_file_stays_and_the_file_goes_up() {
    let mut f = Fixture::new("dirs-not-empty");
    fs::create_dir_all(f.local("d")).unwrap();
    assert!(f.engine.full_scan().errors.is_empty());
    fs::remove_dir(f.remote("d")).unwrap();
    write(f.local("d/new.txt"), "new", 100);
    let r = f.engine.full_scan();
    assert!(r.errors.is_empty(), "{r:?}");
    assert!(f.local("d/new.txt").exists(), "nothing of ours is removed");
    assert!(f.remote("d/new.txt").exists());
}

#[test]
fn unchecked_and_ignored_folders_are_left_alone() {
    let mut f = Fixture::new("dirs-skip");
    f.engine.excluded = vec!["off".into()];
    f.engine.ignore = Rules::parse("build/");
    fs::create_dir_all(f.local("off/x")).unwrap();
    fs::create_dir_all(f.local("build/x")).unwrap();
    fs::create_dir_all(f.remote("off/y")).unwrap();
    fs::create_dir_all(f.remote("build/y")).unwrap();
    let r = f.engine.full_scan();
    assert!(r.errors.is_empty(), "{r:?}");
    assert!(!f.remote("off/x").exists() && !f.remote("build/x").exists());
    assert!(!f.local("off/y").exists() && !f.local("build/y").exists());
}

#[test]
fn a_mirror_never_creates_or_removes_cloud_folders() {
    let mut f = Fixture::new("dirs-mirror");
    f.engine.download_only = true;
    fs::create_dir_all(f.local("only-here")).unwrap();
    fs::create_dir_all(f.remote("only-there")).unwrap();
    let r = f.engine.full_scan();
    assert!(r.errors.is_empty(), "{r:?}");
    assert!(!f.remote("only-here").exists(), "the cloud is never written");
    assert!(
        !f.local("only-here").exists(),
        "a local-only folder goes, the cloud is the truth"
    );
    assert!(f.local("only-there").is_dir());
}

#[test]
fn folders_that_differ_only_by_case_are_left_alone() {
    let mut f = Fixture::new("dirs-case");
    fs::create_dir_all(f.local("Docs")).unwrap();
    fs::create_dir_all(f.remote("docs")).unwrap();
    let r = f.engine.full_scan();
    assert!(r.errors.is_empty(), "{r:?}");
    assert!(!f.local("docs").exists() && !f.remote("Docs").exists());
    assert!(!f.engine.db.dir_synced("Docs").unwrap() && !f.engine.db.dir_synced("docs").unwrap());
}

/// Counts `make_dir` calls; everything else is the plain folder.
struct MkdirCount(DirRemote, AtomicUsize);

impl Remote for MkdirCount {
    fn list(&self) -> Result<Vec<Item>> {
        self.0.list()
    }
    fn list_dir(&self, rel: &str) -> Result<Vec<Item>> {
        self.0.list_dir(rel)
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
    fn remove_empty_dir(&self, rel: &str) -> Result<()> {
        self.0.remove_empty_dir(rel)
    }
    fn make_dir(&self, rel: &str) -> Result<()> {
        self.1.fetch_add(1, Relaxed);
        self.0.make_dir(rel)
    }
}

#[test]
fn a_first_sync_makes_cloud_folders_only_for_the_ones_no_upload_made() {
    let f = Fixture::new("dirs-mkdir-count");
    for d in 0..20 {
        write(f.local(&format!("d{d}/deep/f.txt")), "x", 100);
    }
    fs::create_dir_all(f.local("empty/chain/end")).unwrap();
    let mut e = Engine::new(
        f.engine.root.clone(),
        MkdirCount(DirRemote(f.engine.remote.0.clone()), AtomicUsize::new(0)),
        Db::memory().unwrap(),
    );
    let r = e.full_scan();
    assert!(r.errors.is_empty(), "{r:?}");
    // `empty`, `empty/chain`, `empty/chain/end`: no upload went under them. The 40 others came with their files.
    assert_eq!(e.remote.1.load(Relaxed), 3);
    assert!(f.remote("empty/chain/end").is_dir());
    assert!(e.db.dir_synced("d7/deep").unwrap());
}

#[test]
fn hiding_a_synced_folder_with_a_rule_removes_its_empty_folders_from_the_cloud_and_keeps_the_local_ones() {
    for path_run in [false, true] {
        let mut f = Fixture::new(&format!("dirs-rule-{path_run}"));
        write(f.local("build/x/y.txt"), "y", 100);
        write(f.local("keep.txt"), "k", 100);
        fs::create_dir_all(f.local("build/e")).unwrap();
        assert!(f.engine.full_scan().errors.is_empty());
        assert!(f.remote("build/e").is_dir());
        f.engine.ignore = Rules::parse("build/");
        let r = if path_run {
            // What the runner queues for a new rule: the files it now hides.
            f.engine.sync_paths(&["build/x/y.txt".into()])
        } else {
            f.engine.full_scan()
        };
        assert!(r.errors.is_empty(), "{r:?}");
        assert!(!f.remote("build").exists(), "path_run {path_run}");
        assert!(f.local("build/x/y.txt").exists() && f.local("build/e").is_dir());
        assert!(f.remote("keep.txt").exists());
    }
}

#[test]
fn a_run_the_guard_stops_leaves_the_folders_alone() {
    let mut f = Fixture::new("dirs-guard");
    for i in 0..12 {
        write(f.local(&format!("d/f{i}.txt")), &"x".repeat(i + 1), 100);
    }
    fs::create_dir_all(f.local("empty")).unwrap();
    assert!(f.engine.full_scan().errors.is_empty());
    for i in 0..12 {
        fs::remove_file(f.remote(&format!("d/f{i}.txt"))).unwrap();
    }
    fs::remove_dir(f.remote("empty")).unwrap();
    let r = f.engine.full_scan();
    assert!(r.aborted.is_some(), "{r:?}");
    assert!(f.local("empty").is_dir() && f.local("d/f3.txt").exists());
    assert!(
        f.engine.db.dir_synced("empty").unwrap(),
        "nothing decided: the row stays"
    );
}

#[test]
fn a_cancel_stops_the_folder_actions() {
    let mut f = Fixture::new("dirs-cancel");
    f.engine.cancel.store(true, Relaxed);
    let work = DirWork {
        path: "x".into(),
        local: true,
        cloud: false,
        hidden: false,
    };
    f.engine.run_dirs(vec![work], &[], &mut Report::default());
    assert!(!f.remote("x").exists());
}

#[test]
fn a_local_folder_that_cannot_be_removed_is_a_failure_and_keeps_its_row() {
    use std::os::unix::fs::PermissionsExt;
    let mut f = Fixture::new("dirs-perm");
    fs::create_dir_all(f.local("p/d")).unwrap();
    assert!(f.engine.full_scan().errors.is_empty());
    fs::remove_dir(f.remote("p/d")).unwrap();
    fs::set_permissions(f.local("p"), fs::Permissions::from_mode(0o555)).unwrap();
    // Root (a container build) is not stopped by a read-only parent: nothing to see then.
    if fs::create_dir(f.local("p/probe")).is_ok() {
        fs::set_permissions(f.local("p"), fs::Permissions::from_mode(0o755)).unwrap();
        return;
    }
    let r = f.engine.full_scan();
    fs::set_permissions(f.local("p"), fs::Permissions::from_mode(0o755)).unwrap();
    assert_eq!(r.failed, ["p/d"], "{r:?}");
    // Forgetting it would send the folder back up to the cloud it was deleted from.
    assert!(f.engine.db.dir_synced("p/d").unwrap());
    assert!(!f.remote("p/d").exists());
}

#[test]
fn an_account_upgraded_from_version_1_still_removes_a_folder_deleted_here_in_the_cloud() {
    let f = Fixture::new("dirs-v1");
    let db = f.base.join("state.db");
    let engine = |db: &Path| {
        Engine::new(
            f.engine.root.clone(),
            DirRemote(f.engine.remote.0.clone()),
            Db::open(db).unwrap(),
        )
    };
    let mut e = engine(&db);
    write(f.local("a/x.txt"), "x", 100);
    assert!(e.full_scan().errors.is_empty());
    drop(e);
    // What a version-1 file looks like: files and no folders table.
    rusqlite::Connection::open(&db)
        .unwrap()
        .execute_batch("DROP TABLE folders; PRAGMA user_version=1")
        .unwrap();
    let mut e = engine(&db);
    fs::remove_dir_all(f.local("a")).unwrap();
    let r = e.full_scan();
    assert!(r.errors.is_empty(), "{r:?}");
    assert!(
        !f.remote("a").exists(),
        "seeded as synced, so deleted here means deleted there"
    );
}

#[test]
fn a_mirror_whose_rule_hides_a_synced_folder_leaves_the_cloud_alone() {
    let mut f = Fixture::new("dirs-hidden-mirror");
    write(f.local("build/x/y.txt"), "y", 100);
    fs::create_dir_all(f.local("build/e")).unwrap();
    assert!(f.engine.full_scan().errors.is_empty());
    f.engine.download_only = true;
    f.engine.ignore = Rules::parse("build/");
    let r = f.engine.full_scan();
    assert!(r.errors.is_empty(), "{r:?}");
    assert!(f.remote("build/e").is_dir() && f.remote("build/x/y.txt").exists());
    assert!(!f.engine.db.dir_synced("build/e").unwrap());
}

#[test]
fn a_folder_that_stays_unsettled_is_reported_by_the_walk_once_not_every_time() {
    let mut f = Fixture::new("dirs-throttle");
    // A case twin is left alone, so the local one never gets a row.
    fs::create_dir_all(f.local("Docs")).unwrap();
    fs::create_dir_all(f.remote("docs")).unwrap();
    assert!(f.engine.full_scan().errors.is_empty());
    assert_eq!(f.engine.paths_to_sync(vec![], vec![], true), ["Docs"]);
    assert!(f.engine.paths_to_sync(vec![], vec![], true).is_empty());
    // Not for ever: once the interval has passed it is reported again.
    let past = std::time::Instant::now() - DIR_REPORT_EVERY - std::time::Duration::from_secs(1);
    *f.engine.dir_reported.lock().unwrap().get_mut("Docs").unwrap() = past;
    assert_eq!(f.engine.paths_to_sync(vec![], vec![], true), ["Docs"]);
}

#[test]
fn a_folder_that_settled_is_reported_at_once_when_it_is_deleted_soon_after() {
    let mut f = Fixture::new("dirs-throttle-settled");
    fs::create_dir_all(f.local("x")).unwrap();
    assert_eq!(f.engine.paths_to_sync(vec![], vec![], true), ["x"]);
    assert!(f.engine.sync_paths(&["x".into()]).errors.is_empty());
    assert!(f.engine.paths_to_sync(vec![], vec![], true).is_empty());
    fs::remove_dir(f.local("x")).unwrap();
    assert_eq!(f.engine.paths_to_sync(vec![], vec![], true), ["x"]);
}

/// A cloud whose rmdir always fails with a wording of its own.
struct OddRmdir(DirRemote);

impl Remote for OddRmdir {
    fn list(&self) -> Result<Vec<Item>> {
        self.0.list()
    }
    fn list_dir(&self, rel: &str) -> Result<Vec<Item>> {
        self.0.list_dir(rel)
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
    fn make_dir(&self, rel: &str) -> Result<()> {
        self.0.make_dir(rel)
    }
    fn remove_empty_dir(&self, _: &str) -> Result<()> {
        Err("the service said no".into())
    }
}

#[test]
fn an_rmdir_error_without_the_usual_wording_is_judged_by_looking_inside() {
    let f = Fixture::new("dirs-odd-rmdir");
    let mut e = Engine::new(
        f.engine.root.clone(),
        OddRmdir(DirRemote(f.engine.remote.0.clone())),
        Db::memory().unwrap(),
    );
    fs::create_dir_all(f.local("full")).unwrap();
    fs::create_dir_all(f.local("hollow")).unwrap();
    assert!(e.full_scan().errors.is_empty());
    write(f.remote("full/theirs.txt"), "t", 100);
    fs::remove_dir(f.local("full")).unwrap();
    fs::remove_dir(f.local("hollow")).unwrap();
    let r = e.full_scan();
    // Something is in `full` (the other device's file): it stays, forgotten. `hollow` is empty and refused: retried.
    assert_eq!(r.failed, ["hollow"], "{r:?}");
    assert!(!e.db.dir_synced("full").unwrap() && e.db.dir_synced("hollow").unwrap());
}

#[test]
fn a_cloud_folder_behind_a_local_symlink_is_not_made_through_it() {
    let mut f = Fixture::new("dirs-symlink");
    let outside = f.base.join("outside");
    fs::create_dir_all(&outside).unwrap();
    std::os::unix::fs::symlink(&outside, f.local("lnk")).unwrap();
    fs::create_dir_all(f.remote("lnk/sub")).unwrap();
    let r = f.engine.full_scan();
    assert!(r.errors.is_empty(), "{r:?}");
    assert!(!outside.join("sub").exists());
}

#[test]
fn a_folder_made_while_the_app_was_closed_is_found_by_the_walk() {
    let mut f = Fixture::new("dirs-walk");
    assert!(f.engine.full_scan().errors.is_empty());
    fs::create_dir_all(f.local("late/er")).unwrap();
    let paths = f.engine.paths_to_sync(vec![], vec![], true);
    assert!(paths.contains(&"late".to_string()), "{paths:?}");
    assert!(f.engine.sync_paths(&paths).errors.is_empty());
    assert!(f.remote("late/er").is_dir());
    assert!(
        f.engine.paths_to_sync(vec![], vec![], true).is_empty(),
        "synced: nothing left to report"
    );
}

#[test]
fn a_transfer_marks_the_file_busy_then_leaves_nothing_busy() {
    let f = Fixture::new("filestate");
    write(f.local("up/a.txt"), "local", 100);
    let mut f = f;
    assert_eq!(f.engine.sync_paths(&["up/a.txt".into()]).transferred, 1);
    // The emblem server was told about the file, and nothing stays "syncing" after the run.
    let (dirty, all) = f.engine.files.take_dirty();
    assert!(!all && dirty.contains(&"up/a.txt".to_string()), "{dirty:?}");
    assert!(!f.engine.files.busy("up") && !f.engine.files.busy("up/a.txt"));
}

#[test]
fn a_file_the_db_takes_as_it_is_tells_the_file_manager() {
    let mut f = Fixture::new("filestate-adopt");
    write(f.local("same.txt"), "same", 100);
    write(f.remote("same.txt"), "same", 100);
    let r = f.engine.full_scan();
    assert_eq!(r.transferred, 0, "{r:?}");
    let (dirty, all) = f.engine.files.take_dirty();
    assert!(all || dirty.contains(&"same.txt".to_string()), "{dirty:?}");
}
