//! Status emblems in the file manager, per file: a Nemo-Python extension (`../nemo/fastsync_emblems.py`, installed
//! below) asks this server over a Unix socket what each file shows. Nothing is stored on the files, so with the app
//! closed, or killed, there is no emblem. Nemo only (Nautilus and Caja have no such extension here).
use fastsync_core::{
    accounts,
    db::Db,
    engine::{excluded, ignored as builtin_ignored},
    filestate::FileStates,
    ignore::{RULES, Rules},
    runner::Status,
    settings::data_dir,
};
use std::{
    collections::{BTreeMap, HashMap},
    io::{BufRead, BufReader, ErrorKind, Read, Write},
    os::unix::net::{UnixListener, UnixStream},
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::{Duration, SystemTime},
};

/// The user's "File manager emblems" setting; while it is off every answer is "none" and the extension is removed.
static ENABLED: AtomicBool = AtomicBool::new(true);

pub fn enable(on: bool) {
    ENABLED.store(on, Ordering::Relaxed);
}

/// The badges, drawn like the tray icons (`crates/app/emblems/`). They are installed into the user's hicolor theme,
/// which every icon theme falls back to, so the file manager finds them by name.
const ICONS: [(&str, &str); 6] = [
    ("unknown", include_str!("../emblems/unknown.svg")),
    ("ignored", include_str!("../emblems/ignored.svg")),
    ("idle", include_str!("../emblems/idle.svg")),
    ("syncing", include_str!("../emblems/syncing.svg")),
    ("paused", include_str!("../emblems/paused.svg")),
    ("error", include_str!("../emblems/error.svg")),
];

const EXTENSION: &str = include_str!("../nemo/fastsync_emblems.py");

pub fn name(st: Option<&Status>) -> Option<&'static str> {
    Some(match st? {
        Status::Idle(_) => "emblem-fastsync-idle",
        Status::Syncing(..) => "emblem-fastsync-syncing",
        Status::Paused => "emblem-fastsync-paused",
        Status::Error(_) | Status::Problems(..) | Status::Stopped(..) => "emblem-fastsync-error",
    })
}

/// What the extension is told: the emblem's name without the prefix.
fn word(st: Option<&Status>) -> &'static str {
    name(st).map_or("none", |n| n.trim_start_matches("emblem-fastsync-"))
}

/// Writes the badges where icon themes look (only when missing or changed) and the extension. Cosmetic: failures are
/// ignored.
pub fn install() {
    if let Some(theme) = host_data().map(|p| p.join("icons/hicolor"))
        && let base = theme.join("scalable/emblems")
        && std::fs::create_dir_all(&base).is_ok()
    {
        let mut wrote = false;
        for (n, svg) in ICONS {
            let path = base.join(format!("emblem-fastsync-{n}.svg"));
            if std::fs::read_to_string(&path).ok().as_deref() != Some(svg) {
                wrote |= std::fs::write(path, svg).is_ok();
            }
        }
        // GTK trusts an `icon-theme.cache` that is newer than the theme directory, and a file added two levels down
        // does not change that directory's mtime: without this the new emblems are never found.
        if wrote && let Ok(dir) = std::fs::File::open(&theme) {
            let _ = dir.set_modified(std::time::SystemTime::now());
        }
    }
    sync_extension();
}

pub fn in_flatpak() -> bool {
    std::env::var_os("FLATPAK_ID").is_some()
}

/// The host's `~/.local/share`, where Nemo and the icon themes look. In the Flatpak `XDG_DATA_HOME` is the app's own
/// directory, but `--filesystem=home` lets us write the real one.
pub fn host_data() -> Option<PathBuf> {
    if in_flatpak() {
        return std::env::var_os("HOST_XDG_DATA_HOME")
            .map(PathBuf::from)
            .filter(|p| p.is_absolute())
            .or_else(|| std::env::var_os("HOME").map(|h| PathBuf::from(h).join(".local/share")));
    }
    Some(data_dir()?.parent()?.to_path_buf())
}

/// Where Nemo's Python loader looks (the `nemo-python` package).
fn extension_path() -> Option<PathBuf> {
    Some(host_data()?.join("nemo-python/extensions/fastsync_emblems.py"))
}

/// Puts the extension in place while the setting is on and takes it out when it is off. A running Nemo keeps the old
/// copy until it restarts (`nemo -q`); that copy then simply gets "none".
pub fn sync_extension() {
    let Some(path) = extension_path() else {
        return;
    };
    if !ENABLED.load(Ordering::Relaxed) {
        let _ = std::fs::remove_file(path);
    } else if std::fs::read_to_string(&path).ok().as_deref() != Some(EXTENSION)
        && path.parent().is_some_and(|d| std::fs::create_dir_all(d).is_ok())
    {
        let _ = std::fs::write(path, EXTENSION);
    }
}

/// What FastSync never syncs: its own hidden folder, the folders the user unchecked, and what the rules file hides.
/// `dir` is whether the path is a folder (a rule such as `build/` only matches one).
fn ignored(rel: &str, dir: bool, unchecked: &[String], rules: &Rules) -> bool {
    // The built-in list (`.fastsync`, `.dropbox*`, `.DS_Store`, `*.part`, ...) by any name on the path.
    rel.split('/').any(builtin_ignored)
        || excluded(rel, unchecked)
        || if dir {
            rules.matches_dir(rel)
        } else {
            rules.matches(rel)
        }
}

/// What an emblem shows for a file or folder inside an account's folder (`rel`): only while the account is up to date
/// or syncing (`visible`; otherwise the folder's own emblem says why), a failure first, then a transfer, then "known".
/// `known` is `None` when the DB could not be read: "unknown" makes the extension ask again, "none" would be final.
fn inside(visible: bool, files: Option<&FileStates>, rel: &str, known: impl FnOnce() -> Option<bool>) -> &'static str {
    if !visible {
        return "none";
    }
    if let Some(f) = files {
        if f.failed(rel) {
            return "error";
        }
        if f.busy(rel) {
            return "syncing";
        }
    }
    match known() {
        Some(true) => "idle",
        Some(false) => "none",
        None => "unknown",
    }
}

// ---- the socket -------------------------------------------------------------------------------------------------

/// What the server asks of the app, so a test can put a stub in its place.
pub trait Source: Send + Sync + 'static {
    fn answer(&self, path: &str) -> &'static str;
    /// The synced folders: the extension asks about nothing outside them.
    fn roots(&self) -> Vec<String>;
    /// Lines to push to the watching extension (`c <path>`, `all`, `roots <json>`) since its last call. The first call
    /// only learns the starting point.
    fn changes(&self, last: &mut Last) -> Vec<String>;
}

/// What one watcher was last told: the setting, and for each folder its emblem and whether files inside show one.
#[derive(Default)]
pub struct Last {
    init: bool,
    enabled: bool,
    roots: BTreeMap<String, (&'static str, bool)>,
}

fn roots_line(roots: Vec<String>) -> String {
    serde_json::to_string(&roots).unwrap_or_else(|_| "[]".into())
}

/// The lines for what changed at the folders themselves (their emblem, the list, the setting). Files inside are the
/// engine's `take_dirty`.
fn root_changes(last: &mut Last, enabled: bool, now: BTreeMap<String, (&'static str, bool)>) -> Vec<String> {
    let first = !last.init;
    let mut out = Vec::new();
    if !first {
        let removed = last.roots.keys().any(|k| !now.contains_key(k));
        let shown_changed = now.iter().any(|(k, v)| last.roots.get(k).is_some_and(|o| o.1 != v.1));
        if enabled != last.enabled || removed || shown_changed {
            out.push("all".to_owned());
        }
        out.extend(
            now.iter()
                .filter(|(k, v)| last.roots.get(*k) != Some(v))
                .map(|(k, _)| format!("c {k}")),
        );
        if last.roots.len() != now.len() || removed {
            out.push(format!("roots {}", roots_line(now.keys().cloned().collect())));
        }
    }
    (last.init, last.enabled, last.roots) = (true, enabled, now);
    out
}

const MAX_LINE: u64 = 4300;
const MAX_CLIENTS: usize = 16;

/// `fastsync-emblems.sock` in the runtime directory (never a shared one such as `/tmp`). In the Flatpak only
/// `app/<id>/` is shared with the host, where the Nemo extension looks for it.
fn socket_path() -> Option<PathBuf> {
    let dir = PathBuf::from(std::env::var_os("XDG_RUNTIME_DIR")?);
    Some(match std::env::var("FLATPAK_ID") {
        Ok(app) => dir.join("app").join(app).join("fastsync-emblems.sock"),
        Err(_) => dir.join("fastsync-emblems.sock"),
    })
}

/// Starts the server. Every failure is only logged: emblems are cosmetic and must never touch the sync.
pub fn serve(src: Arc<dyn Source>) {
    let Some(path) = socket_path() else {
        return;
    };
    if let Err(e) = listen_at(&path, src) {
        eprintln!("emblem socket unavailable ({e}); no file emblems");
    }
}

/// Removes the socket file on a normal quit (a crash leaves it; the next start takes it over).
pub fn stop() {
    if let Some(p) = socket_path() {
        let _ = std::fs::remove_file(p);
    }
}

fn listen_at(path: &Path, src: Arc<dyn Source>) -> std::io::Result<()> {
    let listener = match UnixListener::bind(path) {
        Err(e) if e.kind() == ErrorKind::AddrInUse => {
            if UnixStream::connect(path).is_ok() {
                return Err(e);
            }
            // Nobody answers: left by a crashed run.
            let _ = std::fs::remove_file(path);
            UnixListener::bind(path)?
        }
        other => other?,
    };
    let watchers = Watchers::default();
    std::thread::spawn({
        let (src, watchers) = (src.clone(), watchers.clone());
        move || broadcast(&*src, &watchers)
    });
    std::thread::spawn(move || {
        let live = Arc::new(AtomicUsize::new(0));
        for stream in listener.incoming().flatten() {
            if live.fetch_add(1, Ordering::Relaxed) >= MAX_CLIENTS {
                live.fetch_sub(1, Ordering::Relaxed);
                continue;
            }
            let (src, live, watchers) = (src.clone(), live.clone(), watchers.clone());
            std::thread::spawn(move || {
                let _ = client(stream, &*src, &watchers);
                live.fetch_sub(1, Ordering::Relaxed);
            });
        }
    });
    Ok(())
}

/// The streams of the extensions that asked for `watch`.
type Watchers = Arc<Mutex<Vec<UnixStream>>>;

/// One request line without its line break; None at the end of the stream or for a line that is too long.
fn read_request(r: &mut BufReader<UnixStream>) -> std::io::Result<Option<Vec<u8>>> {
    let mut buf = Vec::new();
    r.by_ref().take(MAX_LINE).read_until(b'\n', &mut buf)?;
    if buf.pop() != Some(b'\n') {
        return Ok(None);
    }
    Ok(Some(buf))
}

fn client(stream: UnixStream, src: &dyn Source, watchers: &Watchers) -> std::io::Result<()> {
    // A client that stops reading must not hold this thread for ever.
    stream.set_write_timeout(Some(Duration::from_secs(2)))?;
    // Likewise a client that connects and says nothing: 16 of them would take every slot (`MAX_CLIENTS`). A watcher
    // waits on reads for as long as Nemo lives, so the limit is lifted for it below.
    stream.set_read_timeout(Some(Duration::from_secs(30)))?;
    let mut out = stream.try_clone()?;
    let mut input = BufReader::new(stream);
    if read_request(&mut input)?.as_deref() != Some(b"hello 1") {
        return Ok(());
    }
    writeln!(out, "ok 1 {}", roots_line(src.roots()))?;
    // Greeted: Nemo may be idle for minutes between two questions, and a link closed here would make its next question
    // fail and blank the emblems for a while. The limit is only for a client that never says hello.
    out.set_read_timeout(None)?;
    while let Some(req) = read_request(&mut input)? {
        if let Some(path) = req.strip_prefix(b"q ") {
            // A name that is not UTF-8 is never one of ours (`locate` needs text).
            let t = std::time::Instant::now();
            let a = std::str::from_utf8(path).map_or("none", |p| src.answer(p));
            // Tells whether `snapshot()` (the global `STATE` lock) is what makes the extension time out.
            if t.elapsed() > Duration::from_millis(100) {
                eprintln!("fastsync: emblem answer took {} ms", t.elapsed().as_millis());
            }
            writeln!(out, "{a}")?;
        } else if req == b"watch" {
            let input = input.into_inner();
            input.set_read_timeout(None)?;
            return watch(input, out, watchers);
        } else {
            break;
        }
    }
    Ok(())
}

/// A watcher joins the broadcast and then only waits for the extension to go away (a read of zero bytes: it or Nemo
/// quit). Nemo and `nemo-desktop` (the desktop icons) are two processes, so there can be two.
fn watch(mut input: UnixStream, out: UnixStream, watchers: &Watchers) -> std::io::Result<()> {
    if let Ok(mut w) = watchers.lock() {
        w.push(out);
    }
    let mut buf = [0u8; 64];
    while input.read(&mut buf)? > 0 {}
    Ok(())
}

/// What changed, told to every watcher twice a second. One thread reads the changes (`take_dirty` hands them out
/// once), so every watcher gets all of them, and they are read even with no watcher, so nothing piles up. A watcher
/// whose write fails is gone.
/// shortcut: polls at 2 Hz; upgrade to a channel from the runner if a 100k-file first sync shows up in CPU.
fn broadcast(src: &dyn Source, watchers: &Watchers) {
    let mut last = Last::default();
    loop {
        // A line break in a name would end the line early: such a file just keeps its emblem until Nemo refreshes.
        let text: String = (src.changes(&mut last).iter())
            .filter(|l| !l.contains('\n'))
            .map(|l| format!("{l}\n"))
            .collect();
        if !text.is_empty()
            && let Ok(mut w) = watchers.lock()
        {
            w.retain_mut(|s| s.write_all(text.as_bytes()).is_ok());
        }
        std::thread::sleep(Duration::from_millis(500));
    }
}

// ---- the app's side -----------------------------------------------------------------------------------------------

/// A rules file's parsed text and the modified time it was read at.
type Cached = (Option<SystemTime>, Arc<Rules>);

/// The real source: the accounts, their statuses and runners (`STATE`), and each account's DB read-only.
#[derive(Default)]
pub struct AppSource {
    /// One read-only connection per (account id, folder): an Edit that changes the folder deletes the DB, and the new
    /// key opens the new file.
    dbs: Mutex<HashMap<(String, String), Db>>,
    /// Each account's rules file as last read, with the modified time it had: read again when that changes.
    rules: Mutex<HashMap<String, Cached>>,
    /// Set when a rules file changed, so the next `changes` tells the extension to ask again about everything.
    rules_changed: AtomicBool,
}

impl AppSource {
    fn rules(&self, a: &Acct) -> Arc<Rules> {
        let file = Path::new(&a.local).join(RULES);
        let mtime = std::fs::metadata(&file).and_then(|m| m.modified()).ok();
        let Ok(mut cache) = self.rules.lock() else {
            return Default::default();
        };
        if let Some((m, r)) = cache.get(&a.id)
            && *m == mtime
        {
            return r.clone();
        }
        let rules = Arc::new(Rules::parse(&std::fs::read_to_string(&file).unwrap_or_default()));
        if cache.insert(a.id.clone(), (mtime, rules.clone())).is_some() {
            self.rules_changed.store(true, Ordering::Relaxed);
        }
        rules
    }
}

struct Acct {
    id: String,
    local: String,
    excluded: Vec<String>,
    word: &'static str,
    visible: bool,
    files: Option<Arc<FileStates>>,
}

fn snapshot() -> Vec<Acct> {
    let s = crate::state();
    s.accounts
        .iter()
        .filter(|a| a.local.starts_with('/'))
        .map(|a| {
            let st = s.status.get(&a.id);
            Acct {
                id: a.id.clone(),
                local: a.local.trim_end_matches('/').to_owned(),
                excluded: a.excluded.clone(),
                word: word(st),
                visible: matches!(st, Some(Status::Idle(_) | Status::Syncing(..))),
                files: s.runners.get(&a.id).map(|r| r.files()),
            }
        })
        .collect()
}

impl Source for AppSource {
    fn answer(&self, path: &str) -> &'static str {
        if !ENABLED.load(Ordering::Relaxed) {
            return "none";
        }
        // Copied out of the lock first: the DB lookup below must not hold up the sync's status callbacks.
        let snap = snapshot();
        if let Some(a) = snap.iter().find(|a| a.local == path) {
            return a.word;
        }
        let Some((a, rel)) = snap
            .iter()
            .find_map(|a| Some((a, path.strip_prefix(a.local.as_str())?.strip_prefix('/')?)))
        else {
            return "none";
        };
        if rel.is_empty() || rel.split('/').any(|c| c == "..") {
            return "none";
        }
        if ignored(rel, Path::new(path).is_dir(), &a.excluded, &self.rules(a)) {
            return "ignored";
        }
        inside(a.visible, a.files.as_deref(), rel, || {
            let mut dbs = self.dbs.lock().ok()?;
            let key = (a.id.clone(), a.local.clone());
            if !dbs.contains_key(&key) {
                let file = accounts::account_dir(&a.id)?.join("state.db");
                // No DB yet (nothing synced): that is a real "not known", not a failure.
                if !file.exists() {
                    return Some(false);
                }
                dbs.insert(key.clone(), Db::open_read(&file)?);
            }
            let known = dbs.get(&key)?.knows(rel).ok();
            if known.is_none() {
                // A broken connection must not stay cached: the next question opens a new one.
                dbs.remove(&key);
            }
            known
        })
    }

    fn roots(&self) -> Vec<String> {
        snapshot().into_iter().map(|a| a.local).collect()
    }

    fn changes(&self, last: &mut Last) -> Vec<String> {
        let snap = snapshot();
        let first = !last.init;
        let mut out = root_changes(
            last,
            ENABLED.load(Ordering::Relaxed),
            snap.iter().map(|a| (a.local.clone(), (a.word, a.visible))).collect(),
        );
        for a in &snap {
            if let Some(f) = &a.files {
                // Drained on the first call too, so the starting point is "now".
                let (paths, all) = f.take_dirty();
                if first {
                    continue;
                }
                if all {
                    out.push("all".to_owned());
                } else {
                    out.extend(paths.into_iter().map(|p| format!("c {}/{p}", a.local)));
                }
            }
        }
        if let Ok(mut dbs) = self.dbs.lock() {
            dbs.retain(|k, _| snap.iter().any(|a| (&a.id, &a.local) == (&k.0, &k.1)));
        }
        // The rules files are looked at every tick (one `stat` each): editing one changes what is ignored everywhere.
        snap.iter().for_each(|a| drop(self.rules(a)));
        if self.rules_changed.swap(false, Ordering::Relaxed) && !first {
            out.push("all".to_owned());
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names() {
        assert_eq!(name(Some(&Status::Idle(0))), Some("emblem-fastsync-idle"));
        assert_eq!(name(Some(&Status::Paused)), Some("emblem-fastsync-paused"));
        assert_eq!(
            name(Some(&Status::Stopped(1, 2, vec![]))),
            name(Some(&Status::Error("x".into())))
        );
        assert_eq!(name(None), None);
        assert_eq!(word(None), "none");
        assert_eq!(word(Some(&Status::Paused)), "paused");
        // Every name has its file in ICONS.
        for st in [
            Status::Idle(0),
            Status::Syncing(Default::default(), 0.0),
            Status::Paused,
            Status::Error(String::new()),
        ] {
            let n = name(Some(&st)).unwrap().trim_start_matches("emblem-fastsync-");
            assert!(ICONS.iter().any(|(i, svg)| *i == n && svg.contains("<svg")));
        }
    }

    #[test]
    fn inside_shows_a_failure_before_a_transfer_before_a_check() {
        let f = FileStates::default();
        f.add_busy(["d/busy.txt".to_owned(), "d/both.txt".to_owned()]);
        f.finish(&["d/both.txt".to_owned(), "x/bad.txt".to_owned()], &[], true);
        let known = || Some(true);
        assert_eq!(inside(true, Some(&f), "d/both.txt", known), "error");
        assert_eq!(inside(true, Some(&f), "d/busy.txt", known), "syncing");
        // A folder takes the worst of what is below it.
        assert_eq!(inside(true, Some(&f), "d", known), "error");
        assert_eq!(inside(true, Some(&f), "x", known), "error");
        assert_eq!(inside(true, Some(&f), "other", known), "idle");
        // Not in the DB (new, ignored): nothing. A paused or failed account shows nothing inside.
        assert_eq!(inside(true, Some(&f), "other", || Some(false)), "none");
        // A DB that could not be read is not "new": the extension must ask again.
        assert_eq!(inside(true, Some(&f), "other", || None), "unknown");
        assert_eq!(inside(false, Some(&f), "d/both.txt", known), "none");
        assert_eq!(inside(true, None, "a", known), "idle");
    }

    #[test]
    fn ignored_is_the_hidden_folder_the_unchecked_ones_and_what_the_rules_hide() {
        let rules = Rules::parse("*.tmp\ncache/\n");
        let unchecked = ["Photos/Raw".to_owned()];
        let is = |rel: &str, dir: bool| ignored(rel, dir, &unchecked, &rules);
        // `.fastsync` and everything in it; a name that merely starts like it is a normal folder.
        assert!(is(".fastsync", true) && is(".fastsync/trash/a", false));
        assert!(!is(".fastsync2", true) && !is(".fastsyncignore", false));
        // The built-in names the engine never syncs, at any depth.
        assert!(is(".dropbox.cache", true) && is(".dropbox", false) && is("a/.DS_Store", false));
        // An unchecked folder and below it, not its siblings.
        assert!(is("Photos/Raw", true) && is("Photos/Raw/a.cr2", false));
        assert!(!is("Photos", true) && !is("Photos/Raw2", true));
        // Rules: a folder-only rule hides a folder (and what is in it) but not a file of that name.
        assert!(is("x/a.tmp", false) && is("cache", true) && is("cache/a.txt", false));
        assert!(!is("cache", false) && !is("a.txt", false));
    }

    fn roots(list: &[(&str, &'static str, bool)]) -> BTreeMap<String, (&'static str, bool)> {
        list.iter().map(|(k, w, v)| (k.to_string(), (*w, *v))).collect()
    }

    #[test]
    fn folder_changes_are_pushed_but_the_starting_point_is_not() {
        let mut last = Last::default();
        assert!(root_changes(&mut last, true, roots(&[("/a", "idle", true)])).is_empty());
        assert!(root_changes(&mut last, true, roots(&[("/a", "idle", true)])).is_empty());
        // Idle -> syncing: only the folder's own emblem; files inside still show.
        assert_eq!(
            root_changes(&mut last, true, roots(&[("/a", "syncing", true)])),
            ["c /a"]
        );
        // Paused: files inside lose theirs.
        assert_eq!(
            root_changes(&mut last, true, roots(&[("/a", "paused", false)])),
            ["all", "c /a"]
        );
        // A second account, then the setting off, then the first removed.
        assert_eq!(
            root_changes(&mut last, true, roots(&[("/a", "paused", false), ("/b", "idle", true)])),
            ["c /b", r#"roots ["/a","/b"]"#]
        );
        assert_eq!(
            root_changes(
                &mut last,
                false,
                roots(&[("/a", "paused", false), ("/b", "idle", true)])
            ),
            ["all"]
        );
        assert_eq!(
            root_changes(&mut last, false, roots(&[("/b", "idle", true)])),
            ["all", r#"roots ["/b"]"#]
        );
    }

    struct Stub(AtomicUsize);
    impl Source for Stub {
        fn answer(&self, path: &str) -> &'static str {
            if path == "/r/a b.txt" { "idle" } else { "none" }
        }
        fn roots(&self) -> Vec<String> {
            vec!["/r".into()]
        }
        fn changes(&self, _: &mut Last) -> Vec<String> {
            // The first call is the starting point; after it every call has news, so the test never races the
            // moment its watcher joins.
            match self.0.fetch_add(1, Ordering::Relaxed) {
                0 => vec![],
                _ => vec!["c /r/a b.txt".into(), "c /r/bad\nname".into()],
            }
        }
    }

    fn talk(path: &Path, lines: &str) -> BufReader<UnixStream> {
        let mut s = UnixStream::connect(path).unwrap();
        s.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
        s.write_all(lines.as_bytes()).unwrap();
        BufReader::new(s)
    }

    fn line(r: &mut BufReader<UnixStream>) -> String {
        let mut l = String::new();
        r.read_line(&mut l).unwrap();
        l.trim_end().to_owned()
    }

    #[test]
    fn the_socket_answers_questions_and_pushes_changes() {
        let path = std::env::temp_dir().join(format!("fastsync-emblem-test-{}.sock", std::process::id()));
        let _ = std::fs::remove_file(&path);
        listen_at(&path, Arc::new(Stub(AtomicUsize::new(0)))).unwrap();
        // A client that skips the greeting gets nothing.
        assert_eq!(line(&mut talk(&path, "q /r/a b.txt\n")), "");
        let mut q = talk(&path, "hello 1\nq /r/a b.txt\nq /elsewhere\n");
        assert_eq!(line(&mut q), r#"ok 1 ["/r"]"#);
        assert_eq!(line(&mut q), "idle");
        assert_eq!(line(&mut q), "none");
        // A name that is not UTF-8 is just "none"; the connection goes on.
        q.get_mut().write_all(b"q /r/\xff\xfe\nq /r/a b.txt\n").unwrap();
        assert_eq!((line(&mut q), line(&mut q)), ("none".into(), "idle".into()));
        // The watcher gets the change, but not the name with a line break in it.
        let mut w = talk(&path, "hello 1\nwatch\n");
        assert_eq!(line(&mut w), r#"ok 1 ["/r"]"#);
        assert_eq!(line(&mut w), "c /r/a b.txt");
        // A second one (`nemo-desktop` beside Nemo) gets the news too, and one that left does not stop the other.
        let mut w2 = talk(&path, "hello 1\nwatch\n");
        assert_eq!(line(&mut w2), r#"ok 1 ["/r"]"#);
        drop(w);
        assert_eq!(line(&mut w2), "c /r/a b.txt");
        assert_eq!(line(&mut w2), "c /r/a b.txt");
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn the_extension_is_valid_python() {
        let path = concat!(env!("CARGO_MANIFEST_DIR"), "/nemo/fastsync_emblems.py");
        // A syntax error would only show as a silent "no emblems" inside Nemo. Skipped without python3.
        let Ok(o) = std::process::Command::new("python3")
            .args(["-c", "import ast,sys; ast.parse(open(sys.argv[1]).read())", path])
            .output()
        else {
            return;
        };
        assert!(o.status.success(), "{}", String::from_utf8_lossy(&o.stderr));
    }

    /// Loads the extension as module `m` with `gi` replaced by stubs (the file path is `sys.argv[1]`).
    const PY_STUB: &str = "import sys,types\n\
        for n in ('gi','gi.repository'): sys.modules[n]=types.ModuleType(n)\n\
        sys.modules['gi'].require_version=lambda *a: None\n\
        class A: pass\n\
        class B: pass\n\
        r=sys.modules['gi.repository']\n\
        r.GLib=types.SimpleNamespace(timeout_add_seconds=lambda *a: 0)\n\
        r.GObject=types.SimpleNamespace(GObject=A)\n\
        r.Nemo=types.SimpleNamespace(InfoProvider=B,OperationResult=types.SimpleNamespace(COMPLETE='C',IN_PROGRESS='P'))\n\
        sys.modules['gi'].repository=sys.modules['gi.repository']\n\
        import importlib.util as u\n\
        sp=u.spec_from_file_location('m',sys.argv[1]); m=u.module_from_spec(sp); sp.loader.exec_module(m)\n";

    #[test]
    fn the_extension_asks_without_waiting_and_gives_up_after_a_timeout() {
        let body = r"
import socket
a,b=socket.socketpair(); a.setblocking(False); b.settimeout(5)
e=m.FastSyncEmblems(); e.q=a; e.q_greeted=True; e.roots=['/r']
done=[]
PROV=object()
m.Nemo.info_provider_update_complete_invoke=lambda c,p,h,r: done.append(c) if p is PROV else done.append('wrong provider')
class F:
    def __init__(s,p): s.p=p; s.em=[]
    def get_uri_scheme(s): return 'file'
    def get_location(s): return s
    def get_path(s): return s.p
    def add_emblem(s,n): s.em.append(n)
f1,f2,f3,f4,f5=[F(p) for p in ('/r/a','/r/b','/r/c','/elsewhere','/r/d')]
ask=lambda h,f: e.update_file_info_full(PROV,h,'c'+h,f)
# Nemo gets its answer at once (in progress), and a file outside the synced folders is never asked about.
assert [ask('1',f1),ask('2',f2),ask('3',f3),ask('4',f4)]==['P','P','P','C']
assert b.recv(100)==b'q /r/a\nq /r/b\nq /r/c\n'
# A cancelled one is read but never completed; an 'unknown' answer is asked again.
e.cancel_update(e,'2')
assert e.query_data(b'idle\nsyncing\nunknown\n')
assert f1.em==['emblem-fastsync-idle'] and f2.em==[] and f3.em==['emblem-fastsync-unknown']
assert done==['c1','c3'] and '/r/c' in e.missed and not e.pending
# No answer in time: the badge, the link is dropped and the file is asked again.
assert ask('5',f5)=='P'
e.pending[0][m.ASKED]-=10; e.expire()
assert f5.em==['emblem-fastsync-unknown'] and e.q is None and '/r/d' in e.missed and done[-1]=='c5'
print('ok')";
        let path = concat!(env!("CARGO_MANIFEST_DIR"), "/nemo/fastsync_emblems.py");
        // Skipped without python3.
        let Ok(o) = std::process::Command::new("python3")
            .args(["-c", &format!("{PY_STUB}{body}"), path])
            .output()
        else {
            return;
        };
        assert_eq!(
            String::from_utf8_lossy(&o.stdout).trim(),
            "ok",
            "{}",
            String::from_utf8_lossy(&o.stderr)
        );
    }

    #[test]
    fn the_extension_skips_a_dead_socket_for_the_live_one() {
        use std::io::{BufRead, BufReader, Write};
        use std::os::unix::net::UnixListener;
        let run = std::env::temp_dir().join(format!("fs-ext-{}", std::process::id()));
        let live_dir = run.join("app/io.github.evandro777.FastSync");
        std::fs::create_dir_all(&live_dir).unwrap();
        // A socket file nobody listens on (a native build that was killed), found first by the extension.
        drop(UnixListener::bind(run.join("fastsync-emblems.sock")).unwrap());
        let live = UnixListener::bind(live_dir.join("fastsync-emblems.sock")).unwrap();
        std::thread::spawn(move || {
            if let Ok((mut s, _)) = live.accept() {
                let mut l = String::new();
                let _ = BufReader::new(s.try_clone().unwrap()).read_line(&mut l);
                let _ = s.write_all(b"ok 1 [\"/r\"]\n");
            }
        });
        // The greeting is read by the watch in Nemo; here it is read by hand and handed to `query_data`.
        let py = format!(
            "{PY_STUB}e=m.FastSyncEmblems(); s=e._open(b''); s.settimeout(5); e.query_data(s.recv(100)); print(bool(s), e.roots)"
        );
        let path = concat!(env!("CARGO_MANIFEST_DIR"), "/nemo/fastsync_emblems.py");
        // Skipped without python3.
        let Ok(o) = std::process::Command::new("python3")
            .args(["-c", &py, path])
            .env("XDG_RUNTIME_DIR", &run)
            .output()
        else {
            return;
        };
        let _ = std::fs::remove_dir_all(&run);
        assert_eq!(
            String::from_utf8_lossy(&o.stdout).trim(),
            "True ['/r']",
            "{}",
            String::from_utf8_lossy(&o.stderr)
        );
    }
}
