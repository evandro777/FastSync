//! The sync state of one account: for each file, how both sides looked right after the last successful sync.
use std::path::Path;

use rusqlite::{Connection, OptionalExtension, params};

use crate::decide::{Base, Sig};

pub struct Db(Connection);

pub type Result<T> = rusqlite::Result<T>;

impl Db {
    pub fn open(path: &Path) -> Result<Self> {
        Self::init(Connection::open(path)?)
    }

    /// A DB that refuses to write one meta key (and takes the others): for tests of what the runner does when it cannot
    /// remember one thing but could another.
    #[cfg(test)]
    pub(crate) fn refuse_meta_key(&self, key: &str) {
        self.0
            .execute_batch(&format!(
                "CREATE TRIGGER refuse_{key} BEFORE INSERT ON meta WHEN NEW.k='{key}' \
                 BEGIN SELECT RAISE(ABORT, 'refused'); END;"
            ))
            .unwrap();
    }

    /// A DB that refuses every `set_meta`: for tests of what the runner does when it cannot remember something.
    #[cfg(test)]
    pub(crate) fn break_meta(&self) {
        self.0.execute_batch("DROP TABLE meta").unwrap();
    }

    pub fn memory() -> Result<Self> {
        Self::init(Connection::open_in_memory()?)
    }

    fn init(c: Connection) -> Result<Self> {
        // WAL + NORMAL: a crash may lose the last row, never corrupt the file; a lost row only means one file is
        // compared again as "no base", which never deletes anything.
        c.execute_batch(
            "PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL;
             CREATE TABLE IF NOT EXISTS files(
               path TEXT PRIMARY KEY, l_size INT, l_mtime INT, r_size INT, r_mtime INT, r_id TEXT NOT NULL DEFAULT '',
               r_hash TEXT);
             CREATE INDEX IF NOT EXISTS files_rid ON files(r_id);
             CREATE INDEX IF NOT EXISTS files_lsize ON files(l_size);
             CREATE INDEX IF NOT EXISTS files_lpath ON files(lower(path));
             CREATE TABLE IF NOT EXISTS dirs(id TEXT PRIMARY KEY, path TEXT NOT NULL);
             CREATE TABLE IF NOT EXISTS folders(path TEXT PRIMARY KEY);
             CREATE INDEX IF NOT EXISTS folders_lpath ON folders(lower(path));
             CREATE TABLE IF NOT EXISTS meta(k TEXT PRIMARY KEY, v TEXT);
             CREATE TABLE IF NOT EXISTS log(
               id INTEGER PRIMARY KEY, ts INT NOT NULL, kind TEXT NOT NULL, path TEXT NOT NULL DEFAULT '',
               size INT, msg TEXT NOT NULL DEFAULT '');",
        )?;
        // The first release is version 1. A later release that changes a table reads this to migrate: `CREATE TABLE IF
        // NOT EXISTS` above never adds a column to an existing file.
        let version = c.query_row("PRAGMA user_version", [], |r| r.get::<_, i64>(0))?;
        if version < 2 {
            // 2: folders are synced. A file's folders were on both sides when it was synced, so they start as synced
            // too: without it a folder deleted here before the upgrade would be made again from the cloud's copy.
            let mut seen = std::collections::BTreeSet::new();
            {
                let mut st = c.prepare("SELECT path FROM files")?;
                for p in st.query_map([], |r| r.get::<_, String>(0))? {
                    let p = p?;
                    for (i, _) in p.match_indices('/') {
                        seen.insert(p[..i].to_string());
                    }
                }
            }
            let tx = c.unchecked_transaction()?;
            for d in &seen {
                tx.execute("INSERT OR IGNORE INTO folders(path) VALUES(?)", [d])?;
            }
            tx.execute_batch("PRAGMA user_version=2")?;
            tx.commit()?;
        }
        if version < 3 {
            // 3: the cloud's content hash of a row (`kind:value`, NULL = not known), so the feed's echo of our own
            // upload can be told from a real edit without asking the cloud. A new file has the column already.
            let has = c
                .prepare("SELECT 1 FROM pragma_table_info('files') WHERE name='r_hash'")?
                .exists([])?;
            let tx = c.unchecked_transaction()?;
            if !has {
                tx.execute_batch("ALTER TABLE files ADD COLUMN r_hash TEXT")?;
            }
            tx.execute_batch("PRAGMA user_version=3")?;
            tx.commit()?;
        }
        Ok(Self(c))
    }

    /// Opens a transaction for a long run of small writes (each commit of the first sync onto a folder that is already
    /// there costs several index pages). `false`: none was opened, so no `commit` follows.
    pub fn begin(&self) -> bool {
        self.0.execute_batch("BEGIN").is_ok()
    }

    /// A failed `COMMIT` can leave the transaction open, and every later write (the cursor, the retry list) would join
    /// it unseen and be lost at exit: it is rolled back, and the error is returned so the run can say so.
    pub fn commit(&self) -> Result<()> {
        self.0.execute_batch("COMMIT").inspect_err(|_| {
            let _ = self.0.execute_batch("ROLLBACK");
        })
    }

    /// One line of the activity log. Best effort: a log that cannot be written must never stop a sync.
    pub fn log(&self, kind: &str, path: &str, size: Option<u64>, msg: &str) {
        crate::debug::log(|| format!("{kind:<8} {path} size={size:?} {msg}"));
        let _ = self.0.execute(
            "INSERT INTO log(ts,kind,path,size,msg) VALUES(?,?,?,?,?)",
            params![now(), kind, path, size.map(|s| s as i64), msg],
        );
    }

    /// Drops entries older than `LOG_DAYS` and everything but the newest `LOG_ROWS`.
    pub fn trim_log(&self) {
        let _ = self.0.execute(
            "DELETE FROM log WHERE ts < ?1 OR id <= (SELECT COALESCE(MAX(id),0) FROM log) - ?2",
            params![now() - LOG_DAYS * 86400, LOG_ROWS],
        );
    }

    pub fn base(&self, path: &str) -> Result<Option<Base>> {
        self.0
            .query_row(
                "SELECT l_size,l_mtime,r_size,r_mtime FROM files WHERE path=?",
                [path],
                row_base,
            )
            .optional()
    }

    /// Every known file, for the delete guard and for finding what vanished.
    pub fn all(&self) -> Result<Vec<(String, Base)>> {
        let mut st = self.0.prepare("SELECT l_size,l_mtime,r_size,r_mtime,path FROM files")?;
        st.query_map([], |r| Ok((r.get(4)?, row_base(r)?)))?.collect()
    }

    /// The known paths that are `lower` (already lower-cased, ASCII) once SQLite lower-cases them: the names that differ
    /// from a path only by case, from an index, so a watcher run does not read every row. SQLite's `lower` leaves
    /// non-ASCII letters alone, so those namesakes are found by the caller's own folder listing only.
    pub fn paths_ci(&self, lower: &str) -> Result<Vec<String>> {
        let mut st = self.0.prepare("SELECT path FROM files WHERE lower(path)=?")?;
        st.query_map([lower], |r| r.get(0))?.collect()
    }

    /// True when a file or folder row is at `lower` (already lower-cased, ASCII) or below it, whatever its case. Both
    /// ranges use the expression indexes, so asking about one feed entry reads no table.
    pub fn knows_ci(&self, lower: &str) -> Result<bool> {
        let (lo, hi) = (format!("{lower}/"), format!("{lower}0"));
        for table in ["files", "folders"] {
            let hit: Option<i64> = self
                .0
                .query_row(
                    &format!(
                        "SELECT 1 FROM {table} WHERE lower(path)=?1 OR (lower(path) >= ?2 AND lower(path) < ?3) LIMIT 1"
                    ),
                    [lower, &lo, &hi],
                    |r| r.get(0),
                )
                .optional()?;
            if hit.is_some() {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// `paths_ci` for the synced folders (`folders_lpath`).
    pub fn dirs_ci(&self, lower: &str) -> Result<Vec<String>> {
        let mut st = self.0.prepare("SELECT path FROM folders WHERE lower(path)=?")?;
        st.query_map([lower], |r| r.get(0))?.collect()
    }

    /// The known files whose local copy had exactly `size` bytes: sources for a local copy instead of a download.
    pub fn paths_with_size(&self, size: u64) -> Result<Vec<(String, Sig)>> {
        let mut st = self.0.prepare("SELECT path,l_size,l_mtime FROM files WHERE l_size=?")?;
        st.query_map([size as i64], |r| {
            Ok((
                r.get(0)?,
                Sig {
                    size: r.get::<_, i64>(1)? as u64,
                    mtime: r.get(2)?,
                },
            ))
        })?
        .collect()
    }

    /// The known files under a folder (`dir/`), for a folder that was removed or renamed. A range on the primary key
    /// (`'/'` + 1 is `'0'`): indexed and case-exact, unlike `LIKE`, which scanned every row for every changed path.
    pub fn under(&self, dir: &str) -> Result<Vec<String>> {
        let mut st = self
            .0
            .prepare("SELECT path FROM files WHERE path >= ?1 AND path < ?2")?;
        st.query_map([format!("{dir}/"), format!("{dir}0")], |r| r.get(0))?
            .collect()
    }

    /// The known files directly in a folder (`""` = the root), not below it: how big a listing of it would be.
    pub fn count_in(&self, dir: &str) -> Result<usize> {
        let n: i64 = if dir.is_empty() {
            self.0
                .query_row("SELECT COUNT(*) FROM files WHERE instr(path, '/') = 0", [], |r| {
                    r.get(0)
                })?
        } else {
            self.0.query_row(
                "SELECT COUNT(*) FROM files WHERE path >= ?1 AND path < ?2 \
                 AND instr(substr(path, length(?1) + 1), '/') = 0",
                [format!("{dir}/"), format!("{dir}0")],
                |r| r.get(0),
            )?
        };
        Ok(n as usize)
    }

    /// How many files are known: the delete guard's yardstick, without loading them all.
    pub fn count(&self) -> Result<usize> {
        self.0
            .query_row("SELECT COUNT(*) FROM files", [], |r| r.get::<_, i64>(0))
            .map(|n| n as usize)
    }

    /// Written only after the transfer succeeded, so a kill mid-sync leaves the old row and the file is retried.
    pub fn set(&self, path: &str, b: &Base, remote_id: &str) -> Result<()> {
        self.set_hashed(path, b, remote_id, None)
    }

    /// `set`, also keeping the cloud file's `(kind, value)` hash (as the listing spells it). A row written without one
    /// never keeps an older row's hash: `INSERT OR REPLACE` starts from nothing.
    pub fn set_hashed(&self, path: &str, b: &Base, remote_id: &str, hash: Option<&(String, String)>) -> Result<()> {
        self.0.execute(
            "INSERT OR REPLACE INTO files(path,l_size,l_mtime,r_size,r_mtime,r_id,r_hash) VALUES(?,?,?,?,?,?,?)",
            params![
                path,
                b.local.size as i64,
                b.local.mtime,
                b.remote.size as i64,
                b.remote.mtime,
                remote_id,
                hash.map(|(k, v)| format!("{k}:{v}"))
            ],
        )?;
        Ok(())
    }

    /// The row of a remote file id: its path, how both sides looked, and the cloud hash when one was kept. For
    /// recognising the feed's echo of a file we synced (`Engine::feed_echo_id`).
    #[allow(clippy::type_complexity)]
    pub fn by_id(&self, id: &str) -> Result<Option<(String, Base, Option<(String, String)>)>> {
        self.0
            .query_row(
                "SELECT l_size,l_mtime,r_size,r_mtime,path,r_hash FROM files WHERE r_id=?1 AND r_id<>''",
                [id],
                |r| {
                    let hash: Option<String> = r.get(5)?;
                    Ok((
                        r.get(4)?,
                        row_base(r)?,
                        hash.and_then(|h| h.split_once(':').map(|(k, v)| (k.to_string(), v.to_string()))),
                    ))
                },
            )
            .optional()
    }

    pub fn forget(&self, path: &str) -> Result<()> {
        self.0.execute("DELETE FROM files WHERE path=?", [path])?;
        Ok(())
    }

    /// Folders both sides had after the last sync (empty or not). Kept apart from `files` so the guard's counts and
    /// every "known files" query stay about files.
    pub fn dir_synced(&self, path: &str) -> Result<bool> {
        self.0
            .query_row("SELECT 1 FROM folders WHERE path=?", [path], |_| Ok(()))
            .optional()
            .map(|r| r.is_some())
    }

    pub fn add_dir_row(&self, path: &str) -> Result<()> {
        self.0
            .execute("INSERT OR IGNORE INTO folders(path) VALUES(?)", [path])?;
        Ok(())
    }

    pub fn forget_dir_row(&self, path: &str) -> Result<()> {
        self.0.execute("DELETE FROM folders WHERE path=?", [path])?;
        Ok(())
    }

    /// The folder itself and every folder row below it.
    pub fn dir_rows_under(&self, dir: &str) -> Result<Vec<String>> {
        let mut st = self
            .0
            .prepare("SELECT path FROM folders WHERE path=?1 OR (path >= ?2 AND path < ?3)")?;
        st.query_map([dir.to_string(), format!("{dir}/"), format!("{dir}0")], |r| r.get(0))?
            .collect()
    }

    pub fn all_dir_rows(&self) -> Result<Vec<String>> {
        let mut st = self.0.prepare("SELECT path FROM folders")?;
        st.query_map([], |r| r.get(0))?.collect()
    }

    /// The path of a remote file id, for providers whose delta reports deletions by id only.
    pub fn path_of_id(&self, id: &str) -> Result<Option<String>> {
        self.0
            .query_row("SELECT path FROM files WHERE r_id=? AND r_id<>''", [id], |r| r.get(0))
            .optional()
    }

    /// Replaces the remote folder ids (id -> path) after a full scan.
    pub fn set_dirs(&mut self, dirs: &[(String, String)]) -> Result<()> {
        let tx = self.0.transaction()?;
        tx.execute("DELETE FROM dirs", [])?;
        for (id, path) in dirs {
            tx.execute("INSERT OR REPLACE INTO dirs(id,path) VALUES(?,?)", [id, path])?;
        }
        tx.commit()
    }

    /// Adds folders the feed reported as new (`INSERT OR IGNORE`: a row from a full scan is never overwritten by an
    /// older answer, and writing the same folder twice is harmless).
    pub fn add_dirs(&self, dirs: &[(String, String)]) -> Result<()> {
        for (id, path) in dirs {
            self.0
                .execute("INSERT OR IGNORE INTO dirs(id,path) VALUES(?,?)", [id, path])?;
        }
        Ok(())
    }

    /// A folder renamed or moved in the cloud: it and every folder below it get the new path (`dirs` is id -> path).
    pub fn move_dir_ids(&self, old: &str, new: &str) -> Result<()> {
        self.0.execute(
            "UPDATE dirs SET path = ?2 || substr(path, length(?1) + 1) WHERE path = ?1 OR (path >= ?3 AND path < ?4)",
            params![old, new, format!("{old}/"), format!("{old}0")],
        )?;
        Ok(())
    }

    /// A folder deleted in the cloud: it and every folder below it leave the map.
    pub fn drop_dir_ids(&self, dir: &str) -> Result<()> {
        self.0.execute(
            "DELETE FROM dirs WHERE path = ?1 OR (path >= ?2 AND path < ?3)",
            params![dir, format!("{dir}/"), format!("{dir}0")],
        )?;
        Ok(())
    }

    pub fn dirs(&self) -> Result<Vec<(String, String)>> {
        let mut st = self.0.prepare("SELECT id,path FROM dirs")?;
        st.query_map([], |r| Ok((r.get(0)?, r.get(1)?)))?.collect()
    }

    pub fn meta(&self, k: &str) -> Result<Option<String>> {
        self.0
            .query_row("SELECT v FROM meta WHERE k=?", [k], |r| r.get(0))
            .optional()
    }

    pub fn set_meta(&self, k: &str, v: &str) -> Result<()> {
        self.0.execute("INSERT OR REPLACE INTO meta(k,v) VALUES(?,?)", [k, v])?;
        Ok(())
    }

    pub fn del_meta(&self, k: &str) -> Result<()> {
        self.0.execute("DELETE FROM meta WHERE k=?", [k])?;
        Ok(())
    }
}

/// Today's date (`2026-11-16`) on the user's clock: SQLite knows the local time zone, so no extra dependency.
pub fn local_date() -> Option<String> {
    Connection::open_in_memory()
        .ok()?
        .query_row("SELECT date('now','localtime')", [], |r| r.get(0))
        .ok()
}

/// How long the activity log keeps an entry, and the most rows it keeps.
pub const LOG_DAYS: i64 = 30;
pub const LOG_ROWS: i64 = 2000;

fn now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_secs() as i64)
}

#[derive(Debug, Clone, PartialEq)]
pub struct LogEntry {
    pub ts: i64,
    /// `up`, `down`, `trash`, `delcloud`, `conflict`, `error`, `stopped`, `scan`, `move` or `movedir` (one row for a
    /// folder renamed in the cloud: path = new folder, msg = old one).
    pub kind: String,
    pub path: String,
    pub size: Option<u64>,
    pub msg: String,
}

/// The newest entries, newest first, through a read-only connection of its own: the runner's engine holds the only
/// writer, and WAL lets this one read at the same time. A missing file (account never ran) is an empty log.
pub fn read_log(db: &Path, limit: usize) -> Result<Vec<LogEntry>> {
    use rusqlite::OpenFlags;
    // No file yet is an empty log; any other failure (locked, unreadable) must not read as "nothing happened".
    if !db.exists() {
        return Ok(Vec::new());
    }
    let c = Connection::open_with_flags(db, OpenFlags::SQLITE_OPEN_READ_ONLY)?;
    // An old DB without the table (never opened by this version yet) is an empty log too.
    let Ok(mut st) = c.prepare("SELECT ts,kind,path,size,msg FROM log ORDER BY id DESC LIMIT ?") else {
        return Ok(Vec::new());
    };
    st.query_map([limit as i64], |r| {
        Ok(LogEntry {
            ts: r.get(0)?,
            kind: r.get(1)?,
            path: r.get(2)?,
            size: r.get::<_, Option<i64>>(3)?.map(|s| s as u64),
            msg: r.get(4)?,
        })
    })?
    .collect()
}

fn row_base(r: &rusqlite::Row) -> Result<Base> {
    Ok(Base {
        local: Sig {
            size: r.get::<_, i64>(0)? as u64,
            mtime: r.get(1)?,
        },
        remote: Sig {
            size: r.get::<_, i64>(2)? as u64,
            mtime: r.get(3)?,
        },
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base(n: u64) -> Base {
        let s = Sig {
            size: n,
            mtime: n as i64 * 1000,
        };
        Base { local: s, remote: s }
    }

    #[test]
    fn a_new_db_is_schema_version_3() {
        let db = Db::memory().unwrap();
        let v: i64 = db.0.query_row("PRAGMA user_version", [], |r| r.get(0)).unwrap();
        assert_eq!(v, 3);
    }

    #[test]
    fn a_version_2_db_gains_the_hash_column_and_keeps_its_rows() {
        let c = Connection::open_in_memory().unwrap();
        c.execute_batch(
            "CREATE TABLE files(
               path TEXT PRIMARY KEY, l_size INT, l_mtime INT, r_size INT, r_mtime INT, r_id TEXT NOT NULL DEFAULT '');
             INSERT INTO files VALUES('a.txt',1,1,1,1,'id1');
             PRAGMA user_version=2;",
        )
        .unwrap();
        let db = Db::init(c).unwrap();
        let (path, _, hash) = db.by_id("id1").unwrap().unwrap();
        assert_eq!((path.as_str(), hash), ("a.txt", None));
        db.set_hashed("a.txt", &base(1), "id1", Some(&("md5".into(), "abc".into())))
            .unwrap();
        assert_eq!(
            db.by_id("id1").unwrap().unwrap().2,
            Some(("md5".to_string(), "abc".to_string()))
        );
        // A row written without a hash never keeps the old one.
        db.set("a.txt", &base(1), "id1").unwrap();
        assert_eq!(db.by_id("id1").unwrap().unwrap().2, None);
        assert!(db.by_id("").unwrap().is_none() && db.by_id("nope").unwrap().is_none());
    }

    #[test]
    fn folder_ids_follow_a_renamed_folder_and_leave_with_a_deleted_one() {
        let mut db = Db::memory().unwrap();
        let id = |s: &str| s.to_string();
        db.set_dirs(&[
            (id("A"), id("docs")),
            (id("B"), id("docs/sub")),
            (id("C"), id("docs2")),
            (id("D"), id("docs/sub/deep")),
        ])
        .unwrap();
        db.move_dir_ids("docs", "papers").unwrap();
        let mut got = db.dirs().unwrap();
        got.sort();
        assert_eq!(
            got,
            [
                (id("A"), id("papers")),
                (id("B"), id("papers/sub")),
                (id("C"), id("docs2")),
                (id("D"), id("papers/sub/deep"))
            ]
        );
        db.drop_dir_ids("papers").unwrap();
        assert_eq!(db.dirs().unwrap(), [(id("C"), id("docs2"))]);
    }

    #[test]
    fn a_version_1_db_starts_with_the_folders_of_its_files_as_synced() {
        let c = Connection::open_in_memory().unwrap();
        c.execute_batch(
            "CREATE TABLE files(
               path TEXT PRIMARY KEY, l_size INT, l_mtime INT, r_size INT, r_mtime INT, r_id TEXT NOT NULL DEFAULT '');
             INSERT INTO files VALUES('a/b/c.txt',1,1,1,1,''),('top.txt',1,1,1,1,''),('a/d.txt',1,1,1,1,'');
             PRAGMA user_version=1;",
        )
        .unwrap();
        let db = Db::init(c).unwrap();
        let mut got = db.all_dir_rows().unwrap();
        got.sort();
        assert_eq!(got, ["a", "a/b"]);
        assert!(db.dir_synced("a/b").unwrap() && !db.dir_synced("top.txt").unwrap());
        // Files are untouched, and a second open does not seed again.
        assert_eq!(db.count().unwrap(), 3);
    }

    #[test]
    fn folder_rows_are_apart_from_files() {
        let db = Db::memory().unwrap();
        for d in ["a", "a/b", "ab", "a b"] {
            db.add_dir_row(d).unwrap();
        }
        assert_eq!(db.count().unwrap(), 0);
        let mut got = db.dir_rows_under("a").unwrap();
        got.sort();
        assert_eq!(got, ["a", "a/b"]);
        db.forget_dir_row("a").unwrap();
        assert!(!db.dir_synced("a").unwrap());
    }

    #[test]
    fn paths_ci_finds_the_other_spellings_from_the_index() {
        let db = Db::memory().unwrap();
        for p in ["Docs/A.txt", "docs/a.txt", "Docs/b.txt", "top"] {
            db.set(p, &base(1), "").unwrap();
        }
        let mut got = db.paths_ci("docs/a.txt").unwrap();
        got.sort();
        assert_eq!(got, ["Docs/A.txt", "docs/a.txt"]);
        assert!(db.paths_ci("nothing").unwrap().is_empty());
        // The lookup goes through the index, not a scan of every row.
        let plan: String =
            db.0.query_row(
                "EXPLAIN QUERY PLAN SELECT path FROM files WHERE lower(path)='x'",
                [],
                |r| r.get(3),
            )
            .unwrap();
        assert!(plan.contains("files_lpath"), "{plan}");
    }

    #[test]
    fn count_in_counts_one_level_only() {
        let db = Db::memory().unwrap();
        for p in ["top", "dir/a", "dir/b", "dir/sub/c", "dir2/x"] {
            db.set(p, &base(1), "").unwrap();
        }
        assert_eq!(db.count_in("").unwrap(), 1);
        assert_eq!(db.count_in("dir").unwrap(), 2);
        assert_eq!(db.count_in("dir/sub").unwrap(), 1);
        assert_eq!(db.count_in("nope").unwrap(), 0);
    }

    #[test]
    fn rows_round_trip_and_folders_match_by_prefix() {
        let db = Db::memory().unwrap();
        assert_eq!(db.base("a").unwrap(), None);
        db.set("dir/a_b", &base(1), "id1").unwrap();
        db.set("dir2/x", &base(2), "").unwrap();
        db.set("dir/sub/c", &base(3), "id3").unwrap();
        assert_eq!(db.base("dir/a_b").unwrap(), Some(base(1)));
        let mut under = db.under("dir").unwrap();
        under.sort();
        assert_eq!(
            under,
            ["dir/a_b", "dir/sub/c"],
            "dir2 and the `_` wildcard must not match"
        );
        assert_eq!(db.path_of_id("id3").unwrap().as_deref(), Some("dir/sub/c"));
        assert_eq!(db.path_of_id("").unwrap(), None);
        db.forget("dir/a_b").unwrap();
        assert_eq!(db.all().unwrap().len(), 2);
        assert_eq!(db.count().unwrap(), 2);
        // Case-exact: `Dir` is not `dir`.
        assert!(db.under("Dir").unwrap().is_empty());
    }

    #[test]
    fn log_is_trimmed_by_age_and_count_and_read_back_newest_first() {
        let dir = std::env::temp_dir().join(format!("fastsync-log-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let file = dir.join("state.db");
        assert!(read_log(&file, 10).unwrap().is_empty(), "no file = empty log");
        let db = Db::open(&file).unwrap();
        db.0.execute(
            "INSERT INTO log(ts,kind) VALUES(?, 'up')",
            [now() - (LOG_DAYS + 1) * 86400],
        )
        .unwrap();
        for i in 0..LOG_ROWS + 5 {
            db.log("down", &format!("f{i}"), Some(3), "");
        }
        db.log("error", "last", None, "boom");
        db.trim_log();
        let all = read_log(&file, 100_000).unwrap();
        assert_eq!(all.len() as i64, LOG_ROWS, "old and over-cap rows are gone");
        assert_eq!((all[0].kind.as_str(), all[0].msg.as_str()), ("error", "boom"));
        assert_eq!(all[1].size, Some(3));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn the_local_date_reads_like_a_date() {
        let d = local_date().unwrap();
        assert!(
            d.len() == 10 && d.as_bytes()[4] == b'-' && d.as_bytes()[7] == b'-',
            "{d}"
        );
    }

    #[test]
    fn meta_and_dirs() {
        let mut db = Db::memory().unwrap();
        db.set_meta("cursor", "abc").unwrap();
        assert_eq!(db.meta("cursor").unwrap().as_deref(), Some("abc"));
        db.del_meta("cursor").unwrap();
        assert_eq!(db.meta("cursor").unwrap(), None);
        db.set_dirs(&[("1".into(), "a".into()), ("2".into(), "a/b".into())])
            .unwrap();
        assert_eq!(db.dirs().unwrap().len(), 2);
    }
}
