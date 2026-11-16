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
               path TEXT PRIMARY KEY, l_size INT, l_mtime INT, r_size INT, r_mtime INT, r_id TEXT NOT NULL DEFAULT '');
             CREATE INDEX IF NOT EXISTS files_rid ON files(r_id);
             CREATE INDEX IF NOT EXISTS files_lsize ON files(l_size);
             CREATE INDEX IF NOT EXISTS files_lpath ON files(lower(path));
             CREATE TABLE IF NOT EXISTS dirs(id TEXT PRIMARY KEY, path TEXT NOT NULL);
             CREATE TABLE IF NOT EXISTS meta(k TEXT PRIMARY KEY, v TEXT);
             CREATE TABLE IF NOT EXISTS log(
               id INTEGER PRIMARY KEY, ts INT NOT NULL, kind TEXT NOT NULL, path TEXT NOT NULL DEFAULT '',
               size INT, msg TEXT NOT NULL DEFAULT '');",
        )?;
        // The first release is version 1. A later release that changes a table reads this to migrate: `CREATE TABLE IF
        // NOT EXISTS` above never adds a column to an existing file.
        if c.query_row("PRAGMA user_version", [], |r| r.get::<_, i64>(0))? == 0 {
            c.execute_batch("PRAGMA user_version=1")?;
        }
        Ok(Self(c))
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
        self.0.execute(
            "INSERT OR REPLACE INTO files(path,l_size,l_mtime,r_size,r_mtime,r_id) VALUES(?,?,?,?,?,?)",
            params![
                path,
                b.local.size as i64,
                b.local.mtime,
                b.remote.size as i64,
                b.remote.mtime,
                remote_id
            ],
        )?;
        Ok(())
    }

    pub fn forget(&self, path: &str) -> Result<()> {
        self.0.execute("DELETE FROM files WHERE path=?", [path])?;
        Ok(())
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
    /// `up`, `down`, `trash`, `delcloud`, `conflict`, `error`, `stopped` or `scan`.
    pub kind: String,
    pub path: String,
    pub size: Option<u64>,
    pub msg: String,
}

/// The newest entries, newest first, through a read-only connection of its own: the runner's engine holds the only
/// writer, and WAL lets this one read at the same time. A missing file (account never ran) is an empty log.
pub fn read_log(db: &Path, limit: usize) -> Result<Vec<LogEntry>> {
    use rusqlite::OpenFlags;
    let Ok(c) = Connection::open_with_flags(db, OpenFlags::SQLITE_OPEN_READ_ONLY) else {
        return Ok(Vec::new());
    };
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
    fn a_new_db_is_schema_version_1() {
        let db = Db::memory().unwrap();
        let v: i64 = db.0.query_row("PRAGMA user_version", [], |r| r.get(0)).unwrap();
        assert_eq!(v, 1);
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
