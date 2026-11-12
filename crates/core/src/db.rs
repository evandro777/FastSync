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
             CREATE TABLE IF NOT EXISTS dirs(id TEXT PRIMARY KEY, path TEXT NOT NULL);
             CREATE TABLE IF NOT EXISTS meta(k TEXT PRIMARY KEY, v TEXT);",
        )?;
        Ok(Self(c))
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

    /// The known files under a folder (`dir/`), for a folder that was removed or renamed.
    pub fn under(&self, dir: &str) -> Result<Vec<String>> {
        let like = format!(
            "{}/%",
            dir.replace('\\', "\\\\").replace('%', "\\%").replace('_', "\\_")
        );
        let mut st = self.0.prepare("SELECT path FROM files WHERE path LIKE ? ESCAPE '\\'")?;
        st.query_map([like], |r| r.get(0))?.collect()
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
