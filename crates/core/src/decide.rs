//! The three-way compare: what one file looks like locally, remotely and when we last synced it decides what to do.
//! Pure functions only, so every combination is unit-tested.

/// What we know about one side of a file. `mtime` is in milliseconds.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Sig {
    pub size: u64,
    pub mtime: i64,
}

impl Sig {
    /// Backends round times (Dropbox keeps seconds), so two stamps within 2 s are the same stamp.
    pub fn same_as(&self, other: &Sig) -> bool {
        self.size == other.size && (self.mtime - other.mtime).abs() < 2000
    }
}

/// Both sides as they were right after the last successful sync of the file.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Base {
    pub local: Sig,
    pub remote: Sig,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Action {
    /// Nothing to do.
    None,
    /// Both sides already agree but the DB does not know it (first sync, or a lost DB): record them as the base.
    Adopt,
    /// Both sides are gone: forget the DB row.
    Forget,
    Upload,
    Download,
    /// Moved to the trash folder, never removed.
    DeleteLocal,
    DeleteRemote,
    /// Both changed: the local copy is renamed aside, the remote one is downloaded.
    Conflict,
}

fn changed(now: Option<Sig>, before: Sig) -> bool {
    !now.is_some_and(|n| n.same_as(&before))
}

pub fn decide(local: Option<Sig>, remote: Option<Sig>, base: Option<Base>) -> Action {
    let Some(base) = base else {
        return match (local, remote) {
            (None, None) => Action::None,
            (Some(_), None) => Action::Upload,
            (None, Some(_)) => Action::Download,
            (Some(l), Some(r)) if l.same_as(&r) => Action::Adopt,
            (Some(_), Some(_)) => Action::Conflict,
        };
    };
    match (changed(local, base.local), changed(remote, base.remote)) {
        (false, false) => Action::None,
        (true, false) => local.map_or(Action::DeleteRemote, |_| Action::Upload),
        (false, true) => remote.map_or(Action::DeleteLocal, |_| Action::Download),
        // Both changed. A deletion against an edit keeps the edit: losing data is worse than keeping a file.
        (true, true) => match (local, remote) {
            (None, None) => Action::Forget,
            (Some(_), None) => Action::Upload,
            (None, Some(_)) => Action::Download,
            (Some(l), Some(r)) if l.same_as(&r) => Action::Adopt,
            (Some(_), Some(_)) => Action::Conflict,
        },
    }
}

/// `report.pdf` -> `report (conflict 2026-11-11).pdf`; `n > 1` adds a counter for a second conflict on the same day.
pub fn conflict_name(path: &str, date: &str, n: u32) -> String {
    let (dir, file) = path.rsplit_once('/').map_or(("", path), |(d, f)| (d, f));
    // A leading dot is part of the name (`.bashrc`), not an extension.
    let (stem, ext) = match file.rfind('.') {
        Some(i) if i > 0 => file.split_at(i),
        _ => (file, ""),
    };
    let tag = if n > 1 {
        format!("conflict {date} {n}")
    } else {
        format!("conflict {date}")
    };
    let name = format!("{stem} ({tag}){ext}");
    if dir.is_empty() { name } else { format!("{dir}/{name}") }
}

/// The delete guard: a run that would delete more than `percent` of the known files (once there are at least
/// `MIN_FILES`) is stopped, because an empty or unmounted folder looks exactly like "the user deleted everything".
pub const MIN_FILES: usize = 10;
pub fn delete_guard_ok(deletes: usize, known: usize, percent: usize) -> bool {
    known < MIN_FILES || deletes * 100 <= known * percent
}

/// Groups of paths that differ only by case. Dropbox and OneDrive treat them as one file, so syncing them would
/// make one overwrite the other.
pub fn case_collisions<'a>(paths: impl Iterator<Item = &'a str>) -> std::collections::HashSet<String> {
    let mut seen: std::collections::HashMap<String, &str> = Default::default();
    let mut bad = std::collections::HashSet::new();
    for p in paths {
        match seen.insert(p.to_lowercase(), p) {
            Some(prev) if prev != p => {
                bad.insert(p.to_string());
                bad.insert(prev.to_string());
            }
            _ => {}
        }
    }
    bad
}

#[cfg(test)]
mod tests {
    use super::*;
    use Action::*;

    const A: Sig = Sig {
        size: 10,
        mtime: 1_000_000,
    };
    const B: Sig = Sig {
        size: 20,
        mtime: 2_000_000,
    };
    const C: Sig = Sig {
        size: 30,
        mtime: 3_000_000,
    };
    const BASE: Base = Base { local: A, remote: A };

    #[test]
    fn without_a_base() {
        assert_eq!(decide(Option::None, Option::None, Option::None), Action::None);
        assert_eq!(decide(Some(A), Option::None, Option::None), Upload);
        assert_eq!(decide(Option::None, Some(A), Option::None), Download);
        assert_eq!(decide(Some(A), Some(A), Option::None), Adopt);
        assert_eq!(decide(Some(A), Some(B), Option::None), Conflict);
    }

    // The full table with a base: every combination of local and remote being unchanged, edited or deleted.
    #[test]
    fn with_a_base() {
        let b = Some(BASE);
        let none: Option<Sig> = Option::None;
        // (local, remote, expected)
        let table = [
            (Some(A), Some(A), None),
            (Some(B), Some(A), Upload),
            (none, Some(A), DeleteRemote),
            (Some(A), Some(B), Download),
            (Some(A), none, DeleteLocal),
            (Some(B), Some(C), Conflict),
            (Some(B), Some(B), Adopt),
            (Some(B), none, Upload),
            (none, Some(B), Download),
            (none, none, Forget),
        ];
        for (l, r, want) in table {
            assert_eq!(decide(l, r, b), want, "local {l:?} remote {r:?}");
        }
    }

    #[test]
    fn a_two_second_clock_rounding_is_not_a_change() {
        let rounded = Sig {
            size: 10,
            mtime: 1_000_900,
        };
        assert_eq!(decide(Some(rounded), Some(A), Some(BASE)), None);
        assert_eq!(decide(Some(Sig { size: 11, ..A }), Some(A), Some(BASE)), Upload);
    }

    #[test]
    fn conflict_names() {
        assert_eq!(
            conflict_name("a/b/report.pdf", "2026-11-11", 1),
            "a/b/report (conflict 2026-11-11).pdf"
        );
        assert_eq!(conflict_name("notes", "2026-11-11", 1), "notes (conflict 2026-11-11)");
        assert_eq!(
            conflict_name(".bashrc", "2026-11-11", 1),
            ".bashrc (conflict 2026-11-11)"
        );
        assert_eq!(
            conflict_name("x.tar.gz", "2026-11-11", 2),
            "x.tar (conflict 2026-11-11 2).gz"
        );
    }

    #[test]
    fn delete_guard() {
        assert!(delete_guard_ok(5, 5, 50), "tiny folders are not guarded");
        assert!(delete_guard_ok(50, 100, 50));
        assert!(!delete_guard_ok(51, 100, 50));
        assert!(!delete_guard_ok(100, 100, 50));
        assert!(delete_guard_ok(0, 1000, 50));
    }

    #[test]
    fn case_only_names_collide() {
        let bad = case_collisions(["a.txt", "A.txt", "b.txt", "dir/c", "dir/c"].into_iter());
        assert_eq!(bad.len(), 2);
        assert!(bad.contains("a.txt") && bad.contains("A.txt"));
    }
}
