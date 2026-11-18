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
    /// Never returned by `decide`: the engine pairs a delete with a new path of the same bytes (a rename in the
    /// cloud) and renames the local file instead of trashing it and downloading again.
    MoveLocal,
    /// The mirror case: a local rename becomes a server-side move instead of an upload plus a delete.
    MoveRemote,
}

fn changed(now: Option<Sig>, before: Sig) -> bool {
    !now.is_some_and(|n| n.same_as(&before))
}

/// The local file against its own saved stamp: both come from this disk, so no rounding slack (a same-size edit a
/// second after the last sync would otherwise look untouched).
fn local_changed(now: Option<Sig>, before: Sig) -> bool {
    now != Some(before)
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
    match (local_changed(local, base.local), changed(remote, base.remote)) {
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

/// `myfile.txt` -> `myfile (Case Conflict).txt`, the name Dropbox's own client gives the second of two files whose
/// names differ only by case; `n > 1` adds a counter.
pub fn case_conflict_name(path: &str, n: u32) -> String {
    let (dir, file) = path.rsplit_once('/').map_or(("", path), |(d, f)| (d, f));
    let (stem, ext) = match file.rfind('.') {
        Some(i) if i > 0 => file.split_at(i),
        _ => (file, ""),
    };
    let name = if n > 1 {
        format!("{stem} (Case Conflict {n}){ext}")
    } else {
        format!("{stem} (Case Conflict){ext}")
    };
    if dir.is_empty() { name } else { format!("{dir}/{name}") }
}

/// What one name of a group that differs only by case looks like to the engine.
#[derive(Clone, Debug)]
pub struct CaseFact {
    pub path: String,
    /// A file with exactly this name is on disk.
    pub local: bool,
    /// The DB has a row for exactly this name: we synced it.
    pub row: bool,
    /// The cloud holds a file with exactly this name.
    pub cloud: bool,
    /// Modified time of the local file (to keep the oldest when none was ever synced).
    pub mtime: i64,
}

/// What to do with such a group, decided from the facts alone.
#[derive(Debug, Default, PartialEq)]
pub struct CaseResolution {
    /// `(old, new)`: the file was renamed here by changing case only (`MyFile.txt` -> `myfile.txt`). Both names go
    /// through the normal plan and pair as a move; a case-insensitive cloud would otherwise take the new name for the
    /// old file and the old name for a delete of it.
    pub rename: Vec<(String, String)>,
    /// Local files nobody synced yet that must be renamed to a `Case Conflict` name, as Dropbox's client does, so both
    /// files exist in the cloud under names it can tell apart.
    pub movers: Vec<String>,
    /// Names left alone (shown as a problem): there is nothing safe to do with them.
    pub skip: Vec<String>,
}

pub fn resolve_case_group(group: &[CaseFact]) -> CaseResolution {
    let mut out = CaseResolution::default();
    let unsynced = |f: &CaseFact| f.local && !f.row && !f.cloud;
    if let [a, b] = group {
        for (old, new) in [(a, b), (b, a)] {
            if old.row && !old.local && unsynced(new) {
                out.rename.push((old.path.clone(), new.path.clone()));
                return out;
            }
        }
    }
    let mut movers: Vec<&CaseFact> = group.iter().filter(|f| unsynced(f)).collect();
    let others = group.len() - movers.len();
    if movers.is_empty() {
        out.skip = group.iter().map(|f| f.path.clone()).collect();
        return out;
    }
    // Nothing of them was ever synced: the oldest keeps its name, the rest get the conflict name.
    if others == 0 {
        movers.sort_by(|a, b| (a.mtime, &a.path).cmp(&(b.mtime, &b.path)));
        movers.remove(0);
    }
    out.movers = movers.iter().map(|f| f.path.clone()).collect();
    // Two names that were both synced before (a service that allows it): which one is "the" file is not ours to say.
    if others >= 2 {
        out.skip = group.iter().filter(|f| !unsynced(f)).map(|f| f.path.clone()).collect();
    }
    out
}

/// Download-only (mirror) accounts: the cloud is the truth and is never written. A local edit or a local-only file
/// loses (`Conflict` there means: local copy to the trash, then download); `Upload` and `DeleteRemote` never come out.
pub fn decide_down(local: Option<Sig>, remote: Option<Sig>, base: Option<Base>) -> Action {
    match (local, remote) {
        (None, None) => {
            if base.is_some() {
                Action::Forget
            } else {
                Action::None
            }
        }
        (Some(_), None) => Action::DeleteLocal,
        (None, Some(_)) => Action::Download,
        (Some(l), Some(r)) => match base {
            Some(b) if l == b.local && r.same_as(&b.remote) => Action::None,
            Some(b) if l == b.local => Action::Download,
            _ if l.same_as(&r) => Action::Adopt,
            _ => Action::Conflict,
        },
    }
}

/// The delete guard: a run that would delete more than `percent` of the known files (once there are at least
/// `MIN_FILES`) is stopped, because an empty or unmounted folder looks exactly like "the user deleted everything".
/// A smaller folder (`SMALL_FILES` or more) is stopped only when the run deletes all of it: a percentage means nothing
/// there, but an unmounted drive must not empty even 9 files without a question.
pub const MIN_FILES: usize = 10;
const SMALL_FILES: usize = 3;
pub fn delete_guard_ok(deletes: usize, known: usize, percent: usize) -> bool {
    if known < MIN_FILES {
        return known < SMALL_FILES || deletes < known;
    }
    deletes * 100 <= known * percent
}

/// What to do with one folder (empty or not: its files have their own rows).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DirAction {
    None,
    /// Both sides have it: only the row is written.
    Adopt,
    MakeCloud,
    MakeLocal,
    RemoveLocal,
    RemoveCloud,
    Forget,
}

/// The three-way rule for folders: `synced` = a row says both sides had it after the last sync. `restore` turns
/// a removal into the opposite create (the delete guard's "Restore"). A mirror never touches the cloud.
pub fn decide_dir(local: bool, cloud: bool, synced: bool, download_only: bool, restore: bool) -> DirAction {
    use DirAction::*;
    let a = match (local, cloud, synced) {
        (true, true, _) => Adopt,
        (true, false, false) => MakeCloud,
        (false, true, false) => MakeLocal,
        (true, false, true) => RemoveLocal,
        (false, true, true) => RemoveCloud,
        (false, false, true) => Forget,
        (false, false, false) => None,
    };
    match a {
        RemoveLocal if restore && !download_only => MakeCloud,
        RemoveCloud if restore && !download_only => MakeLocal,
        // The cloud is the truth: a local-only folder goes, a cloud one we had comes back.
        MakeCloud if download_only => RemoveLocal,
        RemoveCloud if download_only => MakeLocal,
        a => a,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use Action::*;

    #[test]
    fn a_same_size_local_edit_within_two_seconds_is_still_an_edit() {
        let s = |mtime| Sig { size: 5, mtime };
        let base = Base {
            local: s(10_000),
            remote: s(10_000),
        };
        assert_eq!(decide(Some(s(11_500)), Some(s(10_000)), Some(base)), Upload);
        assert_eq!(decide(Some(s(10_000)), Some(s(10_500)), Some(base)), None);
    }

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

    // Download-only: never Upload or DeleteRemote, whatever the sides look like.
    #[test]
    fn download_only_never_writes_the_cloud() {
        let none: Option<Sig> = Option::None;
        let sigs = [none, Some(A), Some(B), Some(C)];
        for base in [Option::None, Some(BASE)] {
            for l in sigs {
                for r in sigs {
                    let got = decide_down(l, r, base);
                    assert!(!matches!(got, Upload | DeleteRemote), "{l:?} {r:?} {base:?}");
                }
            }
        }
        let b = Some(BASE);
        // (local, remote, base, expected)
        let table = [
            (Some(A), Some(A), b, Action::None),
            (Some(A), Some(B), b, Download),
            (Some(B), Some(A), b, Conflict),
            (Some(B), Some(C), b, Conflict),
            (Some(B), Some(B), b, Adopt),
            (none, Some(A), b, Download),
            (Some(A), none, b, DeleteLocal),
            (Some(B), none, b, DeleteLocal),
            (none, none, b, Forget),
            (Some(A), none, Option::None, DeleteLocal),
            (none, Some(A), Option::None, Download),
            (Some(A), Some(A), Option::None, Adopt),
            (Some(A), Some(B), Option::None, Conflict),
            (none, none, Option::None, Action::None),
        ];
        for (l, r, base, want) in table {
            assert_eq!(decide_down(l, r, base), want, "local {l:?} remote {r:?} base {base:?}");
        }
    }

    #[test]
    fn a_two_second_clock_rounding_is_not_a_change() {
        let rounded = Sig {
            size: 10,
            mtime: 1_000_900,
        };
        // The cloud rounds its stamps; the local disk does not (see `local_changed`).
        assert_eq!(decide(Some(A), Some(rounded), Some(BASE)), None);
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
        assert!(delete_guard_ok(2, 2, 50), "a folder of two files is not guarded");
        assert!(
            delete_guard_ok(4, 5, 50),
            "a small folder only stops when everything goes"
        );
        assert!(!delete_guard_ok(5, 5, 50));
        assert!(!delete_guard_ok(3, 3, 50));
        assert!(delete_guard_ok(50, 100, 50));
        assert!(!delete_guard_ok(51, 100, 50));
        assert!(!delete_guard_ok(100, 100, 50));
        assert!(delete_guard_ok(0, 1000, 50));
    }

    fn fact(path: &str, local: bool, row: bool, cloud: bool, mtime: i64) -> CaseFact {
        CaseFact {
            path: path.into(),
            local,
            row,
            cloud,
            mtime,
        }
    }

    #[test]
    fn case_conflict_names_follow_dropbox() {
        assert_eq!(case_conflict_name("myfile.txt", 1), "myfile (Case Conflict).txt");
        assert_eq!(
            case_conflict_name("d/e/myfile.txt", 2),
            "d/e/myfile (Case Conflict 2).txt"
        );
        assert_eq!(case_conflict_name(".bashrc", 1), ".bashrc (Case Conflict)");
        assert_eq!(case_conflict_name("a.tar.gz", 1), "a.tar (Case Conflict).gz");
    }

    #[test]
    fn a_case_only_rename_is_a_rename_and_never_a_conflict() {
        // MyFile.txt synced, gone here; myfile.txt new here.
        let g = [
            fact("MyFile.txt", false, true, true, 0),
            fact("myfile.txt", true, false, false, 5),
        ];
        let r = resolve_case_group(&g);
        assert_eq!(r.rename, [("MyFile.txt".to_string(), "myfile.txt".to_string())]);
        assert!(r.movers.is_empty() && r.skip.is_empty());
        // The same either way round.
        let g = [g[1].clone(), g[0].clone()];
        assert_eq!(resolve_case_group(&g).rename.len(), 1);
    }

    #[test]
    fn a_second_file_differing_only_by_case_is_renamed_as_a_case_conflict() {
        // MyFile.txt synced and still here; myfile.txt created next to it.
        let g = [
            fact("MyFile.txt", true, true, true, 0),
            fact("myfile.txt", true, false, false, 9),
        ];
        let r = resolve_case_group(&g);
        assert_eq!(
            (r.movers, r.skip, r.rename.len()),
            (vec!["myfile.txt".to_string()], vec![], 0)
        );
        // The first one only in the cloud (not downloaded yet): same.
        let g = [
            fact("Report.txt", false, false, true, 0),
            fact("report.txt", true, false, false, 9),
        ];
        assert_eq!(resolve_case_group(&g).movers, ["report.txt"]);
    }

    #[test]
    fn two_new_files_keep_the_oldest_name_and_two_synced_ones_are_left_alone() {
        let g = [
            fact("A.txt", true, false, false, 50),
            fact("a.txt", true, false, false, 10),
        ];
        let r = resolve_case_group(&g);
        assert_eq!(r.movers, ["A.txt"], "the older a.txt keeps its name");
        // Both synced before (Drive allows it): nothing safe to do, a third new one is still renamed.
        let g = [
            fact("A.txt", true, true, true, 0),
            fact("a.txt", true, true, true, 0),
            fact("a.TXT", true, false, false, 7),
        ];
        let r = resolve_case_group(&g);
        assert_eq!(r.movers, ["a.TXT"]);
        assert_eq!(r.skip, ["A.txt", "a.txt"]);
        // Nothing new at all: skipped, as before.
        let g = [fact("A.txt", true, true, true, 0), fact("a.txt", true, true, true, 0)];
        assert_eq!(resolve_case_group(&g).skip.len(), 2);
    }

    #[test]
    fn folder_rule_covers_every_input() {
        use DirAction as D;
        // (local, cloud, synced) -> (normal, restore, download_only)
        let table = [
            ((true, true, false), (D::Adopt, D::Adopt, D::Adopt)),
            ((true, true, true), (D::Adopt, D::Adopt, D::Adopt)),
            ((true, false, false), (D::MakeCloud, D::MakeCloud, D::RemoveLocal)),
            ((false, true, false), (D::MakeLocal, D::MakeLocal, D::MakeLocal)),
            ((true, false, true), (D::RemoveLocal, D::MakeCloud, D::RemoveLocal)),
            ((false, true, true), (D::RemoveCloud, D::MakeLocal, D::MakeLocal)),
            ((false, false, true), (D::Forget, D::Forget, D::Forget)),
            ((false, false, false), (D::None, D::None, D::None)),
        ];
        for ((l, c, s), (normal, restore, mirror)) in table {
            assert_eq!(decide_dir(l, c, s, false, false), normal, "{l} {c} {s}");
            assert_eq!(decide_dir(l, c, s, false, true), restore, "restore {l} {c} {s}");
            assert_eq!(decide_dir(l, c, s, true, false), mirror, "mirror {l} {c} {s}");
            // A restore never does anything in a mirror.
            assert_eq!(decide_dir(l, c, s, true, true), mirror, "mirror restore {l} {c} {s}");
        }
    }
}
