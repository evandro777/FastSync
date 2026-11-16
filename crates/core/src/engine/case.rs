//! Names that differ only by upper/lower case. Dropbox and OneDrive take them for one file, so a rename by case alone
//! must be a rename, a second new file gets a `Case Conflict` name, and what is neither is left alone. The facts-only
//! rules are `decide::resolve_case_group`; this file gathers the facts and does the renames.
use std::{
    collections::{BTreeSet, HashMap, HashSet},
    fs,
    path::Path,
};

use super::{Engine, Remote, Report, Result, local_sig, sig_item, under_symlink};
use crate::{
    db::Db,
    decide::{CaseFact, Sig, case_conflict_name, resolve_case_group},
    rclone::Item,
};

/// Why a path is left alone when another name in the same folder differs from it only by upper/lower case: Dropbox and
/// OneDrive treat them as one file, so syncing both would let one overwrite the other. Also what `lookup` answers (as an
/// error) for a path whose cloud folder holds only another spelling of the name.
pub(super) const CLASH_WHY: &str = "another file differs only by upper/lower case, skipped";

/// True when `found`, what the cloud answered for the new name `requested` of a rename by case alone, is the file the
/// old name `old` was and not another file that happens to hold the new name (Drive allows both `MyFile.txt` and
/// `myfile.txt`): by id when both have one, else by the other spelling it answers with, else by size and time. Only
/// then may the new name be read as "no cloud copy of its own" and the two names be moved onto each other.
pub(super) fn is_old_file(found: &Item, old: &Item, requested: &str) -> bool {
    if !found.id.is_empty() && !old.id.is_empty() {
        return found.id == old.id;
    }
    (found.path != requested && found.path.to_lowercase() == requested.to_lowercase())
        || sig_item(found).same_as(&sig_item(old))
}

/// `paths` plus the names in their folders that differ from one of them only by case: the namesakes a watcher event
/// does not mention (on disk, and in the DB, where the old name of a rename by case alone still is). `full_scan` sees
/// every name; a run of a few paths must not skip the check. One `read_dir` per folder and one indexed DB lookup per
/// path (`Db::paths_ci`: never every row of a big tree).
pub(super) fn case_siblings(root: &Path, db: &Db, paths: &[String]) -> BTreeSet<String> {
    let want: HashSet<String> = paths.iter().map(|p| p.to_lowercase()).collect();
    let mut out: BTreeSet<String> = paths.iter().cloned().collect();
    let parent = |p: &str| p.rsplit_once('/').map_or(String::new(), |(d, _)| d.to_string());
    for p in paths {
        let dir = parent(p);
        out.extend(
            db.paths_ci(&p.to_ascii_lowercase())
                .unwrap_or_default()
                .into_iter()
                .filter(|k| parent(k) == dir && k.to_lowercase() == p.to_lowercase()),
        );
    }
    let dirs: BTreeSet<&str> = paths
        .iter()
        .map(|p| p.rsplit_once('/').map_or("", |(d, _)| d))
        .collect();
    for dir in dirs {
        let Ok(rd) = fs::read_dir(root.join(dir)) else {
            continue;
        };
        for e in rd.flatten() {
            let Some(n) = e.file_name().to_str().map(str::to_string) else {
                continue;
            };
            let p = if dir.is_empty() { n } else { format!("{dir}/{n}") };
            if want.contains(&p.to_lowercase()) {
                out.insert(p);
            }
        }
    }
    out
}

/// What `Engine::settle_case_clashes` decided about the names that differ only by case.
#[derive(Default)]
pub(super) struct Settled {
    /// `(old, new)`: a file renamed here by changing case only. Both names go through the plan; `new` must be read as
    /// a file with no cloud copy, because a case-insensitive cloud answers for it with the old file.
    pub(super) renames: Vec<(String, String)>,
    /// `(old, new)`: a new local file renamed to its `Case Conflict` name, which is then a new file like any other.
    pub(super) moved: Vec<(String, String)>,
    /// Names left alone, shown as a problem.
    pub(super) skip: HashSet<String>,
}

/// Added to the new name for the middle step of a rename by case alone.
pub(super) const CASE_TMP: &str = ".fastsync-case-tmp";

/// Renames the cloud file `from` to `to`, which differs from it only by case, through a temporary name. A case-
/// insensitive cloud may refuse (or ignore) a direct rename to "the same name": two steps always change the stored
/// spelling. A failed second step is undone, so the file keeps its old name instead of being left under the temporary
/// one.
pub(super) fn rename_by_case<R: Remote>(remote: &R, from: &str, to: &str) -> Result<()> {
    let tmp = format!("{to}{CASE_TMP}");
    if remote.stat(&tmp)?.is_some() {
        return Err(format!("{tmp} is in the way"));
    }
    remote.rename(from, &tmp)?;
    remote.rename(&tmp, to).map_err(|e| {
        let _ = remote.rename(&tmp, from);
        format!("renaming through a temporary name failed and was undone: {e}")
    })
}

/// Errors unless the cloud's own folder listing shows `path` spelled exactly like that. A case-only rename the cloud
/// answered "ok" to may have changed nothing (the stored spelling is what a listing shows; `stat` may echo the spelling
/// it was asked for), and a row written for it would leave the DB and the cloud disagreeing for ever, every full scan
/// skipping the pair as a name clash. A listing that cannot be had is not an answer: then the rename is accepted.
pub(super) fn spelling_kept<R: Remote>(remote: &R, path: &str) -> Result<()> {
    let parent = path.rsplit_once('/').map_or("", |(d, _)| d);
    match remote.list_dir(parent) {
        Ok(items) if !items.iter().any(|i| i.path == path) => Err(format!("the cloud kept the old spelling of {path}")),
        _ => Ok(()),
    }
}

/// `rename_by_case` for a whole cloud folder.
pub(super) fn move_dir_by_case<R: Remote>(remote: &R, from: &str, to: &str) -> Result<()> {
    let tmp = format!("{to}{CASE_TMP}");
    if remote.stat(&tmp)?.is_some() {
        return Err(format!("{tmp} is in the way"));
    }
    remote.move_dir(from, &tmp)?;
    remote.move_dir(&tmp, to).map_err(|e| {
        let _ = remote.move_dir(&tmp, from);
        format!("renaming through a temporary name failed and was undone: {e}")
    })
}

impl<R: Remote> Engine<R> {
    /// Picks the `Case Conflict` name (`myfile (Case Conflict).txt`, like Dropbox's own client) for the new local file
    /// `path`, because another file in the folder differs from it only by case and the cloud would take them for one
    /// file. Only a regular file that nobody syncs yet, never a name that exists or is already planned. Nothing is
    /// renamed here: the plan is made with the new name and `apply_case_renames` renames after the guard.
    pub(super) fn plan_case_conflict(&self, path: &str, planned: &[String]) -> Result<String> {
        if under_symlink(&self.root, path) {
            return Err("inside a symlinked folder, skipped".into());
        }
        let meta = fs::symlink_metadata(self.root.join(path)).map_err(|e| e.to_string())?;
        if !meta.is_file() {
            return Err("not a regular file".into());
        }
        Ok((1..)
            .map(|n| case_conflict_name(path, n))
            .find(|c| fs::symlink_metadata(self.root.join(c)).is_err() && !planned.contains(c))
            .unwrap_or_default())
    }

    /// The local stamp of `p`, or that of the file about to be renamed to it (`case_renames`): the plan counts a file
    /// under its new name before it is renamed.
    pub(super) fn planned_sig(&self, p: &str) -> Option<Sig> {
        local_sig(&self.root, p).or_else(|| {
            self.case_renames
                .iter()
                .find(|(_, new, _)| new == p)
                .map(|(_, _, sig)| *sig)
        })
    }

    /// Renames the files planned for a `Case Conflict` name. Called by `run` once the delete guard and the cancel checks
    /// have passed, so a stopped run leaves the user's files alone. A file that changed or vanished since the plan is
    /// left where it is and its planned upload is dropped (returned). Logged and counted as a conflict, so the user
    /// is told.
    pub(super) fn apply_case_renames(&mut self, report: &mut Report) -> HashSet<String> {
        let mut dropped = HashSet::new();
        for (old, new, sig) in std::mem::take(&mut self.case_renames) {
            let res = if !under_symlink(&self.root, &old)
                && local_sig(&self.root, &old) == Some(sig)
                && fs::symlink_metadata(self.root.join(&new)).is_err()
            {
                fs::rename(self.root.join(&old), self.root.join(&new)).map_err(|e| e.to_string())
            } else {
                Err("changed since it was planned, will retry".to_string())
            };
            match res {
                Ok(()) => {
                    self.db.log(
                        "conflict",
                        &new,
                        Some(sig.size),
                        &format!("renamed from {old}: another file differs only by upper/lower case"),
                    );
                    report.conflicts.push(new);
                }
                Err(e) => {
                    self.db.log("error", &old, None, &e);
                    report.fail(&old, &e);
                    dropped.insert(new);
                }
            }
        }
        dropped
    }

    /// Decides, for each group of `names` that differ only by case, what is safe (see `resolve_case_group`) and which
    /// local files must get a `Case Conflict` name (`Settled::moved`, planned, not yet renamed). `cloud(name)`: the
    /// cloud holds exactly this name. A mirror never renames here (the cloud is the truth), so there the whole group
    /// is left alone.
    pub(super) fn settle_case_clashes<'a>(
        &self,
        names: impl Iterator<Item = &'a str>,
        cloud: &dyn Fn(&str) -> bool,
        report: &mut Report,
    ) -> Settled {
        let mut groups: HashMap<String, Vec<&str>> = HashMap::new();
        for n in names {
            groups.entry(n.to_lowercase()).or_default().push(n);
        }
        let mut out = Settled::default();
        for mut g in groups.into_values() {
            g.sort_unstable();
            g.dedup();
            if g.len() < 2 {
                continue;
            }
            if self.download_only {
                out.skip.extend(g.iter().map(|p| p.to_string()));
                continue;
            }
            let facts: Vec<CaseFact> = g
                .iter()
                .map(|&p| {
                    let local = local_sig(&self.root, p);
                    CaseFact {
                        path: p.to_string(),
                        local: local.is_some(),
                        row: self.db.base(p).ok().flatten().is_some(),
                        cloud: cloud(p),
                        mtime: local.map_or(0, |s| s.mtime),
                    }
                })
                .collect();
            let res = resolve_case_group(&facts);
            out.renames.extend(res.rename);
            out.skip.extend(res.skip);
            for p in res.movers {
                let planned: Vec<String> = out
                    .moved
                    .iter()
                    .map(|(_, n)| n.clone())
                    .chain(self.case_renames.iter().map(|(_, n, _)| n.clone()))
                    .collect();
                match self.plan_case_conflict(&p, &planned) {
                    Ok(new) => out.moved.push((p, new)),
                    Err(e) => {
                        // Not ours to touch (a link, a vanished file): left alone, shown, looked at again later.
                        self.db.log("error", &p, None, &e);
                        report.fail(&p, &e);
                        out.skip.insert(p);
                    }
                }
            }
        }
        out
    }
}
