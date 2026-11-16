//! Matching same-bytes files before a run acts: a delete plus a new path becomes a move, a new cloud file whose bytes
//! sit in another local file becomes a local copy, and two equal-bytes "conflicts" become an adopt. All of them rest
//! on `Remote::same_content` / `compare`, so a service without a hash (MEGA) never pairs.
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    sync::{
        Mutex,
        atomic::{AtomicUsize, Ordering::Relaxed},
    },
};

use super::{
    CASE_TMP, COPY_MIN, Engine, Kind, Moved, PAIR_TRIES, PARALLEL, Phase, Planned, Progress, Remote, Report, Work,
    excluded, local_sig, log_done, log_failed, move_dir_by_case, refuse, sig_item, spelling_kept, under_symlink,
};
use crate::{
    decide::{Action, Base},
    rclone::Item,
};

/// The file name and the folder of a `/` path: candidates for a move or copy share one of them.
fn name(p: &str) -> String {
    p.rsplit('/').next().unwrap_or(p).to_string()
}

fn parent(p: &str) -> String {
    p.rsplit_once('/').map_or(String::new(), |(d, _)| d.to_string())
}

/// The folders a file's move renamed: what is left of both paths once their common ending (the file name and any
/// folders that kept their names) is cut off. `None` when the file name changed too, or one side has no folder left
/// (a file moved out of or into the root, which is not a folder rename), or one folder lies inside the other.
pub(super) fn dir_pair(old: &str, new: &str) -> Option<(String, String)> {
    let (o, n): (Vec<&str>, Vec<&str>) = (old.split('/').collect(), new.split('/').collect());
    let k = o.iter().rev().zip(n.iter().rev()).take_while(|(x, y)| x == y).count();
    if k == 0 || k >= o.len() || k >= n.len() {
        return None;
    }
    let (a, b) = (o[..o.len() - k].join("/"), n[..n.len() - k].join("/"));
    (!a.starts_with(&format!("{b}/")) && !b.starts_with(&format!("{a}/"))).then_some((a, b))
}

impl<R: Remote> Engine<R> {
    /// Turns a delete plus a new path holding the same bytes into one move: a folder renamed in the cloud is renamed
    /// here instead of trashed and downloaded again, a folder renamed here is moved in the cloud instead of
    /// uploaded again. The proof is the service's own hash (`same_content`), so a service without one (MEGA) never
    /// pairs and keeps today's copy + delete. A wrong guess is impossible: equal hashes are equal bytes.
    /// shortcut: hashes one candidate at a time on this thread, at most `PAIR_TRIES` per new path; parallelise it
    /// if renaming thousands of files with a slow `local_hash` proves too slow.
    pub(super) fn pair_moves(&self, plan: Vec<Planned>) -> Vec<Planned> {
        // Old paths by size: only a delete that was safe to make anyway (we synced it, the local copy untouched; the
        // cloud copy present and the local one gone) can become a move.
        let mut olds: HashMap<u64, Vec<usize>> = HashMap::new();
        for (i, (w, a, base)) in plan.iter().enumerate() {
            let size = match (a, w.local, &w.remote, base) {
                (Action::DeleteLocal, Some(l), _, Some(b)) if b.local == l => Some(l.size),
                (Action::DeleteRemote, None, Some(r), _) => Some(r.size),
                _ => None,
            };
            if let Some(s) = size {
                olds.entry(s).or_default().push(i);
            }
        }
        if olds.is_empty() {
            return plan;
        }
        let mut taken = vec![false; plan.len()];
        let mut pairs: Vec<(usize, usize)> = Vec::new();
        for (n, (w, a, _)) in plan.iter().enumerate() {
            // Hashing is slow: a Pause ends it (the pairs found so far are kept; `run` drops the plan anyway).
            if self.cancelled() {
                break;
            }
            // The new path, and the size it must have.
            let (size, local_move) = match (a, w.local, &w.remote) {
                (Action::Download, None, Some(r)) => (r.size, true),
                (Action::Upload, Some(l), None) => (l.size, false),
                _ => continue,
            };
            let mut cands: Vec<usize> = olds
                .get(&size)
                .into_iter()
                .flatten()
                .copied()
                .filter(|&o| {
                    let (ow, oa, _) = &plan[o];
                    !taken[o]
                        && (*oa == Action::DeleteLocal) == local_move
                        && (name(&ow.path) == name(&w.path) || parent(&ow.path) == parent(&w.path))
                })
                .collect();
            // The same file name first: a folder rename keeps the names, so that is nearly always the match.
            cands.sort_by_key(|&o| name(&plan[o].0.path) != name(&w.path));
            for o in cands.into_iter().take(PAIR_TRIES) {
                let (ow, _, old_base) = &plan[o];
                let same_name = name(&ow.path) == name(&w.path);
                let same = if local_move {
                    // The old local file against the cloud's new item.
                    w.remote.as_ref().is_some_and(|r| {
                        self.remote.same_content(&self.root, &ow.path, r)
                            // A service with no hash (MEGA) cannot prove bytes: the same name, size and time as at
                            // the last sync is all there is, and what makes a folder rename cost nothing instead of
                            // a download. The name must be kept: files from an archive or `rsync -t` share sizes
                            // and times, so a different name proves nothing. With a hash only the hash counts.
                            || (r.hash.is_none()
                                && same_name
                                && old_base.is_some_and(|b| sig_item(r) == b.remote))
                    })
                } else {
                    // The new local file against the cloud's old item.
                    ow.remote.as_ref().is_some_and(|r| {
                        self.remote.same_content(&self.root, &w.path, r)
                            || (r.hash.is_none() && same_name && old_base.is_some_and(|b| w.local == Some(b.local)))
                    })
                };
                if same {
                    taken[o] = true;
                    pairs.push((o, n));
                    break;
                }
            }
        }
        if pairs.is_empty() {
            return plan;
        }
        let mut plan: Vec<Option<Planned>> = plan.into_iter().map(Some).collect();
        for (o, n) in pairs {
            let Some((old, old_action, _)) = plan[o].take() else {
                continue;
            };
            if let Some((new, action, _)) = plan[n].as_mut() {
                new.from = Some(Box::new(Moved {
                    path: old.path,
                    local: old.local,
                    remote: old.remote,
                }));
                *action = if old_action == Action::DeleteLocal {
                    Action::MoveLocal
                } else {
                    Action::MoveRemote
                };
            }
        }
        plan.into_iter().flatten().collect()
    }

    /// A new cloud file whose bytes already sit in another synced local file (a duplicate made in the cloud) is
    /// copied from that file instead of downloaded. Same proof as a move (`same_content`), but the source stays: it
    /// goes in `from` and `fetch` tries it first, falling back to the download on any doubt.
    /// shortcut: hashes one candidate at a time on this thread, `PAIR_TRIES` per file; parallelise it if many big
    /// duplicates arrive at once.
    pub(super) fn pair_copies(&self, plan: &mut [Planned]) {
        for (w, action, _) in plan.iter_mut() {
            if self.cancelled() {
                break;
            }
            let (Action::Download, None, Some(r)) = (*action, w.local, &w.remote) else {
                continue;
            };
            if r.size < COPY_MIN {
                continue;
            }
            let Ok(mut cands) = self.db.paths_with_size(r.size) else {
                continue;
            };
            // Only a file still exactly as synced, that the rules do not hide, and not through a symlinked folder.
            cands.retain(|(p, s)| {
                *p != w.path
                    && !self.ignore.matches(p)
                    && !under_symlink(&self.root, p)
                    && local_sig(&self.root, p) == Some(*s)
            });
            cands.sort_by_key(|(p, _)| (name(p) != name(&w.path), parent(p) != parent(&w.path)));
            for (p, s) in cands.into_iter().take(PAIR_TRIES) {
                if self.remote.same_content(&self.root, &p, r) {
                    w.from = Some(Box::new(Moved {
                        path: p,
                        local: Some(s),
                        remote: None,
                    }));
                    break;
                }
            }
        }
    }

    /// A new local file whose bytes the cloud already holds under another name (a local copy of a synced file) is
    /// copied there instead of uploaded. The proof is live: each candidate (same size, still as synced) is `stat`ed
    /// for its cloud hash and compared with the new file's local hash, which is read once per hash kind. `transfer`
    /// then asks the service to copy, checks the result's hash, and uploads on any doubt.
    /// shortcut: one `stat` per candidate, sequential, at most `PAIR_TRIES`; store the hash in the DB if files of one
    /// size are so many that the right source is often past the cap.
    pub(super) fn pair_uploads(&self, plan: &mut [Planned]) {
        if self.download_only {
            return;
        }
        for (w, action, _) in plan.iter_mut() {
            if self.cancelled() {
                break;
            }
            let (Action::Upload, Some(l), None, None) = (*action, w.local, &w.remote, &w.from) else {
                continue;
            };
            if l.size < COPY_MIN {
                continue;
            }
            let Ok(mut cands) = self.db.paths_with_size(l.size) else {
                continue;
            };
            cands.retain(|(p, s)| {
                *p != w.path
                    && !self.ignore.matches(p)
                    && !under_symlink(&self.root, p)
                    && local_sig(&self.root, p) == Some(*s)
            });
            cands.sort_by_key(|(p, _)| (name(p) != name(&w.path), parent(p) != parent(&w.path)));
            // The new file is read once per hash kind, not once per candidate.
            let mut mine: HashMap<String, Option<String>> = HashMap::new();
            for (p, _) in cands.into_iter().take(PAIR_TRIES) {
                if self.cancelled() {
                    break;
                }
                // A failed `stat` or a missing cloud copy is just "not this one".
                let Ok(Some(item)) = self.remote.stat(&p) else {
                    continue;
                };
                let Some((kind, want)) = item.hash.clone() else {
                    continue;
                };
                if item.is_dir || item.size != l.size {
                    continue;
                }
                let got = mine
                    .entry(kind.clone())
                    .or_insert_with(|| self.remote.local_hash(&self.root, &w.path, &kind).ok().flatten());
                if got.as_deref() == Some(want.as_str()) {
                    w.from = Some(Box::new(Moved {
                        path: p,
                        local: None,
                        remote: Some(item),
                    }));
                    break;
                }
            }
        }
    }

    /// A folder renamed here shows up as one `MoveRemote` per file. Where the files of a folder all went to one new
    /// folder, the cloud folder is renamed with one call instead (rclone uses the service's own folder move), which
    /// takes the same time for 5 files and 5000. Only on proof that the move changes nothing else: every synced
    /// file of the old folder moves, the cloud's own listing of it holds exactly those files with the sizes and times
    /// we synced, the new name is free and the new local files are untouched. Anything else, or a failed call, leaves
    /// the jobs for the one-by-one move. Returns how many jobs it finished (removed from `jobs`).
    pub(super) fn move_dirs(
        &self,
        jobs: &mut Vec<(Work, Action)>,
        (done, total): (usize, usize),
        report: &mut Report,
        gone: &mut Vec<String>,
    ) -> usize {
        let mut groups: BTreeMap<(String, String), Vec<usize>> = BTreeMap::new();
        for (i, (w, a)) in jobs.iter().enumerate() {
            if let (Action::MoveRemote, Some(f)) = (a, &w.from)
                && let Some(dirs) = dir_pair(&f.path, &w.path)
            {
                groups.entry(dirs).or_default().push(i);
            }
        }
        let mut finished = HashSet::new();
        for ((a, b), idx) in groups {
            // A folder renamed by case alone (`Docs` -> `docs`) is not a move file by file: a case-insensitive cloud
            // answers for `docs/a.txt` with the file in `Docs`, so only the folder's own rename changes its name. Even
            // one file in it is worth that.
            let case_only = a.to_lowercase() == b.to_lowercase();
            if (idx.len() < 2 && !case_only) || self.cancelled() {
                continue;
            }
            let group: Vec<&Work> = idx.iter().map(|&i| &jobs[i].0).collect();
            if !self.dir_move_proven(&a, &b, &group, case_only) {
                continue;
            }
            self.set_progress(Progress {
                done,
                total,
                kind: Kind::Up,
                path: b.clone(),
                ..Default::default()
            });
            let moved = if case_only {
                // Checked in the cloud's own listing: an "ok" that changed nothing must not get rows written for it.
                move_dir_by_case(&self.remote, &a, &b).and_then(|()| spelling_kept(&self.remote, &b))
            } else {
                self.remote.move_dir(&a, &b)
            };
            if moved.is_err() {
                continue;
            }
            // The folder id under its old name must not answer any more (see `forget_cache`).
            self.remote.forget_cache();
            // `None`: the listing failed after the folder had moved. Each file then keeps what we knew of it (a
            // move keeps size and time), so a hiccup does not leave rows under the old folder and files that no
            // row explains under the new one.
            let now: Option<HashMap<String, Item>> = self
                .remote
                .list_under(&b)
                .ok()
                .map(|v| v.into_iter().map(|i| (i.path.clone(), i)).collect());
            for &i in &idx {
                let (w, _) = &jobs[i];
                let (Some(f), Some(l)) = (&w.from, w.local) else {
                    continue;
                };
                // A file a good listing does not show moved stays a job: it fails its own checks and is sorted out
                // by the next run, which sees both sides as they are.
                let (sig, id) = match (&now, &f.remote) {
                    (Some(map), _) => match map.get(&w.path).filter(|it| !it.is_dir && it.size == l.size) {
                        Some(it) => (sig_item(it), it.id.clone()),
                        None => continue,
                    },
                    (None, Some(old)) => (sig_item(old), old.id.clone()),
                    (None, None) => continue,
                };
                let base = Base { local: l, remote: sig };
                // The new row first, as in `run_transfers`.
                match self.db.set(&w.path, &base, &id).and_then(|()| self.db.forget(&f.path)) {
                    Ok(()) => {
                        log_done(&self.db, w, Action::MoveRemote, report);
                        gone.push(f.path.clone());
                    }
                    Err(e) => log_failed(&self.db, w, &e.to_string(), report),
                }
                finished.insert(i);
            }
        }
        let n = finished.len();
        let mut i = 0;
        jobs.retain(|_| {
            i += 1;
            !finished.contains(&(i - 1))
        });
        n
    }

    /// True when renaming the cloud folder `a` to `b` is exactly the `group` of file moves (see `move_dirs`).
    fn dir_move_proven(&self, a: &str, b: &str, group: &[&Work], case_only: bool) -> bool {
        if excluded(a, &self.excluded) || excluded(b, &self.excluded) {
            return false;
        }
        // Every synced file of the old folder is in the group (each of them has a row), so nothing else is moved.
        if self.db.under(a).map_or(true, |rows| rows.len() != group.len()) {
            return false;
        }
        // The same checks as one file's move: no link on the way, the new local file as planned.
        let sources_ok = group.iter().all(|w| {
            w.from.as_deref().is_some_and(|f| {
                refuse(&self.root, false, &w.path, Action::MoveRemote).is_ok()
                    && refuse(&self.root, false, &f.path, Action::MoveRemote).is_ok()
                    && local_sig(&self.root, &w.path) == w.local
            })
        });
        if !sources_ok {
            return false;
        }
        // The cloud's own view of the folder: nothing but the synced files, unchanged since the last sync (a file
        // another device added, a hidden or ignored one, an edit).
        let Ok(listed) = self.remote.list_under(a) else {
            return false;
        };
        let files: HashMap<&str, &Item> = listed
            .iter()
            .filter(|i| !i.is_dir)
            .map(|i| (i.path.as_str(), i))
            .collect();
        if files.len() != group.len() {
            return false;
        }
        let same = group.iter().all(|w| {
            let old = w.from.as_deref().and_then(|f| Some((f, f.remote.as_ref()?)));
            old.is_some_and(|(f, old)| {
                files
                    .get(f.path.as_str())
                    .is_some_and(|i| sig_item(i).same_as(&sig_item(old)))
            })
        });
        // The target must be free: a folder move onto an existing one would merge them. For a rename by case alone
        // the target name is the folder itself (a case-insensitive cloud answers for it), so what must be free is the
        // temporary name the two-step rename goes through.
        let target = if case_only {
            format!("{b}{CASE_TMP}")
        } else {
            b.to_string()
        };
        same && matches!(self.remote.stat(&target), Ok(None))
    }

    /// Two different stamps do not make two different files: an upload the app was closed during, or a file put in
    /// the cloud by another tool (or a folder that already holds the same files, like a first sync onto an existing
    /// copy), would otherwise be split into a conflict copy of itself. A `Conflict` of equal sizes whose bytes the
    /// service's hash proves equal becomes `Adopt`. Each check reads a whole local file, so they run `PARALLEL` at a
    /// time (and show their count). A check that itself fails (an unreadable file, rclone busy) is not "different": the
    /// file is skipped with an error and tried again, instead of becoming a conflict copy of identical bytes.
    pub(super) fn adopt_identical(&self, plan: &mut [Planned], report: &mut Report) {
        let todo: Vec<usize> = (0..plan.len())
            .filter(|&i| {
                let (w, a, _) = &plan[i];
                *a == Action::Conflict && matches!((w.local, &w.remote), (Some(l), Some(r)) if l.size == r.size)
            })
            .collect();
        if todo.is_empty() {
            return;
        }
        let (next, finished) = (AtomicUsize::new(0), AtomicUsize::new(0));
        let (same, unknown) = (Mutex::new(Vec::new()), Mutex::new(Vec::new()));
        // Plain references, not `self`: the engine holds the SQLite connection, which must stay on this thread.
        let (remote, root, progress, cancel) = (&self.remote, &self.root, &self.progress, &self.cancel);
        let (view, total) = (&*plan, todo.len());
        std::thread::scope(|s| {
            for _ in 0..PARALLEL.min(total) {
                s.spawn(|| {
                    while !cancel.load(Relaxed)
                        && let Some(&i) = todo.get(next.fetch_add(1, Relaxed))
                    {
                        let (w, ..) = &view[i];
                        match w.remote.as_ref().map(|r| remote.compare(root, &w.path, r)) {
                            Some(Ok(true)) => {
                                if let Ok(mut v) = same.lock() {
                                    v.push(i);
                                }
                            }
                            Some(Err(e)) => {
                                if let Ok(mut v) = unknown.lock() {
                                    v.push((i, e));
                                }
                            }
                            _ => {}
                        }
                        if let Ok(mut g) = progress.lock() {
                            *g = Progress {
                                done: finished.fetch_add(1, Relaxed) + 1,
                                total,
                                phase: Phase::Compare,
                                ..Default::default()
                            };
                        }
                    }
                });
            }
        });
        for i in same.into_inner().unwrap_or_default() {
            plan[i].1 = Action::Adopt;
        }
        for (i, e) in unknown.into_inner().unwrap_or_default() {
            let path = plan[i].0.path.clone();
            let why = format!("could not compare with the cloud copy, will retry: {e}");
            self.db.log("error", &path, None, &why);
            report.fail(&path, &why);
            plan[i].1 = Action::None;
        }
    }
}
