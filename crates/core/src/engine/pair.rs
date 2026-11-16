//! Matching same-bytes files before a run acts: a delete plus a new path becomes a move, a new cloud file whose bytes
//! sit in another local file becomes a local copy, and two equal-bytes "conflicts" become an adopt. All of them rest
//! on `Remote::same_content` / `compare`, so a service without a hash (MEGA) never pairs.
use std::{
    collections::HashMap,
    sync::{
        Mutex,
        atomic::{AtomicUsize, Ordering::Relaxed},
    },
};

use super::{
    COPY_MIN, Engine, Moved, PAIR_TRIES, PARALLEL, Phase, Planned, Progress, Remote, Report, local_sig, under_symlink,
};
use crate::decide::Action;

/// The file name and the folder of a `/` path: candidates for a move or copy share one of them.
fn name(p: &str) -> String {
    p.rsplit('/').next().unwrap_or(p).to_string()
}

fn parent(p: &str) -> String {
    p.rsplit_once('/').map_or(String::new(), |(d, _)| d.to_string())
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
                let ow = &plan[o].0;
                let same = if local_move {
                    // The old local file against the cloud's new item.
                    w.remote
                        .as_ref()
                        .is_some_and(|r| self.remote.same_content(&self.root, &ow.path, r))
                } else {
                    // The new local file against the cloud's old item.
                    ow.remote
                        .as_ref()
                        .is_some_and(|r| self.remote.same_content(&self.root, &w.path, r))
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
            report.errors.push(format!("{path}: {why}"));
            plan[i].1 = Action::None;
        }
    }
}
