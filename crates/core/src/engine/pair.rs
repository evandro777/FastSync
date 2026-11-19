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

/// Cloud folders `delete_dirs` lists per run before the rest are deleted file by file.
const DIR_DELETE_TRIES: usize = 50;

/// Largest local file an `Upload` over a same-size cloud file is hashed for (see `adopt_identical`).
const UPLOAD_COMPARE_MAX: u64 = 256 << 20;

/// The file name and the folder of a `/` path: candidates for a move or copy share one of them.
fn name(p: &str) -> &str {
    p.rsplit('/').next().unwrap_or(p)
}

fn parent(p: &str) -> &str {
    p.rsplit_once('/').map_or("", |(d, _)| d)
}

/// The folders a file's move renamed: what is left of both paths once their common ending (the file name and any
/// folders that kept their names) is cut off. `None` when the file name changed too, or one side has no folder left
/// (a file moved out of or into the root, which is not a folder rename), or one folder lies inside the other.
pub(super) fn dir_pair(old: &str, new: &str) -> Option<(String, String)> {
    let (o, n): (Vec<&str>, Vec<&str>) = (old.split('/').collect(), new.split('/').collect());
    let mut k = o.iter().rev().zip(n.iter().rev()).take_while(|(x, y)| x == y).count();
    if k >= o.len() || k >= n.len() {
        return None;
    }
    // `T/P/f` -> `T/S/P/f` cuts to `T` -> `T/S`, one inside the other: keep the folder that moved (`T/P` -> `T/S/P`).
    while k > 0 {
        let (a, b) = (o[..o.len() - k].join("/"), n[..n.len() - k].join("/"));
        if !a.starts_with(&format!("{b}/")) && !b.starts_with(&format!("{a}/")) {
            return Some((a, b));
        }
        k -= 1;
    }
    None
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
        // Indexed by (size, file name) and (size, folder): a folder of 20k same-sized files renamed must not compare
        // every new path with every old one.
        let mut by_name: HashMap<(u64, &str), Vec<usize>> = HashMap::new();
        let mut by_parent: HashMap<(u64, &str), Vec<usize>> = HashMap::new();
        for (i, (w, a, base)) in plan.iter().enumerate() {
            let size = match (a, w.local, &w.remote, base) {
                (Action::DeleteLocal, Some(l), _, Some(b)) if b.local == l => Some(l.size),
                (Action::DeleteRemote, None, Some(r), _) => Some(r.size),
                _ => None,
            };
            if let Some(s) = size {
                by_name.entry((s, name(&w.path))).or_default().push(i);
                by_parent.entry((s, parent(&w.path))).or_default().push(i);
            }
        }
        if by_name.is_empty() {
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
            let usable = |&o: &usize| !taken[o] && (plan[o].1 == Action::DeleteLocal) == local_move;
            // The same file name first: a folder rename keeps the names, so that is nearly always the match.
            let same_name = by_name.get(&(size, name(&w.path))).into_iter().flatten().copied();
            let same_parent = by_parent
                .get(&(size, parent(&w.path)))
                .into_iter()
                .flatten()
                .copied()
                .filter(|&o| name(&plan[o].0.path) != name(&w.path));
            let cands: Vec<usize> = same_name.chain(same_parent).filter(usable).take(PAIR_TRIES).collect();
            for o in cands {
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
                        // The watcher saw this very rename and the file is untouched since it was synced: nothing to
                        // hash (the cloud side is still proven by `cloud_unchanged` / `dir_move_proven`).
                        (self.renamed_here(&ow.path, &w.path) && old_base.is_some_and(|b| w.local == Some(b.local)))
                            || self.remote.same_content(&self.root, &w.path, r)
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

    /// True when a rename the watcher saw (`local_moves`) took `old` to `new`: the very paths, or a file below a
    /// renamed folder (the same rest of the path under the new folder name).
    fn renamed_here(&self, old: &str, new: &str) -> bool {
        self.local_moves.iter().any(|(from, to)| {
            (old == from && new == to)
                || old
                    .strip_prefix(from.as_str())
                    .zip(new.strip_prefix(to.as_str()))
                    .is_some_and(|(a, b)| a.starts_with('/') && a == b)
        })
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
    /// copied there instead of uploaded. The proof is live: each candidate (same size, still as synced) gets its cloud
    /// hash from a listing of its folder (one request per folder, shared by every file; a `stat` if that fails) and is
    /// compared with the new file's local hash, which is read once per hash kind. `transfer` then asks the service to
    /// copy, checks the result's hash, and uploads on any doubt.
    /// shortcut: sequential, at most `PAIR_TRIES` candidates per file; store the hash in the DB if files of one size are
    /// so many that the right source is often past the cap.
    pub(super) fn pair_uploads(&self, plan: &mut [Planned]) {
        if self.download_only {
            return;
        }
        let (started, mut paired) = (std::time::Instant::now(), 0);
        // The cloud copy of a candidate comes from one listing of its folder, shared by every file that has a candidate
        // there (25 copies of a folder cost one request, not 25 `stat`s of 0.3 s each). A folder that cannot be listed
        // (`None`), or a remote with no `list_dir`, falls back to a `stat` per candidate.
        let mut listed: HashMap<String, Option<HashMap<String, Item>>> = HashMap::new();
        let mut cloud_item = |p: &str| -> Option<Item> {
            let dir = parent(p);
            let known = listed.entry(dir.to_string()).or_insert_with(|| {
                let items = self.remote.list_dir(dir).ok()?;
                Some(items.into_iter().map(|i| (i.path.clone(), i)).collect())
            });
            match known {
                Some(items) => items.get(p).cloned(),
                None => self.remote.stat(p).ok().flatten(),
            }
        };
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
            let file_started = std::time::Instant::now();
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
            let mut tried = 0;
            for (p, _) in cands.into_iter().take(PAIR_TRIES) {
                if self.cancelled() {
                    break;
                }
                tried += 1;
                // A failed lookup or a missing cloud copy is just "not this one".
                let Some(item) = cloud_item(&p) else {
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
                    paired += 1;
                    break;
                }
            }
            crate::debug::log(|| {
                format!(
                    "pair     {}: {tried} candidate(s), {} in {:?}",
                    w.path,
                    if w.from.is_some() { "source found" } else { "no source" },
                    file_started.elapsed()
                )
            });
        }
        crate::debug::log(|| format!("pair     uploads: {paired} paired in {:?}", started.elapsed()));
    }

    /// A folder copied here shows up as one new `Upload` per file (the big ones paired with their source by
    /// `pair_uploads`). Where the cloud folder `a` has an exact twin here, `b`, it is copied with one call instead of
    /// a `stat`, a copy and a `stat` per file. The proof is strict because the copy takes *everything* in `a`: every
    /// file the cloud lists in `a` must be a planned new upload at the same place in `b` with the same hash, or a
    /// file the copy would make (an unchecked or ignored one, one deleted from the copy, an edited one) would
    /// appear in the cloud and come back down. Whatever the call leaves out stays a one-by-one job; extra local files
    /// in `b` stay jobs too. Returns how many jobs it finished (removed from `jobs`).
    /// shortcut: hashes the small files one at a time on this thread; parallelise if copying thousands is slow.
    pub(super) fn copy_dirs(
        &self,
        jobs: &mut Vec<(Work, Action)>,
        (done, total): (usize, usize),
        report: &mut Report,
    ) -> usize {
        if self.download_only {
            return 0;
        }
        // Candidate folders come from the paired big files; two of them, as for a move.
        let mut groups: BTreeMap<(String, String), usize> = BTreeMap::new();
        let mut new_files: HashMap<String, usize> = HashMap::new();
        for (i, (w, a)) in jobs.iter().enumerate() {
            if *a != Action::Upload || w.remote.is_some() {
                continue;
            }
            new_files.insert(w.path.clone(), i);
            if let Some(f) = w.from.as_deref()
                && f.remote.is_some()
                && let Some(dirs) = dir_pair(&f.path, &w.path)
            {
                *groups.entry(dirs).or_default() += 1;
            }
        }
        let mut groups: Vec<_> = groups.into_iter().filter(|(_, n)| *n >= 2).collect();
        groups.sort_by_key(|(_, n)| std::cmp::Reverse(*n));
        crate::debug::log(|| {
            format!(
                "folder copy {} candidate folder pair(s) of {} new file(s)",
                groups.len(),
                new_files.len()
            )
        });
        let mut finished = HashSet::new();
        for ((a, b), _) in groups {
            if self.cancelled() {
                break;
            }
            let proof_started = std::time::Instant::now();
            let Some(proven) = self.dir_copy_proven(&a, &b, jobs, &new_files) else {
                continue;
            };
            crate::debug::log(|| {
                format!(
                    "folder copy {a} -> {b} proven for {} file(s) in {:?}",
                    proven.len(),
                    proof_started.elapsed()
                )
            });
            if proven.iter().any(|(i, _)| finished.contains(i)) {
                continue;
            }
            let copy_started = std::time::Instant::now();
            self.set_progress(Progress {
                done,
                total,
                kind: Kind::CloudCopy,
                path: b.clone(),
                ..Default::default()
            });
            if let Err(e) = self.remote.copy_dir(&a, &b) {
                crate::debug::log(|| format!("folder copy {a} -> {b} failed, keeping what landed: {e}"));
            }
            crate::debug::log(|| format!("folder copy {a} -> {b} took {:?}", copy_started.elapsed()));
            self.remote.forget_cache();
            // Whatever the call said, only what the listing shows, with the proven size and hash, gets a row; a failed
            // listing writes none and the files go one by one (the cloud's copy of them is then found next run).
            let Ok(now) = self.remote.list_under(&b) else {
                continue;
            };
            let now: HashMap<&str, &Item> = now.iter().map(|i| (i.path.as_str(), i)).collect();
            for (i, src) in proven {
                let w = &jobs[i].0;
                let (Some(l), Some(it)) = (w.local, now.get(w.path.as_str())) else {
                    continue;
                };
                if it.is_dir || it.size != l.size || it.hash != src.hash {
                    continue;
                }
                let base = Base {
                    local: l,
                    remote: sig_item(it),
                };
                match self.db.set_hashed(&w.path, &base, &it.id, it.hash.as_ref()) {
                    Ok(()) => log_done(&self.db, w, Action::Upload, report),
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

    /// The jobs (with the cloud file each one copies) when copying the cloud folder `a` to `b` is exactly the new
    /// uploads under `b`; `None` on any doubt (see `copy_dirs`).
    fn dir_copy_proven(
        &self,
        a: &str,
        b: &str,
        jobs: &[(Work, Action)],
        new_files: &HashMap<String, usize>,
    ) -> Option<Vec<(usize, Item)>> {
        let no = |why: String| {
            crate::debug::log(|| format!("folder copy {a} -> {b} not used: {why}"));
            None
        };
        if excluded(a, &self.excluded) || excluded(b, &self.excluded) {
            return no("a folder is unchecked".into());
        }
        let listed = match self.remote.list_under(a) {
            Ok(l) => l,
            Err(e) => return no(format!("listing the cloud folder failed: {e}")),
        };
        let prefix = format!("{a}/");
        let mut proven = Vec::new();
        for src in listed.into_iter().filter(|i| !i.is_dir) {
            if self.cancelled() {
                return None;
            }
            let Some(rel) = src.path.strip_prefix(&prefix) else {
                return no(format!("{} is not under the folder", src.path));
            };
            let target = format!("{b}/{rel}");
            let Some(&i) = new_files.get(&target) else {
                return no(format!("{rel} has no new local copy"));
            };
            let w = &jobs[i].0;
            let (Some(l), Some((kind, want))) = (w.local, src.hash.clone()) else {
                return no(format!("{rel} has no hash to compare"));
            };
            if l.size != src.size
                || local_sig(&self.root, &target) != w.local
                || refuse(&self.root, false, &target, Action::Upload).is_err()
            {
                return no(format!("{rel} differs or sits behind a link"));
            }
            // `pair_uploads` already proved the file it paired with this very source.
            let proved = w
                .from
                .as_deref()
                .is_some_and(|f| f.path == src.path && f.remote.as_ref().is_some_and(|r| r.hash == src.hash));
            if !proved
                && self
                    .remote
                    .local_hash(&self.root, &target, &kind)
                    .ok()
                    .flatten()
                    .as_deref()
                    != Some(&want)
            {
                return no(format!("{rel} has other bytes"));
            }
            proven.push((i, src));
        }
        if proven.len() < 2 {
            return no("fewer than two files".into());
        }
        // The target must not hold files: the copy would merge into them. An empty one an earlier slow run made is fine.
        match self.remote.stat(b) {
            Ok(None) => {}
            Ok(Some(t)) if t.is_dir && self.remote.list_under(b).is_ok_and(|l| l.iter().all(|i| i.is_dir)) => {}
            Ok(Some(_)) => return no(format!("{b} already exists in the cloud")),
            Err(e) => return no(format!("checking {b} failed: {e}")),
        }
        Some(proven)
    }

    /// A folder deleted here shows up as one `DeleteRemote` per file. Where every file of a cloud folder is such a
    /// delete, the folder goes with one call. Strict, because a purge takes *everything*: the folder must be gone from
    /// the disk, every synced file under it must be in the plan, and the cloud's own unfiltered listing must hold
    /// nothing else (a file another device added, a hidden one) and each file as we synced it. Anything else, or a
    /// folder that is not gone afterwards, leaves the jobs for the one-by-one delete. Runs after the delete guard.
    /// shortcut: at most `DIR_DELETE_TRIES` folders listed per run; the rest are deleted file by file.
    pub(super) fn delete_dirs(&self, plan: &mut Vec<Planned>, report: &mut Report) {
        if self.download_only {
            return;
        }
        let is_del = |p: &Planned| p.1 == Action::DeleteRemote && p.0.local.is_none() && p.0.remote.is_some();
        let mut count: HashMap<String, usize> = HashMap::new();
        for p in plan.iter().filter(|p| is_del(p)) {
            let mut d = parent(&p.0.path);
            while !d.is_empty() {
                *count.entry(d.to_string()).or_default() += 1;
                d = parent(d);
            }
        }
        let mut dirs: Vec<(String, usize)> = count.into_iter().filter(|(_, n)| *n >= 2).collect();
        // The top one first: it covers the folders below it.
        dirs.sort_by_key(|(d, _)| (d.matches('/').count(), d.clone()));
        let (mut tries, mut gone): (usize, Vec<String>) = (0, Vec::new());
        for (d, n) in dirs {
            if self.cancelled() || tries >= DIR_DELETE_TRIES {
                break;
            }
            let inside = |p: &str| p.strip_prefix(d.as_str()).is_some_and(|r| r.starts_with('/'));
            if gone.iter().any(|g| inside(g)) {
                continue;
            }
            // Every synced file under it is deleted: a cheap check before the cloud is asked.
            if self.db.under(&d).map_or(true, |rows| rows.len() != n) {
                continue;
            }
            tries += 1;
            if !self.dir_delete_proven(&d, plan, is_del) {
                continue;
            }
            self.set_progress(Progress {
                kind: Kind::Remove,
                path: d.clone(),
                total: plan.len(),
                ..Default::default()
            });
            if let Err(e) = self.remote.purge_dir(&d) {
                crate::debug::log(|| format!("folder delete {d} failed: {e}"));
            }
            self.remote.forget_cache();
            // Whatever the call said, only a folder the cloud no longer has counts.
            if !matches!(self.remote.stat(&d), Ok(None)) {
                continue;
            }
            self.db.log("delcloud", &d, None, "");
            for (w, ..) in plan.iter().filter(|p| is_del(p) && inside(&p.0.path)) {
                if let Err(e) = self.db.forget(&w.path) {
                    log_failed(&self.db, w, &e.to_string(), report);
                }
            }
            plan.retain(|p| !(is_del(p) && inside(&p.0.path)));
            gone.push(d);
        }
    }

    fn dir_delete_proven(&self, d: &str, plan: &[Planned], is_del: impl Fn(&Planned) -> bool) -> bool {
        let no = |why: String| {
            crate::debug::log(|| format!("folder delete {d} not used: {why}"));
            false
        };
        if excluded(d, &self.excluded) || under_symlink(&self.root, d) {
            return no("unchecked, or behind a link".into());
        }
        // Gone from the disk, empty folders included: a new local file or folder there is not ours to purge.
        if !matches!(
            std::fs::symlink_metadata(self.root.join(d)),
            Err(ref e) if e.kind() == std::io::ErrorKind::NotFound
        ) {
            return no("still on the disk".into());
        }
        let listed = match self.remote.list_under(d) {
            Ok(l) => l,
            Err(e) => return no(format!("listing the cloud folder failed: {e}")),
        };
        let planned: HashMap<&str, &Item> = plan
            .iter()
            .filter(|p| is_del(p))
            .filter_map(|p| Some((p.0.path.as_str(), p.0.remote.as_ref()?)))
            .collect();
        let mut files = 0;
        for it in listed.iter().filter(|i| !i.is_dir) {
            files += 1;
            if !planned
                .get(it.path.as_str())
                .is_some_and(|r| sig_item(it).same_as(&sig_item(r)))
            {
                return no(format!("{} is not a delete we planned, or changed", it.path));
            }
        }
        files >= 2 || no("fewer than two files".into())
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
            // Only where the cloud takes both spellings for one folder (Drive and MEGA keep two folders).
            let case_only = self.case_blind && a.to_lowercase() == b.to_lowercase();
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
                kind: Kind::Move,
                path: b.clone(),
                ..Default::default()
            });
            let moved = if case_only {
                // Checked in the cloud's own listing: an "ok" that changed nothing must not get rows written for it.
                move_dir_by_case(&self.remote, &a, &b).and_then(|()| spelling_kept(&self.remote, &b))
            } else {
                self.remote.move_dir(&a, &b)
            };
            if let Err(e) = &moved {
                crate::debug::log(|| format!("folder move {a} -> {b} failed, moving file by file: {e}"));
                continue;
            }
            report.moved_dirs.push(a.clone());
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
            // One row for the folder; the files below only get their DB rows (no log row each: 5000 files would
            // bury the log, and it was one cloud call).
            self.db.log("movedir", &b, None, &a);
            for &i in &idx {
                let (w, _) = &jobs[i];
                let (Some(f), Some(l)) = (&w.from, w.local) else {
                    continue;
                };
                // A file a good listing does not show moved stays a job: it fails its own checks and is sorted out
                // by the next run, which sees both sides as they are.
                let (sig, id, hash) = match (&now, &f.remote) {
                    (Some(map), _) => match map.get(&w.path).filter(|it| !it.is_dir && it.size == l.size) {
                        Some(it) => (sig_item(it), it.id.clone(), it.hash.clone()),
                        None => continue,
                    },
                    (None, Some(old)) => (sig_item(old), old.id.clone(), old.hash.clone()),
                    (None, None) => continue,
                };
                let base = Base { local: l, remote: sig };
                // The new row first, as in `run_transfers`.
                match self
                    .db
                    .set_hashed(&w.path, &base, &id, hash.as_ref())
                    .and_then(|()| self.db.forget(&f.path))
                {
                    Ok(()) => report.transferred += 1,
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
        let no = |why: String| {
            crate::debug::log(|| format!("folder move {a} -> {b} not used: {why}"));
            false
        };
        if excluded(a, &self.excluded) || excluded(b, &self.excluded) {
            return no("a folder is unchecked".into());
        }
        // Every synced file of the old folder is in the group (each of them has a row), so nothing else is moved.
        match self.db.under(a) {
            Ok(rows) if rows.len() == group.len() => {}
            Ok(rows) => {
                return no(format!(
                    "{} synced files in the folder, {} moved",
                    rows.len(),
                    group.len()
                ));
            }
            Err(e) => return no(format!("database: {e}")),
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
            return no("a local file changed or sits behind a link".into());
        }
        // The cloud's own view of the folder: nothing but the synced files, unchanged since the last sync (a file
        // another device added, a hidden or ignored one, an edit).
        let listed = match self.remote.list_under(a) {
            Ok(l) => l,
            Err(e) => return no(format!("listing the cloud folder failed: {e}")),
        };
        let files: HashMap<&str, &Item> = listed
            .iter()
            .filter(|i| !i.is_dir)
            .map(|i| (i.path.as_str(), i))
            .collect();
        if files.len() != group.len() {
            return no(format!(
                "the cloud folder lists {} files, {} moved",
                files.len(),
                group.len()
            ));
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
        if !same {
            return no("a cloud file's size or time differs from the last sync".into());
        }
        match self.remote.stat(&target) {
            Ok(None) => true,
            Ok(Some(_)) => no(format!("{target} already exists in the cloud")),
            Err(e) => no(format!("checking {target} failed: {e}")),
        }
    }

    /// Two different stamps do not make two different files: an upload the app was closed during, or a file put in
    /// the cloud by another tool (or a folder that already holds the same files, like a first sync onto an existing
    /// copy), would otherwise be split into a conflict copy of itself. A `Conflict` of equal sizes whose bytes the
    /// service's hash proves equal becomes `Adopt`. Each check reads a whole local file, so they run `PARALLEL` at a
    /// time (and show their count). A check that itself fails (an unreadable file, rclone busy) is not "different": the
    /// file is skipped with an error and tried again, instead of becoming a conflict copy of identical bytes.
    ///
    /// The same goes for an `Upload` over an existing cloud file of the same size (only the local stamp changed: a
    /// `touch`, another instance's write of the same bytes): equal hashes make it `Adopt`, so nothing is sent. Here a
    /// failed check is no error, the file just goes up as before; a file over `UPLOAD_COMPARE_MAX` is not checked.
    pub(super) fn adopt_identical(&self, plan: &mut [Planned], report: &mut Report) {
        let todo: Vec<usize> = (0..plan.len())
            .filter(|&i| {
                let (w, a, _) = &plan[i];
                // An upload's check is only a saving, so a big file (a database or VM image edited in place, same
                // size every time) skips it: reading it all would hold every other transfer of the run back.
                let wanted = match *a {
                    Action::Conflict => true,
                    Action::Upload => w.local.is_some_and(|l| l.size <= UPLOAD_COMPARE_MAX),
                    _ => false,
                };
                wanted && matches!((w.local, &w.remote), (Some(l), Some(r)) if l.size == r.size)
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
            if plan[i].1 == Action::Upload {
                continue;
            }
            let path = plan[i].0.path.clone();
            let why = format!("could not compare with the cloud copy, will retry: {e}");
            self.db.log("error", &path, None, &why);
            report.fail(&path, &why);
            plan[i].1 = Action::None;
        }
    }
}
