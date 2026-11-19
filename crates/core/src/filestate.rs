//! Which files an account is moving or failed to move right now, for the file manager's per-file emblems.
//! Live only: nothing here is stored, so an emblem can never outlive the app. Written by the engine and the runner,
//! read by the app's emblem server.
use std::{collections::BTreeSet, sync::Mutex};

/// More changed paths than this are reported as "everything changed" (`take_dirty`): a first sync of 100k files must
/// not keep 100k strings for a reader that may not even exist.
const DIRTY_MAX: usize = 5000;

#[derive(Default)]
struct Inner {
    /// Being uploaded, downloaded or moved (queued ones included).
    busy: BTreeSet<String>,
    /// Failed in their last run and waiting for a retry.
    failed: BTreeSet<String>,
    /// Paths whose state changed since the last `take_dirty`.
    dirty: BTreeSet<String>,
    dirty_all: bool,
}

impl Inner {
    fn touch(&mut self, path: &str) {
        if self.dirty_all {
            return;
        }
        if self.dirty.len() >= DIRTY_MAX {
            (self.dirty_all, self.dirty) = (true, BTreeSet::new());
        } else {
            self.dirty.insert(path.to_owned());
        }
    }
}

/// Paths are relative to the account's folder, with `/`.
#[derive(Default)]
pub struct FileStates(Mutex<Inner>);

/// A path, or anything below it, is in `set`.
fn has(set: &BTreeSet<String>, path: &str) -> bool {
    set.contains(path) || set.range(format!("{path}/")..format!("{path}0")).next().is_some()
}

impl FileStates {
    // A poisoned lock (a panicked worker) must not take the emblems, or the sync, down with it.
    fn lock(&self) -> std::sync::MutexGuard<'_, Inner> {
        self.0.lock().unwrap_or_else(|e| e.into_inner())
    }

    pub fn add_busy(&self, paths: impl IntoIterator<Item = String>) {
        let mut g = self.lock();
        for p in paths {
            g.touch(&p);
            g.busy.insert(p);
        }
    }

    pub fn done(&self, path: &str) {
        let mut g = self.lock();
        if g.busy.remove(path) {
            g.touch(path);
        }
    }

    /// A path's row changed without a transfer (a file the DB took as it was): its emblem is asked for again.
    pub fn changed(&self, path: &str) {
        self.lock().touch(path);
    }

    /// The transfers of a run are over (finished, failed or cut short): nothing is busy any more.
    pub fn clear_busy(&self) {
        let mut g = self.lock();
        for p in std::mem::take(&mut g.busy) {
            g.touch(&p);
        }
    }

    /// Failed paths known from before (the retry saved in the DB), so a restart does not show checks on them.
    pub fn seed_failed(&self, paths: impl IntoIterator<Item = String>) {
        let mut g = self.lock();
        for p in paths {
            g.touch(&p);
            g.failed.insert(p);
        }
    }

    /// Takes in a finished run, as `Retry::record` does: a whole comparison says what failed, a run of some paths only
    /// what happened to those.
    pub fn finish(&self, failed: &[String], ran: &[String], whole: bool) {
        let mut g = self.lock();
        let old: Vec<String> = if whole {
            std::mem::take(&mut g.failed).into_iter().collect()
        } else {
            ran.iter().filter(|p| g.failed.remove(*p)).cloned().collect()
        };
        for p in old.iter().chain(failed) {
            g.touch(p);
        }
        g.failed.extend(failed.iter().cloned());
    }

    pub fn failed(&self, path: &str) -> bool {
        has(&self.lock().failed, path)
    }

    pub fn busy(&self, path: &str) -> bool {
        has(&self.lock().busy, path)
    }

    /// What changed since the last call (for the one reader), and whether too much changed to list.
    pub fn take_dirty(&self) -> (Vec<String>, bool) {
        let mut g = self.lock();
        let all = std::mem::take(&mut g.dirty_all);
        (std::mem::take(&mut g.dirty).into_iter().collect(), all)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn folders_see_what_is_below() {
        let s = FileStates::default();
        s.add_busy(["a/b/c.txt".to_owned()]);
        assert!(s.busy("a") && s.busy("a/b") && s.busy("a/b/c.txt"));
        // A sibling whose name starts the same is not below it.
        assert!(!s.busy("a/b/c") && !s.busy("a/bb") && !s.busy("b"));
        s.done("a/b/c.txt");
        assert!(!s.busy("a"));
    }

    #[test]
    fn finish_keeps_failures_of_other_paths_until_a_whole_run() {
        let s = FileStates::default();
        s.finish(&["x".into(), "y".into()], &[], true);
        // A run of other paths changes nothing; a run of `x` that now works clears it.
        s.finish(&[], &["z".into()], false);
        assert!(s.failed("x") && s.failed("y"));
        s.finish(&[], &["x".into()], false);
        assert!(!s.failed("x") && s.failed("y"));
        s.finish(&[], &[], true);
        assert!(!s.failed("y"));
    }

    #[test]
    fn dirty_is_taken_once_and_overflows_to_all() {
        let s = FileStates::default();
        s.add_busy(["a".to_owned()]);
        s.done("a");
        assert_eq!(s.take_dirty(), (vec!["a".to_owned()], false));
        assert_eq!(s.take_dirty(), (vec![], false));
        s.add_busy((0..=DIRTY_MAX).map(|n| n.to_string()));
        let (list, all) = s.take_dirty();
        assert!(all && list.is_empty());
        s.clear_busy();
        assert!(s.take_dirty().1);
        assert_eq!(s.take_dirty(), (vec![], false));
    }
}
