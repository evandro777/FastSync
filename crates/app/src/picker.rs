//! The selective-sync folder picker (its state is the UI thread's `PICKER`).

use super::*;

/// The folder picker's state, on the UI thread only (`PICKER`): worker threads hand their results over with
/// `upgrade_in_event_loop`, so nothing here needs a lock.
#[derive(Default)]
pub(crate) struct Picker {
    pub(crate) id: String,
    pub(crate) kind: String,
    /// `<id>:<cloud folder>`: the rclone fs every path below is relative to.
    pub(crate) fs: String,
    /// The local folder to save (Edit may have changed it).
    pub(crate) local: String,
    /// A new account (saved paused until the picker's Save) rather than an edit.
    pub(crate) adding: bool,
    pub(crate) excluded: Vec<String>,
    pub(crate) download_only: bool,
    /// Flat: the children of an expanded row follow it.
    pub(crate) rows: Vec<PickRow>,
}

pub(crate) struct PickRow {
    pub(crate) path: String,
    pub(crate) depth: i32,
    pub(crate) expanded: bool,
    pub(crate) loading: bool,
    /// Found by `probe` to contain folders: only then does the row show an arrow.
    pub(crate) has_children: bool,
}

thread_local! {
    pub(crate) static PICKER: RefCell<Picker> = RefCell::default();
}

pub(crate) fn remote_fs(a: &Account) -> String {
    format!("{}:", a.id)
}

pub(crate) fn dir_rows(dirs: Vec<String>, depth: i32) -> Vec<PickRow> {
    dirs.into_iter()
        .map(|path| PickRow {
            path,
            depth,
            expanded: false,
            loading: false,
            has_children: false,
        })
        .collect()
}

/// A folder under an unchecked one cannot be ticked on its own.
pub(crate) fn under_unchecked(path: &str, list: &[String]) -> bool {
    path.rsplit_once('/').is_some_and(|(parent, _)| excluded(parent, list))
}

/// Pushes the picker's rows to the window. UI thread only; the picker's borrow ends before Slint reads the model.
pub(crate) fn show_picker(ui: &Main) {
    let rows: Vec<FolderRow> = PICKER.with_borrow(|p| {
        p.rows
            .iter()
            .map(|r| FolderRow {
                path: r.path.as_str().into(),
                name: r.path.rsplit('/').next().unwrap_or(&r.path).into(),
                depth: r.depth,
                checked: !excluded(&r.path, &p.excluded),
                enabled: !under_unchecked(&r.path, &p.excluded),
                expanded: r.expanded,
                loading: r.loading,
                has_children: r.has_children,
            })
            .collect()
    });
    ui.set_folders(ModelRc::new(VecModel::from(rows)));
}

/// Asks the cloud, a few folders at a time, which of `paths` contain folders, so that only those get an arrow. A
/// failed probe counts as "yes": the click then lists the folder again and shows the error. Results come back one by
/// one; the picker may have been reopened meanwhile, which is harmless (a path has one answer).
pub(crate) fn probe(ui: slint::Weak<Main>, id: String, fs: String, kind: String, paths: Vec<String>) {
    let workers = paths.len().min(4);
    let queue = Arc::new(Mutex::new(paths));
    for _ in 0..workers {
        let (queue, ui, id, fs, kind) = (queue.clone(), ui.clone(), id.clone(), fs.clone(), kind.clone());
        std::thread::spawn(move || {
            while let Some(path) = queue.lock().unwrap_or_else(|e| e.into_inner()).pop() {
                let has = rclone()
                    .and_then(|rc| rc.list_dirs(&fs, &path, &exclusions(&kind)))
                    .map_or(true, |dirs| !dirs.is_empty());
                let id = id.clone();
                let _ = ui.upgrade_in_event_loop(move |ui| {
                    PICKER.with_borrow_mut(|p| {
                        if let (true, Some(r)) = (p.id == id, p.rows.iter_mut().find(|r| r.path == path)) {
                            r.has_children = has;
                        }
                    });
                    show_picker(&ui);
                });
            }
        });
    }
}

pub(crate) fn open_picker(ui: &Main, mut picker: Picker, dirs: Vec<String>) {
    let probes = (picker.id.clone(), picker.fs.clone(), picker.kind.clone(), dirs.clone());
    picker.rows = dir_rows(dirs, 0);
    PICKER.with_borrow_mut(|p| *p = picker);
    ui.set_folders_error("".into());
    ui.set_folders_busy(false);
    show_picker(ui);
    ui.set_folders_open(true);
    let (id, fs, kind, paths) = probes;
    probe(ui.as_weak(), id, fs, kind, paths);
}

/// Saves the picker's choice: stops the account's runner (waiting until it lets go of the DB), resets the DB when the
/// local folder changed (the new folder is then merged with the cloud, never read as "everything was deleted"), and
/// starts a runner again. Off the UI thread: a run in progress finishes first.
pub(crate) fn apply_folders(
    id: &str,
    local: &str,
    adding: bool,
    excluded: Vec<String>,
    download_only: bool,
    ui: slint::Weak<Main>,
) {
    let Some(old) = state().accounts.iter().find(|a| a.id == id).cloned() else {
        return;
    };
    let local_changed = old.local != local;
    if !adding && !local_changed && old.excluded == excluded && old.download_only == download_only {
        return;
    }
    let runner = state().runners.remove(id);
    if let Some(r) = runner {
        r.stop_and_wait();
    }
    if local_changed && let Some(dir) = accounts::account_dir(id) {
        let _ = std::fs::remove_dir_all(dir);
    }
    let acct = {
        let mut s = state();
        let Some(a) = s.accounts.iter_mut().find(|a| a.id == id) else {
            return;
        };
        a.local = local.to_string();
        a.excluded = excluded;
        a.download_only = download_only;
        if adding {
            a.paused = false;
        }
        let acct = a.clone();
        s.status.remove(id);
        accounts::save(&s.accounts);
        acct
    };
    start_account(&acct, ui);
}
