//! Canned data for `scripts/ui-snapshot.sh` (`FASTSYNC_FIXTURES=empty|single|many`): every page can be rendered with zero,
//! one or many rows without starting rclone, reading an account or touching the real system.
use crate::{AccountRow, FolderRow, Main, Tr};
use slint::{ComponentHandle, ModelRc, VecModel};

/// 0 empty, 1 single, 2 many; `None` when the variable is unset or unknown.
pub fn mode() -> Option<usize> {
    match std::env::var("FASTSYNC_FIXTURES").ok()?.as_str() {
        "empty" => Some(0),
        "single" => Some(1),
        "many" => Some(2),
        _ => None,
    }
}

fn row(provider: &str, folder: &str, state: i32, detail: &str) -> AccountRow {
    AccountRow {
        id: provider.into(),
        provider: provider.into(),
        folder: folder.into(),
        state,
        detail: detail.into(),
        deletes: 12,
        known: 20,
        download_only: false,
        transfer: 0,
        file: "".into(),
        done: 0,
        total: 0,
        speed: "".into(),
        quota_used: "".into(),
        quota_total: "".into(),
        quota_percent: 0,
        quota_level: 0,
    }
}

fn with_quota(mut r: AccountRow, used: &str, total: &str, percent: i32) -> AccountRow {
    (r.quota_used, r.quota_total) = (used.into(), total.into());
    (r.quota_percent, r.quota_level) = (percent, crate::quota_level(percent));
    r
}

fn folder(name: &str, depth: i32, checked: bool, enabled: bool, expanded: bool) -> FolderRow {
    FolderRow {
        path: name.into(),
        name: name.into(),
        depth,
        checked,
        enabled,
        expanded,
        loading: false,
        has_children: true,
    }
}

/// The folder picker: no folders, one, or a tree with an unchecked subtree, a loading row and a long name.
pub fn folders(ui: &Main, mode: usize) {
    let mut loading = folder("Archive", 0, true, true, true);
    loading.loading = true;
    // One with no subfolders: no arrow.
    let mut leaf = folder("fastsync", 1, true, true, false);
    leaf.has_children = false;
    let all = [
        folder("Documents", 0, true, true, false),
        folder("Photos", 0, false, true, true),
        folder("2019", 1, false, false, false),
        folder("2020", 1, false, false, false),
        folder("Projects", 0, true, true, true),
        leaf,
        folder(
            "A folder with a very long name that cannot fit on one line of the dialog",
            1,
            true,
            true,
            false,
        ),
        loading,
    ];
    let count = [0, 1, 8][mode];
    ui.set_folders(ModelRc::new(VecModel::from(all[..count].to_vec())));
}

/// The share dialog: the "not in a synced folder" error, a link, or a long name with a long provider error.
pub fn share(ui: &Main, mode: usize) {
    let tr = ui.global::<Tr>();
    let (name, link, error) = [
        ("notes.txt", String::new(), tr.invoke_error_share_outside()),
        (
            "Holiday photos.zip",
            "https://www.dropbox.com/scl/fi/abc123def456/Holiday-photos.zip?rlkey=x9y8z7w6v5u4&dl=0".into(),
            "".into(),
        ),
        (
            "A file with a very long name that cannot fit on one line of the dialog.tar.gz",
            String::new(),
            tr.invoke_error_share(
                "operations/publiclink: failed to get link: 403 Forbidden: sharing is disabled by the administrator of this account"
                    .into(),
            ),
        ),
    ][mode]
        .clone();
    ui.set_share_name(name.into());
    ui.set_share_link(link.into());
    ui.set_share_error(error);
    ui.set_share_open(true);
}

/// One fixture covers all three states of the list: none, one row, and one of each kind of status.
pub fn apply(ui: &Main, mode: usize) {
    let mut all = [
        with_quota(row("Dropbox", "/home/user/Dropbox", 0, ""), "3.1 GB", "15.0 GB", 20),
        // Amber, on the syncing card: the gauge must not read as the transfer bar below it.
        with_quota(
            row("Google Drive", "/home/user/GoogleDrive", 1, ""),
            "13.4 GB",
            "15.0 GB",
            89,
        ),
        // A long path, to check the elision; and a full cloud (red).
        with_quota(
            row(
                "OneDrive",
                "/home/user/a/very/long/path/to/a/folder/that/should/be/cut/off/with/an/ellipsis/OneDrive",
                2,
                "",
            ),
            "1.0 TB",
            "1.0 TB",
            100,
        ),
        row(
            "MEGA",
            "/home/user/MEGA",
            3,
            "reports/2026/q3-budget-final-FINAL-v7.xlsx: couldn't upload: quota exceeded (the account has no free space left)",
        ),
        row(
            "Dropbox",
            "/home/user/Work",
            4,
            "Photos/2019/Summer trip to the coast with the whole family/IMG_20190714_153012_HDR_panorama_edited.jpg\nbuild/app.bin\nbuild/app.map\ncache/a.tmp\ncache/b.tmp",
        ),
        // Stopped, two-way: Restore and Delete them (the one above is a mirror: Delete them only).
        row(
            "Google Drive",
            "/home/user/Projects",
            4,
            "test/Untitled Document\ntest/Untitled Document (10th copy)\ntest/Untitled Document (11th copy)",
        ),
        // Syncing with no plan yet: the "Checking for changes…" line (below the fold of the 600x450 snapshot).
        row("OneDrive", "/home/user/OneDrive", 1, ""),
    ];
    // The long path with the "Download only" suffix: the path must be the part that is cut.
    all[2].download_only = true;
    all[4].download_only = true;
    // Drive is uploading; the one-row case shows it too. A long name checks the elision, the speed stays put.
    (all[1].transfer, all[1].file, all[1].done, all[1].total, all[1].speed) = (
        1,
        "holiday-photos-2026-full-resolution-export-part-3-of-9.zip".into(),
        2,
        12,
        "2.4 MB/s".into(),
    );
    let count = [0, 1, 7][mode];
    ui.set_accounts(ModelRc::new(VecModel::from(all[..count].to_vec())));
    // The add dialog: plain when empty, "waiting for the browser" with one row, a long error with many.
    ui.set_add_busy(mode == 1);
    ui.set_add_error(if mode == 2 {
        "Login failed: couldn't get a token: the browser window was closed before the login finished".into()
    } else {
        "".into()
    });
    // The trash in Settings and its confirmation: nothing, one small file, a lot (plus a failed emptying).
    ui.set_trash_files([0, 1, 1284][mode]);
    ui.set_trash_size(["0 B", "2.1 KB", "3.4 GB"][mode].into());
    ui.set_watch_fix(if mode == 2 {
        fastsync_core::runner::watch_fix_command(8192).into()
    } else {
        "".into()
    });
    ui.set_trash_error(if mode == 2 {
        "/home/user/GoogleDrive: Permission denied (os error 13)".into()
    } else {
        "".into()
    });
}
