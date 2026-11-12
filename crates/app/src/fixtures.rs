//! Canned data for `scripts/ui-snapshot.sh` (`FASTSYNC_FIXTURES=empty|single|many`): every page can be rendered with zero,
//! one or many rows without starting rclone, reading an account or touching the real system.
use crate::{AccountRow, Main};
use slint::{ModelRc, VecModel};

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
    }
}

/// One fixture covers all three states of the list: none, one row, and one of each kind of status.
pub fn apply(ui: &Main, mode: usize) {
    let all = [
        row("Dropbox", "/home/user/Dropbox", 0, ""),
        row("Google Drive", "/home/user/GoogleDrive", 1, ""),
        // A long path, to check the elision.
        row(
            "OneDrive",
            "/home/user/a/very/long/path/to/a/folder/that/should/be/cut/off/with/an/ellipsis/OneDrive",
            2,
            "",
        ),
        row(
            "MEGA",
            "/home/user/MEGA",
            3,
            "reports/2026/q3-budget-final-FINAL-v7.xlsx: couldn't upload: quota exceeded (the account has no free space left)",
        ),
        row("Dropbox", "/home/user/Work", 4, ""),
    ];
    let count = [0, 1, 5][mode];
    ui.set_accounts(ModelRc::new(VecModel::from(all[..count].to_vec())));
    // The add dialog: plain when empty, "waiting for the browser" with one row, a long error with many.
    ui.set_add_busy(mode == 1);
    ui.set_add_error(if mode == 2 {
        "Login failed: couldn't get a token: the browser window was closed before the login finished".into()
    } else {
        "".into()
    });
}
