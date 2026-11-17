//! Add / Edit / Remove account: the login flow and the checks on the local folder.

use super::*;

/// Clears the Add/Edit dialog and closes it.
pub(crate) fn reset_add(ui: &Main) {
    state().edit_client = None;
    ui.set_add_busy(false);
    ui.set_add_auth_busy(false);
    ui.set_add_email("".into());
    ui.set_add_error("".into());
    ui.set_add_password("".into());
    ui.set_add_client_id("".into());
    ui.set_add_client_secret("".into());
    ui.set_add_own_client(false);
    ui.set_add_relogin(false);
    ui.set_add_open(false);
    ui.set_add_edit_id("".into());
    ui.set_add_local(accounts::default_local("dropbox").into());
    ui.set_add_download_only(false);
    ui.set_add_kind(0);
}

/// The local folder rules shared by Add and Edit. `except` is the account being edited: its own folder is not a clash.
pub(crate) fn check_local(ui: &Main, local: &str, except: &str) -> Result<(), String> {
    let tr = ui.global::<Tr>();
    if !local.starts_with('/') || local.len() < 2 {
        return Err(tr.invoke_error_no_folder().into());
    }
    // Moving an account's folder inside or around its own old one would upload the old tree again as a subfolder.
    if state()
        .accounts
        .iter()
        .any(|a| a.id == except && a.local != local && clashes(&a.local, local))
    {
        return Err(tr.invoke_error_nested().into());
    }
    if state()
        .accounts
        .iter()
        .any(|a| a.id != except && clashes(&a.local, local))
    {
        return Err(tr.invoke_error_taken().into());
    }
    let home = std::env::var("HOME").unwrap_or_default();
    let data = fastsync_core::settings::data_dir().unwrap_or_default();
    // A symlink is judged by where it leads (`~/Sync` -> `~` would upload the whole home folder).
    let real = real_path(local);
    if bad_folder(local, &home, &data) || bad_folder(&real, &home, &data) {
        return Err(tr.invoke_error_bad_folder().into());
    }
    Ok(())
}

/// Edit's Save, when the login has to be renewed (a changed client ID, a typed MEGA password, or an expired token):
/// log in again and replace the saved login, then go on to the folder picker (`begin_edit`, which will not renew
/// again). The old login stays unless the new one works (`Rclone::reauth`). `then` is `(local folder, download only)`.
pub(crate) fn reauth(ui: &Main, then: (String, bool)) {
    let tr = ui.global::<Tr>();
    let edit_id = ui.get_add_edit_id().to_string();
    let Some(acct) = state().accounts.iter().find(|a| a.id == edit_id).cloned() else {
        return;
    };
    let creds =
        (acct.kind == "mega").then(|| (ui.get_add_email().trim().to_string(), ui.get_add_password().to_string()));
    // Unchecked means rclone's shared client or the built-in one, whatever is left in the fields.
    let (id, secret) = if ui.get_add_own_client() {
        (
            ui.get_add_client_id().trim().to_string(),
            ui.get_add_client_secret().trim().to_string(),
        )
    } else {
        Default::default()
    };
    if acct.kind != "mega" && !valid_client(&id, &secret) {
        return ui.set_add_error(tr.invoke_error_client());
    }
    // An empty password must never replace a working login.
    if creds
        .as_ref()
        .is_some_and(|(user, pass)| user.is_empty() || pass.is_empty())
    {
        return;
    }
    let wanted = (id.clone(), secret.clone());
    let client = (acct.kind != "mega" && !id.is_empty()).then_some((id, secret));
    ui.set_add_error("".into());
    ui.set_add_busy(true);
    ui.set_add_auth_busy(true);
    let my_gen = {
        let mut s = state();
        s.login_gen += 1;
        s.login_gen
    };
    let weak = ui.as_weak();
    // Blocks until the user finishes in the browser, so it runs off the UI thread.
    std::thread::spawn(move || {
        let result = rclone().and_then(|rc| {
            rc.reauth(
                &acct.id,
                &acct.kind,
                creds.as_ref().map(|(u, p)| (u.as_str(), p.as_str())),
                client.as_ref().map(|(i, s)| (i.as_str(), s.as_str())),
                &|| state().login_gen == my_gen,
            )
        });
        // Cancelled: the dialog is already reset, and a login that arrived late was dropped.
        if state().login_gen != my_gen {
            return;
        }
        if result.is_ok() {
            // So a second Save does not log in again.
            state().edit_client = Some(wanted);
            if let Some(r) = state().runners.get(&acct.id) {
                // A "needs attention" caused by the old login clears at once. A held account (stopped by the delete
                // guard) ignores it: a login for another cloud account is exactly what the guard must still catch.
                r.rescan();
            }
        }
        let _ = weak.upgrade_in_event_loop(move |ui| {
            ui.set_add_busy(false);
            ui.set_add_auth_busy(false);
            let tr = ui.global::<Tr>();
            match result {
                Ok(()) => {
                    ui.set_add_password("".into());
                    begin_edit(&ui, &acct.id, then.0, then.1, false);
                }
                Err(e) => {
                    let e: slint::SharedString = e.chars().take(500).collect::<String>().into();
                    ui.set_add_error(if missing_backend(&e) {
                        tr.invoke_error_backend()
                    } else {
                        tr.invoke_error_login(e)
                    });
                }
            }
            refresh(&ui);
        });
    });
}

/// Whether a provider or rclone error means the saved login is no longer accepted (expired or revoked token), as
/// opposed to a network or server problem, where opening the browser would not help.
pub(crate) fn login_expired(error: &str) -> bool {
    const SIGNS: [&str; 10] = [
        "invalid_grant",
        "token expired",
        "expired_access_token",
        "invalid_access_token",
        "invalidauthenticationtoken",
        "unauthorized",
        "couldn't fetch token",
        "failed to refresh",
        "empty token",
        "token revoked",
    ];
    let e = error.to_lowercase();
    SIGNS.iter().any(|s| e.contains(s))
}

/// Edit, step one done: the folder list of the existing connection is the validation. Opens the picker on success.
/// `renew`: a listing that fails because the login expired goes through `reauth` instead of showing the error (once:
/// `reauth` passes false, so a fresh login that still fails shows its error). MEGA has no token to expire.
pub(crate) fn begin_edit(ui: &Main, id: &str, local: String, download_only: bool, renew: bool) {
    let Some(acct) = state().accounts.iter().find(|a| a.id == id).cloned() else {
        return;
    };
    ui.set_add_error("".into());
    ui.set_add_busy(true);
    let my_gen = {
        let mut s = state();
        s.login_gen += 1;
        s.login_gen
    };
    let weak = ui.as_weak();
    std::thread::spawn(move || {
        let fs = remote_fs(&acct);
        let result = rclone().and_then(|rc| rc.list_dirs(&fs, "", &exclusions(&acct.kind)));
        // Cancelled while listing: the dialog is already closed.
        if state().login_gen != my_gen {
            return;
        }
        let _ = weak.upgrade_in_event_loop(move |ui| match result {
            Ok(dirs) => {
                reset_add(&ui);
                let picker = Picker {
                    id: acct.id,
                    kind: acct.kind,
                    fs,
                    local,
                    adding: false,
                    excluded: acct.excluded,
                    download_only,
                    ..Default::default()
                };
                open_picker(&ui, picker, dirs);
            }
            Err(e) if renew && acct.kind != "mega" && login_expired(&e) => {
                ui.set_add_busy(false);
                reauth(&ui, (local, download_only));
            }
            Err(e) => {
                ui.set_add_busy(false);
                let msg = ui
                    .global::<Tr>()
                    .invoke_error_folders(e.chars().take(500).collect::<String>().into());
                ui.set_add_error(msg);
            }
        });
    });
}

/// rclone's answer when it was compiled without a backend (Debian and Ubuntu's package leaves some out, MEGA among them).
pub(crate) fn missing_backend(error: &str) -> bool {
    error.contains("couldn't find backend")
}

/// The guide to creating one's own OAuth client (in the repository, so it can be fixed without a release).
pub(crate) const HELP_URL: &str = concat!(env!("CARGO_PKG_REPOSITORY"), "/blob/master/docs/OWN-CLIENT-ID.md");

/// Both empty (use rclone's shared client) or both filled without spaces (a pasted value with a space is a typo).
pub(crate) fn valid_client(id: &str, secret: &str) -> bool {
    id.is_empty() == secret.is_empty() && !id.contains(char::is_whitespace) && !secret.contains(char::is_whitespace)
}

/// The whole home folder, a folder above it, or anything touching FastSync's own data (tokens, DBs) must not be
/// synced: it would upload the tokens and loop on its own database writes.
pub(crate) fn bad_folder(local: &str, home: &str, data: &Path) -> bool {
    let local = Path::new(local);
    // `..` is never normalised by `starts_with`: `/home/u/x/..` would pass as "not the home folder".
    local.components().any(|c| c == std::path::Component::ParentDir)
        || (!home.is_empty() && Path::new(home).starts_with(local))
        || overlaps(&local.to_string_lossy(), &data.to_string_lossy())
}

/// Two synced folders must not be the same or inside each other: they would sync each other's files.
pub(crate) fn overlaps(a: &str, b: &str) -> bool {
    let (a, b) = (Path::new(a), Path::new(b));
    a.starts_with(b) || b.starts_with(a)
}

/// `overlaps` for the typed paths, and for where they really lead: `~/link` -> an account's folder passes the text
/// check, and two engines on one folder would delete each other's files (a delete in one cloud trashes the local file,
/// which the other account then deletes in its own cloud).
pub(crate) fn clashes(a: &str, b: &str) -> bool {
    overlaps(a, b) || overlaps(&real_path(a), &real_path(b))
}

/// Where a path leads, also when it does not exist yet (Add types a new folder): the nearest existing parent is
/// resolved and the rest appended, so `~/link/New` with `link` -> another account's folder is seen for what it is.
pub(crate) fn real_path(p: &str) -> String {
    let (mut cur, mut rest) = (Path::new(p), std::path::PathBuf::new());
    loop {
        if let Ok(real) = std::fs::canonicalize(cur) {
            return real.join(&rest).to_string_lossy().into_owned();
        }
        match (cur.parent(), cur.file_name()) {
            (Some(parent), Some(name)) => {
                rest = Path::new(name).join(&rest);
                cur = parent;
            }
            _ => return p.to_string(),
        }
    }
}
