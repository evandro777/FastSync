//! Opening things: folders, files in the file manager, the web, and the share link.

use super::*;

/// Opens a folder in the file manager. Waiting in a thread reaps the child; a dropped `Child` would stay a zombie.
pub(crate) fn xdg_open(what: String) {
    std::thread::spawn(move || {
        if let Err(e) = std::process::Command::new("xdg-open").arg(&what).status() {
            eprintln!("cannot open {what}: {e}");
        }
    });
}

/// `file://` URI of an absolute path: every byte outside the unreserved set and `/` is percent-encoded.
pub(crate) fn file_uri(p: &Path) -> String {
    use std::os::unix::ffi::OsStrExt;
    let mut s = String::from("file://");
    for &b in p.as_os_str().as_bytes() {
        if b.is_ascii_alphanumeric() || b"-._~/".contains(&b) {
            s.push(b as char);
        } else {
            s.push_str(&format!("%{b:02X}"));
        }
    }
    s
}

/// Opens the file manager with `file` selected (`FileManager1.ShowItems`: Nemo, Nautilus, Dolphin). Without that
/// service, or in the Flatpak where the call is not allowed, it opens the folder.
pub(crate) fn show_file(file: PathBuf) {
    std::thread::spawn(move || {
        let shown = zbus::blocking::Connection::session().and_then(|c| {
            c.call_method(
                Some("org.freedesktop.FileManager1"),
                "/org/freedesktop/FileManager1",
                Some("org.freedesktop.FileManager1"),
                "ShowItems",
                &(vec![file_uri(&file)], ""),
            )
        });
        if let Err(e) = shown {
            eprintln!("cannot show {}: {e}", file.display());
            xdg_open(file.parent().unwrap_or(&file).to_string_lossy().into_owned());
        }
    });
}

/// The provider's web page. shortcut: the home page, not the synced folder; Drive/OneDrive need folder ids.
pub(crate) fn web_url(kind: &str) -> Option<&'static str> {
    Some(match kind {
        "dropbox" => "https://www.dropbox.com/home",
        "drive" => "https://drive.google.com",
        "onedrive" => "https://onedrive.live.com",
        "mega" => "https://mega.nz/fm",
        _ => return None,
    })
}

/// The command for `--share <file>` or `--web <file>` (the file manager's actions): `share <abs path>` or
/// `web <abs path>`. A newline in the path would split the one-line command sent to a running instance.
pub(crate) fn file_command() -> Option<String> {
    let args: Vec<String> = std::env::args().collect();
    let (verb, at) = args.iter().enumerate().find_map(|(i, a)| match a.as_str() {
        "--share" => Some(("share", i)),
        "--web" => Some(("web", i)),
        _ => None,
    })?;
    let abs = std::path::absolute(args.get(at + 1)?).ok()?.to_str()?.to_owned();
    (!abs.contains('\n')).then(|| format!("{verb} {abs}"))
}

/// Runs a `share <path>` or `web <path>` command (from the command line or the socket); false for anything else.
pub(crate) fn file_action(command: &str, weak: slint::Weak<Main>) -> bool {
    match command.split_once(' ') {
        Some(("share", path)) => share(path.to_owned(), weak),
        Some(("web", path)) => web(path.to_owned()),
        _ => return false,
    }
    true
}

/// Opens the provider's page for a local file ("View on web"). Any failure opens the provider's home page instead:
/// a page is better than none.
pub(crate) fn web(path: String) {
    std::thread::spawn(move || {
        let found = accounts::locate(&state().accounts, Path::new(&path)).map(|(a, rel)| (a.clone(), rel));
        let Some((acct, remote)) = found else { return };
        let url = rclone()
            .and_then(|rc| fastsync_core::web::page_url(&rc, &acct, &remote))
            .unwrap_or_else(|e| {
                eprintln!("cannot find the web page of {path}: {e}");
                web_url(&acct.kind).unwrap_or_default().to_owned()
            });
        if !url.is_empty() {
            xdg_open(url);
        }
    });
}

/// Bumped by every share request: a slow answer for an older file must never fill the dialog of a newer one, or the
/// user would copy a public link to the wrong file.
pub(crate) static SHARE_GEN: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// Shows the share dialog for a local file and makes its public link on a thread (rclone asks the provider).
pub(crate) fn share(path: String, weak: slint::Weak<Main>) {
    use std::sync::atomic::Ordering::SeqCst;
    let generation = SHARE_GEN.fetch_add(1, SeqCst) + 1;
    let name = Path::new(&path)
        .file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_default();
    let _ = weak.upgrade_in_event_loop(move |ui| {
        instance::raise(ui.window());
        ui.set_share_name(name.into());
        ui.set_share_link("".into());
        ui.set_share_error("".into());
        ui.set_share_busy(true);
        ui.set_share_open(true);
    });
    std::thread::spawn(move || {
        // The lock is not held across the slow rclone call.
        let found = accounts::locate(&state().accounts, Path::new(&path)).map(|(a, rel)| (a.id.clone(), rel));
        // Err(None): not in a synced folder; Err(Some(text)): rclone's own message.
        let result = match found {
            None => Err(None),
            Some((id, remote)) => rclone()
                .and_then(|rc| rc.public_link(&format!("{id}:"), &remote))
                .map_err(Some),
        };
        let _ = weak.upgrade_in_event_loop(move |ui| {
            if SHARE_GEN.load(SeqCst) != generation {
                return;
            }
            let tr = ui.global::<Tr>();
            match result {
                Ok(link) => ui.set_share_link(link.into()),
                Err(None) => ui.set_share_error(tr.invoke_error_share_outside()),
                // A file the sync has not uploaded yet is not on the provider.
                Err(Some(e)) if e.contains("not found") => ui.set_share_error(tr.invoke_error_share_unsynced()),
                Err(Some(e)) => {
                    ui.set_share_error(tr.invoke_error_share(e.chars().take(500).collect::<String>().into()));
                }
            }
            ui.set_share_busy(false);
        });
    });
}
