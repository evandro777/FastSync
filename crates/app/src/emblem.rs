//! Status emblem on an account's local root folder, through GIO metadata (Nemo, Nautilus and Caja show it).
//! Per file would need a file-manager extension: not done.
use fastsync_core::{runner::Status, settings::data_dir};
use std::sync::atomic::{AtomicBool, Ordering};

/// The user's "File manager emblems" setting; `set` skips showing an emblem while it is off.
static ENABLED: AtomicBool = AtomicBool::new(true);

pub fn enable(on: bool) {
    ENABLED.store(on, Ordering::Relaxed);
}

/// The badges, drawn like the tray icons (`crates/app/emblems/`). They are installed into the user's hicolor theme,
/// which every icon theme falls back to, so the file manager finds them by name.
const ICONS: [(&str, &str); 4] = [
    ("idle", include_str!("../emblems/idle.svg")),
    ("syncing", include_str!("../emblems/syncing.svg")),
    ("paused", include_str!("../emblems/paused.svg")),
    ("error", include_str!("../emblems/error.svg")),
];

pub fn name(st: Option<&Status>) -> Option<&'static str> {
    Some(match st? {
        Status::Idle(_) => "emblem-fastsync-idle",
        Status::Syncing(..) => "emblem-fastsync-syncing",
        Status::Paused => "emblem-fastsync-paused",
        Status::Error(_) | Status::Stopped(..) => "emblem-fastsync-error",
    })
}

/// Writes the badges where icon themes look (only when missing or changed). Cosmetic: failures are ignored.
pub fn install() {
    if in_flatpak() {
        return;
    }
    let Some(base) = data_dir().and_then(|d| d.parent().map(|p| p.join("icons/hicolor/scalable/emblems"))) else {
        return;
    };
    if std::fs::create_dir_all(&base).is_err() {
        return;
    }
    for (n, svg) in ICONS {
        let path = base.join(format!("emblem-fastsync-{n}.svg"));
        if std::fs::read_to_string(&path).ok().as_deref() != Some(svg) {
            let _ = std::fs::write(path, svg);
        }
    }
}

pub fn in_flatpak() -> bool {
    std::env::var_os("FLATPAK_ID").is_some()
}

/// Sets (or with `None` clears) the emblem. Cosmetic, so every failure is ignored.
pub fn set(local: &str, emblem: Option<&str>) {
    // shortcut: inside the Flatpak gio writes the sandbox's own metadata, not the host's; upgrade with a portal if one appears.
    if local.is_empty() || in_flatpak() || (emblem.is_some() && !ENABLED.load(Ordering::Relaxed)) {
        return;
    }
    let mut cmd = std::process::Command::new("gio");
    cmd.arg("set");
    match emblem {
        Some(e) => cmd.args(["-t", "stringv", local, "metadata::emblems", e]),
        None => cmd.args(["-t", "unset", local, "metadata::emblems"]),
    };
    let _ = cmd.output();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names() {
        assert_eq!(name(Some(&Status::Idle(0))), Some("emblem-fastsync-idle"));
        assert_eq!(name(Some(&Status::Paused)), Some("emblem-fastsync-paused"));
        assert_eq!(
            name(Some(&Status::Stopped(1, 2, vec![]))),
            name(Some(&Status::Error("x".into())))
        );
        assert_eq!(name(None), None);
        // Every name has its file in ICONS.
        for st in [
            Status::Idle(0),
            Status::Syncing(Default::default(), 0.0),
            Status::Paused,
            Status::Error(String::new()),
        ] {
            let n = name(Some(&st)).unwrap().trim_start_matches("emblem-fastsync-");
            assert!(ICONS.iter().any(|(i, svg)| *i == n && svg.contains("<svg")));
        }
    }
}
