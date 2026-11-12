//! Tray icon (StatusNotifierItem): the overall status, Open, Sync now, Pause, Quit. The window hides to it when closed,
//! so without a tray host the window's close button quits instead (see `main.rs`).
use std::sync::Arc;

use ksni::{
    Icon, MenuItem, Tray,
    blocking::{Handle, TrayMethods},
    menu::StandardItem,
};

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Overall {
    None,
    Idle,
    Syncing,
    Paused,
    Error,
}

/// Side of the tray PNGs in `crates/app/tray/`, made into ARGB32 by `build.rs`.
const ICON_SIZE: i32 = 64;

impl Overall {
    fn pixmap(self) -> &'static [u8] {
        match self {
            Overall::None | Overall::Idle => include_bytes!(concat!(env!("OUT_DIR"), "/idle.argb")),
            Overall::Syncing => include_bytes!(concat!(env!("OUT_DIR"), "/syncing.argb")),
            Overall::Paused => include_bytes!(concat!(env!("OUT_DIR"), "/paused.argb")),
            Overall::Error => include_bytes!(concat!(env!("OUT_DIR"), "/error.argb")),
        }
    }
}

/// Already translated, because the tray lives on its own thread and the strings come from Slint's `Tr` global.
#[derive(Clone)]
pub struct Labels {
    pub open: String,
    pub sync: String,
    pub pause: String,
    pub resume: String,
    pub open_folder: String,
    pub quit: String,
    pub idle: String,
    pub syncing: String,
    pub paused: String,
    pub error: String,
    pub none: String,
}

/// What the menu does; each posts work to the UI thread or the runners.
pub struct Actions {
    pub open: Arc<dyn Fn() + Send + Sync>,
    pub sync: Arc<dyn Fn() + Send + Sync>,
    pub pause: Arc<dyn Fn() + Send + Sync>,
    pub quit: Arc<dyn Fn() + Send + Sync>,
}

pub struct SyncTray {
    pub labels: Labels,
    pub overall: Overall,
    /// (provider name, local folder) of every account, for the "Open folder" entries.
    pub folders: Vec<(String, String)>,
    actions: Actions,
}

impl SyncTray {
    fn status(&self) -> &str {
        match self.overall {
            Overall::None => &self.labels.none,
            Overall::Idle => &self.labels.idle,
            Overall::Syncing => &self.labels.syncing,
            Overall::Paused => &self.labels.paused,
            Overall::Error => &self.labels.error,
        }
    }
}

impl Tray for SyncTray {
    fn id(&self) -> String {
        "fastsync".into()
    }

    fn title(&self) -> String {
        format!("FastSync: {}", self.status())
    }

    /// Our own icons as pixmaps, so they show without being installed in a theme (also in the Flatpak).
    fn icon_pixmap(&self) -> Vec<Icon> {
        vec![Icon {
            width: ICON_SIZE,
            height: ICON_SIZE,
            data: self.overall.pixmap().to_vec(),
        }]
    }

    fn activate(&mut self, _x: i32, _y: i32) {
        (self.actions.open)();
    }

    fn menu(&self) -> Vec<MenuItem<Self>> {
        let item = |label: &str, icon: &str, f: fn(&Actions) -> &Arc<dyn Fn() + Send + Sync>| {
            StandardItem {
                label: label.into(),
                icon_name: icon.into(),
                activate: Box::new(move |t: &mut Self| f(&t.actions)()),
                ..Default::default()
            }
            .into()
        };
        let mut menu: Vec<MenuItem<Self>> = vec![
            StandardItem {
                label: self.status().into(),
                enabled: false,
                ..Default::default()
            }
            .into(),
            MenuItem::Separator,
            item(&self.labels.open, "folder-open", |a| &a.open),
            item(&self.labels.sync, "view-refresh", |a| &a.sync),
            if self.overall == Overall::Paused {
                item(&self.labels.resume, "media-playback-start", |a| &a.pause)
            } else {
                item(&self.labels.pause, "media-playback-pause", |a| &a.pause)
            },
        ];
        // One entry per account: "Open folder · Dropbox".
        for (provider, path) in &self.folders {
            let path = path.clone();
            menu.push(
                StandardItem {
                    label: format!("{} · {provider}", self.labels.open_folder),
                    icon_name: "folder".into(),
                    activate: Box::new(move |_: &mut Self| crate::xdg_open(path.clone())),
                    ..Default::default()
                }
                .into(),
            );
        }
        menu.push(MenuItem::Separator);
        menu.push(item(&self.labels.quit, "application-exit", |a| &a.quit));
        menu
    }
}

/// Keep the returned handle alive for as long as the tray should stay. `None` without a tray host or a D-Bus session.
pub fn spawn(labels: Labels, actions: Actions) -> Option<Handle<SyncTray>> {
    SyncTray {
        labels,
        overall: Overall::None,
        folders: Vec::new(),
        actions,
    }
    // A sandbox may not own ksni's pid-based bus name; the unique name is enough for the watcher.
    .disable_dbus_name(std::env::var_os("FLATPAK_ID").is_some())
    .spawn()
    .inspect_err(|e| eprintln!("no tray icon ({e}); closing the window will quit"))
    .ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_status_has_a_full_pixmap() {
        for o in [
            Overall::None,
            Overall::Idle,
            Overall::Syncing,
            Overall::Paused,
            Overall::Error,
        ] {
            assert_eq!(o.pixmap().len(), (ICON_SIZE * ICON_SIZE * 4) as usize, "{o:?}");
        }
    }
}
