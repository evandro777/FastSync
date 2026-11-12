//! User settings in one TOML file under the XDG data dir (`~/.local/share/FastSync`).
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::files::{read_or_backup, write_atomic};

/// Folder name under the XDG data dir; `prepare.sh` rewrites it.
pub const APP_DIR: &str = "FastSync";
pub const MIN_SIZE: (u32, u32) = (400, 300);
/// Upper bound for a hand-edited size; a huge value would fail at window creation.
pub const MAX_SIZE: u32 = 8192;

#[derive(Serialize, Deserialize, Clone)]
#[serde(default)] // older files keep loading when a field is added
pub struct Settings {
    /// "" follows the system locale; otherwise a gettext code such as "pt_BR".
    pub language: String,
    /// "" follows the system light/dark mode; otherwise "light" or "dark".
    pub theme: String,
    pub width: u32,
    pub height: u32,
}

impl Default for Settings {
    fn default() -> Self {
        Self {
            language: String::new(),
            theme: String::new(),
            width: 600,
            height: 450,
        }
    }
}

fn parse(text: &str) -> Result<Settings, toml::de::Error> {
    toml::from_str(text)
}

/// `None` without `XDG_DATA_HOME` or `HOME`: better no settings than a folder dropped in the current directory.
pub fn data_dir() -> Option<PathBuf> {
    let base = std::env::var_os("XDG_DATA_HOME")
        .map(PathBuf::from)
        .filter(|p| p.is_absolute())
        .or_else(|| std::env::var_os("HOME").map(|h| PathBuf::from(h).join(".local/share")))?;
    Some(base.join(APP_DIR))
}

impl Settings {
    pub fn load() -> Self {
        data_dir().map_or_else(Self::default, |dir| Self::load_from(&dir))
    }

    fn load_from(dir: &Path) -> Self {
        // The file is hand-edited; a bad one is moved aside so the next save() must not overwrite what the user wrote.
        let mut s = read_or_backup(&dir.join("settings.toml"), |t| parse(t).map_err(|e| e.to_string())).0;
        s.normalize();
        s
    }

    fn normalize(&mut self) {
        if !matches!(self.theme.as_str(), "light" | "dark") {
            self.theme.clear();
        }
        self.width = self.width.clamp(MIN_SIZE.0, MAX_SIZE);
        self.height = self.height.clamp(MIN_SIZE.1, MAX_SIZE);
    }

    /// Atomic write so a crash can never leave a half-written file. Failures are reported on stderr;
    /// the app keeps working with the in-memory settings.
    pub fn save(&self) {
        let Some(dir) = data_dir() else { return };
        if let Err(e) = self.save_to(&dir) {
            eprintln!("cannot save settings to {}: {e}", dir.display());
        }
    }

    fn save_to(&self, dir: &Path) -> std::io::Result<()> {
        let text = toml::to_string_pretty(self).map_err(std::io::Error::other)?;
        write_atomic(&dir.join("settings.toml"), text)
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use super::*;

    #[test]
    fn old_files_still_load() {
        let parsed: Settings = toml::from_str("width = 700").unwrap();
        assert_eq!((parsed.width, parsed.language.as_str()), (700, ""));
    }

    #[test]
    fn invalid_file_is_an_error_not_defaults() {
        assert!(parse("width = \"wide\"").is_err());
    }

    fn temp_dir(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("fastsync-test-{name}-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        dir
    }

    #[test]
    fn save_then_load_round_trips() {
        let dir = temp_dir("roundtrip");
        let s = Settings {
            language: "pt_BR".into(),
            theme: "dark".into(),
            width: 700,
            height: 500,
        };
        s.save_to(&dir).unwrap();
        let loaded = Settings::load_from(&dir);
        assert_eq!(
            (
                loaded.language.as_str(),
                loaded.theme.as_str(),
                loaded.width,
                loaded.height
            ),
            ("pt_BR", "dark", 700, 500)
        );
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn invalid_file_is_kept_as_bad_and_defaults_are_used() {
        let dir = temp_dir("bad");
        fs::create_dir_all(&dir).unwrap();
        fs::write(dir.join("settings.toml"), "width = \"wide\"").unwrap();
        assert_eq!(Settings::load_from(&dir).width, Settings::default().width);
        assert!(
            fs::read_dir(&dir)
                .unwrap()
                .flatten()
                .any(|e| e.file_name().to_string_lossy().starts_with("settings.toml.bad."))
        );
        assert!(!dir.join("settings.toml").exists());
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn theme_defaults_to_system_and_unknown_values_are_reset() {
        let mut old: Settings = toml::from_str("width = 700").unwrap();
        old.normalize();
        assert_eq!(old.theme, "");
        let mut odd: Settings = toml::from_str("theme = \"blue\"").unwrap();
        odd.normalize();
        assert_eq!(odd.theme, "");
        let mut dark: Settings = toml::from_str("theme = \"dark\"").unwrap();
        dark.normalize();
        assert_eq!(dark.theme, "dark");
    }

    #[test]
    fn window_size_is_clamped_both_ways() {
        let mut s: Settings = toml::from_str("width = 4000000000\nheight = 1").unwrap();
        s.normalize();
        assert_eq!((s.width, s.height), (MAX_SIZE, MIN_SIZE.1));
    }
}
