//! The linked accounts, in `accounts.toml` next to the settings. The OAuth tokens live in rclone's own config file
//! (`rclone.conf`, 0600, ours alone), never in here.
use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use crate::{
    files::{create_private_dir, read_or_backup, write_atomic},
    settings::data_dir,
};

#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq)]
#[serde(default)]
pub struct Account {
    /// Also the rclone remote name, and the folder of this account's DB.
    pub id: String,
    /// rclone backend type: dropbox, drive, onedrive, mega...
    pub kind: String,
    /// The local folder.
    pub local: String,
    /// The folder inside the cloud ("" = its root).
    pub remote_path: String,
    pub paused: bool,
}

#[derive(Serialize, Deserialize, Default)]
#[serde(default)]
struct Store {
    accounts: Vec<Account>,
}

/// The providers offered in the Add account dialog: (rclone type, display name).
pub const PROVIDERS: [(&str, &str); 4] = [
    ("dropbox", "Dropbox"),
    ("drive", "Google Drive"),
    ("onedrive", "OneDrive"),
    ("mega", "MEGA"),
];

pub fn provider_name(kind: &str) -> &str {
    PROVIDERS.iter().find(|(k, _)| *k == kind).map_or(kind, |(_, n)| n)
}

pub fn load() -> Vec<Account> {
    let Some(dir) = data_dir() else { return Vec::new() };
    read_or_backup(&dir.join("accounts.toml"), |t| {
        toml::from_str::<Store>(t).map_err(|e| e.to_string())
    })
    .0
    .accounts
}

pub fn save(accounts: &[Account]) {
    let Some(dir) = data_dir() else { return };
    let text = toml::to_string_pretty(&Store {
        accounts: accounts.to_vec(),
    })
    .unwrap_or_default();
    if let Err(e) = create_private_dir(&dir).and_then(|()| write_atomic(&dir.join("accounts.toml"), text)) {
        eprintln!("cannot save accounts: {e}");
    }
}

pub fn rclone_config() -> Option<PathBuf> {
    data_dir().map(|d| d.join("rclone.conf"))
}

pub fn account_dir(id: &str) -> Option<PathBuf> {
    data_dir().map(|d| d.join("accounts").join(id))
}

/// A fresh id (`a` + 8 hex digits). Letters and digits only: it is an rclone remote name and a folder name.
pub fn new_id() -> String {
    let n = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.subsec_nanos() ^ (d.as_secs() as u32).rotate_left(11));
    format!("a{n:08x}")
}

/// `~/Name` for a new account's default local folder.
pub fn default_local(kind: &str) -> String {
    let home = std::env::var("HOME").unwrap_or_default();
    format!("{home}/{}", provider_name(kind).replace(' ', ""))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn old_and_partial_files_load() {
        let s: Store = toml::from_str("[[accounts]]\nid = \"a1\"\nkind = \"drive\"").unwrap();
        assert_eq!(s.accounts[0].kind, "drive");
        assert!(!s.accounts[0].paused && s.accounts[0].remote_path.is_empty());
    }

    #[test]
    fn provider_names() {
        assert_eq!(provider_name("drive"), "Google Drive");
        assert_eq!(provider_name("sftp"), "sftp");
    }
}
