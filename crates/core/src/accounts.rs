//! The linked accounts, in `accounts.toml` next to the settings. The OAuth tokens live in rclone's own config file
//! (`rclone.conf`, 0600, ours alone), never in here.
use std::path::{Path, PathBuf};

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
    pub paused: bool,
    /// Folders (relative to the synced folder, `/` separated) the user unchecked: neither side is synced for them.
    pub excluded: Vec<String>,
    /// One-way: the folder mirrors the cloud, which is never changed (local edits go to the trash).
    pub download_only: bool,
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
    // Never write an empty file over the account list because serialising failed.
    let Ok(text) = toml::to_string_pretty(&Store {
        accounts: accounts.to_vec(),
    }) else {
        eprintln!("cannot save accounts: serialisation failed");
        return;
    };
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

/// The account whose local folder holds `path`, and the path's place inside the cloud (the same, below the root).
/// None for a path outside every account, the folder itself, `.fastsync` (our trash/tmp) or an unchecked folder.
/// Compares path components, so `/h/Drop` does not hold `/h/Dropbox2/x`.
pub fn locate<'a>(accounts: &'a [Account], path: &Path) -> Option<(&'a Account, String)> {
    accounts.iter().find_map(|a| {
        let rel = path.strip_prefix(&a.local).ok()?;
        let rel = rel.to_str()?;
        let hidden = rel == ".fastsync" || rel.starts_with(".fastsync/");
        let excluded = a
            .excluded
            .iter()
            .any(|e| rel == e || rel.strip_prefix(e.as_str()).is_some_and(|r| r.starts_with('/')));
        if rel.is_empty() || hidden || excluded || rel.split('/').any(|c| c == "..") {
            return None;
        }
        Some((a, rel.to_owned()))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn locate_finds_the_account_and_the_cloud_path() {
        let acct = |id: &str, local: &str, excluded: &[&str]| Account {
            id: id.into(),
            local: local.into(),
            excluded: excluded.iter().map(|s| s.to_string()).collect(),
            ..Account::default()
        };
        let all = [acct("a", "/h/Drop", &[]), acct("b", "/h/Dropbox2", &["Skip"])];
        let find = |p: &str| locate(&all, Path::new(p)).map(|(a, r)| (a.id.clone(), r));
        assert_eq!(find("/h/Drop/x/y.txt"), Some(("a".into(), "x/y.txt".into())));
        assert_eq!(find("/h/Dropbox2/y.txt"), Some(("b".into(), "y.txt".into())));
        assert_eq!(find("/h/Dropbox2/Skip/y.txt"), None);
        assert_eq!(
            find("/h/Dropbox2/Skipper/y.txt").map(|x| x.1),
            Some("Skipper/y.txt".into())
        );
        assert_eq!(find("/h/Drop/.fastsync/trash/y"), None);
        assert_eq!(find("/h/Drop"), None);
        assert_eq!(find("/h/Drop/../etc/passwd"), None);
        assert_eq!(find("/h/Other/y"), None);
    }

    #[test]
    fn old_and_partial_files_load() {
        let s: Store = toml::from_str("[[accounts]]\nid = \"a1\"\nkind = \"drive\"").unwrap();
        assert_eq!(s.accounts[0].kind, "drive");
        assert!(!s.accounts[0].paused);
        assert!(s.accounts[0].excluded.is_empty());
        let s: Store = toml::from_str("[[accounts]]\nid = \"a1\"\nexcluded = [\"Photos\", \"a/b\"]").unwrap();
        assert_eq!(s.accounts[0].excluded, ["Photos", "a/b"]);
        assert!(!s.accounts[0].download_only);
        let s: Store = toml::from_str("[[accounts]]\nid = \"a1\"\ndownload_only = true").unwrap();
        assert!(s.accounts[0].download_only);
    }

    #[test]
    fn provider_names() {
        assert_eq!(provider_name("drive"), "Google Drive");
        assert_eq!(provider_name("sftp"), "sftp");
    }
}
