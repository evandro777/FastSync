//! The engine through a real `rclone rcd`, with rclone's `local` backend as the "cloud". Skipped (with a note) when
//! rclone is not installed, so CI needs nothing extra: `FASTSYNC_RCLONE=/path/to/rclone cargo test` to run it.
use std::{fs, sync::Arc};

use fastsync_core::{
    db::Db,
    engine::Engine,
    rclone::{Rclone, RcloneRemote, available},
};

#[test]
fn sync_through_rclone_both_ways() {
    if !available() {
        eprintln!("skipped: rclone not found");
        return;
    }
    let base = std::env::temp_dir().join(format!("fastsync-rc-{}", std::process::id()));
    let _ = fs::remove_dir_all(&base);
    let (local, remote) = (base.join("local"), base.join("remote"));
    fs::create_dir_all(&local).unwrap();
    fs::create_dir_all(&remote).unwrap();
    fs::write(base.join("rclone.conf"), "").unwrap();
    let rc = Arc::new(Rclone::start(&base.join("rclone.conf")).expect("rclone rcd starts"));

    fs::write(local.join("up.txt"), "up").unwrap();
    fs::create_dir_all(remote.join("sub")).unwrap();
    fs::write(remote.join("sub/down.txt"), "down").unwrap();
    // An absolute path as the fs is rclone's local backend.
    let fs_name = remote.to_string_lossy().to_string();
    // OneDrive's locked vault is never listed or synced: the filter must keep it out.
    fs::create_dir_all(remote.join("Personal Vault")).unwrap();
    fs::write(remote.join("Personal Vault/secret.txt"), "vault").unwrap();
    let exclude = fastsync_core::rclone::exclusions("onedrive");
    let mut engine = Engine::new(
        local.clone(),
        RcloneRemote {
            rc,
            fs: fs_name,
            exclude,
        },
        Db::memory().unwrap(),
    );

    let r = engine.full_scan();
    assert!(r.errors.is_empty() && r.aborted.is_none(), "{r:?}");
    assert_eq!(fs::read_to_string(remote.join("up.txt")).unwrap(), "up");
    assert_eq!(fs::read_to_string(local.join("sub/down.txt")).unwrap(), "down");
    assert!(!local.join("Personal Vault").exists());
    assert_eq!(engine.full_scan().transferred, 0, "a second pass finds nothing to do");

    fs::write(local.join("up.txt"), "up, edited").unwrap();
    assert_eq!(engine.sync_paths(&["up.txt".into()]).transferred, 1);
    assert_eq!(fs::read_to_string(remote.join("up.txt")).unwrap(), "up, edited");
    let _ = fs::remove_dir_all(&base);
}
