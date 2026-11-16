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
            dropbox: false,
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

    // A one-level listing of a sub-folder names its files from the root, like `stat` does: the engine matches on it.
    use fastsync_core::engine::Remote;
    let dir = engine.remote.list_dir("sub").unwrap();
    assert_eq!(dir.len(), 1, "{dir:?}");
    assert_eq!(dir[0].path, "sub/down.txt");
    assert_eq!(Some(dir[0].clone()), engine.remote.stat("sub/down.txt").unwrap());
    assert!(
        engine.remote.list_dir("nope").is_err(),
        "a missing folder is an error, not an empty list"
    );

    // The same bytes with other times (an upload cut short by a quit, a file put there by another tool): the hashes
    // from rclone's listing and its local stat must agree, so no "(conflict …)" copy appears.
    fs::write(local.join("same.bin"), "identical").unwrap();
    fs::write(remote.join("same.bin"), "identical").unwrap();
    let old = std::time::SystemTime::now() - std::time::Duration::from_secs(86_400);
    fs::File::options()
        .write(true)
        .open(remote.join("same.bin"))
        .unwrap()
        .set_modified(old)
        .unwrap();
    let r = engine.full_scan();
    assert!(r.errors.is_empty(), "{r:?}");
    assert_eq!(r.transferred, 0);
    let names: Vec<_> = fs::read_dir(&local)
        .unwrap()
        .flatten()
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .collect();
    assert!(!names.iter().any(|n| n.contains("conflict")), "{names:?}");

    // A rename here is `operations/movefile` in the cloud (the same file there, not a copy), a rename in the cloud
    // is a local rename: nothing crosses the network either way.
    use std::os::unix::fs::MetadataExt;
    let ino = fs::metadata(remote.join("up.txt")).unwrap().ino();
    fs::create_dir_all(local.join("moved")).unwrap();
    fs::rename(local.join("up.txt"), local.join("moved/up.txt")).unwrap();
    let r = engine.full_scan();
    assert!(r.errors.is_empty() && r.aborted.is_none(), "{r:?}");
    assert_eq!(fs::metadata(remote.join("moved/up.txt")).unwrap().ino(), ino);
    assert!(!remote.join("up.txt").exists());
    let ino = fs::metadata(local.join("sub/down.txt")).unwrap().ino();
    fs::rename(remote.join("sub"), remote.join("renamed")).unwrap();
    let r = engine.full_scan();
    assert!(r.errors.is_empty() && r.aborted.is_none(), "{r:?}");
    assert_eq!(fs::metadata(local.join("renamed/down.txt")).unwrap().ino(), ino);
    assert!(!local.join("sub").exists());

    // A folder emptied by a delete goes in the cloud too (`operations/rmdir`); one that still holds a file stays.
    fs::create_dir_all(local.join("gone/deep")).unwrap();
    fs::write(local.join("gone/deep/x.txt"), "x").unwrap();
    fs::write(local.join("gone/stay.txt"), "stay").unwrap();
    let r = engine.full_scan();
    assert!(r.errors.is_empty(), "{r:?}");
    fs::remove_file(local.join("gone/deep/x.txt")).unwrap();
    let r = engine.full_scan();
    assert!(r.errors.is_empty() && r.aborted.is_none(), "{r:?}");
    assert!(
        !remote.join("gone/deep").exists(),
        "the emptied folder stays in the cloud"
    );
    assert!(remote.join("gone/stay.txt").exists());
    let _ = fs::remove_dir_all(&base);
}

/// Many new files in a folder the cloud does not have yet: one listing says "not found", and the uploads must still
/// create the folder (it was "directory not found" on OneDrive, 84 times).
#[test]
fn many_new_files_in_a_new_cloud_folder_upload_through_rclone() {
    if !available() {
        eprintln!("skipped: rclone not found");
        return;
    }
    let base = std::env::temp_dir().join(format!("fastsync-rc-new-{}", std::process::id()));
    let _ = fs::remove_dir_all(&base);
    let (local, remote) = (base.join("local"), base.join("remote"));
    fs::create_dir_all(&local).unwrap();
    fs::create_dir_all(&remote).unwrap();
    fs::write(base.join("rclone.conf"), "").unwrap();
    let rc = Arc::new(Rclone::start(&base.join("rclone.conf")).expect("rclone rcd starts"));
    let mut engine = Engine::new(
        local.clone(),
        RcloneRemote {
            rc,
            fs: remote.to_string_lossy().to_string(),
            exclude: vec![],
            dropbox: false,
        },
        Db::memory().unwrap(),
    );
    fs::create_dir_all(local.join("tmp")).unwrap();
    let paths: Vec<String> = (0..20).map(|i| format!("tmp/Untitled Document ({i} copy)")).collect();
    for p in &paths {
        fs::write(local.join(p), "").unwrap();
    }
    let r = engine.sync_paths(&paths);
    assert!(r.errors.is_empty() && r.transferred == 20, "{r:?}");
    assert_eq!(fs::read_dir(remote.join("tmp")).unwrap().count(), 20);
    let _ = fs::remove_dir_all(&base);
}

/// The listing split per top-level folder (parallel, with a count) is the same list as one recursive call: root
/// files, nested folders and the excluded vault included, and it counts every top-level folder.
#[test]
fn a_listing_split_by_folder_equals_the_recursive_one() {
    if !available() {
        eprintln!("skipped: rclone not found");
        return;
    }
    use fastsync_core::engine::Remote;
    use std::sync::Mutex;
    let base = std::env::temp_dir().join(format!("fastsync-rc-split-{}", std::process::id()));
    let _ = fs::remove_dir_all(&base);
    let remote = base.join("remote");
    fs::create_dir_all(&remote).unwrap();
    fs::write(base.join("rclone.conf"), "").unwrap();
    for (dir, files) in [
        ("a", 3),
        ("b/deep/er", 2),
        ("c", 0),
        // Deeper than the split levels: the last folders go in one recursive call each.
        ("d/e/f/g/h", 2),
        ("Personal Vault", 1),
    ] {
        fs::create_dir_all(remote.join(dir)).unwrap();
        for i in 0..files {
            fs::write(remote.join(dir).join(format!("f{i}.txt")), "x").unwrap();
        }
    }
    fs::write(remote.join("root.txt"), "r").unwrap();
    let rc = Arc::new(Rclone::start(&base.join("rclone.conf")).expect("rclone rcd starts"));
    let r = RcloneRemote {
        rc,
        fs: remote.to_string_lossy().to_string(),
        exclude: fastsync_core::rclone::exclusions("onedrive"),
        dropbox: false,
    };
    let seen = Mutex::new(Vec::new());
    let mut split = r
        .list_progress(&|d, t| seen.lock().unwrap().push((d, t)), &|| false)
        .unwrap();
    let mut whole = r.list().unwrap();
    split.sort_by(|a, b| a.path.cmp(&b.path));
    whole.sort_by(|a, b| a.path.cmp(&b.path));
    assert_eq!(split, whole);
    assert!(split.iter().any(|i| i.path == "b/deep/er/f1.txt") && split.iter().all(|i| !i.path.contains("Vault")));
    let seen = seen.into_inner().unwrap();
    assert_eq!(seen.first(), Some(&(0, 1)), "{seen:?}");
    // The total grows as folders are found, and the end meets it: root, a, b, c, d, b/deep, d/e (one level per call)
    // plus b/deep/er and d/e/f (depth 3: one recursive call each; the vault is filtered out).
    assert_eq!(seen.last(), Some(&(9, 9)), "{seen:?}");
    assert!(split.iter().any(|i| i.path == "d/e/f/g/h/f1.txt"));
    let _ = fs::remove_dir_all(&base);
}

/// A Drive login made before shortcuts were skipped gets the setting added, with its token and client kept; a login
/// that has it, or is not Drive, is left alone. No network: the token is a made-up one and nothing is listed.
#[test]
fn an_old_drive_login_gets_skip_shortcuts_and_keeps_the_rest() {
    if !available() {
        eprintln!("skipped: rclone not found");
        return;
    }
    let base = std::env::temp_dir().join(format!("fastsync-rc-sc-{}", std::process::id()));
    let _ = fs::remove_dir_all(&base);
    fs::create_dir_all(&base).unwrap();
    let token = r#"{"access_token":"a","token_type":"Bearer","refresh_token":"r","expiry":"2099-01-01T00:00:00Z"}"#;
    fs::write(
        base.join("rclone.conf"),
        format!("[old]\ntype = drive\nclient_id = cid\nskip_gdocs = true\ntoken = {token}\n\n[plain]\ntype = local\n"),
    )
    .unwrap();
    let rc = Rclone::start(&base.join("rclone.conf")).expect("rclone rcd starts");
    rc.skip_shortcuts("old").unwrap();
    let cfg = rc.config_get("old").unwrap();
    assert_eq!(cfg["skip_shortcuts"], "true");
    assert_eq!(cfg["skip_gdocs"], "true");
    assert_eq!(cfg["client_id"], "cid");
    assert_eq!(cfg["token"].as_str().unwrap(), token);
    // Again: nothing to do. And a remote that is not Drive is not touched.
    rc.skip_shortcuts("old").unwrap();
    rc.skip_shortcuts("plain").unwrap();
    assert!(rc.config_get("plain").unwrap().get("skip_shortcuts").is_none());
    let _ = fs::remove_dir_all(&base);
}
