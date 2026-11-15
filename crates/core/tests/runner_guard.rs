//! The delete guard through a real runner: a mass deletion stops and holds the account (also across a restart) until
//! the user answers Restore or Delete; Delete never approves more than the count the card showed. Uses rclone's
//! `alias` backend as the cloud; skipped when rclone is not installed. Its own test binary: it sets `XDG_DATA_HOME`
//! for the whole process.
use std::{
    fs,
    path::Path,
    sync::{Arc, mpsc},
    time::{Duration, Instant},
};

use fastsync_core::{
    accounts::Account,
    rclone::{Rclone, available},
    runner::{Decision, Status, spawn},
};

/// A held account says nothing: any status in this time (an "up to date" from the walk, an error) is a failure.
fn quiet_for(rx: &mpsc::Receiver<Status>, secs: u64) {
    if let Ok(st) = rx.recv_timeout(Duration::from_secs(secs)) {
        panic!("a held account sent {st:?}");
    }
}

fn wait_for(rx: &mpsc::Receiver<Status>, what: &str, ok: impl Fn(&Status) -> bool) {
    let end = Instant::now() + Duration::from_secs(60);
    let mut seen = Vec::new();
    while let Some(left) = end.checked_duration_since(Instant::now()) {
        match rx.recv_timeout(left) {
            Ok(st) if ok(&st) => return,
            Ok(st) => seen.push(st),
            Err(_) => break,
        }
    }
    panic!("never saw {what}; saw {seen:?}");
}

fn count(dir: &Path) -> usize {
    // Not `.fastsync` (the local folder's trash and temp files).
    fs::read_dir(dir)
        .unwrap()
        .flatten()
        .filter(|e| !e.file_name().to_string_lossy().starts_with('.'))
        .count()
}

#[test]
fn a_stopped_account_waits_for_restore_or_delete_across_restarts() {
    if !available() {
        eprintln!("skipped: rclone not found");
        return;
    }
    let base = std::env::temp_dir().join(format!("fastsync-guard-{}", std::process::id()));
    let _ = fs::remove_dir_all(&base);
    let (local, remote) = (base.join("local"), base.join("remote"));
    fs::create_dir_all(&local).unwrap();
    fs::create_dir_all(&remote).unwrap();
    // SAFETY: the only test of this binary, and nothing else runs yet that reads the environment.
    unsafe { std::env::set_var("XDG_DATA_HOME", base.join("data")) };
    let conf = base.join("rclone.conf");
    fs::write(&conf, format!("[a1]\ntype = alias\nremote = {}\n", remote.display())).unwrap();
    for i in 0..12 {
        fs::write(local.join(format!("f{i:02}.txt")), format!("file {i}")).unwrap();
    }
    let rc = Arc::new(Rclone::start(&conf).expect("rclone rcd starts"));
    let start = || {
        let (tx, rx) = mpsc::channel();
        let tx = std::sync::Mutex::new(tx);
        let acct = Account {
            id: "a1".into(),
            kind: "local".into(),
            local: local.to_string_lossy().into_owned(),
            ..Account::default()
        };
        let runner = spawn(
            acct,
            rc.clone(),
            move |st| {
                let _ = tx.lock().unwrap().send(st);
            },
            |_, _| {},
        );
        (runner, rx)
    };
    let remove = |range: std::ops::Range<usize>| {
        for i in range {
            let _ = fs::remove_file(local.join(format!("f{i:02}.txt")));
        }
    };

    let (runner, rx) = start();
    wait_for(&rx, "the first sync to finish", |s| matches!(s, Status::Idle(_)));
    assert_eq!(count(&remote), 12);

    remove(0..8);
    wait_for(&rx, "the guard to stop the run", |s| {
        matches!(s, Status::Stopped(8, ..))
    });
    assert_eq!(count(&remote), 12);

    // Held: neither Sync now, a full scan nor a new file wakes it, and no walk says "up to date".
    runner.sync_now();
    runner.rescan();
    fs::write(local.join("new.txt"), "new").unwrap();
    quiet_for(&rx, 8);
    assert_eq!(count(&remote), 12, "a held account synced");

    // A restart does not forget it, and does not sync either.
    runner.stop_and_wait();
    let (runner, rx) = start();
    wait_for(&rx, "the held card after a restart", |s| {
        matches!(s, Status::Stopped(8, ..))
    });
    quiet_for(&rx, 5);
    assert_eq!(count(&remote), 12);

    // Restore: the 8 files come back from the cloud, the new one goes up.
    runner.resolve(Decision::Restore);
    wait_for(&rx, "the restore to finish", |s| matches!(s, Status::Idle(_)));
    assert_eq!(count(&local), 13);
    assert_eq!(count(&remote), 13);

    // Delete: the count the card showed is a cap. Two more files gone meanwhile stop it again, with the new count.
    remove(0..8);
    wait_for(&rx, "the second stop", |s| matches!(s, Status::Stopped(8, ..)));
    remove(8..10);
    runner.resolve(Decision::Delete);
    wait_for(&rx, "the bigger plan to stop again", |s| {
        matches!(s, Status::Stopped(10, ..))
    });
    assert_eq!(
        count(&remote),
        13,
        "the old confirmation approved more than the card named"
    );
    runner.resolve(Decision::Delete);
    wait_for(&rx, "the confirmed run to finish", |s| matches!(s, Status::Idle(_)));
    assert_eq!(count(&remote), 3);

    runner.stop_and_wait();
    let _ = fs::remove_dir_all(&base);
}
