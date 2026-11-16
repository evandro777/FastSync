//! The activity log says why a full scan ran, through a real runner (rclone's `alias` backend as the cloud; skipped
//! without rclone). Its own test binary: it sets `XDG_DATA_HOME` for the whole process.
use std::{
    fs,
    sync::Arc,
    time::{Duration, Instant},
};

use fastsync_core::{
    accounts::{Account, account_dir},
    db::read_log,
    rclone::{Rclone, available},
    runner::spawn,
};

fn causes(db: &std::path::Path) -> Vec<String> {
    read_log(db, 50)
        .unwrap_or_default()
        .into_iter()
        .filter(|e| e.kind == "scan")
        .map(|e| e.path)
        .collect()
}

#[test]
fn every_full_scan_is_logged_with_its_cause() {
    if !available() {
        eprintln!("skipped: rclone not found");
        return;
    }
    let base = std::env::temp_dir().join(format!("fastsync-cause-{}", std::process::id()));
    let _ = fs::remove_dir_all(&base);
    let (local, remote) = (base.join("local"), base.join("remote"));
    fs::create_dir_all(&local).unwrap();
    fs::create_dir_all(&remote).unwrap();
    // SAFETY: the only test of this binary, and nothing else runs yet that reads the environment.
    unsafe { std::env::set_var("XDG_DATA_HOME", base.join("data")) };
    let conf = base.join("rclone.conf");
    fs::write(&conf, format!("[a1]\ntype = alias\nremote = {}\n", remote.display())).unwrap();
    let rc = Arc::new(Rclone::start(&conf).expect("rclone rcd starts"));
    let acct = Account {
        id: "a1".into(),
        kind: "local".into(),
        local: local.to_string_lossy().into_owned(),
        ..Account::default()
    };
    let runner = spawn(acct, rc, |_| {}, |_, _| {});
    let db = account_dir("a1").unwrap().join("state.db");
    let wait = |want: &[&str]| {
        let end = Instant::now() + Duration::from_secs(60);
        loop {
            let seen = causes(&db);
            if want.iter().all(|w| seen.iter().any(|s| s == w)) {
                return;
            }
            assert!(Instant::now() < end, "wanted {want:?}, the log has {seen:?}");
            std::thread::sleep(Duration::from_millis(200));
        }
    };
    // Nothing to move, yet the start-up scan is logged: that is what shows a scan that found nothing.
    wait(&["full:rescan"]);
    runner.sync_now();
    wait(&["full:rescan", "full:now"]);
    runner.stop_and_wait();
}
