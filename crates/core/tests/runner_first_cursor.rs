//! A feed with no saved cursor makes one first full scan, not two: the cursor arrives late (here after 4 s, as when
//! the provider is slow) and must not start a second scan after the first. Through a real runner (rclone's `alias`
//! backend as the cloud; skipped without rclone). Its own test binary: it sets `XDG_DATA_HOME`.
use std::{
    fs,
    sync::Arc,
    time::{Duration, Instant},
};

use fastsync_core::{
    accounts::{Account, account_dir},
    db::read_log,
    delta::{Poll, RemoteChanges, Result},
    rclone::{Rclone, available},
    runner::spawn_feed,
};

/// `start` is slow; `poll` never has news.
struct SlowStart;

impl RemoteChanges for SlowStart {
    fn start(&mut self) -> Result<String> {
        std::thread::sleep(Duration::from_secs(4));
        Ok("c1".into())
    }
    fn poll(&mut self, cursor: &str) -> Result<Poll> {
        Ok(Poll::Changes(Vec::new(), cursor.into()))
    }
    fn idle(&self) -> Duration {
        Duration::from_secs(3600)
    }
}

#[test]
fn a_late_first_cursor_does_not_start_a_second_full_scan() {
    if !available() {
        eprintln!("skipped: rclone not found");
        return;
    }
    let base = std::env::temp_dir().join(format!("fastsync-cursor-{}", std::process::id()));
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
    let runner = spawn_feed(acct, rc, |_, _| Some(Box::new(SlowStart)), |_| {}, |_, _| {});
    let db = account_dir("a1").unwrap().join("state.db");
    let scans = || -> Vec<String> {
        read_log(&db, 50)
            .unwrap_or_default()
            .into_iter()
            .filter(|e| e.kind == "scan")
            .map(|e| e.path)
            .collect()
    };
    let end = Instant::now() + Duration::from_secs(60);
    while scans().is_empty() {
        assert!(Instant::now() < end, "the first scan never ran");
        std::thread::sleep(Duration::from_millis(200));
    }
    // Long enough for a second scan to show up if one were queued behind the first.
    std::thread::sleep(Duration::from_secs(6));
    assert_eq!(scans(), ["full:rescan"], "one scan only");
    // The clean scan confirms the first cursor, or the next start would scan everything again.
    let saved = fastsync_core::db::Db::open(&db).unwrap().meta("cursor").unwrap();
    assert_eq!(saved.as_deref(), Some("c1"), "the cursor was not saved");
    runner.stop_and_wait();
}
