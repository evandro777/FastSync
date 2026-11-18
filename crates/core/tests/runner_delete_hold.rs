//! A file removed and written again with the same bytes a few seconds later reaches ONE run (the runner holds the run
//! for `LOCAL_DELETE_HOLD` after the first local delete), so the cloud copy is never deleted and the bytes are not sent
//! again. Through a real runner with its watcher (rclone's `alias` backend as the cloud; skipped without rclone). Its
//! own test binary: it sets `XDG_DATA_HOME`.
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

/// A feed that starts at once and never has news.
struct Quiet;

impl RemoteChanges for Quiet {
    fn start(&mut self) -> Result<String> {
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
fn a_file_removed_and_written_again_is_not_deleted_in_the_cloud_nor_sent_twice() {
    if !available() {
        eprintln!("skipped: rclone not found");
        return;
    }
    let base = std::env::temp_dir().join(format!("fastsync-delhold-{}", std::process::id()));
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
    fs::write(local.join("a.txt"), "the same bytes").unwrap();
    let runner = spawn_feed(acct, rc, |_, _| Some(Box::new(Quiet)), |_| {}, |_, _| {});
    let db = account_dir("a1").unwrap().join("state.db");
    let rows = |kind: &str| -> usize {
        read_log(&db, 200)
            .unwrap_or_default()
            .into_iter()
            .filter(|e| e.kind == kind && (e.kind != "scan" || e.msg != "start"))
            .count()
    };
    // The first sync: a finished scan row says the run is over and the file's row is written.
    let end = Instant::now() + Duration::from_secs(60);
    while rows("scan") == 0 {
        assert!(Instant::now() < end, "the first scan never finished");
        std::thread::sleep(Duration::from_millis(200));
    }
    assert!(remote.join("a.txt").exists(), "the first sync uploaded it");
    let ups = rows("up");
    // The tool's way: remove, pause longer than the runner's 3 s burst wait, write the same bytes again.
    fs::remove_file(local.join("a.txt")).unwrap();
    std::thread::sleep(Duration::from_secs(5));
    fs::write(local.join("a.txt"), "the same bytes").unwrap();
    // Burst wait (3 s) + hold (10 s) + the run, with room to spare. Without the hold the delete runs at about 3 s.
    std::thread::sleep(Duration::from_secs(25));
    assert_eq!(rows("delcloud"), 0, "the cloud copy was deleted");
    assert_eq!(rows("up"), ups, "the same bytes were sent again");
    assert!(remote.join("a.txt").exists());
    assert_eq!(fs::read_to_string(remote.join("a.txt")).unwrap(), "the same bytes");
    runner.stop_and_wait();
}
