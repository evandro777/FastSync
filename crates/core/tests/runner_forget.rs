//! The runner tells its cloud to forget the folders an unfinished listing kept (`Remote::forget_listed`) on Sync now and
//! after a run of some paths, through a real runner with a spy in place of the cloud (rclone's `alias` backend under
//! it; skipped without rclone). Its own test binary: it sets `XDG_DATA_HOME` for the whole process.
//! Not covered: the feed's events (`Remote`, `Reset`), which need a provider with a feed.
use std::{
    fs,
    path::Path,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use fastsync_core::{
    accounts::Account,
    engine::{Remote, Result},
    rclone::{Item, Rclone, RcloneRemote, available},
    runner::spawn_with,
};

/// Every `forget_listed` so far: `None` = everything.
type Calls = Vec<Option<Vec<String>>>;
type Seen = Arc<Mutex<Calls>>;

/// The real remote for everything, and a note of every `forget_listed`.
struct Spy(RcloneRemote, Seen);

impl Remote for Spy {
    fn list(&self) -> Result<Vec<Item>> {
        self.0.list()
    }
    fn stat(&self, rel: &str) -> Result<Option<Item>> {
        self.0.stat(rel)
    }
    fn upload(&self, local_root: &Path, rel: &str) -> Result<()> {
        self.0.upload(local_root, rel)
    }
    fn download(&self, rel: &str, dest: &Path) -> Result<()> {
        self.0.download(rel, dest)
    }
    fn delete(&self, rel: &str) -> Result<()> {
        self.0.delete(rel)
    }
    fn forget_listed(&self, paths: Option<&[String]>) {
        self.1.lock().unwrap().push(paths.map(<[String]>::to_vec));
    }
}

#[test]
fn sync_now_and_a_run_of_paths_make_the_cloud_forget_what_it_kept() {
    if !available() {
        eprintln!("skipped: rclone not found");
        return;
    }
    let base = std::env::temp_dir().join(format!("fastsync-forget-{}", std::process::id()));
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
    let seen: Seen = Arc::default();
    let spy = seen.clone();
    let runner = spawn_with(
        acct,
        rc,
        move |a, rc| Spy(RcloneRemote::new(rc.clone(), format!("{}:", a.id), vec![], false), spy),
        |_| {},
        |_, _| {},
    );
    let wait = |what: &str, found: &dyn Fn(&Calls) -> bool| {
        let end = Instant::now() + Duration::from_secs(60);
        while !found(&seen.lock().unwrap()) {
            assert!(Instant::now() < end, "never saw {what}: {:?}", seen.lock().unwrap());
            std::thread::sleep(Duration::from_millis(200));
        }
    };
    // Sync now: the user asked for the truth, so everything goes.
    runner.sync_now();
    wait("Sync now forgetting everything", &|s| s.iter().any(Option::is_none));
    seen.lock().unwrap().clear();
    // A file saved here is uploaded by a run of that path: the folders around it go, whatever the feed says later.
    fs::write(local.join("new.txt"), "x").unwrap();
    wait("a path run forgetting its path", &|s| {
        s.iter().flatten().any(|p| p.iter().any(|f| f == "new.txt"))
    });
    runner.stop_and_wait();
}
