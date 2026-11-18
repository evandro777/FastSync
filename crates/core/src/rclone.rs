//! rclone as the transport and login layer: `rclone rcd` on localhost, driven over HTTP/JSON. The sync decisions are
//! ours; rclone only logs in, lists, copies and deletes.
use std::{
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    time::{Duration, Instant},
};

use serde_json::{Value, json};

pub type Result<T> = std::result::Result<T, String>;

/// One entry of a remote listing. `mtime` in ms; `id` is the provider's file id (empty when it has none).
#[derive(Clone, Debug, PartialEq)]
pub struct Item {
    pub path: String,
    pub size: u64,
    pub mtime: i64,
    pub is_dir: bool,
    pub id: String,
    /// (hash type, lowercase hex) of the content, when the service lists one (not MEGA).
    pub hash: Option<(String, String)>,
}

/// Hash types in the order they are trusted: what each service reports for its files (Drive md5, OneDrive quickxor,
/// Dropbox its own).
const HASHES: [&str; 5] = ["md5", "sha1", "quickxor", "dropbox", "sha256"];

pub struct Rclone {
    agent: ureq::Agent,
    base: String,
    auth: String,
    child: std::sync::Mutex<Child>,
    /// What `revive` needs to start the same rclone again: same port and password, so every holder of this handle
    /// (one per account runner) keeps working.
    config: PathBuf,
    port: u16,
    pass: String,
    /// Set by `shutdown`: a quit must not bring rclone back.
    closed: std::sync::atomic::AtomicBool,
    /// How many times rclone was started again (lets a failed call tell "it was restarted since" from "still the
    /// same process").
    restarts: std::sync::atomic::AtomicU32,
    /// The rclone job of the browser login in progress (see `call_job`), so a cancel can stop it.
    login_job: std::sync::Mutex<Option<i64>>,
}

/// Starts `rclone rcd` on `127.0.0.1:port`.
fn spawn_rcd(config: &Path, port: u16, pass: &str) -> Result<Child> {
    let mut cmd = Command::new(binary());
    // The password goes through the environment: argv is readable by every local user in /proc/<pid>/cmdline.
    // (If rclone ignored these, `config/*` calls would be refused as unauthenticated, not silently open.)
    cmd.env("RCLONE_RC_USER", "fastsync")
        .env("RCLONE_RC_PASS", pass)
        .args(["rcd", "--rc-addr", &format!("127.0.0.1:{port}")])
        .arg("--config")
        .arg(config)
        .args(["--log-level", if crate::debug::enabled() { "INFO" } else { "ERROR" }])
        // UTC like `debug::stamp`, which copies this shape, so the two logs read as one (rclone's default is local).
        .args(["--log-format", "date,time,UTC"])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(if crate::debug::enabled() {
            Stdio::inherit()
        } else {
            Stdio::null()
        });
    cmd.spawn().map_err(|e| format!("cannot start rclone: {e}"))
}

/// `rclone` from `$FASTSYNC_RCLONE` (a dev override), else from `PATH` (the Flatpak puts it in /app/bin).
fn binary() -> PathBuf {
    std::env::var_os("FASTSYNC_RCLONE").map_or_else(|| "rclone".into(), PathBuf::from)
}

const MIN_RCLONE: (u64, u64) = (1, 76);

/// `operations/about`'s answer as `(used, total)`; no `used` means the service reports nothing useful.
fn quota(v: &Value) -> Option<(u64, Option<u64>)> {
    Some((v["used"].as_u64()?, v["total"].as_u64().filter(|&t| t > 0)))
}

/// `decomposed` is `core/version`'s `[major, minor, patch]`; anything unreadable counts as too old.
fn version_ok(decomposed: &Value) -> bool {
    let n = |i: usize| decomposed[i].as_u64();
    matches!((n(0), n(1)), (Some(a), Some(b)) if (a, b) >= MIN_RCLONE)
}

pub fn available() -> bool {
    Command::new(binary())
        .arg("version")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .is_ok_and(|s| s.success())
}

impl Rclone {
    /// Starts `rclone rcd` with its own config file (never the user's `rclone.conf`) and a random password: any
    /// local process could otherwise use the cloud tokens through the open port.
    pub fn start(config: &Path) -> Result<Self> {
        let port = std::net::TcpListener::bind("127.0.0.1:0")
            .and_then(|l| l.local_addr())
            .map_err(|e| e.to_string())?
            .port();
        let pass = random_hex();
        let child = spawn_rcd(config, port, &pass)?;
        let agent = ureq::Agent::config_builder().http_status_as_error(false).build().into();
        let me = Self {
            agent,
            base: format!("http://127.0.0.1:{port}/"),
            auth: format!("Basic {}", base64(format!("fastsync:{pass}").as_bytes())),
            child: std::sync::Mutex::new(child),
            config: config.to_path_buf(),
            port,
            pass,
            closed: std::sync::atomic::AtomicBool::new(false),
            restarts: std::sync::atomic::AtomicU32::new(0),
            login_job: std::sync::Mutex::new(None),
        };
        // Dropping `me` on failure kills the child.
        let ours = u64::from(me.child.lock().map_err(|e| e.to_string())?.id());
        me.wait_ready(ours)?;
        // Older rclone lacks what login relies on (1.76: the extra "shared client_id" question), so refuse it here
        // instead of failing mid-login.
        let v = me.call("core/version", json!({}))?;
        if !version_ok(&v["decomposed"]) {
            return Err(format!(
                "rclone {} is too old, FastSync needs {}.{} or newer",
                v["version"].as_str().unwrap_or("?"),
                MIN_RCLONE.0,
                MIN_RCLONE.1
            ));
        }
        Ok(me)
    }

    /// Waits until the rclone with process id `ours` answers. The port was free a moment ago, not reserved: whoever
    /// answers must be the child we started, or the login data would go to another program.
    fn wait_ready(&self, ours: u64) -> Result<()> {
        let start = Instant::now();
        loop {
            // A quit waits for the lock this runs under: do not keep it for the whole start-up.
            if self.closed.load(std::sync::atomic::Ordering::Relaxed) {
                return Err("closed".into());
            }
            match self
                .post("core/pid", &json!({}), None)
                .and_then(|mut r| r.body_mut().read_json::<Value>().map_err(|e| e.to_string()))
            {
                Ok(v) if v["pid"].as_u64() == Some(ours) => return Ok(()),
                Ok(_) => return Err("another program answers on rclone's port".into()),
                Err(_) => {}
            }
            if start.elapsed() > Duration::from_secs(10) {
                return Err("rclone did not start".into());
            }
            std::thread::sleep(Duration::from_millis(100));
        }
    }

    /// Starts rclone again when it has exited (killed by the system on a huge listing, or crashed): without it every
    /// account would fail until the app was restarted. `seen` is the `restarts` count from before the failed call.
    /// True when a new rclone is up (started here, or by another thread since `seen`): the call may be repeated.
    /// False when rclone is still the same running process, so the failure was something else.
    fn revive(&self, seen: u32) -> bool {
        use std::sync::atomic::Ordering::Relaxed;
        if self.closed.load(Relaxed) {
            return false;
        }
        let Ok(mut child) = self.child.lock() else { return false };
        // Another thread restarted it while this one waited for the lock (it holds the lock until rclone answers).
        if self.restarts.load(Relaxed) != seen {
            return true;
        }
        if matches!(child.try_wait(), Ok(None)) {
            return false;
        }
        let Ok(new) = spawn_rcd(&self.config, self.port, &self.pass) else {
            return false;
        };
        let ours = u64::from(new.id());
        *child = new;
        self.restarts.fetch_add(1, Relaxed);
        // The old login job died with the old process.
        if let Ok(mut j) = self.login_job.lock() {
            *j = None;
        }
        self.wait_ready(ours).is_ok()
    }

    fn send(&self, method: &str, body: &Value) -> Result<ureq::http::Response<ureq::Body>> {
        self.send_within(method, body, None)
    }

    /// `send` with a time limit for the whole call. Most calls have none (a listing of a big cloud takes minutes);
    /// only the ones a run waits on for no good reason (`speed`) get one.
    fn send_within(
        &self,
        method: &str,
        body: &Value,
        limit: Option<Duration>,
    ) -> Result<ureq::http::Response<ureq::Body>> {
        let seen = self.restarts.load(std::sync::atomic::Ordering::Relaxed);
        // shortcut: after a crash the old port is reused without checking who holds it, so another local process that
        // grabbed it in between would see this one request (`wait_ready` checks the pid only afterwards). Needs a
        // crash plus a hostile local user; upgrade by asking rclone for its own port (`--rc-addr :0`) if that matters.
        let first = self.post(method, body, limit);
        // A failed call with a dead rclone behind it: start it again and ask once more. The caller's own retry
        // logic still sees an error when that fails too.
        if first.is_err() && self.revive(seen) {
            return self.post(method, body, limit);
        }
        first
    }

    fn post(&self, method: &str, body: &Value, limit: Option<Duration>) -> Result<ureq::http::Response<ureq::Body>> {
        self.agent
            .post(&format!("{}{method}", self.base))
            .config()
            .timeout_global(limit)
            .build()
            .header("Authorization", &self.auth)
            // Exactly `application/json`: with ureq's `; charset=utf-8` rclone ignores the body ("Didn't find key").
            .header("Content-Type", "application/json")
            .send(serde_json::to_vec(body).map_err(|e| e.to_string())?)
            .map_err(|e| e.to_string())
    }

    /// The method name tells which call failed; rclone's own message does not.
    fn failure(method: &str, v: &Value) -> String {
        format!("{method}: {}", v["error"].as_str().unwrap_or("rclone error"))
    }

    pub fn call(&self, method: &str, body: Value) -> Result<Value> {
        let mut resp = self.send(method, &body)?;
        let ok = resp.status().is_success();
        let v: Value = resp
            .body_mut()
            .with_config()
            .limit(u64::MAX)
            .read_json()
            .map_err(|e| e.to_string())?;
        if ok { Ok(v) } else { Err(Self::failure(method, &v)) }
    }

    /// Listing of `fs:remote`, the whole tree when `recurse`. Every error is an error, "not found" included: the
    /// cloud's root always exists, so such an answer (seen right after a token change) is a glitch, and reading it as
    /// an empty cloud made every synced file look deleted.
    /// `exclude`: rclone filter rules (`/Folder/**`) for what must not be listed at all.
    pub fn list(&self, fs: &str, remote: &str, exclude: &[String], recurse: bool) -> Result<Vec<Item>> {
        let method = "operations/list";
        let body = json!({"fs": fs, "remote": remote, "opt": {"recurse": recurse, "noMimeType": true, "showHash": true}, "_filter": {"ExcludeRule": exclude}});
        let mut resp = self.send(method, &body)?;
        if !resp.status().is_success() {
            let v: Value = resp.body_mut().read_json().map_err(|e| e.to_string())?;
            return Err(Self::failure(method, &v));
        }
        // Straight into the few fields we keep, read as a stream: a tree of 500k files as a generic JSON value
        // took hundreds of MB. A listing that does not parse is an error, never a shorter list (which would read as
        // "deleted in the cloud").
        let listing: Listing = resp
            .body_mut()
            .with_config()
            .limit(u64::MAX)
            .read_json()
            .map_err(|e| format!("{method}: {e}"))?;
        Ok(listing.list.into_iter().filter_map(Entry::into_item).collect())
    }

    /// The folders directly inside `fs:remote` (one level, for the folder picker): paths from the root of `fs`.
    /// `exclude`: filter rules for folders that cannot be listed (see `exclusions`).
    pub fn list_dirs(&self, fs: &str, remote: &str, exclude: &[String]) -> Result<Vec<String>> {
        let v = self.call(
            "operations/list",
            json!({"fs": fs, "remote": remote, "opt": {"dirsOnly": true, "noMimeType": true}, "_filter": {"ExcludeRule": exclude}}),
        )?;
        let mut dirs: Vec<String> = v["list"].as_array().map_or_else(Vec::new, |a| {
            a.iter()
                .filter_map(parse_item)
                .filter(|i| i.is_dir)
                .map(|i| i.path)
                .collect()
        });
        dirs.sort_by_key(|d| d.to_lowercase());
        Ok(dirs)
    }

    pub fn stat(&self, fs: &str, remote: &str) -> Result<Option<Item>> {
        let v = self.call(
            "operations/stat",
            json!({"fs": fs, "remote": remote, "opt": {"noMimeType": true, "showHash": true}}),
        )?;
        Ok(parse_item(&v["item"]))
    }

    /// The `kind` hash of a local file, computed by rclone (it reads the whole file). `Err` = the call failed (an
    /// unreadable or vanished file, rclone busy): the answer is unknown, not "different".
    pub fn local_hash(&self, dir: &str, rel: &str, kind: &str) -> Result<Option<String>> {
        let v = self.call(
            "operations/stat",
            json!({"fs": dir, "remote": rel, "opt": {"noMimeType": true, "showHash": true, "hashTypes": [kind]}}),
        )?;
        Ok(v["item"]["Hashes"][kind]
            .as_str()
            .filter(|h| !h.is_empty())
            .map(str::to_lowercase))
    }

    /// `group` (the account id) keeps `speed` per account: `core/stats` otherwise adds every account together.
    pub fn copyfile(&self, group: &str, src_fs: &str, src: &str, dst_fs: &str, dst: &str) -> Result<()> {
        self.call(
            "operations/copyfile",
            json!({"srcFs": src_fs, "srcRemote": src, "dstFs": dst_fs, "dstRemote": dst, "_group": group}),
        )
        .map(drop)
    }

    /// `(used, total)` bytes of the cloud account behind `fs` (`operations/about`); `total` is None when the plan has
    /// no limit or the service does not say.
    pub fn about(&self, fs: &str) -> Result<(u64, Option<u64>)> {
        let v = self.call("operations/about", json!({"fs": fs}))?;
        quota(&v).ok_or_else(|| "operations/about: no usage reported".to_owned())
    }

    /// Server-side move inside one remote (`operations/movefile`): no bytes cross the network.
    pub fn movefile(&self, fs: &str, src: &str, dst: &str) -> Result<()> {
        self.call(
            "operations/movefile",
            json!({"srcFs": fs, "srcRemote": src, "dstFs": fs, "dstRemote": dst}),
        )
        .map(drop)
    }

    /// Renames the folder `src` to `dst` inside one remote (`sync/move` of one folder onto another): rclone uses the
    /// service's own folder move when it has one, else moves the files server-side one by one.
    pub fn movedir(&self, fs: &str, src: &str, dst: &str) -> Result<()> {
        // `fs` is `<id>:` for a login, a plain path for rclone's local backend (the tests).
        let at = |p: &str| {
            if fs.ends_with([':', '/']) {
                format!("{fs}{p}")
            } else {
                format!("{fs}/{p}")
            }
        };
        self.call(
            "sync/move",
            json!({"srcFs": at(src), "dstFs": at(dst), "deleteEmptySrcDirs": true}),
        )
        .map(drop)
    }

    /// Bytes per second of the transfers running in `group` right now (None when rclone does not say).
    /// Limited to 5 s: the runner's ticker asks every second inside the run's thread scope, so a rclone that stopped
    /// answering would otherwise keep a finished run from ever returning (the card stuck on "Syncing").
    pub fn speed(&self, group: &str) -> Option<f64> {
        let mut resp = self
            .send_within("core/stats", &json!({"group": group}), Some(Duration::from_secs(5)))
            .ok()?;
        let v: Value = resp.body_mut().read_json().ok()?;
        v.get("speed")?.as_f64()
    }

    /// Creates the folder `fs:remote` with its parents; fine when it exists already.
    pub fn mkdir(&self, fs: &str, remote: &str) -> Result<()> {
        self.call("operations/mkdir", json!({"fs": fs, "remote": remote}))
            .map(drop)
    }

    /// Removes the folder `fs:remote` only when it is empty (`operations/rmdir`, never a purge).
    pub fn rmdir(&self, fs: &str, remote: &str) -> Result<()> {
        self.call("operations/rmdir", json!({"fs": fs, "remote": remote}))
            .map(drop)
    }

    pub fn deletefile(&self, fs: &str, remote: &str) -> Result<()> {
        self.call("operations/deletefile", json!({"fs": fs, "remote": remote}))
            .map(drop)
    }

    /// A public share link for `fs:remote` (a file; folders work on most providers too).
    pub fn public_link(&self, fs: &str, remote: &str) -> Result<String> {
        let v = self.call("operations/publiclink", json!({"fs": fs, "remote": remote}))?;
        v["url"]
            .as_str()
            .filter(|u| !u.is_empty())
            .map(str::to_owned)
            .ok_or_else(|| "operations/publiclink: no link returned".to_owned())
    }

    /// A call that can block for minutes (the login waits for the browser), run as an rclone job so `cancel_login`
    /// can stop it. A login nobody finished otherwise keeps rclone's auth server on port 53682 until rclone's own
    /// timeout, and the next login fails with "address already in use". Errors read like `call`'s.
    fn call_job(&self, method: &str, mut body: Value) -> Result<Value> {
        body["_async"] = json!(true);
        let id = self.call(method, body)?["jobid"]
            .as_i64()
            .ok_or("rclone gave no job id")?;
        *self.login_job.lock().map_err(|e| e.to_string())? = Some(id);
        let done = loop {
            match self.call("job/status", json!({"jobid": id})) {
                Ok(s) if s["finished"].as_bool() == Some(true) => break Ok(s),
                Ok(_) => std::thread::sleep(Duration::from_millis(200)),
                Err(e) => break Err(e),
            }
        };
        if let Ok(mut j) = self.login_job.lock()
            && *j == Some(id)
        {
            *j = None;
        }
        let s = done?;
        if s["success"].as_bool() == Some(true) {
            Ok(s["output"].clone())
        } else {
            Err(format!("{method}: {}", s["error"].as_str().unwrap_or("rclone error")))
        }
    }

    /// Stops the login that is waiting for the browser (if any), freeing the auth server's port. Its `login` call
    /// returns an error at once.
    pub fn cancel_login(&self) {
        let id = self.login_job.lock().ok().and_then(|mut j| j.take());
        if let Some(id) = id {
            let _ = self.call("job/stop", json!({"jobid": id}));
        }
    }

    pub fn config_get(&self, name: &str) -> Result<Value> {
        self.call("config/get", json!({"name": name}))
    }

    pub fn config_delete(&self, name: &str) {
        let _ = self.call("config/delete", json!({"name": name}));
    }

    /// Browser login. rclone opens the browser itself and the call blocks until the user finishes, so run it off
    /// the UI thread. Every follow-up question (a Drive "shared drive?" prompt and the like) takes its default.
    /// `client` is the user's own OAuth `(client_id, client_secret)`: a private quota instead of rclone's shared one.
    pub fn login(
        &self,
        name: &str,
        kind: &str,
        user_pass: Option<(&str, &str)>,
        client: Option<(&str, &str)>,
    ) -> Result<()> {
        // MEGA (and anything else without OAuth): the account's own credentials, stored obscured by rclone.
        if let Some((user, pass)) = user_pass {
            let body = json!({"name": name, "type": kind, "parameters": {"user": user, "pass": pass}, "opt": {"obscure": true}});
            return self.call("config/create", body).map(drop);
        }
        // A login abandoned earlier would still hold the auth server's port.
        self.cancel_login();
        // Sent with every call, as rclone asks for config defaults.
        let params = login_params(kind, client);
        let first = self.call_job(
            "config/create",
            json!({"name": name, "type": kind, "parameters": params, "opt": {"nonInteractive": true}}),
        )?;
        self.finish_config(name, &params, first)
    }

    /// Answers rclone's config questions (see `answer`) until it is done. `v` is the reply to the first call.
    fn finish_config(&self, name: &str, params: &Value, mut v: Value) -> Result<()> {
        for _ in 0..20 {
            if let Some(e) = v["Error"].as_str().filter(|e| !e.is_empty()) {
                return Err(e.to_string());
            }
            let state = v["State"].as_str().unwrap_or("").to_string();
            if state.is_empty() {
                return Ok(());
            }
            let result = answer(&v["Option"]);
            v = self.call_job("config/update", json!({"name": name, "parameters": params, "opt": {"nonInteractive": true, "continue": true, "state": state, "result": result}}))?;
        }
        Err("login did not finish".into())
    }

    /// Writes `params` as the whole remote `id`. `create` over an existing name replaces it; its settings already
    /// include the token, so rclone asks whether to replace it (`answer` says no); `noObscure`: MEGA's password comes
    /// back obscured.
    fn replace_remote(&self, id: &str, kind: &str, params: &Value) -> Result<()> {
        let first = self.call(
            "config/create",
            json!({"name": id, "type": kind, "parameters": params, "opt": {"nonInteractive": true, "noObscure": true}}),
        )?;
        self.finish_config(id, params, first)?;
        // rclone keeps the filesystems it opened, with their old settings and token: clear them. A failure here only
        // means the old ones live until rclone drops them.
        let _ = self.call("fscache/clear", json!({}));
        Ok(())
    }

    /// Drive logins made before FastSync skipped shortcuts still follow them, which shows a shortcut's folder as a
    /// second real copy (downloaded again; a delete inside it would delete the original). Adds the setting to such
    /// a login, keeping everything else. Local to rclone's config, no network. Not Drive, or already set: nothing.
    pub fn skip_shortcuts(&self, id: &str) -> Result<()> {
        let cfg = self.config_get(id)?;
        if cfg["type"] != "drive" || cfg["skip_shortcuts"].as_str() == Some("true") {
            return Ok(());
        }
        let old = copy_params(&cfg);
        let mut new = old.clone();
        new["skip_shortcuts"] = "true".into();
        self.replace_remote(id, "drive", &new).inspect_err(|_| {
            let _ = self.replace_remote(id, "drive", &old);
        })
    }

    /// Replaces the login of the remote `id` (same name, so its DB and runner stay valid) with a new one, never
    /// leaving it half-made: the new login is made under a temporary name and checked by listing the root; only then
    /// are its settings copied over the real remote, as a whole (an old client ID would not fit the new token). Any
    /// failure leaves the old login untouched.
    pub fn reauth(
        &self,
        id: &str,
        kind: &str,
        user_pass: Option<(&str, &str)>,
        client: Option<(&str, &str)>,
        // Asked once the new login is checked, just before it replaces the old one: false (the user cancelled while
        // the browser was open) drops it.
        proceed: &dyn Fn() -> bool,
    ) -> Result<()> {
        let tmp = format!("{id}x");
        let result = self.swap_login(id, &tmp, kind, user_pass, client, proceed);
        self.config_delete(&tmp);
        result
    }

    fn swap_login(
        &self,
        id: &str,
        tmp: &str,
        kind: &str,
        user_pass: Option<(&str, &str)>,
        client: Option<(&str, &str)>,
        proceed: &dyn Fn() -> bool,
    ) -> Result<()> {
        // A leftover of a crashed run would make rclone ask whether to replace its token.
        self.config_delete(tmp);
        self.login(tmp, kind, user_pass, client)?;
        self.list_dirs(&format!("{tmp}:"), "", &exclusions(kind))?;
        if kind == "mega" {
            mega_login_works(&self.config_get(tmp)?)?;
        }
        if !proceed() {
            return Err("cancelled".into());
        }
        let new = copy_params(&self.config_get(tmp)?);
        let old = copy_params(&self.config_get(id)?);
        let apply = |params: &Value| self.replace_remote(id, kind, params);
        // The copy is checked on the real remote too: a setting that did not survive the round trip (MEGA's
        // obscured password) would otherwise replace a working login with a broken one.
        let checked = apply(&new).and_then(|()| self.list_dirs(&format!("{id}:"), "", &exclusions(kind)).map(drop));
        if let Err(e) = checked {
            let _ = apply(&old);
            return Err(e);
        }
        Ok(())
    }

    /// The OAuth access token and its expiry (epoch seconds, 0 when unknown). A cheap call first makes rclone
    /// refresh a token that is about to expire and save it, so delta clients always get a live one.
    pub fn token(&self, remote: &str) -> Result<(String, i64)> {
        let read = || -> Result<(String, i64)> {
            let cfg = self.config_get(remote)?;
            let t: Value = serde_json::from_str(cfg["token"].as_str().unwrap_or("{}")).map_err(|e| e.to_string())?;
            let exp = t["expiry"].as_str().and_then(parse_time).map_or(0, |ms| ms / 1000);
            Ok((t["access_token"].as_str().unwrap_or("").to_string(), exp))
        };
        let (tok, exp) = read()?;
        if exp != 0 && exp - now_secs() < 120 {
            let _ = self.call("operations/about", json!({"fs": format!("{remote}:")}));
            return read();
        }
        Ok((tok, exp))
    }
}

/// The synced cloud folder as the engine sees it. `fs` is `name:folder/inside/the/cloud`.
pub struct RcloneRemote {
    pub rc: std::sync::Arc<Rclone>,
    pub fs: String,
    /// Filter rules for folders that cannot be listed (see `exclusions`).
    pub exclude: Vec<String>,
    /// A Dropbox account: its symlinks (empty files whose target lives in the metadata) can be read, see `link_target`.
    pub dropbox: bool,
    /// Folders a failed or cut-short listing already got, kept for the next try (see `Listed`).
    listed: std::sync::Mutex<Listed>,
    /// How old a kept folder may be. The feed drops the folders it reports, so an hour is safe with one; a provider
    /// without a feed (MEGA) has nothing to say what changed, so it gets a short one.
    pub max_age: std::time::Duration,
}

/// The folders of an unfinished cloud listing, so a failed or paused scan resumes instead of starting over (a big
/// Dropbox takes ~10 min, and one folder failing at the end used to throw all of it away). Only a folder listed *whole*
/// is kept, never a partial answer. It is cleared when a listing completes, by `forget` for every change the cloud's
/// feed reports (`Remote::forget_listed`), and by age.
#[derive(Default)]
struct Listed(std::collections::HashMap<String, (bool, std::time::Instant, Vec<Item>)>);

impl Listed {
    fn get(&self, dir: &str, recursive: bool, max_age: std::time::Duration) -> Option<Vec<Item>> {
        let (r, at, items) = self.0.get(dir)?;
        (*r == recursive && at.elapsed() < max_age).then(|| items.clone())
    }
    fn put(&mut self, dir: &str, recursive: bool, items: &[Item]) {
        self.0
            .insert(dir.to_string(), (recursive, std::time::Instant::now(), items.to_vec()));
    }
    /// Drops every kept folder that holds, or is held by, one of `paths` (`None` = all): a change at `a/b/x` is in
    /// the listing of `a/b`, of `a` and of the root, and a new folder `a/n` also in whatever was listed under it.
    fn forget(&mut self, paths: Option<&[String]>) {
        let Some(paths) = paths else {
            return self.0.clear();
        };
        // Case-blind: Dropbox's feed may spell a folder differently (`/docs/New` beside `/Docs/Work`) than the listing.
        let near = |k: &str, p: &str| {
            k.is_empty() || p.is_empty() || k == p || p.starts_with(&format!("{k}/")) || k.starts_with(&format!("{p}/"))
        };
        // Lowercased once: a run of thousands of paths against hundreds of kept folders.
        let paths: Vec<String> = paths.iter().map(|p| p.to_lowercase()).collect();
        self.0.retain(|k, _| {
            let k = k.to_lowercase();
            !paths.iter().any(|p| near(&k, p))
        });
    }
}

/// Folders a service keeps in the tree but refuses to list. OneDrive Personal's locked "Personal Vault" fails with
/// `invalidResourceId: ObjectHandle is Invalid`, and that one error aborts the whole recursive listing. The vault is
/// locked storage meant to stay off the disk, so it is simply not synced.
pub fn exclusions(kind: &str) -> Vec<String> {
    if kind == "onedrive" {
        vec!["/Personal Vault/**".into()]
    } else {
        Vec::new()
    }
}

/// `exclusions` plus the unchecked folders, so a big excluded tree is not even listed. A name with a glob character
/// is left out: the rule would also hide a sibling (`a[1]` matches `a1`), which looks like a remote delete. The engine
/// skips unchecked folders by itself, so those are only listed, not synced.
pub fn exclude_rules(kind: &str, unchecked: &[String]) -> Vec<String> {
    let mut rules = exclusions(kind);
    rules.extend(
        unchecked
            .iter()
            .filter(|p| !p.contains(['*', '?', '[', ']', '{', '}', '\\']))
            .map(|p| format!("/{p}/**")),
    );
    rules
}

impl RcloneRemote {
    pub fn new(rc: std::sync::Arc<Rclone>, fs: String, exclude: Vec<String>, dropbox: bool) -> Self {
        Self {
            rc,
            fs,
            exclude,
            dropbox,
            listed: Default::default(),
            max_age: std::time::Duration::from_secs(3600),
        }
    }
    /// The remote's name (`id:` before the colon) is the account id: the stats group of its transfers.
    pub fn group(&self) -> &str {
        self.fs.split(':').next().unwrap_or("")
    }
}

/// A folder listing that took at least this long is kept for the next try (see `Listed`).
const KEEP_IF_SLOWER: std::time::Duration = std::time::Duration::from_secs(2);

/// An error worth asking again: the service or the network hiccuped. A login that expired, a folder that is gone or
/// anything unknown fails at once, so a real problem is not shown minutes late.
fn transient(e: &str) -> bool {
    let e = e.to_lowercase();
    // A status code only as a word of its own: "2500 pics" in a folder name is not a 500.
    e.split(|c: char| !c.is_alphanumeric())
        .any(|w| matches!(w, "500" | "502" | "503" | "504"))
        || [
            "unexpected error",
            "too many requests",
            "timeout",
            "timed out",
            "temporar",
            "connection reset",
            "connection refused",
        ]
        .iter()
        .any(|t| e.contains(t))
}

/// Runs `f`, and after a transient failure again up to 3 times, waiting `base`, 2x, 3x seconds (cut short by
/// `cancelled`). Retrying one folder is cheap; failing the whole listing threw away minutes of work.
fn retry_transient<T>(cancelled: &dyn Fn() -> bool, base: u64, f: impl Fn() -> Result<T>) -> Result<T> {
    for n in 1..=3 {
        match f() {
            Err(e) if transient(&e) && !cancelled() => {
                for _ in 0..base * n {
                    if cancelled() {
                        break;
                    }
                    std::thread::sleep(std::time::Duration::from_secs(1));
                }
            }
            r => return r,
        }
    }
    f()
}

/// Folders above this depth (the root is 0) are listed one level at a time by `walk_cloud`.
const SPLIT_DEPTH: usize = 3;

/// The whole cloud through `list_one(folder, recursive)` as a queue of folders worked off `PARALLEL` at a time: the
/// top `SPLIT_DEPTH` levels one level per call (their subfolders join the queue, so the total grows as they are
/// found), deeper folders as one recursive call each. One recursive call over a 100k-file Dropbox answers only at the
/// very end (minutes, nothing to show), and splitting only the top level left the two biggest folders as two long
/// calls stuck at "24 of 26". `on_progress(done, found)` after every folder; `cancelled()` is asked before each one
/// and ends the walk with `Err("cancelled")`.
///
/// Any failed folder fails the whole listing, as one call did: a shorter list would read as "deleted in the cloud".
/// `Ok(None)` = a folder holds two subfolders of one name (Drive): a path reaches only one of them, so the caller
/// must list the old single way.
fn walk_cloud(
    list_one: &(dyn Fn(&str, bool) -> Result<Vec<Item>> + Sync),
    on_progress: &(dyn Fn(usize, usize) + Sync),
    cancelled: &(dyn Fn() -> bool + Sync),
) -> Result<Option<Vec<Item>>> {
    use std::{collections::HashSet, sync::Condvar};
    #[derive(Default)]
    struct Walk {
        queue: std::collections::VecDeque<(String, usize)>,
        /// Folders being listed right now: they may still add to the queue.
        active: usize,
        found: usize,
        done: usize,
        items: Vec<Item>,
        error: Option<String>,
        clash: bool,
    }
    let walk = std::sync::Mutex::new(Walk {
        queue: [(String::new(), 0)].into(),
        found: 1,
        ..Default::default()
    });
    let wake = Condvar::new();
    on_progress(0, 1);
    std::thread::scope(|s| {
        for _ in 0..crate::engine::PARALLEL {
            s.spawn(|| {
                loop {
                    let Ok(mut w) = walk.lock() else { return };
                    // Between folders (a call under way finishes): an error, never `Ok` with what was found so far.
                    if w.error.is_none() && cancelled() {
                        w.error = Some("cancelled".into());
                    }
                    let (dir, depth) = loop {
                        if w.error.is_some() || w.clash || (w.queue.is_empty() && w.active == 0) {
                            wake.notify_all();
                            return;
                        }
                        if let Some(job) = w.queue.pop_front() {
                            w.active += 1;
                            break job;
                        }
                        w = match wake.wait(w) {
                            Ok(w) => w,
                            Err(_) => return,
                        };
                    };
                    drop(w);
                    let split = depth < SPLIT_DEPTH;
                    let listed = list_one(&dir, !split);
                    let Ok(mut w) = walk.lock() else { return };
                    w.active -= 1;
                    match listed {
                        Ok(v) => {
                            if split {
                                let subs: Vec<&str> = v.iter().filter(|i| i.is_dir).map(|i| i.path.as_str()).collect();
                                if subs.iter().collect::<HashSet<_>>().len() != subs.len() {
                                    w.clash = true;
                                }
                                w.found += subs.len();
                                w.queue.extend(subs.into_iter().map(|p| (p.to_string(), depth + 1)));
                            }
                            w.items.extend(v);
                            w.done += 1;
                            let (done, found) = (w.done, w.found);
                            drop(w);
                            on_progress(done, found);
                        }
                        Err(e) => {
                            let what = if dir.is_empty() { e } else { format!("{dir}: {e}") };
                            w.error.get_or_insert(what);
                        }
                    }
                    wake.notify_all();
                }
            });
        }
    });
    let w = walk.into_inner().map_err(|e| e.to_string())?;
    match w.error {
        Some(e) => Err(e),
        None if w.clash => Ok(None),
        None => Ok(Some(w.items)),
    }
}

impl crate::engine::Remote for RcloneRemote {
    fn list(&self) -> Result<Vec<Item>> {
        self.rc.list(&self.fs, "", &self.exclude, true)
    }
    /// Only Dropbox: `fs` is `<account id>:` there (the root), the id names the login whose token is used.
    fn link_target(&self, rel: &str) -> Result<Option<String>> {
        if !self.dropbox {
            return Ok(None);
        }
        let (rc, name) = (self.rc.clone(), self.fs.trim_end_matches(':').to_string());
        crate::delta::dropbox_link_target(Box::new(move || rc.token(&name).map(|t| t.0)), rel)
    }
    /// The cloud as a queue of folders worked off a few at a time: the top `SPLIT_DEPTH` levels one level per call
    /// (their subfolders join the queue, so the total grows as they are found), deeper folders as one recursive call
    /// each. One recursive call over a 100k-file Dropbox answers only at the very end (minutes, nothing to show), and
    /// splitting only the top level left the two biggest folders as two long calls at "24 of 26". Any failed part
    /// fails the whole listing, as before: a shorter list would read as "deleted in the cloud".
    fn list_progress(
        &self,
        on_progress: &(dyn Fn(usize, usize) + Sync),
        cancelled: &(dyn Fn() -> bool + Sync),
    ) -> Result<Vec<Item>> {
        // A folder that fails (Dropbox's "unexpected error occurred" after a rate-limit wait) is listed again by
        // itself: failing the whole walk threw away minutes of listing and the retry hit the same limit, in a loop.
        let one = |dir: &str, recursive| {
            if let Some(kept) = self
                .listed
                .lock()
                .ok()
                .and_then(|l| l.get(dir, recursive, self.max_age))
            {
                return Ok(kept);
            }
            let began = std::time::Instant::now();
            let items = retry_transient(cancelled, 20, || self.rc.list(&self.fs, dir, &self.exclude, recursive))?;
            // Only a slow folder is worth keeping: a fast one is cheap to list again, and keeping every folder would
            // hold a second copy of the whole cloud's listing in memory during the scan.
            if began.elapsed() >= KEEP_IF_SLOWER
                && let Ok(mut l) = self.listed.lock()
            {
                l.put(dir, recursive, &items);
            }
            Ok(items)
        };
        // Kept only while the listing is unfinished: a failure or a cancel leaves it for the next try.
        let walked = walk_cloud(&one, on_progress, cancelled);
        if walked.is_ok() {
            self.forget_listed(None);
        }
        match walked? {
            Some(items) => Ok(items),
            // The old single walk, which rclone resolves folder by folder itself.
            None => retry_transient(cancelled, 20, || self.list()),
        }
    }
    fn forget_listed(&self, paths: Option<&[String]>) {
        if let Ok(mut l) = self.listed.lock() {
            l.forget(paths);
        }
    }
    fn list_dir(&self, rel: &str) -> Result<Vec<Item>> {
        self.rc.list(&self.fs, rel, &self.exclude, false)
    }
    fn stat(&self, rel: &str) -> Result<Option<Item>> {
        self.rc.stat(&self.fs, rel)
    }
    fn upload(&self, local_root: &Path, rel: &str) -> Result<()> {
        let copy = || {
            self.rc
                .copyfile(self.group(), &local_root.to_string_lossy(), rel, &self.fs, rel)
        };
        match (copy(), rel.rsplit_once('/')) {
            // OneDrive's copyfile into a folder it does not have yet fails (Drive and Dropbox create it): make the
            // folder, then once more. Only on this error, so the usual upload costs no extra request.
            (Err(e), Some((dir, _))) if e.contains("directory not found") => {
                self.rc.mkdir(&self.fs, dir)?;
                copy()
            }
            (r, _) => r,
        }
    }
    fn download(&self, rel: &str, dest: &Path) -> Result<()> {
        let (dir, name) = (dest.parent().ok_or("bad path")?, dest.file_name().ok_or("bad path")?);
        self.rc.copyfile(
            self.group(),
            &self.fs,
            rel,
            &dir.to_string_lossy(),
            &name.to_string_lossy(),
        )
    }
    fn delete(&self, rel: &str) -> Result<()> {
        self.rc.deletefile(&self.fs, rel)
    }
    fn remove_empty_dir(&self, rel: &str) -> Result<()> {
        self.rc.rmdir(&self.fs, rel)
    }
    fn make_dir(&self, rel: &str) -> Result<()> {
        self.rc.mkdir(&self.fs, rel)
    }
    fn forget_cache(&self) {
        // One cache for every account's remote in the shared rcd: the others just rebuild theirs on next use.
        let _ = self.rc.call("fscache/clear", json!({}));
    }
    fn rename(&self, from: &str, to: &str) -> Result<()> {
        // OneDrive does not create the target folder (see `upload`); a failure falls back to upload + delete anyway.
        match (self.rc.movefile(&self.fs, from, to), to.rsplit_once('/')) {
            (Err(e), Some((dir, _))) if e.contains("directory not found") => {
                self.rc.mkdir(&self.fs, dir)?;
                self.rc.movefile(&self.fs, from, to)
            }
            (r, _) => r,
        }
    }
    fn list_under(&self, dir: &str) -> Result<Vec<Item>> {
        self.rc.list(&self.fs, dir, &[], true)
    }
    fn move_dir(&self, from: &str, to: &str) -> Result<()> {
        self.rc.movedir(&self.fs, from, to)
    }
    fn copy(&self, from: &str, to: &str) -> Result<()> {
        // Inside one remote rclone asks the service to copy (Dropbox, Drive, OneDrive): no bytes cross the network.
        // OneDrive does not create the target folder (see `upload`).
        let go = || self.rc.copyfile(self.group(), &self.fs, from, &self.fs, to);
        match (go(), to.rsplit_once('/')) {
            (Err(e), Some((dir, _))) if e.contains("directory not found") => {
                self.rc.mkdir(&self.fs, dir)?;
                go()
            }
            (r, _) => r,
        }
    }
    fn local_hash(&self, local_root: &Path, rel: &str, kind: &str) -> Result<Option<String>> {
        self.rc.local_hash(&local_root.to_string_lossy(), rel, kind)
    }
    fn same_content(&self, local_root: &Path, rel: &str, remote: &Item) -> bool {
        self.compare(local_root, rel, remote).unwrap_or(false)
    }
    fn compare(&self, local_root: &Path, rel: &str, remote: &Item) -> Result<bool> {
        // A service with no hash (MEGA) is a known "cannot tell": false, not an error.
        let Some((kind, want)) = &remote.hash else {
            return Ok(false);
        };
        Ok(self
            .rc
            .local_hash(&local_root.to_string_lossy(), rel, kind)?
            .is_some_and(|got| &got == want))
    }
}

impl Drop for Rclone {
    fn drop(&mut self) {
        self.shutdown();
    }
}

impl Rclone {
    /// Stops rclone. Called on quit too: statics are never dropped, so an exit would leave it running.
    pub fn shutdown(&self) {
        self.closed.store(true, std::sync::atomic::Ordering::Relaxed);
        if let Ok(mut c) = self.child.lock() {
            let _ = c.kill();
            let _ = c.wait();
        }
    }
}

/// MEGA's login, tried for real. rclone keeps a logged-in MEGA session per email, and any remote with that email
/// reuses it without looking at its password, so a listing through the running rclone passes with a wrong or an empty
/// password. A fresh process has no session. The remote is made from environment variables, not the command line,
/// which every local user can read in /proc.
fn mega_login_works(cfg: &Value) -> Result<()> {
    let out = Command::new(binary())
        .args([
            "lsf",
            "--dirs-only",
            "--max-depth",
            "1",
            "--retries",
            "1",
            "--low-level-retries",
            "1",
        ])
        .args(["--contimeout", "20s", "--timeout", "30s", "probe:"])
        .envs(mega_probe_env(cfg))
        .stdin(Stdio::null())
        .output()
        .map_err(|e| format!("cannot start rclone: {e}"))?;
    if out.status.success() {
        return Ok(());
    }
    let err = String::from_utf8_lossy(&out.stderr);
    Err(err
        .lines()
        .rev()
        .find(|l| !l.trim().is_empty())
        .unwrap_or("MEGA login failed")
        .trim()
        .to_string())
}

/// The remote `probe:` of `mega_login_works`, from a `config/get` (its password is already obscured, as rclone wants).
fn mega_probe_env(cfg: &Value) -> [(&'static str, String); 4] {
    let get = |k: &str| cfg[k].as_str().unwrap_or("").to_string();
    [
        ("RCLONE_CONFIG", "/dev/null".into()),
        ("RCLONE_CONFIG_PROBE_TYPE", "mega".into()),
        ("RCLONE_CONFIG_PROBE_USER", get("user")),
        ("RCLONE_CONFIG_PROBE_PASS", get("pass")),
    ]
}

/// The settings of a remote (`config/get`) minus its `type`, as the parameters for creating another one.
fn copy_params(cfg: &Value) -> Value {
    let mut p = serde_json::Map::new();
    for (k, v) in cfg.as_object().into_iter().flatten() {
        if k != "type"
            && let Some(s) = v.as_str()
        {
            p.insert(k.clone(), s.into());
        }
    }
    Value::Object(p)
}

/// Our answer to one of rclone's login questions. Yes to "use a web browser on this machine" (we are a desktop
/// app) and to newer rclone's "continue using the shared client_id anyway?" (default No: asked only when the user
/// gave no client ID of their own, and then the shared one is what they chose). Everything else: the default.
fn answer(opt: &Value) -> String {
    // "Token already configured - replace it?": no. It only comes up when the settings being created carry a token
    // (a re-authentication copying a checked one), and yes would open the browser again.
    if opt["Name"] == "config_refresh_token" {
        return "false".into();
    }
    let help = opt["Help"].as_str().unwrap_or("").to_lowercase();
    if opt["Name"] == "config_is_local" || (opt["Type"] == "bool" && help.contains("shared client")) {
        return "true".into();
    }
    match &opt["Default"] {
        Value::String(s) => s.clone(),
        Value::Null => String::new(),
        other => other.to_string(),
    }
}

/// Whether `id` is the project's own built-in client ID for `kind` (the saved login holds it too, so it must not
/// show up as "the user's own client" when editing).
pub fn is_embedded_client(kind: &str, id: &str) -> bool {
    embedded_client(kind).is_some_and(|(i, _)| i == id)
}

/// The project's own OAuth client, baked in at build time from CI secrets (never in the repo). It is only a default:
/// the user's own ID wins, and without these variables (local builds, forks) rclone's shared ID is used. The secret
/// is readable in the binary, as for any desktop app.
fn embedded_client(kind: &str) -> Option<(&'static str, &'static str)> {
    let (id, secret) = match kind {
        "drive" => (
            option_env!("FASTSYNC_GOOGLE_CLIENT_ID"),
            option_env!("FASTSYNC_GOOGLE_CLIENT_SECRET"),
        ),
        "dropbox" => (
            option_env!("FASTSYNC_DROPBOX_CLIENT_ID"),
            option_env!("FASTSYNC_DROPBOX_CLIENT_SECRET"),
        ),
        "onedrive" => (
            option_env!("FASTSYNC_ONEDRIVE_CLIENT_ID"),
            option_env!("FASTSYNC_ONEDRIVE_CLIENT_SECRET"),
        ),
        _ => return None,
    };
    id.zip(secret).filter(|(i, s)| !i.is_empty() && !s.is_empty())
}

/// The config parameters of an OAuth login. Google Docs and Sheets have no file content: without `skip_gdocs`
/// rclone lists them as .docx/.xlsx exports, and a later local edit would upload a duplicate. A shortcut is a link,
/// not content: without `skip_shortcuts` rclone shows its target under the shortcut's path, so the files would sync
/// twice and a delete in one place would delete the original (`Rclone::skip_shortcuts` fixes older logins).
pub fn login_params(kind: &str, client: Option<(&str, &str)>) -> Value {
    let mut p = serde_json::Map::new();
    if kind == "drive" {
        p.insert("skip_gdocs".into(), "true".into());
        p.insert("skip_shortcuts".into(), "true".into());
    }
    if let Some((id, secret)) = client.or_else(|| embedded_client(kind)) {
        p.insert("client_id".into(), id.into());
        p.insert("client_secret".into(), secret.into());
    }
    Value::Object(p)
}

pub fn now_secs() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_secs() as i64)
}

/// The reply of `operations/list`.
#[derive(serde::Deserialize)]
struct Listing {
    // No default: rclone always sends `"list": []` for an empty folder, so a reply without the key is a glitch, not
    // an empty cloud.
    list: Vec<Entry>,
}

/// One entry of an rclone listing: only the fields `Item` keeps.
#[derive(serde::Deserialize)]
#[serde(rename_all = "PascalCase")]
struct Entry {
    path: Option<String>,
    size: Option<i64>,
    mod_time: Option<String>,
    is_dir: Option<bool>,
    #[serde(rename = "ID")]
    id: Option<String>,
    hashes: Option<std::collections::HashMap<String, Option<String>>>,
}

impl Entry {
    /// None for an entry without a path.
    fn into_item(self) -> Option<Item> {
        let hash = self.hashes.and_then(|m| {
            HASHES.iter().find_map(|k| {
                let h = m.get(*k)?.as_deref().filter(|h| !h.is_empty())?;
                Some((k.to_string(), h.to_lowercase()))
            })
        });
        Some(Item {
            path: self.path?,
            size: self.size.unwrap_or(0).max(0) as u64,
            mtime: self.mod_time.as_deref().and_then(parse_time).unwrap_or(0),
            is_dir: self.is_dir.unwrap_or(false),
            id: self.id.unwrap_or_default(),
            hash,
        })
    }
}

fn parse_item(v: &Value) -> Option<Item> {
    serde_json::from_value::<Entry>(v.clone()).ok()?.into_item()
}

/// RFC 3339 (`2026-11-11T15:10:08.123456789Z`, `...+02:00`) to epoch milliseconds.
pub fn parse_time(s: &str) -> Option<i64> {
    let n = |a: usize, b: usize| s.get(a..b)?.parse::<i64>().ok();
    let (y, mo, d, h, mi, sec) = (n(0, 4)?, n(5, 7)?, n(8, 10)?, n(11, 13)?, n(14, 16)?, n(17, 19)?);
    // Days since 1970-01-01 (Howard Hinnant's civil-from-days, inverted).
    let y = if mo <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let doy = (153 * (if mo > 2 { mo - 3 } else { mo + 9 }) + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let days = era * 146_097 + doe - 719_468;
    let rest = &s[19..];
    let frac_len = rest
        .strip_prefix('.')
        .map_or(0, |f| f.bytes().take_while(u8::is_ascii_digit).count());
    let ms = if frac_len > 0 {
        format!("{:0<3}", &rest[1..1 + frac_len.min(3)]).parse().ok()?
    } else {
        0
    };
    let tz = &rest[if frac_len > 0 { 1 + frac_len } else { 0 }..];
    let offset = match tz.as_bytes().first() {
        Some(b'+') | Some(b'-') => {
            let sign = if tz.starts_with('-') { -1 } else { 1 };
            sign * (tz.get(1..3)?.parse::<i64>().ok()? * 3600 + tz.get(4..6)?.parse::<i64>().ok()? * 60)
        }
        _ => 0,
    };
    Some(((days * 86_400 + h * 3600 + mi * 60 + sec - offset) * 1000) + ms)
}

pub(crate) fn random_hex() -> String {
    use std::io::Read;
    let mut b = [0u8; 16];
    // /dev/urandom always exists on Linux; a failure would leave a zero password, so stop instead.
    std::fs::File::open("/dev/urandom")
        .and_then(|mut f| f.read_exact(&mut b))
        .expect("cannot read /dev/urandom");
    b.iter().map(|x| format!("{x:02x}")).collect()
}

fn base64(data: &[u8]) -> String {
    const T: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::new();
    for c in data.chunks(3) {
        let n = (c[0] as u32) << 16 | (*c.get(1).unwrap_or(&0) as u32) << 8 | *c.get(2).unwrap_or(&0) as u32;
        for i in 0..4 {
            out.push(if i <= c.len() {
                T[(n >> (18 - 6 * i) & 63) as usize] as char
            } else {
                '='
            });
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_rclone_that_died_is_started_again_and_a_closed_one_stays_down() {
        if !available() {
            eprintln!("skipped: rclone not found");
            return;
        }
        let dir = std::env::temp_dir().join(format!("fastsync-revive-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let conf = dir.join("rclone.conf");
        std::fs::write(&conf, "").unwrap();
        let rc = Rclone::start(&conf).expect("rclone rcd starts");
        {
            let mut c = rc.child.lock().unwrap();
            c.kill().unwrap();
            c.wait().unwrap();
        }
        assert!(rc.call("core/version", json!({})).is_ok(), "the call restarts rclone");
        assert_eq!(rc.restarts.load(std::sync::atomic::Ordering::Relaxed), 1);
        rc.shutdown();
        assert!(
            rc.call("core/version", json!({})).is_err(),
            "a quit does not bring it back"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A cloud as `(path, is_dir)`: `list_one` answers like `operations/list` (paths from the root).
    fn fake_cloud(
        entries: &'static [(&'static str, bool)],
        fail_on: Option<&'static str>,
    ) -> impl Fn(&str, bool) -> Result<Vec<Item>> + Sync {
        move |dir, recursive| {
            if fail_on == Some(dir) {
                return Err("boom".into());
            }
            let under = |p: &str| match p.rsplit_once('/') {
                _ if dir.is_empty() => recursive || !p.contains('/'),
                Some((parent, _)) => recursive && p.starts_with(&format!("{dir}/")) || parent == dir,
                None => false,
            };
            Ok(entries
                .iter()
                .filter(|(p, _)| under(p))
                .map(|(p, d)| Item {
                    path: (*p).into(),
                    size: 1,
                    mtime: 0,
                    is_dir: *d,
                    id: String::new(),
                    hash: None,
                })
                .collect())
        }
    }

    const DEEP: &[(&str, bool)] = &[
        ("root.txt", false),
        ("a", true),
        ("a/b", true),
        ("a/b/c", true),
        ("a/b/c/d", true),
        ("a/b/c/d/f.txt", false),
        ("a/b/c/d/e", true),
        ("a/b/c/d/e/g.txt", false),
        ("x", true),
        ("x/h.txt", false),
    ];

    #[test]
    fn the_walk_lists_every_path_once_and_its_progress_meets_its_total() {
        let seen = std::sync::Mutex::new(Vec::new());
        let items = walk_cloud(
            &fake_cloud(DEEP, None),
            &|d, t| seen.lock().unwrap().push((d, t)),
            &|| false,
        )
        .unwrap()
        .unwrap();
        let mut paths: Vec<&str> = items.iter().map(|i| i.path.as_str()).collect();
        paths.sort();
        let mut want: Vec<&str> = DEEP.iter().map(|(p, _)| *p).collect();
        want.sort();
        assert_eq!(paths, want);
        // The root, a, a/b, x one level at a time; a/b/c (depth 3) in one recursive call.
        assert_eq!(seen.lock().unwrap().last(), Some(&(5, 5)));
    }

    #[test]
    fn a_status_code_is_a_word_of_its_own() {
        assert!(transient("HTTP error 503 Service Unavailable") && transient("googleapi: Error 500: backend"));
        assert!(!transient("directory not found: Fotos/2500 pics"));
    }

    #[test]
    fn only_a_transient_error_is_asked_again() {
        let calls = std::cell::Cell::new(0);
        let r: Result<()> = retry_transient(&|| false, 0, || {
            calls.set(calls.get() + 1);
            Err("operations/list: invalid_grant 401".into())
        });
        assert!(r.is_err() && calls.get() == 1);
        calls.set(0);
        let r = retry_transient(&|| false, 0, || {
            calls.set(calls.get() + 1);
            if calls.get() < 3 {
                Err("error in ListJSON: unexpected error occurred".into())
            } else {
                Ok(7)
            }
        });
        assert_eq!((r, calls.get()), (Ok(7), 3));
    }

    /// `list_one` of the real remote: asks `Listed` first, keeps what it lists, counts the calls that reached the cloud.
    fn kept<'a>(
        cache: &'a std::sync::Mutex<Listed>,
        calls: &'a std::sync::Mutex<Vec<String>>,
        cloud: impl Fn(&str, bool) -> Result<Vec<Item>> + Sync + 'a,
    ) -> impl Fn(&str, bool) -> Result<Vec<Item>> + Sync + 'a {
        move |dir, recursive| {
            if let Some(k) = cache
                .lock()
                .unwrap()
                .get(dir, recursive, std::time::Duration::from_secs(60))
            {
                return Ok(k);
            }
            calls.lock().unwrap().push(dir.to_string());
            let items = cloud(dir, recursive)?;
            cache.lock().unwrap().put(dir, recursive, &items);
            Ok(items)
        }
    }

    #[test]
    fn a_second_try_lists_only_what_the_first_did_not_finish() {
        let (cache, calls) = (Default::default(), Default::default());
        let first = kept(&cache, &calls, fake_cloud(DEEP, Some("a/b")));
        assert!(walk_cloud(&first, &|_, _| {}, &|| false).is_err());
        calls.lock().unwrap().clear();
        let again = kept(&cache, &calls, fake_cloud(DEEP, None));
        let items = walk_cloud(&again, &|_, _| {}, &|| false).unwrap().unwrap();
        assert_eq!(items.len(), DEEP.len(), "nothing lost or doubled");
        let calls = calls.lock().unwrap();
        assert!(
            calls.contains(&"a/b".to_string()),
            "the failed folder is listed: {calls:?}"
        );
        assert!(
            !calls.contains(&String::new()) && !calls.contains(&"a".to_string()),
            "{calls:?}"
        );
    }

    #[test]
    fn a_change_drops_the_folders_around_it_and_only_those() {
        let mut l = Listed::default();
        for d in ["", "a", "a/b", "c", "a/bb"] {
            l.put(d, false, &[]);
        }
        l.forget(Some(&["A/B/x.txt".to_string()]));
        let left: std::collections::BTreeSet<_> = l.0.keys().cloned().collect();
        assert_eq!(
            left,
            ["a/bb".to_string(), "c".to_string()].into(),
            "the root, a and a/b go"
        );
        l.forget(None);
        assert!(l.0.is_empty());
    }

    #[test]
    fn a_kept_folder_is_not_used_when_old_or_listed_the_other_way() {
        let mut l = Listed::default();
        l.put("a", true, &[]);
        assert!(l.get("a", true, std::time::Duration::from_secs(60)).is_some());
        assert!(l.get("a", false, std::time::Duration::from_secs(60)).is_none());
        assert!(l.get("a", true, std::time::Duration::ZERO).is_none());
    }

    #[test]
    fn one_failed_folder_fails_the_whole_listing() {
        let err = walk_cloud(&fake_cloud(DEEP, Some("a/b")), &|_, _| {}, &|| false).unwrap_err();
        assert_eq!(err, "a/b: boom");
        let err = walk_cloud(&fake_cloud(DEEP, Some("")), &|_, _| {}, &|| false).unwrap_err();
        assert_eq!(err, "boom", "a root failure keeps its own message");
    }

    #[test]
    fn a_cancel_between_folders_fails_the_listing_instead_of_returning_a_part() {
        use std::sync::atomic::{AtomicBool, Ordering::Relaxed};
        let cancel = AtomicBool::new(false);
        // Pause is pressed once the first folder (the root) has been listed.
        let r = walk_cloud(&fake_cloud(DEEP, None), &|d, _| cancel.store(d >= 1, Relaxed), &|| {
            cancel.load(Relaxed)
        });
        assert_eq!(r.unwrap_err(), "cancelled");
    }

    #[test]
    fn two_sibling_folders_with_one_name_send_the_caller_to_the_single_walk() {
        const TWINS: &[(&str, bool)] = &[("a", true), ("a", true), ("f.txt", false)];
        assert!(
            walk_cloud(&fake_cloud(TWINS, None), &|_, _| {}, &|| false)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn quota_reads_used_and_an_optional_total() {
        assert_eq!(quota(&json!({"used": 5, "free": 10, "total": 15})), Some((5, Some(15))));
        assert_eq!(quota(&json!({"used": 5})), Some((5, None)));
        assert_eq!(quota(&json!({"used": 5, "total": 0})), Some((5, None)));
        assert_eq!(quota(&json!({})), None);
    }

    #[test]
    fn old_rclone_is_refused() {
        assert!(version_ok(&json!([1, 76, 0])));
        assert!(version_ok(&json!([2, 0, 0])));
        assert!(!version_ok(&json!([1, 75, 9])));
        assert!(!version_ok(&json!(null)));
    }

    #[test]
    fn times_parse_with_fractions_and_offsets() {
        assert_eq!(parse_time("1970-01-01T00:00:00Z"), Some(0));
        assert_eq!(parse_time("2026-11-11T15:10:08.123456789Z"), Some(1_794_409_808_123));
        assert_eq!(parse_time("2026-11-11T17:10:08+02:00"), Some(1_794_409_808_000));
        assert_eq!(parse_time("2024-02-29T00:00:00Z"), Some(1_709_164_800_000));
        assert_eq!(parse_time("garbage"), None);
    }

    #[test]
    fn listings_carry_the_trusted_hash_and_ignore_empty_ones() {
        let item = |h: Value| parse_item(&json!({"Path": "a", "Size": 1, "Hashes": h})).unwrap().hash;
        assert_eq!(
            item(json!({"quickxor": "ABCD"})),
            Some(("quickxor".into(), "abcd".into()))
        );
        // An empty value is "no hash"; the next type in the order is used.
        assert_eq!(
            item(json!({"md5": "", "dropbox": "ff"})),
            Some(("dropbox".into(), "ff".into()))
        );
        assert_eq!(item(json!({})), None);
        assert_eq!(parse_item(&json!({"Path": "a"})).unwrap().hash, None);
    }

    #[test]
    fn login_questions_are_answered() {
        assert_eq!(answer(&json!({"Name": "config_is_local", "Default": true})), "true");
        let shared = json!({"Name": "x", "Type": "bool", "Default": false, "Help": "Continue using the shared client_id anyway?"});
        assert_eq!(answer(&shared), "true");
        assert_eq!(
            answer(
                &json!({"Name": "config_team_drive", "Type": "bool", "Default": false, "Help": "Configure this as a Shared Drive?"})
            ),
            "false"
        );
        assert_eq!(answer(&json!({"Name": "x", "Default": "abc"})), "abc");
        assert_eq!(answer(&json!({"Name": "x"})), "");
        // A token that is already there is kept, whatever rclone's default says.
        assert_eq!(
            answer(&json!({"Name": "config_refresh_token", "Default": true})),
            "false"
        );
    }

    #[test]
    fn the_mega_probe_carries_the_login_in_the_environment() {
        let env = mega_probe_env(&json!({"type": "mega", "user": "me@example.com", "pass": "obscured"}));
        assert!(env.contains(&("RCLONE_CONFIG_PROBE_USER", "me@example.com".to_string())));
        assert!(env.contains(&("RCLONE_CONFIG_PROBE_PASS", "obscured".to_string())));
        // No settings file: nothing of the user's is read or written.
        assert!(env.contains(&("RCLONE_CONFIG", "/dev/null".to_string())));
        // A missing password is an empty one, which MEGA refuses: never a skipped check.
        assert!(mega_probe_env(&json!({"user": "x"})).contains(&("RCLONE_CONFIG_PROBE_PASS", String::new())));
    }

    #[test]
    fn a_remote_is_copied_without_its_type() {
        let cfg = json!({"type": "drive", "token": "{\"a\":1}", "client_id": "id", "drive_id": "d", "n": 3});
        assert_eq!(
            copy_params(&cfg),
            json!({"token": "{\"a\":1}", "client_id": "id", "drive_id": "d"})
        );
        assert_eq!(copy_params(&Value::Null), json!({}));
    }

    #[test]
    fn login_params_add_the_own_client_and_skip_google_docs() {
        // Without the build-time variables (the usual test run) nothing is added.
        if embedded_client("drive").is_none() {
            assert_eq!(
                login_params("drive", None),
                json!({"skip_gdocs": "true", "skip_shortcuts": "true"})
            );
            assert_eq!(login_params("dropbox", None), json!({}));
        }
        assert!(embedded_client("mega").is_none());
        assert_eq!(
            login_params("drive", Some(("id", "sec"))),
            json!({"skip_gdocs": "true", "skip_shortcuts": "true", "client_id": "id", "client_secret": "sec"})
        );
        assert_eq!(
            login_params("onedrive", Some(("id", "sec"))),
            json!({"client_id": "id", "client_secret": "sec"})
        );
    }

    #[test]
    fn base64_matches_the_known_vectors() {
        assert_eq!(base64(b"fastsync:x"), "ZmFzdHN5bmM6eA==");
        assert_eq!(base64(b"ab"), "YWI=");
        assert_eq!(base64(b"abc"), "YWJj");
    }

    #[test]
    fn a_listing_that_does_not_parse_is_an_error_not_a_shorter_list() {
        let parse = |s: &str| serde_json::from_str::<Listing>(s).map(|l| l.list.len());
        assert_eq!(parse(r#"{"list": []}"#).unwrap(), 0, "an empty cloud says so");
        assert_eq!(parse(r#"{"list": [{"Path": "a", "Size": 1}]}"#).unwrap(), 1);
        assert!(parse("{}").is_err(), "no `list` is not an empty cloud");
        assert!(parse(r#"{"list": [{"Path": "a", "Size": "x"}]}"#).is_err());
    }

    #[test]
    fn items_parse_and_skip_junk() {
        let v = json!({"Path": "a/b.txt", "Size": 5, "ModTime": "1970-01-01T00:00:01Z", "IsDir": false, "ID": "x"});
        let i = parse_item(&v).unwrap();
        assert_eq!(
            (i.path.as_str(), i.size, i.mtime, i.id.as_str()),
            ("a/b.txt", 5, 1000, "x")
        );
        assert!(parse_item(&json!({"Size": 1})).is_none());
        assert_eq!(
            parse_item(&json!({"Path": "d", "Size": -1, "IsDir": true}))
                .unwrap()
                .size,
            0
        );
    }
}
