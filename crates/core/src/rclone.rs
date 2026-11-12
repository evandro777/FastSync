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
}

pub struct Rclone {
    agent: ureq::Agent,
    base: String,
    auth: String,
    child: std::sync::Mutex<Child>,
}

/// `rclone` from `$FASTSYNC_RCLONE` (a dev override), else from `PATH` (the Flatpak puts it in /app/bin).
fn binary() -> PathBuf {
    std::env::var_os("FASTSYNC_RCLONE").map_or_else(|| "rclone".into(), PathBuf::from)
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
        let mut cmd = Command::new(binary());
        // The password goes through the environment: argv is readable by every local user in /proc/<pid>/cmdline.
        // (If rclone ignored these, `config/*` calls would be refused as unauthenticated, not silently open.)
        cmd.env("RCLONE_RC_USER", "fastsync")
            .env("RCLONE_RC_PASS", &pass)
            .args(["rcd", "--rc-addr", &format!("127.0.0.1:{port}")])
            .arg("--config")
            .arg(config)
            .args(["--log-level", "ERROR"])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        let child = cmd.spawn().map_err(|e| format!("cannot start rclone: {e}"))?;
        let agent = ureq::Agent::config_builder().http_status_as_error(false).build().into();
        let me = Self {
            agent,
            base: format!("http://127.0.0.1:{port}/"),
            auth: format!("Basic {}", base64(format!("fastsync:{pass}").as_bytes())),
            child: std::sync::Mutex::new(child),
        };
        let start = Instant::now();
        // Dropping `me` on failure kills the child.
        while me.call("core/pid", json!({})).is_err() {
            if start.elapsed() > Duration::from_secs(10) {
                return Err("rclone did not start".into());
            }
            std::thread::sleep(Duration::from_millis(100));
        }
        Ok(me)
    }

    pub fn call(&self, method: &str, body: Value) -> Result<Value> {
        let mut resp = self
            .agent
            .post(&format!("{}{method}", self.base))
            .header("Authorization", &self.auth)
            // Exactly `application/json`: with ureq's `; charset=utf-8` rclone ignores the body ("Didn't find key").
            .header("Content-Type", "application/json")
            .send(serde_json::to_vec(&body).map_err(|e| e.to_string())?)
            .map_err(|e| e.to_string())?;
        let ok = resp.status().is_success();
        let v: Value = resp
            .body_mut()
            .with_config()
            .limit(u64::MAX)
            .read_json()
            .map_err(|e| e.to_string())?;
        if ok {
            Ok(v)
        } else {
            // The method name tells which call failed; rclone's own message does not.
            Err(format!("{method}: {}", v["error"].as_str().unwrap_or("rclone error")))
        }
    }

    /// Recursive listing of `fs:remote`. A missing folder is an empty list, not an error.
    /// `exclude`: rclone filter rules (`/Folder/**`) for what must not be listed at all.
    pub fn list(&self, fs: &str, remote: &str, exclude: &[String]) -> Result<Vec<Item>> {
        let v = match self.call(
            "operations/list",
            json!({"fs": fs, "remote": remote, "opt": {"recurse": true, "noMimeType": true}, "_filter": {"ExcludeRule": exclude}}),
        ) {
            Ok(v) => v,
            Err(e) if e.contains("not found") => return Ok(Vec::new()),
            Err(e) => return Err(e),
        };
        Ok(v["list"]
            .as_array()
            .map_or_else(Vec::new, |a| a.iter().filter_map(parse_item).collect()))
    }

    pub fn stat(&self, fs: &str, remote: &str) -> Result<Option<Item>> {
        let v = self.call(
            "operations/stat",
            json!({"fs": fs, "remote": remote, "opt": {"noMimeType": true}}),
        )?;
        Ok(parse_item(&v["item"]))
    }

    pub fn copyfile(&self, src_fs: &str, src: &str, dst_fs: &str, dst: &str) -> Result<()> {
        self.call(
            "operations/copyfile",
            json!({"srcFs": src_fs, "srcRemote": src, "dstFs": dst_fs, "dstRemote": dst}),
        )
        .map(drop)
    }

    pub fn deletefile(&self, fs: &str, remote: &str) -> Result<()> {
        self.call("operations/deletefile", json!({"fs": fs, "remote": remote}))
            .map(drop)
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
        // Sent with every call, as rclone asks for config defaults.
        let params = login_params(kind, client);
        let mut v = self.call(
            "config/create",
            json!({"name": name, "type": kind, "parameters": params, "opt": {"nonInteractive": true}}),
        )?;
        for _ in 0..20 {
            if let Some(e) = v["Error"].as_str().filter(|e| !e.is_empty()) {
                return Err(e.to_string());
            }
            let state = v["State"].as_str().unwrap_or("").to_string();
            if state.is_empty() {
                return Ok(());
            }
            let result = answer(&v["Option"]);
            v = self.call("config/update", json!({"name": name, "parameters": params, "opt": {"nonInteractive": true, "continue": true, "state": state, "result": result}}))?;
        }
        Err("login did not finish".into())
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

impl crate::engine::Remote for RcloneRemote {
    fn list(&self) -> Result<Vec<Item>> {
        self.rc.list(&self.fs, "", &self.exclude)
    }
    fn stat(&self, rel: &str) -> Result<Option<Item>> {
        self.rc.stat(&self.fs, rel)
    }
    fn upload(&self, local_root: &Path, rel: &str) -> Result<()> {
        self.rc.copyfile(&local_root.to_string_lossy(), rel, &self.fs, rel)
    }
    fn download(&self, rel: &str, dest: &Path) -> Result<()> {
        let (dir, name) = (dest.parent().ok_or("bad path")?, dest.file_name().ok_or("bad path")?);
        self.rc
            .copyfile(&self.fs, rel, &dir.to_string_lossy(), &name.to_string_lossy())
    }
    fn delete(&self, rel: &str) -> Result<()> {
        self.rc.deletefile(&self.fs, rel)
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
        if let Ok(mut c) = self.child.lock() {
            let _ = c.kill();
            let _ = c.wait();
        }
    }
}

/// Our answer to one of rclone's login questions. Yes to "use a web browser on this machine" (we are a desktop
/// app) and to newer rclone's "continue using the shared client_id anyway?" (default No: asked only when the user
/// gave no client ID of their own, and then the shared one is what they chose). Everything else: the default.
fn answer(opt: &Value) -> String {
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

/// The config parameters of an OAuth login. Google Docs and Sheets have no file content: without `skip_gdocs`
/// rclone lists them as .docx/.xlsx exports, and a later local edit would upload a duplicate.
pub fn login_params(kind: &str, client: Option<(&str, &str)>) -> Value {
    let mut p = serde_json::Map::new();
    if kind == "drive" {
        p.insert("skip_gdocs".into(), "true".into());
    }
    if let Some((id, secret)) = client {
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

fn parse_item(v: &Value) -> Option<Item> {
    Some(Item {
        path: v["Path"].as_str()?.to_string(),
        size: v["Size"].as_i64().unwrap_or(0).max(0) as u64,
        mtime: v["ModTime"].as_str().and_then(parse_time).unwrap_or(0),
        is_dir: v["IsDir"].as_bool().unwrap_or(false),
        id: v["ID"].as_str().unwrap_or("").to_string(),
    })
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

fn random_hex() -> String {
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
    fn times_parse_with_fractions_and_offsets() {
        assert_eq!(parse_time("1970-01-01T00:00:00Z"), Some(0));
        assert_eq!(parse_time("2026-11-11T15:10:08.123456789Z"), Some(1_794_409_808_123));
        assert_eq!(parse_time("2026-11-11T17:10:08+02:00"), Some(1_794_409_808_000));
        assert_eq!(parse_time("2024-02-29T00:00:00Z"), Some(1_709_164_800_000));
        assert_eq!(parse_time("garbage"), None);
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
    }

    #[test]
    fn login_params_add_the_own_client_and_skip_google_docs() {
        assert_eq!(login_params("drive", None), json!({"skip_gdocs": "true"}));
        assert_eq!(login_params("dropbox", None), json!({}));
        assert_eq!(
            login_params("drive", Some(("id", "sec"))),
            json!({"skip_gdocs": "true", "client_id": "id", "client_secret": "sec"})
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
