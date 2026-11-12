//! Remote change feeds: each provider's delta API, behind one trait. A feed says which files changed since a
//! cursor, so a remote edit is noticed without listing the whole tree. Providers without one (MEGA, ...) use the
//! engine's periodic full scan instead.
use std::{collections::HashMap, time::Duration};

use serde_json::{Value, json};

pub type Result<T> = std::result::Result<T, String>;

/// A live access token (the caller refreshes it when it is close to expiring).
pub type Token = Box<dyn Fn() -> Result<String> + Send>;

/// One changed entry. Dropbox reports paths (`path`, relative to the synced folder); Drive and OneDrive report
/// ids, so the name and the parent id are needed to find the path.
#[derive(Debug, Default, PartialEq)]
pub struct Change {
    pub id: String,
    pub name: String,
    pub parent: String,
    pub path: Option<String>,
    pub removed: bool,
    pub is_dir: bool,
}

pub enum Poll {
    /// An empty list is fine: nothing changed in this window.
    Changes(Vec<Change>, String),
    /// The cursor expired (Dropbox `reset`, Drive/OneDrive 410): do one full scan, then `start` again.
    Reset,
}

pub trait RemoteChanges: Send {
    /// A cursor for "now". Taken before a full scan, so changes during the scan are not missed.
    fn start(&mut self) -> Result<String>;
    /// May block (Dropbox long-polls for up to 30 s).
    fn poll(&mut self, cursor: &str) -> Result<Poll>;
    /// The provider's id of its root folder, for feeds that report parent ids.
    fn root_id(&mut self) -> Result<Option<String>> {
        Ok(None)
    }
    /// How long to wait between polls when `poll` returns at once.
    fn idle(&self) -> Duration {
        Duration::from_secs(30)
    }
}

/// `kind` is the rclone backend type. `folder` is the synced folder inside the cloud ("" = the root).
pub fn for_kind(kind: &str, token: Token, folder: &str) -> Option<Box<dyn RemoteChanges>> {
    let http = Http::new(token);
    match kind {
        "dropbox" => Some(Box::new(Dropbox {
            http,
            folder: folder.trim_matches('/').to_string(),
        })),
        "drive" => Some(Box::new(Drive { http })),
        "onedrive" => Some(Box::new(OneDrive { http })),
        _ => None,
    }
}

struct Http {
    agent: ureq::Agent,
    token: Token,
}

impl Http {
    fn new(token: Token) -> Self {
        // A long poll is 30 s plus Dropbox's jitter (up to 90 s). Without a limit, a laptop that slept or changed
        // network would leave the poll hanging forever and remote changes would silently stop.
        let config = ureq::Agent::config_builder()
            .http_status_as_error(false)
            .timeout_global(Some(Duration::from_secs(150)))
            .build();
        Self {
            agent: config.into(),
            token,
        }
    }

    /// `(status, body)`; a body that is not JSON comes back as `Null`.
    fn get(&self, url: &str) -> Result<(u16, Value)> {
        let req = self
            .agent
            .get(url)
            .header("Authorization", format!("Bearer {}", (self.token)()?));
        finish(req.call())
    }

    fn post(&self, url: &str, body: &Value, auth: bool) -> Result<(u16, Value)> {
        let mut req = self.agent.post(url);
        if auth {
            req = req.header("Authorization", format!("Bearer {}", (self.token)()?));
        }
        finish(req.send_json(body))
    }
}

fn finish(r: std::result::Result<ureq::http::Response<ureq::Body>, ureq::Error>) -> Result<(u16, Value)> {
    let mut resp = r.map_err(|e| e.to_string())?;
    let status = resp.status().as_u16();
    let text = resp.body_mut().read_to_string().map_err(|e| e.to_string())?;
    Ok((status, serde_json::from_str(&text).unwrap_or(Value::Null)))
}

fn s(v: &Value) -> String {
    v.as_str().unwrap_or("").to_string()
}

fn fail<T>(what: &str, status: u16, body: &Value) -> Result<T> {
    // Dropbox puts the reason in `error_summary`, Google in `error.message` (and `error.errors[0].reason`),
    // Microsoft in `error.message`.
    let why = [
        &body["error_summary"],
        &body["error"]["message"],
        &body["error"]["errors"][0]["reason"],
    ]
    .iter()
    .map(|v| s(v))
    .filter(|t| !t.is_empty())
    .collect::<Vec<_>>()
    .join(", ");
    Err(format!(
        "{what}: HTTP {status} {}",
        why.chars().take(200).collect::<String>()
    ))
}

// ---- Dropbox: list_folder cursor + long poll ----

struct Dropbox {
    http: Http,
    folder: String,
}

impl Dropbox {
    fn root(&self) -> String {
        if self.folder.is_empty() {
            String::new()
        } else {
            format!("/{}", self.folder)
        }
    }
}

impl RemoteChanges for Dropbox {
    fn start(&mut self) -> Result<String> {
        let body = json!({"path": self.root(), "recursive": true, "include_deleted": true});
        let (st, v) = self.http.post(
            "https://api.dropboxapi.com/2/files/list_folder/get_latest_cursor",
            &body,
            true,
        )?;
        if st != 200 {
            return fail("dropbox cursor", st, &v);
        }
        Ok(s(&v["cursor"]))
    }

    fn poll(&mut self, cursor: &str) -> Result<Poll> {
        // The long poll endpoint takes no token and returns when something changed or after the timeout.
        let (st, v) = self.http.post(
            "https://notify.dropboxapi.com/2/files/list_folder/longpoll",
            &json!({"cursor": cursor, "timeout": 30}),
            false,
        )?;
        if st == 409 && s(&v["error_summary"]).starts_with("reset") {
            return Ok(Poll::Reset);
        }
        if st != 200 {
            return fail("dropbox longpoll", st, &v);
        }
        if !v["changes"].as_bool().unwrap_or(false) {
            return Ok(Poll::Changes(Vec::new(), cursor.to_string()));
        }
        let (mut cursor, mut out) = (cursor.to_string(), Vec::new());
        loop {
            let (st, v) = self.http.post(
                "https://api.dropboxapi.com/2/files/list_folder/continue",
                &json!({"cursor": cursor}),
                true,
            )?;
            if st == 409 && s(&v["error_summary"]).starts_with("reset") {
                return Ok(Poll::Reset);
            }
            if st != 200 {
                return fail("dropbox changes", st, &v);
            }
            out.extend(dropbox_entries(&v, &self.root()));
            cursor = s(&v["cursor"]);
            if !v["has_more"].as_bool().unwrap_or(false) {
                return Ok(Poll::Changes(out, cursor));
            }
        }
    }

    fn idle(&self) -> Duration {
        Duration::ZERO
    }
}

/// Entries of a `list_folder/continue` answer, with paths relative to the synced folder `root` ("" or "/Docs").
pub fn dropbox_entries(v: &Value, root: &str) -> Vec<Change> {
    let strip = |p: &str| -> Option<String> {
        let head = p.get(..root.len())?;
        let rest = p.get(root.len()..)?;
        (head.eq_ignore_ascii_case(root) && (rest.starts_with('/') || (rest.is_empty() && root.is_empty())))
            .then(|| rest.trim_start_matches('/').to_string())
    };
    v["entries"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|e| {
            let path = strip(e["path_display"].as_str()?).filter(|p| !p.is_empty())?;
            let tag = e[".tag"].as_str()?;
            Some(Change {
                id: s(&e["id"]),
                name: s(&e["name"]),
                path: Some(path),
                removed: tag == "deleted",
                is_dir: tag == "folder",
                ..Default::default()
            })
        })
        .collect()
}

// ---- Google Drive: changes.list with a page token ----

struct Drive {
    http: Http,
}

const DRIVE: &str = "https://www.googleapis.com/drive/v3";
const DRIVE_FOLDER: &str = "application/vnd.google-apps.folder";

impl RemoteChanges for Drive {
    fn start(&mut self) -> Result<String> {
        let (st, v) = self.http.get(&format!("{DRIVE}/changes/startPageToken"))?;
        if st != 200 {
            return fail("drive token", st, &v);
        }
        Ok(s(&v["startPageToken"]))
    }

    fn poll(&mut self, cursor: &str) -> Result<Poll> {
        let (mut token, mut out) = (cursor.to_string(), Vec::new());
        loop {
            let url = format!(
                "{DRIVE}/changes?pageToken={token}&pageSize=1000&includeRemoved=true&fields=nextPageToken,newStartPageToken,changes(fileId,removed,file(name,parents,mimeType,trashed))"
            );
            let (st, v) = self.http.get(&url)?;
            if st == 410 || st == 404 {
                return Ok(Poll::Reset);
            }
            if st != 200 {
                return fail("drive changes", st, &v);
            }
            out.extend(drive_changes(&v));
            if let Some(next) = v["nextPageToken"].as_str() {
                token = next.to_string();
            } else {
                return Ok(Poll::Changes(out, s(&v["newStartPageToken"])));
            }
        }
    }

    /// Drive has no push and rclone's shared client id has a small per-minute quota: poll gently.
    fn idle(&self) -> Duration {
        Duration::from_secs(60)
    }

    fn root_id(&mut self) -> Result<Option<String>> {
        let (st, v) = self.http.get(&format!("{DRIVE}/files/root?fields=id"))?;
        if st != 200 {
            return fail("drive root", st, &v);
        }
        Ok(Some(s(&v["id"])))
    }
}

pub fn drive_changes(v: &Value) -> Vec<Change> {
    v["changes"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|c| {
            let f = &c["file"];
            let mime = f["mimeType"].as_str().unwrap_or("");
            // Docs, Sheets and the like have no file content to sync.
            if mime.starts_with("application/vnd.google-apps.") && mime != DRIVE_FOLDER {
                return None;
            }
            Some(Change {
                id: s(&c["fileId"]),
                name: s(&f["name"]),
                parent: s(&f["parents"][0]),
                removed: c["removed"].as_bool().unwrap_or(false) || f["trashed"].as_bool().unwrap_or(false),
                is_dir: mime == DRIVE_FOLDER,
                path: None,
            })
        })
        .collect()
}

// ---- OneDrive: /delta with a deltaLink ----

struct OneDrive {
    http: Http,
}

const GRAPH: &str = "https://graph.microsoft.com/v1.0/me/drive";

impl RemoteChanges for OneDrive {
    fn start(&mut self) -> Result<String> {
        // `token=latest` answers with the link for "from now" without sending the whole tree.
        let (st, v) = self.http.get(&format!("{GRAPH}/root/delta?token=latest"))?;
        if st != 200 {
            return fail("onedrive delta", st, &v);
        }
        Ok(s(&v["@odata.deltaLink"]))
    }

    fn poll(&mut self, cursor: &str) -> Result<Poll> {
        let (mut url, mut out) = (cursor.to_string(), Vec::new());
        loop {
            let (st, v) = self.http.get(&url)?;
            if st == 410 {
                return Ok(Poll::Reset);
            }
            if st != 200 {
                return fail("onedrive delta", st, &v);
            }
            out.extend(onedrive_items(&v));
            if let Some(next) = v["@odata.nextLink"].as_str() {
                url = next.to_string();
            } else {
                return Ok(Poll::Changes(out, s(&v["@odata.deltaLink"])));
            }
        }
    }

    fn root_id(&mut self) -> Result<Option<String>> {
        let (st, v) = self.http.get(&format!("{GRAPH}/root?$select=id"))?;
        if st != 200 {
            return fail("onedrive root", st, &v);
        }
        Ok(Some(s(&v["id"])))
    }
}

pub fn onedrive_items(v: &Value) -> Vec<Change> {
    v["value"]
        .as_array()
        .into_iter()
        .flatten()
        .filter(|i| i.get("root").is_none())
        .map(|i| Change {
            id: s(&i["id"]),
            name: s(&i["name"]),
            parent: s(&i["parentReference"]["id"]),
            removed: i.get("deleted").is_some(),
            is_dir: i.get("folder").is_some(),
            path: None,
        })
        .collect()
}

// ---- From changes to work for the engine ----

#[derive(Debug, Default, PartialEq)]
pub struct Impact {
    pub paths: Vec<String>,
    /// A folder was created, moved or renamed: its children's paths changed without being reported.
    pub full_scan: bool,
}

/// `dirs` maps folder id -> path inside the synced folder; `root` is the synced folder's own id; `path_of_id`
/// finds the path of a file we already synced (deletions and moves only carry the id).
pub fn impact(
    changes: &[Change],
    dirs: &HashMap<String, String>,
    root: Option<&str>,
    path_of_id: impl Fn(&str) -> Option<String>,
) -> Impact {
    let mut out = Impact::default();
    for c in changes {
        if let Some(p) = &c.path {
            // A new or moved Dropbox folder: what is inside it is not listed, so look at everything.
            if c.is_dir && !c.removed {
                out.full_scan = true;
            } else {
                out.paths.push(p.clone());
            }
            continue;
        }
        let parent = if Some(c.parent.as_str()) == root {
            Some("")
        } else {
            dirs.get(&c.parent).map(String::as_str)
        };
        // A deleted folder arrives without its folder facet (Drive `removed`, OneDrive `deleted`), so a known folder
        // id counts whatever the entry says it is.
        if dirs.contains_key(&c.id) {
            out.full_scan = true;
            continue;
        }
        if c.is_dir {
            out.full_scan |= parent.is_some();
            continue;
        }
        if let (Some(dir), false) = (parent, c.removed) {
            out.paths.push(if dir.is_empty() {
                c.name.clone()
            } else {
                format!("{dir}/{}", c.name)
            });
        }
        // The old location of a renamed, moved or deleted file.
        out.paths.extend(path_of_id(&c.id));
    }
    out.paths.sort();
    out.paths.dedup();
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dropbox_paths_are_made_relative_and_other_folders_are_ignored() {
        let v = json!({"entries": [
            {".tag": "file", "id": "id:1", "name": "a.txt", "path_display": "/Docs/Work/a.txt"},
            {".tag": "deleted", "name": "b.txt", "path_display": "/Docs/b.txt"},
            {".tag": "folder", "id": "id:2", "name": "New", "path_display": "/docs/New"},
            {".tag": "file", "id": "id:3", "name": "x", "path_display": "/Docsmore/x"},
            {".tag": "file", "id": "id:4", "name": "y", "path_display": "/Other/y"}]});
        let c = dropbox_entries(&v, "/Docs");
        let got: Vec<_> = c
            .iter()
            .map(|c| (c.path.as_deref().unwrap(), c.removed, c.is_dir))
            .collect();
        assert_eq!(
            got,
            [
                ("Work/a.txt", false, false),
                ("b.txt", true, false),
                ("New", false, true)
            ]
        );
        assert_eq!(
            dropbox_entries(
                &json!({"entries": [{".tag": "file", "name": "z", "path_display": "/z"}]}),
                ""
            )[0]
            .path
            .as_deref(),
            Some("z")
        );
    }

    #[test]
    fn drive_skips_docs_and_treats_trashed_as_removed() {
        let v = json!({"changes": [
            {"fileId": "1", "file": {"name": "a.txt", "parents": ["P"], "mimeType": "text/plain"}},
            {"fileId": "2", "file": {"name": "Doc", "parents": ["P"], "mimeType": "application/vnd.google-apps.document"}},
            {"fileId": "3", "file": {"name": "old", "parents": ["P"], "mimeType": "text/plain", "trashed": true}},
            {"fileId": "4", "removed": true},
            {"fileId": "5", "file": {"name": "dir", "parents": ["P"], "mimeType": "application/vnd.google-apps.folder"}}]});
        let c = drive_changes(&v);
        let got: Vec<_> = c.iter().map(|c| (c.id.as_str(), c.removed, c.is_dir)).collect();
        assert_eq!(
            got,
            [
                ("1", false, false),
                ("3", true, false),
                ("4", true, false),
                ("5", false, true)
            ]
        );
        assert_eq!(c[0].parent, "P");
    }

    #[test]
    fn onedrive_items_map_facets() {
        let v = json!({"value": [
            {"id": "r", "root": {}, "name": "root"},
            {"id": "1", "name": "a", "parentReference": {"id": "P"}, "file": {}},
            {"id": "2", "name": "d", "parentReference": {"id": "P"}, "folder": {}},
            {"id": "3", "name": "z", "parentReference": {"id": "P"}, "deleted": {}}]});
        let c = onedrive_items(&v);
        assert_eq!(c.len(), 3);
        assert!(!c[0].is_dir && c[1].is_dir && c[2].removed);
    }

    fn ch(id: &str, name: &str, parent: &str) -> Change {
        Change {
            id: id.into(),
            name: name.into(),
            parent: parent.into(),
            ..Default::default()
        }
    }

    #[test]
    fn id_changes_resolve_to_paths_or_a_full_scan() {
        let dirs: HashMap<String, String> = [("D".to_string(), "docs".to_string())].into();
        let known = |id: &str| (id == "old").then(|| "docs/was.txt".to_string());
        let mut moved = ch("old", "now.txt", "D");
        let removed = Change {
            removed: true,
            ..ch("old", "", "")
        };
        let outside = ch("x", "n", "ELSEWHERE");
        let new_dir = Change {
            is_dir: true,
            ..ch("N", "sub", "D")
        };
        let other_dir = Change {
            is_dir: true,
            ..ch("O", "sub", "ELSEWHERE")
        };

        let i = impact(
            &[ch("f", "a.txt", "R"), ch("g", "b.txt", "D"), outside],
            &dirs,
            Some("R"),
            known,
        );
        assert_eq!(
            i,
            Impact {
                paths: vec!["a.txt".into(), "docs/b.txt".into()],
                full_scan: false
            }
        );
        // Renamed inside the folder: the new and the old path.
        moved.parent = "D".into();
        assert_eq!(
            impact(&[moved], &dirs, Some("R"), known).paths,
            ["docs/now.txt", "docs/was.txt"]
        );
        // Deleted: only the id is reported, the DB knows the path.
        assert_eq!(impact(&[removed], &dirs, Some("R"), known).paths, ["docs/was.txt"]);
        assert!(impact(&[new_dir], &dirs, Some("R"), known).full_scan);
        assert!(
            !impact(&[other_dir], &dirs, Some("R"), known).full_scan,
            "a folder elsewhere in the drive is not ours"
        );
        // A deleted known folder: no folder facet, just its id.
        let gone_dir = Change {
            removed: true,
            ..ch("D", "", "")
        };
        assert!(impact(&[gone_dir], &dirs, Some("R"), known).full_scan);
    }

    #[test]
    fn dropbox_folder_entries_trigger_a_scan_and_deletions_do_not() {
        let file = Change {
            path: Some("a/b.txt".into()),
            ..Default::default()
        };
        let dir = Change {
            path: Some("a".into()),
            is_dir: true,
            ..Default::default()
        };
        let gone = Change {
            path: Some("a".into()),
            removed: true,
            ..Default::default()
        };
        let none = |_: &str| None;
        assert_eq!(
            impact(&[file, gone], &HashMap::new(), None, none).paths,
            ["a", "a/b.txt"]
        );
        assert!(impact(&[dir], &HashMap::new(), None, none).full_scan);
    }
}
