//! "View on web": the provider's own page for a synced file or folder. No provider documents a link that opens its
//! share window, so the page is where its Share button is one click away.
use std::sync::Arc;

use crate::{
    accounts::Account,
    delta::{self, Token},
    rclone::Rclone,
};

/// Percent-encodes one URL path or query part: everything but unreserved characters.
fn enc(s: &str) -> String {
    s.bytes()
        .map(|b| match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => (b as char).to_string(),
            _ => format!("%{b:02X}"),
        })
        .collect()
}

/// Dropbox needs no call: a file opens in the preview of its folder, a folder is browsed. `path` is from the root of
/// the Dropbox.
pub fn dropbox_url(path: &str, is_dir: bool) -> String {
    let path = path.trim_matches('/');
    let (parent, name) = path.rsplit_once('/').unwrap_or(("", path));
    let enc_path = |p: &str| {
        p.split('/')
            .filter(|c| !c.is_empty())
            .map(enc)
            .collect::<Vec<_>>()
            .join("/")
    };
    if is_dir || name.is_empty() {
        format!("https://www.dropbox.com/home/{}", enc_path(path))
    } else {
        format!(
            "https://www.dropbox.com/home/{}?preview={}",
            enc_path(parent),
            enc(name)
        )
    }
}

/// The web page of `remote` (the path inside the account's cloud). Drive, OneDrive and MEGA need
/// the provider's id of the item, which rclone's `stat` reports. MEGA opens the folder holding the file.
pub fn page_url(rc: &Arc<Rclone>, acct: &Account, remote: &str) -> Result<String, String> {
    let fs = format!("{}:", acct.id);
    let item = rc.stat(&fs, remote)?.ok_or("not in the cloud yet")?;
    let id = || {
        Some(item.id.as_str())
            .filter(|i| !i.is_empty())
            .ok_or_else(|| format!("{} gave no id", acct.kind))
    };
    match acct.kind.as_str() {
        "dropbox" => Ok(dropbox_url(remote, item.is_dir)),
        "drive" => Ok(format!("https://drive.google.com/open?id={}", enc(id()?))),
        "onedrive" => {
            let (rc, name) = (rc.clone(), acct.id.clone());
            let token: Token = Box::new(move || rc.token(&name).map(|t| t.0));
            delta::onedrive_web_url(token, id()?)
        }
        "mega" => {
            let folder = if item.is_dir {
                remote
            } else {
                remote.rsplit_once('/').map_or("", |p| p.0)
            };
            match rc.stat(&fs, folder)? {
                Some(dir) if !dir.id.is_empty() && !folder.is_empty() => {
                    Ok(format!("https://mega.nz/fm/{}", enc(&dir.id)))
                }
                _ => Ok("https://mega.nz/fm".into()),
            }
        }
        other => Err(format!("no web page known for {other}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dropbox_pages() {
        assert_eq!(
            dropbox_url("Work/Q3 report.pdf", false),
            "https://www.dropbox.com/home/Work?preview=Q3%20report.pdf"
        );
        assert_eq!(
            dropbox_url("a.txt", false),
            "https://www.dropbox.com/home/?preview=a.txt"
        );
        assert_eq!(
            dropbox_url("/Fotos/Férias/", true),
            "https://www.dropbox.com/home/Fotos/F%C3%A9rias"
        );
        assert_eq!(
            dropbox_url("a&b/c#d?.txt", false),
            "https://www.dropbox.com/home/a%26b?preview=c%23d%3F.txt"
        );
    }
}
