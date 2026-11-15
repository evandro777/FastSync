//! A folder picker through the XDG desktop portal (`FileChooser`): the desktop's own dialog, also inside the Flatpak.
//! Blocks until the user answers: call it from a thread, never the UI thread.
use std::{collections::HashMap, path::PathBuf};
use zbus::{
    blocking::{Connection, Proxy},
    zvariant::{OwnedValue, Value},
};

/// `None` when cancelled, or when there is no portal (a missing one only costs the button, typing still works).
pub fn pick_folder(title: &str) -> Option<PathBuf> {
    pick(title).unwrap_or_else(|e| {
        eprintln!("no folder picker ({e})");
        None
    })
}

fn pick(title: &str) -> zbus::Result<Option<PathBuf>> {
    let bus = Connection::session()?;
    // The request's path is known from our bus name and the token, so the signal can be subscribed before the call.
    let sender = bus
        .unique_name()
        .map(|n| n.trim_start_matches(':').replace('.', "_"))
        .unwrap_or_default();
    let token = format!("fastsync{}", std::process::id());
    let request = Proxy::new(
        &bus,
        "org.freedesktop.portal.Desktop",
        format!("/org/freedesktop/portal/desktop/request/{sender}/{token}"),
        "org.freedesktop.portal.Request",
    )?;
    let mut responses = request.receive_signal("Response")?;
    bus.call_method(
        Some("org.freedesktop.portal.Desktop"),
        "/org/freedesktop/portal/desktop",
        Some("org.freedesktop.portal.FileChooser"),
        "OpenFile",
        &(
            "",
            title,
            HashMap::from([
                ("directory", Value::from(true)),
                ("handle_token", Value::from(token.as_str())),
            ]),
        ),
    )?;
    let Some(m) = responses.next() else { return Ok(None) };
    let (code, results): (u32, HashMap<String, OwnedValue>) = m.body().deserialize()?;
    if code != 0 {
        return Ok(None);
    }
    let uris: Vec<String> = results
        .get("uris")
        .and_then(|v| v.try_clone().ok())
        .and_then(|v| v.try_into().ok())
        .unwrap_or_default();
    Ok(uris
        .first()
        .and_then(|u| u.strip_prefix("file://"))
        .map(|p| PathBuf::from(unescape(p))))
}

/// Percent-decoding of a `file://` path.
fn unescape(s: &str) -> String {
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        let hex = (b[i] == b'%' && i + 2 < b.len())
            .then(|| {
                std::str::from_utf8(&b[i + 1..i + 3])
                    .ok()
                    .and_then(|h| u8::from_str_radix(h, 16).ok())
            })
            .flatten();
        match hex {
            Some(v) => {
                out.push(v);
                i += 3;
            }
            None => {
                out.push(b[i]);
                i += 1;
            }
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

#[cfg(test)]
mod tests {
    #[test]
    fn decodes_percent_escapes() {
        assert_eq!(super::unescape("/home/a%20b/%C3%A9"), "/home/a b/é");
        assert_eq!(super::unescape("/100%"), "/100%");
    }
}
