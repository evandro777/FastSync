//! A desktop notification (`org.freedesktop.Notifications`), for what the user must not miss while the window sits
//! in the tray. Blocks on D-Bus: call it from a thread, never the UI thread.
use std::{collections::HashMap, sync::Mutex};
use zbus::{
    blocking::{Connection, Proxy},
    zvariant::Value,
};

/// What a click does: gets the action's key (`default` is a click on the notification itself).
type Handler = std::sync::Arc<dyn Fn(&str) + Send + Sync>;

/// Best effort: no session bus or no notification server means no notification, never an error. `actions` are
/// (key, label) buttons; `default` is also sent so a click on the body is reported (its label is not shown).
pub fn send(
    summary: &str,
    body: &str,
    actions: Vec<(&'static str, String)>,
    on_action: impl Fn(&str) + Send + Sync + 'static,
) {
    if let Err(e) = try_send(summary, body, actions, std::sync::Arc::new(on_action)) {
        eprintln!("no notification ({e})");
    }
}

/// Kept open: the desktop drops a notification whose sender leaves the bus at once (it showed up for `gdbus`, which
/// lingers, but not for a connection closed right after the call). Not opened until the first notification.
static BUS: Mutex<Option<Connection>> = Mutex::new(None);

/// The click handler of every notification still on screen, by the id the server gave it.
static HANDLERS: Mutex<Option<HashMap<u32, Handler>>> = Mutex::new(None);

/// Listens for clicks. The match rules are in place when this returns (before the first `Notify`), so no click is
/// missed; one thread per signal, for as long as the connection lives.
fn listen(bus: &Connection) -> zbus::Result<()> {
    let proxy = Proxy::new(
        bus,
        "org.freedesktop.Notifications",
        "/org/freedesktop/Notifications",
        "org.freedesktop.Notifications",
    )?;
    let clicks = proxy.receive_signal("ActionInvoked")?;
    let closed = proxy.receive_signal("NotificationClosed")?;
    std::thread::spawn(move || {
        for m in clicks {
            if let Ok((id, key)) = m.body().deserialize::<(u32, String)>() {
                // Cloned out, the lock is not held while it runs (a handler may send a notification itself). It
                // stays in the map for a second button; only `NotificationClosed` removes it, so a close during
                // the click cannot leave one behind.
                let h = HANDLERS
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .as_ref()
                    .and_then(|h| h.get(&id).cloned());
                if let Some(h) = h {
                    h(&key);
                }
            }
        }
    });
    std::thread::spawn(move || {
        for m in closed {
            if let Ok((id, _)) = m.body().deserialize::<(u32, u32)>() {
                HANDLERS
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .get_or_insert_default()
                    .remove(&id);
            }
        }
    });
    Ok(())
}

fn try_send(summary: &str, body: &str, actions: Vec<(&'static str, String)>, handler: Handler) -> zbus::Result<u32> {
    let mut bus = BUS.lock().unwrap_or_else(|e| e.into_inner());
    if bus.is_none() {
        let c = Connection::session()?;
        listen(&c)?;
        *bus = Some(c);
    }
    let mut flat = vec!["default".to_string(), String::new()];
    for (key, label) in actions {
        flat.extend([key.to_string(), label]);
    }
    let reply = bus.as_ref().expect("just set").call_method(
        Some("org.freedesktop.Notifications"),
        "/org/freedesktop/Notifications",
        Some("org.freedesktop.Notifications"),
        "Notify",
        &(
            "FastSync",
            0u32,
            // A stock theme icon: our own app icon is only in the theme once installed (not for a dev binary).
            "dialog-warning",
            summary,
            body,
            flat,
            HashMap::from([
                // Critical stays until dismissed: the user must look at it (a stop, or files kept twice).
                ("urgency", Value::from(2u8)),
                // Lets the shell tie the popup to FastSync's launcher entry.
                ("desktop-entry", Value::from("io.github.evandro777.FastSync")),
            ]),
            -1i32,
        ),
    )?;
    let id: u32 = reply.body().deserialize()?;
    HANDLERS
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .get_or_insert_default()
        .insert(id, handler);
    Ok(id)
}

#[cfg(test)]
mod tests {
    /// Shows a real notification: `cargo test notification -- --ignored`. Needs a desktop session.
    #[test]
    #[ignore]
    fn shows_a_real_notification() {
        super::try_send(
            "FastSync test",
            "If you see this, notifications work.",
            vec![],
            std::sync::Arc::new(|_| {}),
        )
        .unwrap();
    }
}
