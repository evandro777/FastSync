//! `fastsync --debug` (or `FASTSYNC_DEBUG=1`): every action and decision as a timestamped line on stderr.
use std::sync::atomic::{AtomicBool, Ordering};

static ON: AtomicBool = AtomicBool::new(false);

pub fn enable() {
    ON.store(true, Ordering::Relaxed);
}

pub fn enabled() -> bool {
    // The environment is read once: this is asked for every file of a plan.
    static ENV: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    ON.load(Ordering::Relaxed)
        || *ENV.get_or_init(|| std::env::var_os("FASTSYNC_DEBUG").is_some_and(|v| !v.is_empty() && v != "0"))
}

/// UTC (no time-zone code in the tree), in rclone's own shape (`rclone --log-format date,time,UTC`) so the two logs
/// read as one: `2026/11/16 14:03:09`.
fn stamp() -> String {
    let d = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    crate::engine::stamp(d.as_secs() as i64).replace('-', "/")
}

/// The message is built only when debugging is on. `DEBUG :` is rclone's level column.
pub fn log(msg: impl FnOnce() -> String) {
    if enabled() {
        eprintln!("{} DEBUG : {}", stamp(), msg());
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn stamp_has_rclones_shape() {
        let s = super::stamp();
        assert_eq!(
            (s.len(), &s[4..5], &s[7..8], &s[10..11], &s[13..14]),
            (19, "/", "/", " ", ":")
        );
        assert!(s.starts_with("20"));
    }
}
