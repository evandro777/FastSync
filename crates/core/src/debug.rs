//! `fastsync --debug` (or `FASTSYNC_DEBUG=1`): every action and decision as a timestamped line on stderr.
use std::sync::atomic::{AtomicBool, Ordering};

static ON: AtomicBool = AtomicBool::new(false);

pub fn enable() {
    ON.store(true, Ordering::Relaxed);
}

pub fn enabled() -> bool {
    ON.load(Ordering::Relaxed) || std::env::var_os("FASTSYNC_DEBUG").is_some_and(|v| !v.is_empty() && v != "0")
}

/// UTC (no time-zone code in the tree): `2026-11-16T14:03:09.123Z`.
fn stamp() -> String {
    let d = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    format!(
        "{}.{:03}Z",
        crate::engine::stamp(d.as_secs() as i64).replace(' ', "T"),
        d.subsec_millis()
    )
}

/// The message is built only when debugging is on.
pub fn log(msg: impl FnOnce() -> String) {
    if enabled() {
        eprintln!("{} {}", stamp(), msg());
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn stamp_has_the_iso_shape() {
        let s = super::stamp();
        assert_eq!((s.len(), &s[4..5], &s[10..11], &s[19..20]), (24, "-", "T", "."));
        assert!(s.starts_with("20") && s.ends_with('Z'));
    }
}
