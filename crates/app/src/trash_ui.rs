//! The trash in Settings: size, emptying and the age purge.

use super::*;

/// Bumped by every trash job: a count that started before an emptying must not show its stale numbers afterwards.
pub(crate) static TRASH_GEN: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// The age limit of the trash in days (0 = keep forever), for the purge thread: it must not touch the UI thread's
/// `Settings`. Set at start-up and whenever Settings changes it.
pub(crate) static TRASH_DAYS: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(90);

/// Deletes the trash batches older than the limit in every account's folder. Errors only go to stderr: what stays is
/// tried again on the next pass.
pub(crate) fn purge_all() {
    let days = TRASH_DAYS.load(std::sync::atomic::Ordering::SeqCst);
    // The lock is not held while the disk is busy.
    let roots: Vec<String> = state().accounts.iter().map(|a| a.local.clone()).collect();
    for root in roots {
        if let Err(e) = purge_trash(Path::new(&root), days) {
            eprintln!("cannot clean the trash of {root}: {e}");
        }
    }
}

/// "512 B", "3.4 KB", "340.2 MB". 1024-based, one decimal from 10 units up.
/// shortcut: always a "." decimal point; use the locale's separator if a translation asks for it.
pub(crate) fn human_size(bytes: u64) -> String {
    let mut v = bytes as f64;
    let mut unit = 0;
    while v >= 1024.0 && unit < 4 {
        v /= 1024.0;
        unit += 1;
    }
    match unit {
        0 => format!("{bytes} B"),
        _ => format!("{v:.1} {}", ["", "KB", "MB", "GB", "TB"][unit]),
    }
}

/// Counts the trash of every account (and empties it first when `empty`) off the UI thread, then shows the result.
pub(crate) fn trash_job(ui: &Main, empty: bool) {
    use std::sync::atomic::Ordering::SeqCst;
    let generation = TRASH_GEN.fetch_add(1, SeqCst) + 1;
    ui.set_trash_busy(true);
    ui.set_trash_error("".into());
    let weak = ui.as_weak();
    std::thread::spawn(move || {
        let roots: Vec<String> = state().accounts.iter().map(|a| a.local.clone()).collect();
        let mut error = String::new();
        let (mut files, mut bytes) = (0, 0);
        for root in &roots {
            let root = Path::new(root);
            if empty
                && let Err(e) = empty_trash(root)
                && error.is_empty()
            {
                error = format!("{}: {e}", root.display());
            }
            let (f, b) = trash_usage(root);
            files += f;
            bytes += b;
        }
        let _ = weak.upgrade_in_event_loop(move |ui| {
            if TRASH_GEN.load(SeqCst) != generation {
                return;
            }
            ui.set_trash_files(files.min(i32::MAX as u64) as i32);
            ui.set_trash_size(human_size(bytes).into());
            ui.set_trash_error(error.chars().take(500).collect::<String>().into());
            ui.set_trash_busy(false);
        });
    });
}
