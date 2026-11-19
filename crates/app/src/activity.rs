//! The Activity dialog: reads one account's log (`fastsync_core::db::read_log`, its own read-only connection) on a
//! thread and words it for the UI. The full list stays here so a tab click only filters it, with no new read.
use crate::{LogRow, Main, Tr, human_size, state};
use fastsync_core::{
    accounts,
    db::{LogEntry, read_log},
    engine::stamp,
    rclone::now_secs,
};
use slint::{ComponentHandle, ModelRc, VecModel};
use std::cell::RefCell;

/// Entries read per open, and rows drawn at once (a ScrollView of thousands of rows would be slow).
const LOADED: usize = 1000;
const SHOWN: usize = 200;

thread_local! {
    /// The account being shown and everything read for it (UI thread only).
    static LOG: RefCell<(String, Vec<LogEntry>)> = RefCell::default();
}

fn kind_of(k: &str) -> i32 {
    match k {
        "up" => 0,
        "down" => 1,
        "trash" => 2,
        "delcloud" => 3,
        "conflict" => 4,
        "error" => 5,
        "stopped" => 6,
        "move" => 8,
        "movedir" => 9,
        _ => 7,
    }
}

/// A file that was moved, removed or kept (the "files" kinds): a size belongs to it and the last one is "last synced".
fn is_file(kind: i32) -> bool {
    kind <= 4 || kind == 8
}

fn ago(tr: &Tr, ts: i64) -> String {
    let s = (now_secs() - ts).max(0);
    match s {
        0..60 => tr.invoke_ago_now(),
        60..3600 => tr.invoke_ago_min((s / 60) as i32),
        3600..86_400 => tr.invoke_ago_hour((s / 3600) as i32),
        _ => tr.invoke_ago_day((s / 86_400) as i32),
    }
    .to_string()
}

/// The tab's filter: 0 all, 1 files moved, 2 problems. A conflict (4) is in both: a file was moved, and it needs a look.
fn shown(filter: i32, kind: i32) -> bool {
    filter == 0 || (filter == 1 && (is_file(kind) || kind == 9)) || (filter == 2 && (4..=6).contains(&kind))
}

#[test]
fn conflicts_are_problems_too() {
    assert!(shown(2, kind_of("conflict")) && shown(1, kind_of("conflict")));
    assert!(!shown(2, kind_of("up")));
}

/// A `stopped` row's text: which guard rule tripped (the row's path holds its code: "share" or "folder:<dir>"), then
/// the raw line. Rows written before the code existed have no path and show the raw line alone.
fn stop_text(tr: &Tr, e: &LogEntry) -> String {
    let why = match (e.path.as_str(), e.path.strip_prefix("folder:")) {
        ("share", _) => tr.invoke_log_stop_share(),
        ("cap", _) => tr.invoke_log_stop_cap(),
        (_, Some(dir)) => tr.invoke_log_stop_folder(dir.into()),
        _ => return e.msg.clone(),
    };
    format!("{why}\n{}", e.msg)
}

/// One row: the path (or what the entry is about), a caption "kind · size" and the time in its own column. A `scan` row holds values only
/// (path = mode, size = milliseconds, msg = "transfers problems"), worded here so it can be translated.
fn row(tr: &Tr, e: &LogEntry) -> LogRow {
    let kind = kind_of(&e.kind);
    let when = ago(tr, e.ts);
    if kind == 7 && e.msg == "start" {
        return LogRow {
            kind,
            title: tr.invoke_log_scan_start(),
            meta: e
                .path
                .strip_prefix("full:")
                .map_or("".into(), |w| tr.invoke_log_scan_why(w.into())),
            when: when.into(),
            detail: "".into(),
            path: "".into(),
        };
    }
    if kind == 7 {
        let mut n = e.msg.split(' ').map(|v| v.parse::<i32>().unwrap_or(0));
        let (changed, failed) = (n.next().unwrap_or(0), n.next().unwrap_or(0));
        let secs = format!("{:.1}", e.size.unwrap_or(0) as f64 / 1000.0);
        let meta = e.path.strip_prefix("full:").map(|w| tr.invoke_log_scan_why(w.into()));
        let title = if e.path.starts_with("full") {
            tr.invoke_log_scan_full(changed, failed, secs.into())
        } else {
            tr.invoke_log_scan_paths(changed, failed, secs.into())
        };
        return LogRow {
            kind,
            title,
            meta: meta.unwrap_or_default(),
            when: when.into(),
            detail: "".into(),
            path: "".into(),
        };
    }
    if kind == 6 {
        return LogRow {
            kind,
            title: tr.invoke_log_kind(kind),
            meta: "".into(),
            when: when.into(),
            detail: stop_text(tr, e).into(),
            path: "".into(),
        };
    }
    // With no path the label is the title, so the caption must not repeat it.
    let mut meta = Vec::new();
    if !e.path.is_empty() {
        meta.push(tr.invoke_log_kind(kind).to_string());
    }
    if let Some(s) = e.size.filter(|_| is_file(kind)) {
        meta.push(human_size(s));
    }
    LogRow {
        kind,
        title: if e.path.is_empty() {
            tr.invoke_log_kind(kind)
        } else {
            e.path.as_str().into()
        },
        meta: meta.join(" · ").into(),
        when: when.into(),
        detail: match kind {
            8 => tr.invoke_log_moved_from(e.msg.as_str().into()),
            9 => tr.invoke_log_renamed_from(e.msg.as_str().into()),
            5.. => e.msg.as_str().into(),
            _ => "".into(),
        },
        // Only kinds that leave a file on disk (sent, received, conflict, moved), not trashed or cloud deletes.
        path: if matches!(kind, 0 | 1 | 4 | 8 | 9) {
            e.path.as_str().into()
        } else {
            "".into()
        },
    }
}

/// Fills the dialog from the loaded entries and the chosen tab. UI thread only.
fn render(ui: &Main) {
    let tr = ui.global::<Tr>();
    let filter = ui.get_log_filter();
    LOG.with_borrow(|(_, all)| {
        // Newest first, so the first of each kind is the last one.
        let last_ok = all.iter().find(|e| is_file(kind_of(&e.kind)));
        let last_bad = all.iter().find(|e| matches!(e.kind.as_str(), "error" | "stopped"));
        ui.set_log_last_ok(last_ok.map_or("".into(), |e| tr.invoke_log_last_ok(ago(&tr, e.ts).into())));
        ui.set_log_last_bad(last_bad.map_or("".into(), |e| tr.invoke_log_last_bad(ago(&tr, e.ts).into())));
        ui.set_log_last_bad_msg(last_bad.map_or("".into(), |e| {
            if e.kind == "stopped" {
                stop_text(&tr, e)
            } else if e.path.is_empty() {
                e.msg.clone()
            } else {
                format!("{}: {}", e.path, e.msg)
            }
            .into()
        }));
        let visible: Vec<&LogEntry> = all.iter().filter(|e| shown(filter, kind_of(&e.kind))).collect();
        let rows: Vec<LogRow> = visible.iter().take(SHOWN).map(|e| row(&tr, e)).collect();
        // Plain text for a bug report: absolute UTC times, and the whole visible list, not only what is drawn.
        let text: String = visible
            .iter()
            .map(|e| {
                let size = e.size.map_or(String::new(), |s| format!(" {s} B"));
                format!("{} {} {}{size} {}\n", stamp(e.ts), e.kind, e.path, e.msg)
            })
            .collect();
        ui.set_log_copy_text(text.into());
        ui.set_log_rows(ModelRc::new(VecModel::from(rows)));
    });
}

/// Reads the log of `id` off the UI thread, then shows it. Ignored when another account was opened meanwhile.
fn load(ui: &Main, id: String) {
    ui.set_log_busy(true);
    let weak = ui.as_weak();
    std::thread::spawn(move || {
        let entries = accounts::account_dir(&id)
            .and_then(|d| {
                read_log(&d.join("state.db"), LOADED)
                    .inspect_err(|e| eprintln!("cannot read the activity log: {e}"))
                    .ok()
            })
            .unwrap_or_default();
        let _ = weak.upgrade_in_event_loop(move |ui| {
            if LOG.with_borrow(|(cur, _)| *cur != id) {
                return;
            }
            LOG.with_borrow_mut(|l| l.1 = entries);
            ui.set_log_busy(false);
            render(&ui);
        });
    });
}

/// Opens the dialog for an account; `filter` 2 lands on the problems.
pub fn open(ui: &Main, id: &str, filter: i32) {
    let Some(name) = state()
        .accounts
        .iter()
        .find(|a| a.id == id)
        .map(|a| accounts::provider_name(&a.kind).to_string())
    else {
        return;
    };
    LOG.with_borrow_mut(|l| *l = (id.to_string(), Vec::new()));
    ui.set_log_name(name.into());
    ui.set_log_filter(filter);
    ui.set_log_rows(ModelRc::new(VecModel::from(Vec::<LogRow>::new())));
    ui.set_log_last_ok("".into());
    ui.set_log_last_bad("".into());
    ui.set_log_last_bad_msg("".into());
    ui.set_log_copy_text("".into());
    ui.set_log_open(true);
    load(ui, id.to_string());
}

pub fn wire(ui: &Main) {
    ui.on_open_log({
        let weak = ui.as_weak();
        move |id| {
            if let Some(ui) = weak.upgrade() {
                open(&ui, &id, 0);
            }
        }
    });
    ui.on_log_show_file(|path| {
        let id = LOG.with_borrow(|l| l.0.clone());
        let local = state().accounts.iter().find(|a| a.id == id).map(|a| a.local.clone());
        if let Some(local) = local {
            crate::show_file(std::path::Path::new(&local).join(path.as_str()));
        }
    });
    ui.on_log_refresh({
        let weak = ui.as_weak();
        move || {
            if let Some(ui) = weak.upgrade() {
                let id = LOG.with_borrow(|l| l.0.clone());
                load(&ui, id);
            }
        }
    });
    ui.on_log_filter_changed({
        let weak = ui.as_weak();
        move |_| {
            if let Some(ui) = weak.upgrade() {
                render(&ui);
            }
        }
    });
}

/// Fixture for `FASTSYNC_PAGE=log`: no entries, a few, or a mixed list with a long problem. 0 empty, 1 single, 2 many.
#[cfg(debug_assertions)]
pub fn fixture(ui: &Main, mode: usize) {
    let now = now_secs();
    let e = |ago: i64, kind: &str, path: &str, size: Option<u64>, msg: &str| LogEntry {
        ts: now - ago,
        kind: kind.into(),
        path: path.into(),
        size,
        msg: msg.into(),
    };
    let all = [
        e(20, "up", "Documents/report-2026-q3.xlsx", Some(184_320), ""),
        e(25, "down", "Photos/holiday/IMG_2041.jpg", Some(4_404_019), ""),
        e(
            25,
            "move",
            "Photos/2026/IMG_2042.jpg",
            Some(3_100_220),
            "Photos/holiday/IMG_2042.jpg",
        ),
        e(26, "movedir", "Photos/2026", None, "Photos/holiday"),
        e(26, "scan", "full:guard", Some(541_551), "0 0"),
        e(28, "scan", "full:delete", None, "start"),
        e(30, "scan", "full:feed", Some(2100), "2 0"),
        e(32, "scan", "full:feed", None, "start"),
        e(40, "scan", "full:reset", Some(9400), "0 0"),
        e(
            3 * 3600,
            "error",
            "reports/2026/q3-budget-final-FINAL-v7.xlsx",
            None,
            "couldn't upload: googleapi: Error 403: The user's Drive storage quota has been exceeded (storageQuotaExceeded)",
        ),
        e(
            3 * 3600,
            "stopped",
            "folder:Photos/2026",
            None,
            "11 deletes (11 in the cloud) of 120 known; 11 paths in this run, 0 seen in the cloud",
        ),
        e(
            4 * 3600,
            "stopped",
            "share",
            None,
            "60 deletes (60 in the cloud) of 100 known; 60 paths in this run, 0 seen in the cloud",
        ),
        e(26 * 3600, "trash", "old/notes.txt", Some(812), ""),
        e(2 * 86_400, "conflict", "todo.md", Some(1203), ""),
        e(3 * 86_400, "delcloud", "build/app.bin", Some(52_428_800), ""),
    ];
    let count = [0, 3, 12][mode];
    LOG.with_borrow_mut(|l| *l = ("fixture".into(), all[..count].to_vec()));
    ui.set_log_name("Dropbox".into());
    // FASTSYNC_LOG_FILTER=2 shows the Problems tab (the list cannot be scrolled in a snapshot).
    ui.set_log_filter(
        std::env::var("FASTSYNC_LOG_FILTER")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(0),
    );
    ui.set_log_open(true);
    render(ui);
}
