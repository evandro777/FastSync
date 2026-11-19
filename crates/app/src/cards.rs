//! The account cards: row states, the live status text, the shared card model and the tray labels.

use super::*;

pub(crate) fn row_state(acct: &Account, st: Option<&Status>) -> (i32, String, (i32, i32)) {
    match st {
        _ if acct.paused => (2, String::new(), (0, 0)),
        Some(Status::Idle(_)) => (0, String::new(), (0, 0)),
        Some(Status::Paused) => (2, String::new(), (0, 0)),
        Some(Status::Error(e)) => (3, e.clone(), (0, 0)),
        // The count rides in `deletes`, which only state 4 otherwise reads.
        Some(Status::Problems(e, n)) => (3, e.clone(), (*n as i32, 0)),
        // The raw paths, not translated, like a problem's message.
        Some(Status::Stopped(d, k, paths)) => (4, paths.join("\n"), (*d as i32, *k as i32)),
        Some(Status::Syncing(..)) | None => (1, String::new(), (0, 0)),
    }
}

/// (kind 0-6, file name, done, total, speed text) of a running sync; all empty when nothing is planned yet. Kinds 1-3
/// are transfers; 4 listing the cloud (done/total = top-level folders), 5 reading the local folder, 6 comparing
/// (done/total = files): those carry no file, and the "speed" is the elapsed time as `m:ss`.
pub(crate) fn live(st: Option<&Status>) -> (i32, String, i32, i32, String) {
    use fastsync_core::engine::{Kind, Phase};
    let Some(Status::Syncing(p, speed)) = st else {
        return Default::default();
    };
    let phase = match p.phase {
        Phase::Transfer => None,
        Phase::ListCloud => Some(4),
        Phase::ScanLocal => Some(5),
        Phase::Compare => Some(6),
    };
    if let Some(kind) = phase {
        let clock = format!("{}:{:02}", p.secs / 60, p.secs % 60);
        return (kind, String::new(), p.done as i32, p.total as i32, clock);
    }
    if p.total == 0 {
        return Default::default();
    }
    let kind = match p.kind {
        Kind::Up => 1,
        Kind::Down => 2,
        Kind::Remove => 3,
        Kind::Move => 7,
        Kind::CloudCopy => 8,
    };
    let file = p.path.rsplit('/').next().unwrap_or(&p.path).to_string();
    // A removal or move sends no bytes (the speed is rclone's, of other files), and rclone reports 0 between files.
    let speed = if kind == 3 || kind == 7 || kind == 8 || *speed < 1.0 {
        String::new()
    } else {
        format!("{}/s", human_size(*speed as u64))
    };
    (kind, file, p.done as i32, p.total as i32, speed)
}

pub(crate) fn overall(s: &State) -> Overall {
    let states: Vec<i32> = s.accounts.iter().map(|a| row_state(a, s.status.get(&a.id)).0).collect();
    if states.is_empty() {
        Overall::None
    } else if states.contains(&3) {
        Overall::Error
    } else if states.contains(&4) {
        Overall::Stopped
    } else if states.contains(&1) {
        Overall::Syncing
    } else if states.iter().all(|&x| x == 2) {
        Overall::Paused
    } else {
        Overall::Idle
    }
}

/// 0 normal, 1 from 80 % (amber), 2 from 95 % (red).
pub(crate) fn quota_level(percent: i32) -> i32 {
    match percent {
        95.. => 2,
        80.. => 1,
        _ => 0,
    }
}

/// Rebuilds the list from the shared state. UI thread only.
pub(crate) fn refresh(ui: &Main) {
    let (rows, missing, overall, tray, roots) = {
        let s = state();
        let rows: Vec<AccountRow> = s
            .accounts
            .iter()
            .map(|a| {
                let (state, detail, (deletes, known)) = row_state(a, s.status.get(&a.id));
                let (transfer, file, done, total, speed) = if state == 1 {
                    live(s.status.get(&a.id))
                } else {
                    Default::default()
                };
                // No line unless the service gave a limit.
                let (quota_used, quota_total, quota_percent) = match s.quota.get(&a.id) {
                    // Floored: "100 %" only when really full. `total` is never 0 (`rclone::quota`).
                    Some(&(used, Some(total))) => (
                        human_size(used),
                        human_size(total),
                        (used * 100 / total).min(999) as i32,
                    ),
                    _ => Default::default(),
                };
                AccountRow {
                    quota_used: quota_used.as_str().into(),
                    quota_total: quota_total.as_str().into(),
                    quota_percent,
                    quota_level: quota_level(quota_percent),
                    transfer,
                    file: file.as_str().into(),
                    done,
                    total,
                    speed: speed.as_str().into(),
                    id: a.id.as_str().into(),
                    provider: accounts::provider_name(&a.kind).into(),
                    folder: a.local.as_str().into(),
                    state,
                    detail: detail.as_str().into(),
                    deletes,
                    known,
                    download_only: a.download_only,
                }
            })
            .collect();
        let folders: Vec<(String, String)> = s
            .accounts
            .iter()
            .map(|a| (accounts::provider_name(&a.kind).to_string(), a.local.clone()))
            .collect();
        let roots: Vec<String> = folders.iter().map(|f| f.1.clone()).collect();
        (
            rows,
            s.rclone_error.is_some(),
            (overall(&s), folders),
            s.tray.clone(),
            roots,
        )
    };
    // Nemo's share action shows only inside the synced folders: it follows the account list (a string compare when
    // nothing changed).
    nemo::install(&roots);
    // One model for good, changed row by row: a new model each second (every status of a running sync) would recreate
    // every card, and a click or a text selection spanning that moment would be lost.
    let model = ROWS.with(Rc::clone);
    sync_rows(&model, rows);
    let model = ModelRc::from(model);
    if ui.get_accounts() != model {
        ui.set_accounts(model);
    }
    ui.set_rclone_missing(missing);
    if let Some(t) = tray {
        t.update(|t| (t.overall, t.folders) = overall);
    }
}

thread_local! {
    /// The account cards' model (UI thread only): `refresh` keeps it and only changes what differs.
    pub(crate) static ROWS: Rc<VecModel<AccountRow>> = Rc::default();
}

/// Makes `model` hold `rows`, touching only the rows that differ (and adding or dropping at the end).
pub(crate) fn sync_rows(model: &VecModel<AccountRow>, rows: Vec<AccountRow>) {
    use slint::Model;
    let old = model.row_count();
    for (i, row) in rows.iter().enumerate() {
        if i >= old {
            model.push(row.clone());
        } else if model.row_data(i).as_ref() != Some(row) {
            model.set_row_data(i, row.clone());
        }
    }
    while model.row_count() > rows.len() {
        model.remove(model.row_count() - 1);
    }
}

pub(crate) fn tray_labels(ui: &Main) -> tray::Labels {
    let tr = ui.global::<Tr>();
    tray::Labels {
        open: tr.invoke_tray_open().into(),
        sync: tr.invoke_tray_sync().into(),
        pause: tr.invoke_tray_pause().into(),
        resume: tr.invoke_tray_resume().into(),
        open_folder: tr.invoke_tray_open_folder().into(),
        quit: tr.invoke_tray_quit().into(),
        idle: tr.invoke_tray_idle().into(),
        syncing: tr.invoke_tray_syncing().into(),
        paused: tr.invoke_tray_paused().into(),
        error: tr.invoke_tray_error().into(),
        stopped: tr.invoke_tray_stopped().into(),
        none: tr.invoke_tray_none().into(),
    }
}
