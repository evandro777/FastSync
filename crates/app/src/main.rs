use fastsync_core::{
    accounts::{self, Account},
    engine::{empty_trash, excluded, purge_trash, toggle_excluded, trash_usage},
    rclone::{Rclone, exclusions, is_embedded_client},
    runner::{self, Runner, Status},
    settings::{MAX_TRASH_DAYS, MAX_WALK_MINUTES, Settings},
};
use slint::{ComponentHandle, ModelRc, VecModel};
use std::{
    cell::RefCell,
    collections::{HashMap, HashSet},
    path::{Path, PathBuf},
    rc::Rc,
    sync::{Arc, LazyLock, Mutex, MutexGuard},
};

slint::include_modules!();

mod account;
mod activity;
mod autostart;
mod cards;
mod emblem;
#[cfg(debug_assertions)]
mod fixtures;
mod instance;
mod nemo;
mod notify;
mod open;
mod picker;
mod portal;
mod trash_ui;
mod tray;

use account::*;
use cards::*;
use open::*;
use picker::*;
use trash_ui::*;
use tray::Overall;

/// Everything the worker threads, the tray and the UI thread share. Locked briefly, never across a slow call.
struct State {
    accounts: Vec<Account>,
    runners: HashMap<String, Runner>,
    status: HashMap<String, Status>,
    rc: Option<Arc<Rclone>>,
    /// Bumped by every login attempt and by Cancel: a login that finishes under an old number is discarded.
    login_gen: u64,
    /// Edit: the own client `(id, secret)` of the saved login (empty = shared or built-in; None = not read yet), so
    /// Save can tell whether the fields changed and a re-authentication is due.
    edit_client: Option<(String, String)>,
    rclone_error: Option<String>,
    tray: Option<ksni::blocking::Handle<tray::SyncTray>>,
    /// Accounts whose runner is being started (rclone may still be starting).
    starting: HashSet<String>,
    /// Accounts whose current stop by the delete guard was already announced with a notification.
    warned: HashSet<String>,
    /// Cloud usage per account id: (used, total) bytes, from `operations/about`. A failed read keeps the old value.
    quota: HashMap<String, (u64, Option<u64>)>,
}

static STATE: LazyLock<Mutex<State>> = LazyLock::new(|| {
    Mutex::new(State {
        accounts: Vec::new(),
        runners: HashMap::new(),
        status: HashMap::new(),
        rc: None,
        login_gen: 0,
        edit_client: None,
        rclone_error: None,
        tray: None,
        starting: HashSet::new(),
        warned: HashSet::new(),
        quota: HashMap::new(),
    })
});

fn state() -> MutexGuard<'static, State> {
    // A panicked worker must not take the whole app down with a poisoned lock.
    STATE.lock().unwrap_or_else(|e| e.into_inner())
}

/// The shared rclone process, started on first use. Started outside the lock: it takes a moment.
fn rclone() -> Result<Arc<Rclone>, String> {
    if let Some(rc) = state().rc.clone() {
        return Ok(rc);
    }
    let result = accounts::rclone_config()
        .ok_or_else(|| "no data folder".to_string())
        .and_then(|cfg| {
            fastsync_core::files::create_private_dir(cfg.parent().unwrap_or(Path::new(".")))
                .map_err(|e| e.to_string())?;
            Rclone::start(&cfg)
        });
    let mut s = state();
    match result {
        Ok(rc) => Ok(s.rc.get_or_insert(Arc::new(rc)).clone()),
        Err(e) => {
            s.rclone_error = Some(e.clone());
            Err(e)
        }
    }
}

/// Starts the account's runner unless one is running or being started: two workers on one DB would race, and a
/// replaced `Runner` cannot be stopped any more (its timers keep it alive).
fn start_account(acct: &Account, ui: slint::Weak<Main>) {
    {
        let mut s = state();
        if s.runners.contains_key(&acct.id) || !s.starting.insert(acct.id.clone()) {
            return;
        }
    }
    let result = rclone();
    let mut s = state();
    s.starting.remove(&acct.id);
    let rc = match result {
        Ok(rc) => rc,
        Err(e) => {
            s.status.insert(acct.id.clone(), Status::Error(e));
            return;
        }
    };
    let id = acct.id.clone();
    let provider = accounts::provider_name(&acct.kind).to_string();
    let conflict_ui = ui.clone();
    // Spawning is quick (the worker is a thread), so it stays under the lock: no gap where a second start slips in.
    let runner = runner::spawn(
        acct.clone(),
        rc,
        move |st| {
            let stopped = match &st {
                Status::Stopped(d, k, _) => Some((*d as i32, *k as i32)),
                _ => None,
            };
            let clean = matches!(st, Status::Idle(_));
            let mut s = state();
            s.status.insert(id.clone(), st);
            // One notification per stop, not one per scan: a clean run (the user answered the card) re-arms it.
            let warn = match stopped {
                Some(counts) => s.warned.insert(id.clone()).then_some(counts),
                None => {
                    if clean {
                        s.warned.remove(&id);
                    }
                    None
                }
            };
            drop(s);
            let provider = provider.clone();
            let _ = ui.upgrade_in_event_loop(move |ui| {
                refresh(&ui);
                if let Some((deletes, known)) = warn {
                    // The words come from `Tr` (UI thread); the D-Bus call blocks, so it gets its own thread.
                    let tr = ui.global::<Tr>();
                    let title = tr.invoke_notify_stopped_title().to_string();
                    let body = tr
                        .invoke_notify_stopped(deletes, known, provider.as_str().into())
                        .to_string();
                    // A click brings the window forward: the card says what to do.
                    let weak = ui.as_weak();
                    std::thread::spawn(move || {
                        notify::send(&title, &body, vec![], move |_| {
                            let _ = weak.upgrade_in_event_loop(|ui| instance::raise(ui.window()));
                        })
                    });
                }
            });
        },
        {
            let provider = accounts::provider_name(&acct.kind).to_string();
            let (id, local) = (acct.id.clone(), acct.local.clone());
            move |n, first| {
                let (provider, id, local) = (provider.clone(), id.clone(), local.clone());
                let _ = conflict_ui.upgrade_in_event_loop(move |ui| {
                    let tr = ui.global::<Tr>();
                    let title = tr.invoke_notify_conflict_title().to_string();
                    let body = if n == 1 {
                        tr.invoke_notify_conflict(provider.as_str().into(), first.as_str().into())
                    } else {
                        tr.invoke_notify_conflicts(n as i32, provider.as_str().into(), first.as_str().into())
                    }
                    .to_string();
                    // One file can be shown; for several the log has a button per row.
                    let actions = if n == 1 {
                        vec![("show", tr.invoke_notify_show_file().to_string())]
                    } else {
                        vec![]
                    };
                    let weak = ui.as_weak();
                    std::thread::spawn(move || {
                        notify::send(&title, &body, actions, move |key| {
                            let (id, file) = (id.clone(), Path::new(&local).join(&first));
                            if key == "show" {
                                show_file(file);
                                return;
                            }
                            let _ = weak.upgrade_in_event_loop(move |ui| {
                                instance::raise(ui.window());
                                activity::open(&ui, &id, 2);
                            });
                        })
                    });
                });
            }
        },
    );
    s.runners.insert(acct.id.clone(), runner);
}

fn main() {
    if std::env::args().skip(1).any(|a| a == "-V" || a == "--version") {
        return println!("fastsync {}", env!("CARGO_PKG_VERSION"));
    }
    if std::env::args().skip(1).any(|a| a == "--debug") {
        fastsync_core::debug::enable();
    }
    let file_cmd = file_command();
    let command = file_cmd.clone().unwrap_or_else(|| "show".to_owned());
    // Fixtures (scripts/ui-snapshot.sh) never touch a real running instance. Debug builds only, like the fixtures.
    #[cfg(debug_assertions)]
    let fixture_run = fixtures::mode().is_some();
    #[cfg(not(debug_assertions))]
    let fixture_run = false;
    // One window: a copy is already open, so it comes to the front and this one leaves before setting anything up.
    if !fixture_run && instance::send(&command) {
        return;
    }
    // Forced so the 100 MB GL renderer is never pulled in by accident.
    slint::BackendSelector::new()
        .backend_name("winit".into())
        .renderer_name("software".into())
        .select()
        .expect("cannot start the Slint software renderer");

    let settings = Rc::new(RefCell::new(Settings::load()));
    let ui = Main::new().unwrap();
    // Take the socket now that the window exists; a later start sends "show" and this window is raised.
    if !fixture_run {
        let weak = ui.as_weak();
        let on_command = move |command: String| {
            if !file_action(&command, weak.clone()) {
                let _ = weak.upgrade_in_event_loop(|ui| instance::raise(ui.window()));
            }
        };
        if instance::claim(&command, on_command) {
            return;
        }
    }
    {
        let s = settings.borrow();
        select_language(&language(&s));
        ui.set_language_index(LANGUAGES.iter().position(|l| *l == s.language).unwrap_or(0) as i32);
        ui.set_color_scheme_index(theme_index(&s.theme));
        ui.set_file_emblems(s.file_emblems);
        emblem::enable(s.file_emblems);
        #[cfg(debug_assertions)]
        let real = fixtures::mode().is_none();
        #[cfg(not(debug_assertions))]
        let real = true;
        if real {
            ui.set_autostart(autostart::is_enabled());
        }
        ui.set_trash_days(s.trash_days as i32);
        TRASH_DAYS.store(s.trash_days, std::sync::atomic::Ordering::SeqCst);
        ui.set_walk_minutes(s.walk_minutes as i32);
        ui.set_walk_fallback_minutes(s.walk_fallback_minutes as i32);
        runner::WALK_MINUTES.store(s.walk_minutes, std::sync::atomic::Ordering::Relaxed);
        runner::WALK_FALLBACK_MINUTES.store(s.walk_fallback_minutes, std::sync::atomic::Ordering::Relaxed);
    }
    // Fixture runs must not touch the real system.
    if !fixture_run {
        std::thread::spawn(emblem::install);
    }
    ui.set_version(env!("CARGO_PKG_VERSION").into());
    ui.set_add_local(accounts::default_local("dropbox").into());

    let mut has_tray = false;
    if !fixture_run {
        state().accounts = accounts::load();
        emblem::serve(Arc::new(emblem::AppSource::default()));
        // Old trash goes in the background: a pass now, then every few hours (the limit is counted in days).
        std::thread::spawn(|| {
            loop {
                purge_all();
                std::thread::sleep(std::time::Duration::from_secs(6 * 3600));
            }
        });
        // Cloud usage for the cards: a pass now, then every 15 minutes. shortcut: an account added meanwhile shows
        // its line at the next pass.
        let quota_weak = ui.as_weak();
        std::thread::spawn(move || {
            loop {
                let accts = state().accounts.clone();
                if !accts.is_empty()
                    && let Ok(rc) = rclone()
                {
                    for a in &accts {
                        if let Ok(q) = rc.about(&remote_fs(a)) {
                            state().quota.insert(a.id.clone(), q);
                        }
                    }
                    let _ = quota_weak.upgrade_in_event_loop(|ui| refresh(&ui));
                }
                std::thread::sleep(std::time::Duration::from_secs(15 * 60));
            }
        });
        let weak = ui.as_weak();
        let (w_open, w_quit) = (weak.clone(), weak.clone());
        let actions = tray::Actions {
            open: Arc::new(move || {
                let _ = w_open.upgrade_in_event_loop(|ui| instance::raise(ui.window()));
            }),
            // A plain rescan: a held account ignores it, only the card's own buttons answer a stopped run.
            sync: Arc::new(|| state().runners.values().for_each(Runner::rescan)),
            pause: Arc::new({
                let weak = weak.clone();
                move || {
                    let _ = weak.upgrade_in_event_loop(|ui| toggle_all_paused(&ui));
                }
            }),
            quit: Arc::new(move || {
                let _ = w_quit.upgrade_in_event_loop(|_| quit());
            }),
        };
        let handle = tray::spawn(tray_labels(&ui), actions);
        has_tray = handle.is_some();
        state().tray = handle;
        refresh(&ui);
        // rclone and the runners start off the UI thread: a missing rclone or a slow disk must not delay the window.
        let weak = ui.as_weak();
        std::thread::spawn(move || {
            let accounts = state().accounts.clone();
            for a in &accounts {
                start_account(a, weak.clone());
            }
            let _ = weak.upgrade_in_event_loop(|ui| refresh(&ui));
        });
        // Started by the file manager's action with no copy running: the accounts are loaded now, so it can go ahead.
        if let Some(command) = file_cmd {
            file_action(&command, ui.as_weak());
        }
    }
    // With fixtures (scripts/ui-snapshot.sh) nothing is started, so the real system is never read or changed.
    // Debug builds only: a release build must not have a hidden fake-data switch.
    #[cfg(debug_assertions)]
    if let Some(mode) = fixtures::mode() {
        fixtures::apply(&ui, mode);
    }

    ui.on_sync_now(|id| {
        if let Some(r) = state().runners.get(id.as_str()) {
            r.sync_now();
        }
    });
    // The two answers to a stopped run; the runner ignores them when nothing is held.
    ui.on_accept_deletes(|id| {
        if let Some(r) = state().runners.get(id.as_str()) {
            r.resolve(fastsync_core::runner::Decision::Delete);
        }
    });
    ui.on_restore_files(|id| {
        if let Some(r) = state().runners.get(id.as_str()) {
            r.resolve(fastsync_core::runner::Decision::Restore);
        }
    });
    ui.on_toggle_pause({
        let weak = ui.as_weak();
        move |id| {
            let start = {
                let mut s = state();
                let Some(a) = s.accounts.iter_mut().find(|a| a.id == id.as_str()) else {
                    return;
                };
                a.paused = !a.paused;
                let acct = a.clone();
                let start = if let Some(r) = s.runners.get(id.as_str()) {
                    r.pause(acct.paused);
                    None
                } else {
                    // Saved paused by the folder picker's Cancel and never started: Resume starts it.
                    (!acct.paused).then_some(acct)
                };
                accounts::save(&s.accounts);
                start
            };
            if let Some(acct) = start {
                let weak = weak.clone();
                std::thread::spawn(move || start_account(&acct, weak));
            }
            if let Some(ui) = weak.upgrade() {
                refresh(&ui);
            }
        }
    });
    ui.on_open_folder(|id| {
        let folder = state()
            .accounts
            .iter()
            .find(|a| a.id == id.as_str())
            .map(|a| a.local.clone());
        if let Some(f) = folder {
            xdg_open(f);
        }
    });
    activity::wire(&ui);
    ui.on_open_web(|id| {
        let url = state()
            .accounts
            .iter()
            .find(|a| a.id == id.as_str())
            .map(|a| web_url(&a.kind));
        if let Some(Some(u)) = url {
            xdg_open(u.to_string());
        }
    });
    ui.on_remove_account({
        let weak = ui.as_weak();
        move |id| {
            let (rc, id, local, runner) = {
                let mut s = state();
                let runner = s.runners.remove(id.as_str());
                let local = s.accounts.iter().find(|a| a.id == id.as_str()).map(|a| a.local.clone());
                s.accounts.retain(|a| a.id != id.as_str());
                s.status.remove(id.as_str());
                accounts::save(&s.accounts);
                (s.rc.clone(), id.to_string(), local.unwrap_or_default(), runner)
            };
            // Only the link is removed: the local files stay where they are. The token and the DB go.
            std::thread::spawn(move || {
                // A run in progress ends at its next file or folder (`stop` raises the cancel flag): wait for it, or
                // it keeps moving files for a removed account, then its last status comes back.
                if let Some(r) = runner {
                    r.stop_and_wait();
                }
                state().status.remove(id.as_str());
                // The trash of a removed account is no longer counted or aged by anything: drop what is already old.
                // Only old batches: the same folder may already belong to a new account, which is writing into
                // `.fastsync` (so neither `tmp` nor the folder itself is removed here).
                if !local.is_empty() {
                    let _ = purge_trash(Path::new(&local), TRASH_DAYS.load(std::sync::atomic::Ordering::SeqCst));
                }
                if let Some(rc) = rc {
                    rc.config_delete(&id);
                }
                if let Some(dir) = accounts::account_dir(&id) {
                    let _ = std::fs::remove_dir_all(dir);
                }
            });
            if let Some(ui) = weak.upgrade() {
                refresh(&ui);
            }
        }
    });
    ui.on_edit_account({
        let weak = ui.as_weak();
        move |id| {
            let Some(ui) = weak.upgrade() else { return };
            let Some(a) = state().accounts.iter().find(|a| a.id == id.as_str()).cloned() else {
                return;
            };
            // The id first: it tells the kind-changed handler not to touch the folder.
            ui.set_add_edit_id(a.id.as_str().into());
            ui.set_add_kind(accounts::PROVIDERS.iter().position(|(k, _)| *k == a.kind).unwrap_or(0) as i32);
            ui.set_add_local(a.local.as_str().into());
            ui.set_add_download_only(a.download_only);
            ui.set_add_error("".into());
            ui.set_add_open(true);
            // The saved login fills the fields (MEGA's email, an own client ID): off the UI thread, as rclone may
            // still be starting. Nothing is shown if the user has typed meanwhile.
            let weak = weak.clone();
            std::thread::spawn(move || {
                let Ok(cfg) = rclone().and_then(|rc| rc.config_get(&a.id)) else {
                    return;
                };
                let get = |k: &str| cfg[k].as_str().unwrap_or("").to_owned();
                let (user, client_id, client_secret) = (get("user"), get("client_id"), get("client_secret"));
                let _ = weak.upgrade_in_event_loop(move |ui| {
                    if ui.get_add_edit_id() != a.id.as_str() {
                        return;
                    }
                    let own = !client_id.is_empty() && !is_embedded_client(&a.kind, &client_id);
                    state().edit_client = Some(if own {
                        (client_id.clone(), client_secret.clone())
                    } else {
                        Default::default()
                    });
                    if a.kind == "mega" {
                        if ui.get_add_email().is_empty() {
                            ui.set_add_email(user.into());
                        }
                    } else if !client_id.is_empty()
                        && !is_embedded_client(&a.kind, &client_id)
                        && ui.get_add_client_id().is_empty()
                    {
                        ui.set_add_client_id(client_id.into());
                        ui.set_add_client_secret(client_secret.into());
                        ui.set_add_own_client(true);
                    }
                });
            });
        }
    });
    ui.on_folders_toggle({
        let weak = ui.as_weak();
        move |i| {
            let Some(ui) = weak.upgrade() else { return };
            PICKER.with_borrow_mut(|p| {
                let Some(path) = usize::try_from(i)
                    .ok()
                    .and_then(|i| p.rows.get(i))
                    .map(|r| r.path.clone())
                else {
                    return;
                };
                if under_unchecked(&path, &p.excluded) {
                    return;
                }
                let checked = !excluded(&path, &p.excluded);
                p.excluded = toggle_excluded(&p.excluded, &path, !checked);
            });
            show_picker(&ui);
        }
    });
    ui.on_folders_expand({
        let weak = ui.as_weak();
        move |i| {
            let Some(ui) = weak.upgrade() else { return };
            // Collapsing is instant; expanding returns what to list.
            let load = PICKER.with_borrow_mut(|p| {
                let i = usize::try_from(i).ok().filter(|i| *i < p.rows.len())?;
                if p.rows[i].loading || !p.rows[i].has_children {
                    return None;
                }
                if p.rows[i].expanded {
                    let depth = p.rows[i].depth;
                    let end = p.rows[i + 1..]
                        .iter()
                        .position(|r| r.depth <= depth)
                        .map_or(p.rows.len(), |n| i + 1 + n);
                    p.rows.drain(i + 1..end);
                    p.rows[i].expanded = false;
                    return None;
                }
                p.rows[i].loading = true;
                Some((p.id.clone(), p.fs.clone(), p.kind.clone(), p.rows[i].path.clone()))
            });
            show_picker(&ui);
            let Some((id, fs, kind, path)) = load else { return };
            ui.set_folders_error("".into());
            let weak = ui.as_weak();
            std::thread::spawn(move || {
                let result = rclone().and_then(|rc| rc.list_dirs(&fs, &path, &exclusions(&kind)));
                let _ = weak.upgrade_in_event_loop(move |ui| {
                    let new_paths = PICKER.with_borrow_mut(|p| {
                        // The picker may have been closed or reopened for another account meanwhile.
                        let i = (p.id == id)
                            .then(|| p.rows.iter().position(|r| r.path == path))
                            .flatten()?;
                        p.rows[i].loading = false;
                        let dirs = result.as_ref().ok()?;
                        if dirs.is_empty() {
                            // The probe said yes but the folder is empty now (changed meanwhile): drop the arrow.
                            p.rows[i].has_children = false;
                            return None;
                        }
                        // `expanded` already: an older listing of the same folder got here first (expand, collapse
                        // its parent, expand again): adding the children twice would duplicate them.
                        if p.rows[i].expanded {
                            return None;
                        }
                        p.rows[i].expanded = true;
                        let depth = p.rows[i].depth + 1;
                        p.rows.splice(i + 1..i + 1, dir_rows(dirs.clone(), depth));
                        Some(dirs.clone())
                    });
                    if let Some(paths) = new_paths {
                        probe(ui.as_weak(), id, fs, kind, paths);
                    }
                    if let Err(e) = result {
                        let msg = ui
                            .global::<Tr>()
                            .invoke_error_folders(e.chars().take(500).collect::<String>().into());
                        ui.set_folders_error(msg);
                    }
                    show_picker(&ui);
                });
            });
        }
    });
    // No cancel handler: after an Add the account stays saved, paused (Resume starts it); after an Edit nothing changed.
    ui.on_folders_save({
        let weak = ui.as_weak();
        move || {
            let Some(ui) = weak.upgrade() else { return };
            if ui.get_folders_busy() {
                return;
            }
            let (id, local, adding, excluded, download_only) = PICKER.with_borrow(|p| {
                (
                    p.id.clone(),
                    p.local.clone(),
                    p.adding,
                    p.excluded.clone(),
                    p.download_only,
                )
            });
            ui.set_folders_error("".into());
            ui.set_folders_busy(true);
            let weak = ui.as_weak();
            std::thread::spawn(move || {
                apply_folders(&id, &local, adding, excluded, download_only, weak.clone());
                let _ = weak.upgrade_in_event_loop(|ui| {
                    ui.set_folders_busy(false);
                    ui.set_folders_open(false);
                    refresh(&ui);
                });
            });
        }
    });
    ui.on_add_kind_changed({
        let weak = ui.as_weak();
        move |index| {
            let Some(ui) = weak.upgrade() else { return };
            let local = ui.get_add_local();
            // Follow the provider's default folder name, unless the user typed their own (never when editing).
            let untouched = ui.get_add_edit_id().is_empty()
                && (local.is_empty()
                    || accounts::PROVIDERS
                        .iter()
                        .any(|(k, _)| accounts::default_local(k) == local.as_str()));
            if let (true, Some((kind, _))) = (
                untouched,
                usize::try_from(index).ok().and_then(|i| accounts::PROVIDERS.get(i)),
            ) {
                ui.set_add_local(accounts::default_local(kind).into());
            }
        }
    });
    ui.on_add_cancel({
        let weak = ui.as_weak();
        move || {
            state().login_gen += 1;
            // Frees rclone's auth server (port 53682) at once, or the next login says "address already in use".
            let rc = state().rc.clone();
            if let Some(rc) = rc {
                std::thread::spawn(move || rc.cancel_login());
            }
            if let Some(ui) = weak.upgrade() {
                ui.set_add_busy(false);
                ui.set_add_auth_busy(false);
                ui.set_add_error("".into());
                ui.set_add_password("".into());
                ui.set_add_client_secret("".into());
                // An edit must not leave its folder and service in the next Add.
                if !ui.get_add_edit_id().is_empty() {
                    reset_add(&ui);
                }
            }
        }
    });
    ui.on_add_submit({
        let weak = ui.as_weak();
        move || {
            let Some(ui) = weak.upgrade() else { return };
            let tr = ui.global::<Tr>();
            let local = ui.get_add_local().trim().trim_end_matches('/').to_string();
            let fail = |m: String| ui.set_add_error(m.into());
            let edit_id = ui.get_add_edit_id().to_string();
            if let Err(m) = check_local(&ui, &local, &edit_id) {
                return fail(m);
            }
            if !edit_id.is_empty() {
                let download_only = ui.get_add_download_only();
                let is_mega = state().accounts.iter().any(|a| a.id == edit_id && a.kind == "mega");
                // The client fields only take effect through a new login (a token belongs to its client), so a
                // changed pair logs in first and then goes on to the picker. Not yet read = only a filled pair counts.
                let wanted = if ui.get_add_own_client() {
                    (
                        ui.get_add_client_id().trim().to_string(),
                        ui.get_add_client_secret().trim().to_string(),
                    )
                } else {
                    Default::default()
                };
                let changed = match state().edit_client.clone() {
                    Some(saved) => saved != wanted,
                    None => !wanted.0.is_empty(),
                };
                // MEGA: a typed password is the request to update the login (the field is never prefilled).
                if (is_mega && !ui.get_add_password().is_empty()) || (!is_mega && (changed || ui.get_add_relogin())) {
                    return reauth(&ui, (local, download_only));
                }
                return begin_edit(&ui, &edit_id, local, download_only, true);
            }
            let kind = accounts::PROVIDERS[ui.get_add_kind().clamp(0, 3) as usize].0;
            let creds = (kind == "mega").then(|| (ui.get_add_email().to_string(), ui.get_add_password().to_string()));
            // The user's own OAuth app (empty = rclone's shared one). MEGA has no OAuth, so it is ignored there.
            let (id, secret) = (
                ui.get_add_client_id().trim().to_string(),
                ui.get_add_client_secret().trim().to_string(),
            );
            if kind != "mega" && !valid_client(&id, &secret) {
                return fail(tr.invoke_error_client().into());
            }
            let client = (kind != "mega" && !id.is_empty()).then_some((id, secret));
            ui.set_add_error("".into());
            ui.set_add_busy(true);
            let my_gen = {
                let mut s = state();
                s.login_gen += 1;
                s.login_gen
            };
            let acct = Account {
                id: accounts::new_id(&state().accounts),
                kind: kind.into(),
                local,
                download_only: ui.get_add_download_only(),
                ..Default::default()
            };
            let weak = ui.as_weak();
            // The login blocks until the user finishes in the browser, so it runs off the UI thread. Listing the
            // folders right after it is the check that the connection works: only then is the account kept.
            std::thread::spawn(move || {
                let fs = remote_fs(&acct);
                // The flag tells a failed listing (`true`) from a failed login.
                let result = rclone()
                    .and_then(|rc| {
                        rc.login(
                            &acct.id,
                            kind,
                            creds.as_ref().map(|(u, p)| (u.as_str(), p.as_str())),
                            client.as_ref().map(|(i, s)| (i.as_str(), s.as_str())),
                        )
                    })
                    .map_err(|e| (false, e))
                    .and_then(|()| {
                        rclone()
                            .and_then(|rc| rc.list_dirs(&fs, "", &exclusions(kind)))
                            .map_err(|e| (true, e))
                    });
                let mut s = state();
                let current = s.login_gen == my_gen;
                match result {
                    Ok(dirs) if current => {
                        // Saved paused: nothing downloads until the picker's Save (or Resume after its Cancel).
                        let mut saved = acct.clone();
                        saved.paused = true;
                        s.accounts.push(saved);
                        accounts::save(&s.accounts);
                        drop(s);
                        let _ = weak.upgrade_in_event_loop(move |ui| {
                            reset_add(&ui);
                            let picker = Picker {
                                id: acct.id,
                                kind: acct.kind,
                                fs,
                                local: acct.local,
                                adding: true,
                                download_only: acct.download_only,
                                ..Default::default()
                            };
                            open_picker(&ui, picker, dirs);
                            refresh(&ui);
                        });
                    }
                    Err((listing, e)) if current => {
                        let rc = s.rc.clone();
                        drop(s);
                        // The login may have worked and only the listing failed: no half-made account is kept.
                        if let Some(rc) = rc {
                            rc.config_delete(&acct.id);
                        }
                        let _ = weak.upgrade_in_event_loop(move |ui| {
                            ui.set_add_busy(false);
                            let tr = ui.global::<Tr>();
                            let e: slint::SharedString = e.chars().take(500).collect::<String>().into();
                            let msg = if missing_backend(&e) {
                                tr.invoke_error_backend()
                            } else if listing {
                                tr.invoke_error_folders(e)
                            } else {
                                tr.invoke_error_login(e)
                            };
                            ui.set_add_error(msg);
                        });
                    }
                    _ => {
                        // Cancelled while the browser was open: forget the token that just arrived.
                        let rc = s.rc.clone();
                        drop(s);
                        if let Some(rc) = rc {
                            rc.config_delete(&acct.id);
                        }
                    }
                }
            });
        }
    });
    ui.on_theme_changed({
        let settings = settings.clone();
        move |index| {
            let mut s = settings.borrow_mut();
            let theme = theme_name(index);
            // Also fires once at start-up; saving then would rewrite the file (and, with fixtures, the real one).
            if s.theme != theme {
                s.theme = theme.into();
                s.save();
            }
        }
    });
    ui.on_trash_days_changed({
        let settings = settings.clone();
        move |days| {
            let days = days.clamp(0, MAX_TRASH_DAYS as i32) as u32;
            let mut s = settings.borrow_mut();
            // Also fires once at start-up, when the stored value is put in the box: only a real change is saved.
            if s.trash_days == days {
                return;
            }
            s.trash_days = days;
            s.save();
            TRASH_DAYS.store(days, std::sync::atomic::Ordering::SeqCst);
            // A lower limit takes effect now, not at the next pass.
            if !fixture_run {
                std::thread::spawn(purge_all);
            }
        }
    });
    ui.on_walk_minutes_changed({
        let settings = settings.clone();
        move |minutes| {
            let minutes = minutes.clamp(1, MAX_WALK_MINUTES as i32) as u32;
            let mut s = settings.borrow_mut();
            // Also fires once at start-up, when the stored value is put in the box: only a real change is saved.
            if s.walk_minutes != minutes {
                s.walk_minutes = minutes;
                s.save();
                runner::WALK_MINUTES.store(minutes, std::sync::atomic::Ordering::Relaxed);
            }
        }
    });
    ui.on_walk_fallback_minutes_changed({
        let settings = settings.clone();
        move |minutes| {
            let minutes = minutes.clamp(1, MAX_WALK_MINUTES as i32) as u32;
            let mut s = settings.borrow_mut();
            if s.walk_fallback_minutes != minutes {
                s.walk_fallback_minutes = minutes;
                s.save();
                runner::WALK_FALLBACK_MINUTES.store(minutes, std::sync::atomic::Ordering::Relaxed);
            }
        }
    });
    ui.on_file_emblems_changed({
        let settings = settings.clone();
        move |on| {
            let mut s = settings.borrow_mut();
            // Also fires once at start-up (the property starts true): only a real change is saved.
            if s.file_emblems == on {
                return;
            }
            s.file_emblems = on;
            s.save();
            emblem::enable(on);
            // The running extension is told by the server's next push; the file itself goes or comes now.
            std::thread::spawn(emblem::sync_extension);
        }
    });
    ui.on_autostart_changed({
        let weak = ui.as_weak();
        move |on| {
            // Also fires at start-up and when a failure puts the box back: only a real change goes on.
            if autostart::is_enabled() == on {
                return;
            }
            let weak = weak.clone();
            // A thread: in Flatpak the portal blocks while the desktop asks the user.
            std::thread::spawn(move || {
                if let Err(e) = autostart::set(on) {
                    eprintln!("cannot change the autostart entry: {e}");
                    let _ = weak.upgrade_in_event_loop(|w| w.set_autostart(autostart::is_enabled()));
                }
            });
        }
    });
    ui.on_language_changed({
        let settings = settings.clone();
        let weak = ui.as_weak();
        move |index| {
            let code = usize::try_from(index)
                .ok()
                .and_then(|i| LANGUAGES.get(i))
                .copied()
                .unwrap_or("");
            let mut s = settings.borrow_mut();
            // Same guard as the theme: the start-up notification must not rewrite the file.
            if s.language != code {
                s.language = code.into();
                s.save();
                select_language(&language(&s));
                // Bound first: the lock must not be held while the tray updates (a tray action may wait on it).
                let tray = state().tray.clone();
                if let (Some(ui), Some(t)) = (weak.upgrade(), tray) {
                    let labels = tray_labels(&ui);
                    t.update(|t| t.labels = labels);
                }
            }
        }
    });
    // Fixtures show canned numbers: nothing may touch the real folders.
    ui.on_scan_trash({
        let weak = ui.as_weak();
        move || {
            // Not while a job runs: Settings opens again right after Delete, and a second job would hide the
            // emptying's answer and could count before it finished.
            if !fixture_run && let Some(ui) = weak.upgrade() {
                // Every opening of Settings, also while a count runs: the flag may have been set meanwhile.
                let hit = fastsync_core::runner::WATCH_LIMIT_HIT.load(std::sync::atomic::Ordering::Relaxed);
                ui.set_watch_fix(if hit {
                    fastsync_core::runner::watch_fix_command(fastsync_core::runner::watch_limit()).into()
                } else {
                    "".into()
                });
                if !ui.get_trash_busy() {
                    trash_job(&ui, false);
                }
            }
        }
    });
    ui.on_empty_trash({
        let weak = ui.as_weak();
        move || {
            if !fixture_run && let Some(ui) = weak.upgrade() {
                trash_job(&ui, true);
            }
        }
    });
    ui.on_open_website(|| xdg_open(env!("CARGO_PKG_REPOSITORY").into()));
    ui.on_open_help(|| xdg_open(HELP_URL.into()));
    ui.on_add_pick_folder({
        let weak = ui.as_weak();
        move || {
            let Some(ui) = weak.upgrade() else { return };
            let title = ui.global::<Tr>().invoke_pick_folder_title().to_string();
            let weak = weak.clone();
            std::thread::spawn(move || {
                if let Some(p) = portal::pick_folder(&title) {
                    let _ = weak.upgrade_in_event_loop(move |ui| ui.set_add_local(p.to_string_lossy().as_ref().into()));
                }
            });
        }
    });
    // Opens a dialog directly, for the same script.
    #[cfg(debug_assertions)]
    match std::env::var("FASTSYNC_PAGE").as_deref() {
        Ok("remove") => {
            ui.set_remove_name("Google Drive".into());
            ui.set_remove_folder("/home/user/GoogleDrive".into());
            ui.set_remove_open(true);
        }
        Ok("settings") => ui.set_settings_open(true),
        Ok("about") => ui.set_about_open(true),
        Ok("empty-trash") => ui.set_trash_confirm_open(true),
        Ok("add") => ui.set_add_open(true),
        Ok("add-own") => {
            ui.set_add_own_client(true);
            ui.set_add_client_id("1234567890-abcdefg.apps.googleusercontent.com".into());
            ui.set_add_client_secret("GOCSPX-sample".into());
            ui.set_add_open(true);
        }
        Ok("add-mega") => {
            ui.set_add_kind(3);
            ui.set_add_open(true);
        }
        Ok("edit") => {
            ui.set_add_edit_id("a1".into());
            ui.set_add_kind(1);
            ui.set_add_local("/home/user/GoogleDrive".into());
            ui.set_add_download_only(true);
            // An account that uses the user's own client ID.
            ui.set_add_own_client(true);
            ui.set_add_client_id("1234567890-abcdefg.apps.googleusercontent.com".into());
            ui.set_add_client_secret("GOCSPX-sample".into());
            ui.set_add_open(true);
        }
        Ok("edit-mega") => {
            ui.set_add_edit_id("a4".into());
            ui.set_add_kind(3);
            ui.set_add_local("/home/user/MEGA".into());
            ui.set_add_email("someone@example.com".into());
            ui.set_add_open(true);
        }
        Ok("folders") => {
            fixtures::folders(&ui, fixtures::mode().unwrap_or(2));
            ui.set_folders_open(true);
        }
        Ok("missing") => ui.set_rclone_missing(true),
        Ok("share") => fixtures::share(&ui, fixtures::mode().unwrap_or(1)),
        Ok("log") => activity::fixture(&ui, fixtures::mode().unwrap_or(2)),
        Ok("main") | Err(_) => {}
        Ok(p) => {
            panic!(
                "FASTSYNC_PAGE={p} is not main, add, add-own, add-mega, edit, edit-mega, folders, missing, remove, share, log, settings or about"
            )
        }
    }

    {
        let s = settings.borrow();
        ui.window()
            .set_size(slint::LogicalSize::new(s.width as f32, s.height as f32));
    }
    // Closing the window hides it: syncing goes on and the tray brings it back. Without a tray there would be no way
    // back to a hidden window, so then it quits.
    ui.window().on_close_requested({
        let (weak, settings) = (ui.as_weak(), settings.clone());
        move || {
            if let Some(ui) = weak.upgrade() {
                save_size(&ui, &settings);
            }
            if !has_tray {
                quit();
            }
            slint::CloseRequestResponse::HideWindow
        }
    });
    // `--hidden` (the login entry): stay in the tray, but never hide the only way in.
    if !(has_tray && std::env::args().any(|a| a == "--hidden")) {
        ui.show().unwrap();
    }
    slint::run_event_loop_until_quit().unwrap();
    // Normal exit only; the snapshot script kills the app, so fixtures never overwrite the real file.
    save_size(&ui, &settings);
}

/// Stops rclone (statics are never dropped, so it would keep running) and ends the event loop.
fn quit() {
    for r in state().runners.values() {
        r.stop();
    }
    // The extension sees the socket close and drops every emblem.
    emblem::stop();
    if let Some(rc) = state().rc.take() {
        rc.shutdown();
    }
    let _ = slint::quit_event_loop();
}

fn toggle_all_paused(ui: &Main) {
    let to_start: Vec<Account> = {
        let mut s = state();
        let pause = !s.accounts.iter().all(|a| a.paused);
        let State { accounts, runners, .. } = &mut *s;
        let mut to_start = Vec::new();
        for a in accounts.iter_mut() {
            a.paused = pause;
            if let Some(r) = runners.get(&a.id) {
                r.pause(pause);
            } else if !pause {
                // Saved paused by the folder picker's Cancel and never started.
                to_start.push(a.clone());
            }
        }
        accounts::save(&s.accounts);
        to_start
    };
    for a in to_start {
        let weak = ui.as_weak();
        std::thread::spawn(move || start_account(&a, weak));
    }
    refresh(ui);
}

fn save_size(ui: &Main, settings: &RefCell<Settings>) {
    let size = ui.window().size().to_logical(ui.window().scale_factor());
    let mut settings = settings.borrow_mut();
    settings.width = size.width.round() as u32;
    settings.height = size.height.round() as u32;
    settings.theme = theme_name(ui.get_color_scheme_index()).into();
    settings.save();
}

/// `settings.language` for each entry of the Language selector: system, English, Português.
const LANGUAGES: [&str; 3] = ["", "en", "pt_BR"];

/// An unknown locale (`de_DE`) leaves the previous language selected, so fall back to the English source strings
/// explicitly: otherwise going from Português back to System on such a desktop would stay Portuguese.
fn select_language(code: &str) {
    if slint::select_bundled_translation(code).is_err() {
        let _ = slint::select_bundled_translation("");
    }
}

/// `settings.theme` for each entry of the Appearance list: system, light, dark.
const THEMES: [&str; 3] = ["", "light", "dark"];

/// The Appearance list index of a `settings.theme` value; anything unknown follows the system.
fn theme_index(theme: &str) -> i32 {
    THEMES.iter().position(|t| *t == theme).unwrap_or(0) as i32
}

/// The `settings.theme` value for an Appearance list index.
fn theme_name(index: i32) -> &'static str {
    THEMES.get(index as usize).copied().unwrap_or("")
}

/// `settings.language`, or the system locale (`LC_ALL`/`LC_MESSAGES`/`LANGUAGE`/`LANG`, minus `.UTF-8`).
fn language(settings: &Settings) -> String {
    if !settings.language.is_empty() {
        return settings.language.clone();
    }
    ["LC_ALL", "LC_MESSAGES", "LANGUAGE", "LANG"]
        .iter()
        .filter_map(|v| std::env::var(v).ok())
        .find(|v| !v.is_empty())
        .map(|v| locale_code(&v).to_string())
        .unwrap_or_default()
}

/// `pt_BR.UTF-8` / `pt_BR:en` / `pt_BR@euro` -> `pt_BR`.
fn locale_code(v: &str) -> &str {
    v.split(['.', ':', '@']).next().unwrap_or("")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_card_model_is_changed_in_place_and_ends_up_equal_to_the_rows() {
        use slint::Model;
        let row = |id: &str, done: i32| AccountRow {
            id: id.into(),
            done,
            ..Default::default()
        };
        let model = VecModel::default();
        let read = |m: &VecModel<AccountRow>| (0..m.row_count()).map(|i| m.row_data(i).unwrap()).collect::<Vec<_>>();
        for rows in [
            vec![row("a", 0), row("b", 0), row("c", 0)],
            vec![row("a", 1), row("b", 0), row("c", 0)],
            vec![row("a", 1)],
            vec![],
            vec![row("z", 5), row("y", 6)],
        ] {
            sync_rows(&model, rows.clone());
            assert_eq!(read(&model), rows);
        }
    }

    #[test]
    fn a_symlink_to_another_accounts_folder_is_a_clash() {
        let base = std::env::temp_dir().join(format!("fastsync-clash-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&base);
        let (real, other) = (base.join("Dropbox"), base.join("Other"));
        std::fs::create_dir_all(&real).unwrap();
        std::fs::create_dir_all(&other).unwrap();
        std::os::unix::fs::symlink(&real, base.join("link")).unwrap();
        let s = |p: &Path| p.to_string_lossy().into_owned();
        assert!(
            !overlaps(&s(&real), &s(&base.join("link"))),
            "the text check alone misses it"
        );
        assert!(clashes(&s(&real), &s(&base.join("link"))));
        assert!(clashes(&s(&base.join("link")), &s(&real.join("sub"))));
        // A folder that does not exist yet, under the link: Add creates it inside the other account's tree.
        assert!(clashes(&s(&real), &s(&base.join("link/New/deeper"))));
        assert!(!clashes(&s(&real), &s(&base.join("fresh/New"))));
        assert!(!clashes(&s(&real), &s(&other)));
        let _ = std::fs::remove_dir_all(&base);
    }

    #[test]
    fn the_card_shows_the_phase_of_a_first_scan_with_a_clock() {
        use fastsync_core::engine::{Phase, Progress};
        let at = |phase, done, total, secs| {
            let p = Progress {
                phase,
                done,
                total,
                secs,
                ..Default::default()
            };
            live(Some(&Status::Syncing(p, 0.0)))
        };
        assert_eq!(
            at(Phase::ListCloud, 3, 12, 252),
            (4, String::new(), 3, 12, "4:12".into())
        );
        assert_eq!(at(Phase::ScanLocal, 0, 0, 41), (5, String::new(), 0, 0, "0:41".into()));
        assert_eq!(at(Phase::Compare, 40000, 104000, 543).0, 6);
        // Idle and planning-less transfers stay empty, as before.
        assert_eq!(at(Phase::Transfer, 0, 0, 0), Default::default());
    }

    #[test]
    fn file_uris_are_percent_encoded() {
        assert_eq!(file_uri(Path::new("/a b/ç#x.txt")), "file:///a%20b/%C3%A7%23x.txt");
    }

    #[test]
    fn expired_logins_are_told_from_other_failures() {
        for e in [
            r#"Post "https://oauth2.googleapis.com/token": oauth2: "invalid_grant" "Token has been expired or revoked.""#,
            "couldn't fetch token: maybe token expired? - try refreshing with \"rclone config reconnect\"",
            "HTTP error 401 (401 Unauthorized) returned body: {\"error\":{\"code\":\"InvalidAuthenticationToken\"}}",
            "expired_access_token/..",
            "failed to refresh token",
        ] {
            assert!(login_expired(e), "{e}");
        }
        for e in [
            "dial tcp: lookup www.googleapis.com: no such host",
            "couldn't list directory: context deadline exceeded",
            "HTTP error 503 (503 Service Unavailable)",
            "userRateLimitExceeded",
        ] {
            assert!(!login_expired(e), "{e}");
        }
    }

    #[test]
    fn themes_and_their_indexes_agree() {
        for (i, t) in THEMES.iter().enumerate() {
            assert_eq!(theme_index(t), i as i32);
            assert_eq!(theme_name(i as i32), *t);
        }
        assert_eq!(theme_index("bogus"), 0);
    }

    #[test]
    fn locale_code_drops_encoding_fallbacks_and_modifier() {
        assert_eq!(locale_code("pt_BR:en"), "pt_BR");
        assert_eq!(locale_code("pt_BR.UTF-8"), "pt_BR");
        assert_eq!(locale_code("pt_BR@euro"), "pt_BR");
        assert_eq!(locale_code("C"), "C");
    }

    #[test]
    fn a_folder_under_an_unchecked_one_cannot_be_ticked_alone() {
        let list = vec!["Photos".to_string()];
        assert!(under_unchecked("Photos/2020", &list) && under_unchecked("Photos/2020/x", &list));
        assert!(!under_unchecked("Photos", &list) && !under_unchecked("Docs/Photos", &list));
    }

    #[test]
    fn local_folders_cannot_nest() {
        assert!(overlaps("/home/u/Sync", "/home/u/Sync/sub"));
        assert!(overlaps("/home/u/Sync", "/home/u/Sync"));
        assert!(!overlaps("/home/u/Sync", "/home/u/Syncother"));
        let data = Path::new("/home/u/.local/share/FastSync");
        for bad in [
            "/home/u",
            "/home",
            "/",
            "/home/u/.local/share",
            "/home/u/.local/share/FastSync/x",
            "/home/u/x/..",
            "/home/u/Dropbox/../..",
        ] {
            assert!(bad_folder(bad, "/home/u", data), "{bad}");
        }
        assert!(!bad_folder("/home/u/Dropbox", "/home/u", data));
    }

    #[test]
    fn a_backend_missing_from_rclone_is_recognised() {
        assert!(missing_backend(
            "config/create: couldn't find backend for type \"mega\""
        ));
        assert!(!missing_backend("config/create: some other failure"));
    }

    #[test]
    fn own_client_needs_both_values_and_no_spaces() {
        assert!(valid_client("", ""));
        assert!(valid_client("123-abc.apps.googleusercontent.com", "GOCSPX-x_y"));
        assert!(!valid_client("id", ""));
        assert!(!valid_client("", "secret"));
        assert!(!valid_client("id with space", "secret"));
        assert!(!valid_client("id", "sec\nret"));
    }

    #[test]
    fn sizes_read_in_the_biggest_fitting_unit() {
        assert_eq!(human_size(0), "0 B");
        assert_eq!(human_size(1023), "1023 B");
        assert_eq!(human_size(2150), "2.1 KB");
        assert_eq!(human_size(3 * 1024 * 1024), "3.0 MB");
        assert_eq!(human_size(3_650_722_201), "3.4 GB");
        assert_eq!(human_size(5 * 1024 * 1024 * 1024 * 1024), "5.0 TB");
    }

    #[test]
    fn row_states_and_the_overall_status() {
        let acct = |paused| Account {
            id: "a".into(),
            paused,
            ..Default::default()
        };
        assert_eq!(row_state(&acct(false), None).0, 1);
        assert_eq!(
            row_state(&acct(true), Some(&Status::Idle(0))).0,
            2,
            "a paused account shows as paused"
        );
        let stopped = Status::Stopped(5, 9, vec!["a.log".into(), "b/c".into()]);
        let (_, detail, counts) = row_state(&acct(false), Some(&stopped));
        assert_eq!((detail.as_str(), counts), ("a.log\nb/c", (5, 9)));
        let mut s = State {
            accounts: vec![acct(false)],
            ..empty_state()
        };
        assert_eq!(overall(&s), Overall::Syncing);
        s.status.insert("a".into(), Status::Idle(0));
        assert_eq!(overall(&s), Overall::Idle);
        s.status.insert("a".into(), Status::Error("x".into()));
        assert_eq!(overall(&s), Overall::Error);
        s.status.insert("a".into(), stopped);
        assert_eq!(overall(&s), Overall::Stopped);
    }

    #[test]
    fn quota_levels_start_at_80_and_95_percent() {
        assert_eq!([0, 79, 80, 94, 95, 100].map(quota_level), [0, 0, 1, 1, 2, 2]);
    }

    fn empty_state() -> State {
        State {
            accounts: Vec::new(),
            runners: HashMap::new(),
            status: HashMap::new(),
            rc: None,
            login_gen: 0,
            edit_client: None,
            rclone_error: None,
            tray: None,
            starting: HashSet::new(),
            warned: HashSet::new(),
            quota: HashMap::new(),
        }
    }
}
