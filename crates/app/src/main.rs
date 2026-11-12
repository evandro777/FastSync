use fastsync_core::{
    accounts::{self, Account},
    rclone::Rclone,
    runner::{self, Runner, Status},
    settings::Settings,
};
use slint::{ComponentHandle, ModelRc, VecModel};
use std::{
    cell::RefCell,
    collections::HashMap,
    path::Path,
    rc::Rc,
    sync::{Arc, LazyLock, Mutex, MutexGuard},
};

slint::include_modules!();

#[cfg(debug_assertions)]
mod fixtures;
mod instance;
mod tray;

use tray::Overall;

/// Everything the worker threads, the tray and the UI thread share. Locked briefly, never across a slow call.
struct State {
    accounts: Vec<Account>,
    runners: HashMap<String, Runner>,
    status: HashMap<String, Status>,
    rc: Option<Arc<Rclone>>,
    /// Bumped by every login attempt and by Cancel: a login that finishes under an old number is discarded.
    login_gen: u64,
    rclone_error: Option<String>,
    tray: Option<ksni::blocking::Handle<tray::SyncTray>>,
}

static STATE: LazyLock<Mutex<State>> = LazyLock::new(|| {
    Mutex::new(State {
        accounts: Vec::new(),
        runners: HashMap::new(),
        status: HashMap::new(),
        rc: None,
        login_gen: 0,
        rclone_error: None,
        tray: None,
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

fn start_account(acct: &Account, ui: slint::Weak<Main>) {
    let rc = match rclone() {
        Ok(rc) => rc,
        Err(e) => {
            state().status.insert(acct.id.clone(), Status::Error(e));
            return;
        }
    };
    let id = acct.id.clone();
    let runner = runner::spawn(acct.clone(), rc, move |st| {
        state().status.insert(id.clone(), st);
        let _ = ui.upgrade_in_event_loop(|ui| refresh(&ui));
    });
    state().runners.insert(acct.id.clone(), runner);
}

fn row_state(acct: &Account, st: Option<&Status>) -> (i32, String, (i32, i32)) {
    match st {
        _ if acct.paused => (2, String::new(), (0, 0)),
        Some(Status::Idle(_)) => (0, String::new(), (0, 0)),
        Some(Status::Paused) => (2, String::new(), (0, 0)),
        Some(Status::Error(e)) => (3, e.clone(), (0, 0)),
        Some(Status::Stopped(d, k)) => (4, String::new(), (*d as i32, *k as i32)),
        Some(Status::Syncing) | None => (1, String::new(), (0, 0)),
    }
}

fn overall(s: &State) -> Overall {
    let states: Vec<i32> = s.accounts.iter().map(|a| row_state(a, s.status.get(&a.id)).0).collect();
    if states.is_empty() {
        Overall::None
    } else if states.iter().any(|&x| x >= 3) {
        Overall::Error
    } else if states.contains(&1) {
        Overall::Syncing
    } else if states.iter().all(|&x| x == 2) {
        Overall::Paused
    } else {
        Overall::Idle
    }
}

/// Rebuilds the list from the shared state. UI thread only.
fn refresh(ui: &Main) {
    let (rows, missing, overall, tray) = {
        let s = state();
        let rows: Vec<AccountRow> = s
            .accounts
            .iter()
            .map(|a| {
                let (state, detail, (deletes, known)) = row_state(a, s.status.get(&a.id));
                AccountRow {
                    id: a.id.as_str().into(),
                    provider: accounts::provider_name(&a.kind).into(),
                    folder: a.local.as_str().into(),
                    state,
                    detail: detail.as_str().into(),
                    deletes,
                    known,
                }
            })
            .collect();
        let folders: Vec<(String, String)> = s
            .accounts
            .iter()
            .map(|a| (accounts::provider_name(&a.kind).to_string(), a.local.clone()))
            .collect();
        (rows, s.rclone_error.is_some(), (overall(&s), folders), s.tray.clone())
    };
    ui.set_accounts(ModelRc::new(VecModel::from(rows)));
    ui.set_rclone_missing(missing);
    if let Some(t) = tray {
        t.update(|t| (t.overall, t.folders) = overall);
    }
}

fn tray_labels(ui: &Main) -> tray::Labels {
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
        none: tr.invoke_tray_none().into(),
    }
}

/// Opens a folder in the file manager. Waiting in a thread reaps the child; a dropped `Child` would stay a zombie.
fn xdg_open(what: String) {
    std::thread::spawn(move || {
        if let Err(e) = std::process::Command::new("xdg-open").arg(&what).status() {
            eprintln!("cannot open {what}: {e}");
        }
    });
}

/// The provider's web page. shortcut: the home page, not the synced folder; Drive/OneDrive need folder ids.
fn web_url(kind: &str) -> Option<&'static str> {
    Some(match kind {
        "dropbox" => "https://www.dropbox.com/home",
        "drive" => "https://drive.google.com",
        "onedrive" => "https://onedrive.live.com",
        "mega" => "https://mega.nz/fm",
        _ => return None,
    })
}

/// `Some(clean path)` for a folder inside the cloud, `None` when it tries to leave it (`..`).
fn clean_remote(p: &str) -> Option<String> {
    let p = p.trim().trim_matches('/');
    (!p.split('/').any(|c| c == "..")).then(|| p.to_string())
}

/// Two synced folders must not be the same or inside each other: they would sync each other's files.
/// The whole home folder, a folder above it, or anything touching FastSync's own data (tokens, DBs) must not be
/// synced: it would upload the tokens and loop on its own database writes.
/// rclone's answer when it was compiled without a backend (Debian and Ubuntu's package leaves some out, MEGA among them).
fn missing_backend(error: &str) -> bool {
    error.contains("couldn't find backend")
}

/// The guide to creating one's own OAuth client (in the repository, so it can be fixed without a release).
const HELP_URL: &str = concat!(env!("CARGO_PKG_REPOSITORY"), "/blob/master/docs/OWN-CLIENT-ID.md");

/// Both empty (use rclone's shared client) or both filled without spaces (a pasted value with a space is a typo).
fn valid_client(id: &str, secret: &str) -> bool {
    id.is_empty() == secret.is_empty() && !id.contains(char::is_whitespace) && !secret.contains(char::is_whitespace)
}

fn bad_folder(local: &str, home: &str, data: &Path) -> bool {
    let local = Path::new(local);
    (!home.is_empty() && Path::new(home).starts_with(local))
        || overlaps(&local.to_string_lossy(), &data.to_string_lossy())
}

fn overlaps(a: &str, b: &str) -> bool {
    let (a, b) = (Path::new(a), Path::new(b));
    a.starts_with(b) || b.starts_with(a)
}

fn main() {
    if std::env::args().skip(1).any(|a| a == "-V" || a == "--version") {
        return println!("fastsync {}", env!("CARGO_PKG_VERSION"));
    }
    // Fixtures (scripts/ui-snapshot.sh) never touch a real running instance. Debug builds only, like the fixtures.
    #[cfg(debug_assertions)]
    let fixture_run = fixtures::mode().is_some();
    #[cfg(not(debug_assertions))]
    let fixture_run = false;
    // One window: a copy is already open, so it comes to the front and this one leaves before setting anything up.
    if !fixture_run && instance::send("show") {
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
        let raise = move |_command: String| {
            let _ = weak.upgrade_in_event_loop(|ui| instance::raise(ui.window()));
        };
        if instance::claim("show", raise) {
            return;
        }
    }
    {
        let s = settings.borrow();
        select_language(&language(&s));
        ui.set_language_index(LANGUAGES.iter().position(|l| *l == s.language).unwrap_or(0) as i32);
        ui.set_color_scheme_index(theme_index(&s.theme));
    }
    ui.set_version(env!("CARGO_PKG_VERSION").into());
    ui.set_add_local(accounts::default_local("dropbox").into());

    let mut has_tray = false;
    if !fixture_run {
        state().accounts = accounts::load();
        let weak = ui.as_weak();
        let (w_open, w_quit) = (weak.clone(), weak.clone());
        let actions = tray::Actions {
            open: Arc::new(move || {
                let _ = w_open.upgrade_in_event_loop(|ui| instance::raise(ui.window()));
            }),
            // A plain rescan: it must never confirm a stopped deletion, which is for the card's own button.
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
    ui.on_toggle_pause({
        let weak = ui.as_weak();
        move |id| {
            {
                let mut s = state();
                let Some(a) = s.accounts.iter_mut().find(|a| a.id == id.as_str()) else {
                    return;
                };
                a.paused = !a.paused;
                let paused = a.paused;
                if let Some(r) = s.runners.get(id.as_str()) {
                    r.pause(paused);
                }
                accounts::save(&s.accounts);
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
            let (rc, id) = {
                let mut s = state();
                if let Some(r) = s.runners.remove(id.as_str()) {
                    r.stop();
                }
                s.accounts.retain(|a| a.id != id.as_str());
                s.status.remove(id.as_str());
                accounts::save(&s.accounts);
                (s.rc.clone(), id.to_string())
            };
            // Only the link is removed: the local files stay where they are. The token and the DB go.
            std::thread::spawn(move || {
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
    ui.on_add_kind_changed({
        let weak = ui.as_weak();
        move |index| {
            let Some(ui) = weak.upgrade() else { return };
            let local = ui.get_add_local();
            // Follow the provider's default folder name, unless the user typed their own.
            let untouched = local.is_empty()
                || accounts::PROVIDERS
                    .iter()
                    .any(|(k, _)| accounts::default_local(k) == local.as_str());
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
            if let Some(ui) = weak.upgrade() {
                ui.set_add_busy(false);
                ui.set_add_error("".into());
                ui.set_add_password("".into());
                ui.set_add_client_secret("".into());
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
            let Some(remote_path) = clean_remote(&ui.get_add_remote()) else {
                return fail(tr.invoke_error_login("..".into()).into());
            };
            if !local.starts_with('/') || local.len() < 2 {
                return fail(tr.invoke_error_no_folder().into());
            }
            if state().accounts.iter().any(|a| overlaps(&a.local, &local)) {
                return fail(tr.invoke_error_taken().into());
            }
            let home = std::env::var("HOME").unwrap_or_default();
            let data = fastsync_core::settings::data_dir().unwrap_or_default();
            if bad_folder(&local, &home, &data) {
                return fail(tr.invoke_error_bad_folder().into());
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
                id: accounts::new_id(),
                kind: kind.into(),
                local,
                remote_path,
                paused: false,
            };
            let weak = ui.as_weak();
            // The login blocks until the user finishes in the browser, so it runs off the UI thread.
            std::thread::spawn(move || {
                let result = rclone().and_then(|rc| {
                    rc.login(
                        &acct.id,
                        kind,
                        creds.as_ref().map(|(u, p)| (u.as_str(), p.as_str())),
                        client.as_ref().map(|(i, s)| (i.as_str(), s.as_str())),
                    )
                });
                let mut s = state();
                let current = s.login_gen == my_gen;
                match result {
                    Ok(()) if current => {
                        s.accounts.push(acct.clone());
                        accounts::save(&s.accounts);
                        drop(s);
                        start_account(&acct, weak.clone());
                        let _ = weak.upgrade_in_event_loop(|ui| {
                            ui.set_add_busy(false);
                            ui.set_add_password("".into());
                            ui.set_add_client_id("".into());
                            ui.set_add_client_secret("".into());
                            ui.set_add_own_client(false);
                            ui.set_add_open(false);
                            ui.set_add_local(accounts::default_local("dropbox").into());
                            ui.set_add_kind(0);
                            refresh(&ui);
                        });
                    }
                    Ok(()) => {
                        // Cancelled while the browser was open: forget the token that just arrived.
                        let rc = s.rc.clone();
                        drop(s);
                        if let Some(rc) = rc {
                            rc.config_delete(&acct.id);
                        }
                    }
                    Err(e) if current => {
                        drop(s);
                        let _ = weak.upgrade_in_event_loop(move |ui| {
                            ui.set_add_busy(false);
                            let tr = ui.global::<Tr>();
                            let msg = if missing_backend(&e) {
                                tr.invoke_error_backend()
                            } else {
                                tr.invoke_error_login(e.chars().take(200).collect::<String>().into())
                            };
                            ui.set_add_error(msg);
                        });
                    }
                    Err(_) => {}
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
    ui.on_open_website(|| xdg_open(env!("CARGO_PKG_REPOSITORY").into()));
    ui.on_open_help(|| xdg_open(HELP_URL.into()));
    // Opens a dialog directly, for the same script.
    #[cfg(debug_assertions)]
    match std::env::var("FASTSYNC_PAGE").as_deref() {
        Ok("settings") => ui.set_settings_open(true),
        Ok("about") => ui.set_about_open(true),
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
        Ok("missing") => ui.set_rclone_missing(true),
        Ok("main") | Err(_) => {}
        Ok(p) => panic!("FASTSYNC_PAGE={p} is not main, add, add-own, add-mega, missing, settings or about"),
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
    ui.show().unwrap();
    slint::run_event_loop_until_quit().unwrap();
    // Normal exit only; the snapshot script kills the app, so fixtures never overwrite the real file.
    save_size(&ui, &settings);
}

/// Stops rclone (statics are never dropped, so it would keep running) and ends the event loop.
fn quit() {
    for r in state().runners.values() {
        r.stop();
    }
    if let Some(rc) = state().rc.take() {
        rc.shutdown();
    }
    let _ = slint::quit_event_loop();
}

fn toggle_all_paused(ui: &Main) {
    {
        let mut s = state();
        let pause = !s.accounts.iter().all(|a| a.paused);
        let State { accounts, runners, .. } = &mut *s;
        for a in accounts.iter_mut() {
            a.paused = pause;
            if let Some(r) = runners.get(&a.id) {
                r.pause(pause);
            }
        }
        accounts::save(&s.accounts);
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
    fn cloud_folders_cannot_escape_and_local_folders_cannot_nest() {
        assert_eq!(clean_remote(" /Docs/Work/ ").as_deref(), Some("Docs/Work"));
        assert_eq!(clean_remote("").as_deref(), Some(""));
        assert_eq!(clean_remote("a/../b"), None);
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
        assert_eq!(row_state(&acct(false), Some(&Status::Stopped(5, 9))).2, (5, 9));
        let mut s = State {
            accounts: vec![acct(false)],
            ..empty_state()
        };
        assert_eq!(overall(&s), Overall::Syncing);
        s.status.insert("a".into(), Status::Idle(0));
        assert_eq!(overall(&s), Overall::Idle);
        s.status.insert("a".into(), Status::Error("x".into()));
        assert_eq!(overall(&s), Overall::Error);
    }

    fn empty_state() -> State {
        State {
            accounts: Vec::new(),
            runners: HashMap::new(),
            status: HashMap::new(),
            rc: None,
            login_gen: 0,
            rclone_error: None,
            tray: None,
        }
    }
}
