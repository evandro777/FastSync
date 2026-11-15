//! Nemo's right-click "FastSync" submenu: one `.nemo_action` per item in `~/.local/share/nemo/actions`, grouped by an
//! entry in the user's own layout file `~/.config/nemo/actions-tree.json` (the file Nemo's "Actions layout editor"
//! saves). That file is the user's: we only add or refresh our submenu and keep everything else as it is.
//! Cosmetic like the emblems, so failures are ignored. Skipped in the Flatpak, which cannot write outside its sandbox
//! (the README says how to add an action by hand there).
use fastsync_core::settings::data_dir;
use serde_json::{Value, json};
use std::{
    path::{Path, PathBuf},
    sync::Mutex,
};

const MENU_UUID: &str = "fastsync-menu";

/// The submenu's icon: the app's own, by theme name. Nemo takes a name, so the file is put in the user's hicolor
/// theme (`install_icon`), like the emblems.
const APP_ICON: &str = "io.github.evandro777.FastSync";
const APP_ICON_SVG: &str = include_str!("../../../packaging/io.github.evandro777.FastSync.svg");

/// (file name, menu text, text in pt_BR, tooltip, tooltip in pt_BR, command line option, icon).
const ACTIONS: [(&str, &str, &str, &str, &str, &str, &str); 2] = [
    (
        "fastsync-link.nemo_action",
        "Copy share link",
        "Copiar link de compartilhamento",
        "Create a public link to this file and copy it",
        "Cria um link público para este arquivo e o copia",
        "--share",
        "emblem-shared",
    ),
    (
        "fastsync-web.nemo_action",
        "View on web",
        "Ver na web",
        "Open this file on the provider's website",
        "Abre este arquivo no site do serviço",
        "--web",
        "applications-internet",
    ),
];

/// The action file of the first version of the share link (one top-level entry, no submenu).
const OLD_ACTION: &str = "fastsync-share.nemo_action";

/// What `install` last did, so the frequent `refresh` calls cost a string compare.
static LAST: Mutex<Option<Option<String>>> = Mutex::new(None);

/// Makes the menu match the accounts: shown only inside their synced folders (`Files=<root>/*;`), gone with no
/// accounts. `roots` are the accounts' local folders.
pub fn install(roots: &[String]) {
    if crate::emblem::in_flatpak() {
        return;
    }
    let globs = file_globs(roots);
    let mut last = LAST.lock().unwrap_or_else(|e| e.into_inner());
    if last.as_ref() == Some(&globs) {
        return;
    }
    // An AppImage runs from a temporary mount: `APPIMAGE` is the file that stays.
    let Some(exe) = std::env::var_os("APPIMAGE")
        .map(PathBuf::from)
        .or_else(|| std::env::current_exe().ok())
    else {
        return;
    };
    let (Some(actions), Some(config)) = (
        data_dir().and_then(|d| d.parent().map(|p| p.join("nemo/actions"))),
        config_dir().map(|c| c.join("nemo/actions-tree.json")),
    ) else {
        return;
    };
    let _ = std::fs::remove_file(actions.join(OLD_ACTION));
    for (file, name, name_pt, tip, tip_pt, option, icon) in ACTIONS {
        let path = actions.join(file);
        match &globs {
            Some(globs) => {
                let exe = exec_quote(&exe.to_string_lossy());
                let text = format!(
                    "[Nemo Action]\nName={name}\nName[pt_BR]={name_pt}\nComment={tip}\nComment[pt_BR]={tip_pt}\n\
                     Exec=\"{exe}\" {option} %F\nIcon-Name={icon}\nSelection=s\n\
                     Extensions=any;\nFiles={globs}\n"
                );
                if std::fs::create_dir_all(&actions).is_ok() {
                    let _ = std::fs::write(&path, text);
                }
            }
            None => {
                let _ = std::fs::remove_file(&path);
            }
        }
    }
    if globs.is_some() {
        install_icon();
    }
    update_layout(&config, globs.is_some());
    *last = Some(globs);
}

/// A path for inside the quotes of an `Exec=` line: `%` is a field code, and `\`, `$`, `` ` `` and `"` are escaped
/// in a quoted argument (desktop entry spec). A line break would end the line, so it is dropped.
fn exec_quote(path: &str) -> String {
    let mut out = String::new();
    for c in path.chars().filter(|c| *c != '\n' && *c != '\r') {
        match c {
            '%' => out.push_str("%%"),
            // Two layers of escaping (string value, then Exec argument): a literal `\` takes four.
            '\\' => out.push_str(r"\\\\"),
            '"' | '$' | '`' => {
                out.push_str(r"\\");
                out.push(c);
            }
            _ => out.push(c),
        }
    }
    out
}

/// The `Files=` value: everything below each synced folder. The leading `*` is what Nemo's documentation promises to
/// match against the full path (a pattern starting with `/` may be compared as a plain path); it takes in nothing
/// else, as the rest starts with `/`.
/// shortcut: a folder whose name has a glob or list character (`*?[];\`) is left out, as its pattern would match
/// other paths; upgrade with an `exec` condition if anyone needs such a folder.
fn file_globs(roots: &[String]) -> Option<String> {
    let globs: String = roots
        .iter()
        .filter(|r| r.starts_with('/') && !r.contains(['*', '?', '[', ']', ';', '\\', '\n']))
        .map(|r| format!("*{}/*;", r.trim_end_matches('/')))
        .collect();
    (!globs.is_empty()).then_some(globs)
}

/// Puts the app icon where icon themes look (only when missing or changed), as `emblem::install` does for the emblems.
fn install_icon() {
    let Some(dir) = data_dir().and_then(|d| d.parent().map(|p| p.join("icons/hicolor/scalable/apps"))) else {
        return;
    };
    let path = dir.join(format!("{APP_ICON}.svg"));
    if std::fs::read_to_string(&path).ok().as_deref() != Some(APP_ICON_SVG) && std::fs::create_dir_all(&dir).is_ok() {
        let _ = std::fs::write(path, APP_ICON_SVG);
    }
}

fn config_dir() -> Option<PathBuf> {
    std::env::var_os("XDG_CONFIG_HOME")
        .map(PathBuf::from)
        .filter(|p| p.is_absolute())
        .or_else(|| std::env::var_os("HOME").map(|h| PathBuf::from(h).join(".config")))
}

/// Merges our submenu into the user's layout file (or takes it out). A file that cannot be read as a layout is left
/// alone: the actions then show at the top level of Nemo's menu, which still works. The first time an existing
/// file is changed, a copy is kept next to it.
fn update_layout(path: &Path, show: bool) {
    let existing = std::fs::read_to_string(path).ok();
    let Some(new) = merge_tree(existing.as_deref(), show) else {
        return;
    };
    if existing.as_deref() == Some(&new) {
        return;
    }
    if existing.is_some() {
        let backup = path.with_extension("json.fastsync.bak");
        if !backup.exists() {
            let _ = std::fs::copy(path, backup);
        }
    }
    // Not `files::write_atomic`: it makes the folder 0700 and the file 0600, which is right for our own data but
    // not for Nemo's config folder.
    let tmp = path.with_extension("json.fastsync.tmp");
    if path.parent().is_some_and(|d| std::fs::create_dir_all(d).is_ok())
        && std::fs::write(&tmp, new)
            .and_then(|()| std::fs::rename(&tmp, path))
            .is_err()
    {
        let _ = std::fs::remove_file(&tmp);
    }
}

/// The layout file's new text: our submenu (`MENU_UUID`) holding our actions, added at the end if missing and
/// refreshed in place if present (so the user's position, label and icon for it stay). Our actions are taken out of
/// every other place first (`show == false` removes the submenu too). `None` when there is nothing to write: the file
/// does not parse as a layout, or there is no file and no submenu to add.
fn merge_tree(existing: Option<&str>, show: bool) -> Option<String> {
    let mut root: Value = match existing {
        Some(text) => serde_json::from_str(text).ok()?,
        None if show => json!({"toplevel": []}),
        None => return None,
    };
    let top = root.get_mut("toplevel")?.as_array_mut()?;
    remove_ours(top);
    if show {
        let children: Vec<Value> = ACTIONS
            .iter()
            .map(|a| json!({"uuid": a.0, "type": "action", "user-label": null, "user-icon": null, "accelerator": null}))
            .collect();
        match find_menu(top) {
            Some(menu) => {
                menu["children"] = Value::Array(children);
                // An icon the user chose (or "no icon", which is "") stays; null means "none chosen yet".
                if menu["user-icon"].is_null() {
                    menu["user-icon"] = json!(APP_ICON);
                }
            }
            None => top.push(json!({
                "uuid": MENU_UUID, "type": "submenu", "user-label": "FastSync", "user-icon": APP_ICON,
                "accelerator": null, "children": children
            })),
        }
    } else {
        remove_menu(top);
    }
    serde_json::to_string_pretty(&root).ok()
}

/// Takes our actions out of every submenu level, whatever the user did with them.
fn remove_ours(nodes: &mut Vec<Value>) {
    nodes.retain(|n| {
        let uuid = n["uuid"].as_str().unwrap_or("");
        uuid != OLD_ACTION && !ACTIONS.iter().any(|a| a.0 == uuid)
    });
    for n in nodes.iter_mut() {
        if let Some(children) = n.get_mut("children").and_then(Value::as_array_mut) {
            remove_ours(children);
        }
    }
}

fn find_menu(nodes: &mut [Value]) -> Option<&mut Value> {
    for n in nodes {
        if n["uuid"] == MENU_UUID {
            return Some(n);
        }
        if let Some(found) = n
            .get_mut("children")
            .and_then(Value::as_array_mut)
            .and_then(|c| find_menu(c))
        {
            return Some(found);
        }
    }
    None
}

fn remove_menu(nodes: &mut Vec<Value>) {
    nodes.retain(|n| n["uuid"] != MENU_UUID);
    for n in nodes.iter_mut() {
        if let Some(children) = n.get_mut("children").and_then(Value::as_array_mut) {
            remove_menu(children);
        }
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn exec_paths_are_escaped_for_a_quoted_argument() {
        assert_eq!(super::exec_quote("/opt/Fast Sync/app"), "/opt/Fast Sync/app");
        assert_eq!(super::exec_quote("/a%b"), "/a%%b");
        assert_eq!(super::exec_quote("/a\"b$c"), r#"/a\\"b\\$c"#);
        assert_eq!(super::exec_quote("/a\\b\nc"), r"/a\\\\bc");
    }

    use super::*;

    fn parse(s: &str) -> Value {
        serde_json::from_str(s).unwrap()
    }
    fn uuids(v: &Value) -> Vec<&str> {
        v.as_array()
            .unwrap()
            .iter()
            .map(|n| n["uuid"].as_str().unwrap())
            .collect()
    }

    #[test]
    fn globs_cover_the_synced_folders_only() {
        let roots = [
            "/home/u/Dropbox-Test/".to_owned(),
            "/home/u/a*b".to_owned(),
            "rel".to_owned(),
            "/m/MEGA".to_owned(),
        ];
        assert_eq!(
            file_globs(&roots).as_deref(),
            Some("*/home/u/Dropbox-Test/*;*/m/MEGA/*;")
        );
        assert_eq!(file_globs(&[]), None);
    }

    #[test]
    fn creates_the_file_and_keeps_nothing_else_when_there_is_none() {
        let v = parse(&merge_tree(None, true).unwrap());
        assert_eq!(uuids(&v["toplevel"]), [MENU_UUID]);
        assert_eq!(
            uuids(&v["toplevel"][0]["children"]),
            ["fastsync-link.nemo_action", "fastsync-web.nemo_action"]
        );
        assert_eq!(merge_tree(None, false), None);
    }

    #[test]
    fn keeps_the_users_layout_and_extra_keys() {
        let user = r#"{"extra": 1, "toplevel": [
            {"uuid": "a.nemo_action", "type": "action", "user-label": "Mine"},
            {"uuid": "grp", "type": "submenu", "children": [{"uuid": "b.nemo_action", "type": "action"}]}]}"#;
        let v = parse(&merge_tree(Some(user), true).unwrap());
        assert_eq!(v["extra"], 1);
        assert_eq!(uuids(&v["toplevel"]), ["a.nemo_action", "grp", MENU_UUID]);
        assert_eq!(v["toplevel"][0]["user-label"], "Mine");
        assert_eq!(uuids(&v["toplevel"][1]["children"]), ["b.nemo_action"]);
    }

    #[test]
    fn our_submenu_stays_where_the_user_put_it_and_our_strays_move_in() {
        let user = r#"{"toplevel": [
            {"uuid": "fastsync-link.nemo_action", "type": "action"},
            {"uuid": "grp", "type": "submenu", "children": [
                {"uuid": "fastsync-menu", "type": "submenu", "user-label": "Cloud", "children": []},
                {"uuid": "fastsync-share.nemo_action", "type": "action"}]}]}"#;
        let v = parse(&merge_tree(Some(user), true).unwrap());
        assert_eq!(uuids(&v["toplevel"]), ["grp"]);
        let menu = &v["toplevel"][0]["children"][0];
        assert_eq!(
            (menu["uuid"].as_str(), menu["user-label"].as_str()),
            (Some(MENU_UUID), Some("Cloud"))
        );
        assert_eq!(uuids(&menu["children"]).len(), 2);
        assert_eq!(uuids(&v["toplevel"][0]["children"]), [MENU_UUID]);
    }

    #[test]
    fn the_submenu_gets_the_app_icon_unless_the_user_chose_one() {
        let icon_of = |user: &str| {
            let v = parse(&merge_tree(Some(user), true).unwrap());
            v["toplevel"][0]["user-icon"].as_str().map(str::to_owned)
        };
        let menu = |icon: &str| {
            format!(r#"{{"toplevel": [{{"uuid": "fastsync-menu", "type": "submenu", "user-icon": {icon}}}]}}"#)
        };
        assert_eq!(icon_of(&menu("null")).as_deref(), Some(APP_ICON));
        assert_eq!(icon_of(&menu(r#""folder""#)).as_deref(), Some("folder"));
        assert_eq!(icon_of(&menu(r#""""#)).as_deref(), Some(""));
        let fresh = parse(&merge_tree(None, true).unwrap());
        assert_eq!(fresh["toplevel"][0]["user-icon"], APP_ICON);
    }

    #[test]
    fn no_accounts_removes_the_submenu_only() {
        let with = merge_tree(Some(r#"{"toplevel": [{"uuid": "a", "type": "action"}]}"#), true).unwrap();
        let v = parse(&merge_tree(Some(&with), false).unwrap());
        assert_eq!(uuids(&v["toplevel"]), ["a"]);
    }

    #[test]
    fn a_file_that_is_not_a_layout_is_left_alone() {
        assert_eq!(merge_tree(Some("not json"), true), None);
        assert_eq!(merge_tree(Some(r#"{"toplevel": 3}"#), true), None);
        assert_eq!(merge_tree(Some("{}"), true), None);
    }

    #[test]
    fn merging_twice_changes_nothing() {
        let once = merge_tree(None, true).unwrap();
        assert_eq!(merge_tree(Some(&once), true).unwrap(), once);
    }
}
