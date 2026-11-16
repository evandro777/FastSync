//! The local trash: everything the engine replaces or removes goes to `<root>/.fastsync/trash/<unix seconds>/<path>`
//! first. Counting, emptying and ageing it are here; moving a file in is `Engine::trash`.
use std::{
    fs,
    path::{Path, PathBuf},
};

use crate::{engine::HIDDEN, rclone::now_secs};

/// Where everything we replace or remove locally goes: `<root>/.fastsync/trash/<time>/<path>`.
pub fn trash_dir(root: &Path) -> PathBuf {
    root.join(HIDDEN).join("trash")
}

/// (files, bytes) in the trash. Never follows a symlink: a link counts as a file of its own size.
pub fn trash_usage(root: &Path) -> (u64, u64) {
    fn go(dir: &Path, total: &mut (u64, u64)) {
        let Ok(entries) = fs::read_dir(dir) else { return };
        for e in entries.flatten() {
            let Ok(m) = fs::symlink_metadata(e.path()) else {
                continue;
            };
            if m.is_dir() {
                go(&e.path(), total);
            } else {
                total.0 += 1;
                total.1 += m.len();
            }
        }
    }
    let mut total = (0, 0);
    go(&trash_dir(root), &mut total);
    total
}

/// Removes the trash for good. A missing folder is fine. `remove_dir_all` does not follow symlinks.
pub fn empty_trash(root: &Path) -> std::io::Result<()> {
    match fs::remove_dir_all(trash_dir(root)) {
        Err(e) if e.kind() != std::io::ErrorKind::NotFound => Err(e),
        _ => Ok(()),
    }
}

/// Deletes the trash batches (`trash/<unix seconds>/…`, named when the files were trashed) at least `days` old.
/// 0 keeps everything. A folder whose name is not a number is not ours and stays.
pub fn purge_trash(root: &Path, days: u32) -> std::io::Result<()> {
    purge_trash_at(root, days, now_secs())
}

pub(crate) fn purge_trash_at(root: &Path, days: u32, now: i64) -> std::io::Result<()> {
    if days == 0 {
        return Ok(());
    }
    let limit = now.saturating_sub(i64::from(days) * 86_400);
    let entries = match fs::read_dir(trash_dir(root)) {
        Ok(e) => e,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(e),
    };
    let mut first_error = None;
    for e in entries.flatten() {
        let Some(stamp) = e.file_name().to_str().and_then(|n| n.parse::<i64>().ok()) else {
            continue;
        };
        if stamp > limit {
            continue;
        }
        // One stuck batch must not keep the others from going.
        match fs::remove_dir_all(e.path()) {
            Err(e) if e.kind() != std::io::ErrorKind::NotFound => first_error = first_error.or(Some(e)),
            _ => {}
        }
    }
    first_error.map_or(Ok(()), Err)
}
