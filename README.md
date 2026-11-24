# <img src="packaging/io.github.evandro777.FastSync.svg" width="36" height="36" alt=""> FastSync

[![CI](https://github.com/evandro777/FastSync/actions/workflows/ci.yml/badge.svg)](https://github.com/evandro777/FastSync/actions/workflows/ci.yml)
[![Flatpak](https://github.com/evandro777/FastSync/actions/workflows/flatpak.yml/badge.svg)](https://github.com/evandro777/FastSync/actions/workflows/flatpak.yml)
[![AppImage](https://github.com/evandro777/FastSync/actions/workflows/appimage.yml/badge.svg)](https://github.com/evandro777/FastSync/actions/workflows/appimage.yml)
[![VirusTotal](https://github.com/evandro777/FastSync/actions/workflows/virustotal.yml/badge.svg)](https://github.com/evandro777/FastSync/actions/workflows/virustotal.yml)
[![made-for-linux](https://img.shields.io/badge/Linux-1f425f?logo=linux)](https://www.linux.org/)
[![License: GPL-3.0](https://img.shields.io/badge/license-GPL--3.0-orange)](https://opensource.org/licenses/GPL-3.0)

Two-way folder sync for Dropbox, Google Drive, OneDrive and MEGA, built with Rust + [Slint](https://slint.dev) (software renderer, no GL). Pick a folder, log in, and its files stay in sync both ways and available offline. It owns the sync logic (what changed, what to upload, download or delete) and uses [rclone](https://rclone.org) only to log in and move the bytes.

Page: <https://evandro777.github.io/FastSync/>

![The FastSync window: one card per linked account with its status, the Pause, Open folder and Activity buttons and a ⋯ menu (Sync now, Open in browser, Edit, Remove)](docs/screenshots/main.png)

## Disclaimer

> [!WARNING]
> This is an early version. It works, but it is not yet polished and may have bugs. Please back up your files before using it.
> MEGA is not fully supported yet: it lacks a lot of features, and might have a lot of problems. Use it at your own risk.
> Never sync the same local folder with FastSync and another sync client at the same time (the official Dropbox or Google Drive client, Maestral, Insync...). Two clients fighting over one folder can duplicate or delete files.
> Network drives and some external drives are not tested. File changes there may not be seen at once (the folder watcher can miss them, so FastSync falls back to a check every 2 minutes), and slow or dropped mounts look like deleted files to a sync. Keep your synced folder on a local disk.

## Why FastSync

- **One app for four clouds.** Dropbox, Google Drive, OneDrive and MEGA, several accounts at once, each with its own local folder.
- **Light and fast.** Software renderer, no GL, no async runtime: about 27 MB of RAM. Local edits are seen at once (inotify) and cloud edits come from the service's change feed, so the whole cloud is not listed again.
- **Never loses your files.** Deletes go to `.fastsync/trash`, downloads are written to a temp file and renamed, and a **delete guard** stops any run that would remove more than half of your files (or every file of a folder that is still there but emptied, as an unmounted drive would be) until you answer on the card. A folder you delete yourself, or that is deleted or renamed in the cloud, goes through without a question.
- **Real two-way sync, offline.** Everything stays on disk. A conflict keeps both copies, and renames, moves and copies are done in the cloud instead of being sent again.
- **Selective sync, download-only mode and ignore rules** (`rules.fastsyncignore`, gitignore syntax, shared by all your computers).
- **Your own client ID**, so you do not depend on rclone's shared one and its rate limits ([how to](docs/OWN-CLIENT-ID.md)).
- **Linux desktop integration.** Start at login (Preferences), tray, notifications, Nemo "Copy share link" and "View on web", per-file sync emblems in Nemo (needs the `python-nemo` package, `nemo-python` on Ubuntu, then `nemo -q`), activity log per account, cloud space gauge.
- **Free and open source** (GPL-3.0). Flatpak with automatic updates, or an AppImage.

The full list, with every rule and edge case, is in [docs/FEATURES.md](docs/FEATURES.md).

## How it compares

|                                                     | FastSync   | Dropbox (official)     | \*Maestral | \*\*Insync | `rclone bisync`  |
|-----------------------------------------------------|------------|------------------------|------------|------------|------------------|
| Dropbox                                             | ✅         | ✅                     | ✅         | ✅         | ✅               |
| Google Drive                                        | ✅         | ❌                     | ❌         | ✅         | ✅               |
| OneDrive                                            | ✅         | ❌                     | ❌         | ✅         | ✅               |
| MEGA                                                | ⚠️ partial | ❌                     | ❌         | ❌         | ✅               |
| Dropbox only: Doesn't count as 3-device limit       | ✅         | ❌                     | ✅         | n/a        | n/a              |
| Dropbox only: Block-level sync (only changed parts) | ❌         | ✅                     | ❌         | ❌         | ❌               |
| Several accounts at once                            | ✅         | ⚠️ 1 personal + 1 work | ❌         | ✅         | ✅ (by hand)     |
| Graphical app and tray                              | ✅         | ✅                     | ✅         | ✅         | ❌ command line  |
| Real-time change detection                          | ✅         | ✅                     | ✅         | ✅         | ❌ you run it    |
| Selective sync                                      | ✅         | ✅                     | ✅         | ✅         | ⚠️ filters       |
| Delete guard (asks before mass deletes)             | ✅         | ❌                     | ❌         | ❌         | ⚠️ `--max-delete`|
| Deleted files kept in a local trash                 | ✅ 90 days | ❌                     | ❌         | ❌         | ❌               |
| Your own OAuth client ID                            | ✅         | n/a                    | n/a        | ❌         | ✅               |
| Online-only (on-demand) files                       | ❌         | ✅                     | ❌         | ✅         | ❌               |
| Open source                                         | ✅ GPL-3.0 | ❌                     | ✅ GPL-3.0 | ❌         | ✅ MIT           |
| Price                                               | Free       | Free tier              | Free       | Paid       | Free             |

This is a rough guide from each project's public documentation and may be out of date: check before you decide. FastSync is young and mostly tested by hand; see the disclaimer above.

**A drawback: no block-level sync.** The official Dropbox client sends only the changed blocks of a file, which is faster when a big file changes a little (a large database, a VM image, a long document). FastSync, through rclone, sends the whole file again. To make up for it, it works hard not to transfer anything it does not need: a file that is already identical is adopted, not sent or downloaded again (size, time and hash are checked first), and renames, moves and copies are done inside the cloud (a server-side move or copy) or on your disk (a local copy), instead of a new upload or download.

- \*[Maestral](https://maestral.app)
- \*\*[Insync](https://www.insynchq.com)

## Install

### Flatpak

#### Install from file (no updates)

A `.flatpak` file downloaded from Releases is a one-off install (will not auto update). To get automatic updates, go to [Install with automatic updates](<#Install with automatic updates>).

Download `FastSync-<version>-x86_64.flatpak` from [Releases](https://github.com/evandro777/FastSync/releases), then execute:

```bash
flatpak install --user ~/Downloads/FastSync-<version>-x86_64.flatpak
```

#### Install with automatic updates

This app is not in Flathub yet. You need to add this repository as a remote. Every release is also published as a Flatpak remote, so `flatpak update` (or your software centre) keeps the app updated.

```bash
flatpak remote-add --system --if-not-exists fastsync https://evandro777.github.io/FastSync/fastsync.flatpakrepo
flatpak install --system fastsync io.github.evandro777.FastSync
```

**`--system` vs `--user` matters for updates.** Linux Mint's Update Manager (and probably other software centres) only looks at *system-wide* Flatpaks; With `--user`, update by hand: `flatpak update --user`.

The remote is GPG-signed (system-wide installs refuse unsigned remotes), but the key lives in this same repository. It needs the Flathub remote for the runtime (`flatpak remote-add --if-not-exists flathub https://dl.flathub.org/repo/flathub.flatpakrepo`, with `--system` or `--user` to match).

### AppImage (with delta updates)

Download `FastSync-<version>-x86_64.AppImage` from [Releases](https://github.com/evandro777/FastSync/releases), then execute:

```bash
sudo chmod +x ~/Downloads/FastSync-<version>-x86_64.AppImage
~/Downloads/FastSync-<version>-x86_64.AppImage
```

It uses the fontconfig, xkbcommon and X11 libraries already on your system, and it needs `rclone` installed. Use the official build from [rclone.org](https://rclone.org/install/): Debian and Ubuntu's `apt` package is old and leaves out some services (MEGA, for one). The Flatpak bundles the official one.

The AppImage does not update itself, but it has update information built in, so a tool such as [AppImageUpdate](https://github.com/AppImageCommunity/AppImageUpdate) (`appimageupdatetool FastSync-*.AppImage`), [Gear Lever](https://github.com/mijorus/gearlever) or [AppManager](https://github.com/kem-a/AppManager) downloads only the changed parts of the next release.

## Desktop Integration (optional)

### Nemo integration

For the Nemo file manager (Linux Mint's default), FastSync adds two things inside your synced folders:

- **Status emblems** on each file and folder: up to date, syncing, error, paused, or ignored (by `rules.fastsyncignore` or an unchecked folder). A grey "?" means FastSync did not answer in time; it asks again within a second.
- **A "FastSync" submenu** in the right-click menu, with "Copy share link" and "View on web".

To turn it on:

1. Install the `python-nemo` package (`nemo-python` on Ubuntu). Linux Mint usually has it already. If not: `sudo apt install python-nemo`.
2. Start FastSync. On start it writes the Nemo extension, the emblem icons and the menu actions into your home folder (`~/.local/share/nemo-python/extensions`, `~/.local/share/icons/hicolor`, `~/.local/share/nemo/actions`).
3. Restart Nemo once so it loads them: `nemo -q`.

Good to know:

- Emblems are live: they come from the running app, and nothing is written to your files. Quit FastSync and they disappear.
- Emblems can be turned off in Settings, which removes the extension.
- Nemo hides emblems on thumbnails that are too small for them. At the default zoom, wide images and videos show none; zoom in to see them.
- Nemo only. Works in the Flatpak too (it has access to your home folder, where Nemo looks for the extension).
- Most of this is untested in a live Nemo, so tell us if something does not show.

## Build & Configurations

[Build and configurations information](BUILD.md)

## Contributing and releases

See [`CONTRIBUTING.md`](CONTRIBUTING.md) to build, test and send a change, and [`RELEASING.md`](RELEASING.md) for how a release is cut. CI (format, clippy, tests, MSRV, packaging checks, and the AppImage and Flatpak builds) runs on every pull request.

## License

GPL-3.0-or-later. See [`LICENSE`](LICENSE).

The Flatpak also contains the unmodified official [rclone](https://rclone.org) binary, which is MIT licensed (copyright Nick Craig-Wood and contributors; source and licence at <https://github.com/rclone/rclone>).
