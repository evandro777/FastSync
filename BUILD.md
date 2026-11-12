# Build

## How it is built

- **Rust + Slint** with the winit backend and the **software renderer** (no GPU/GL context, which is where most of the memory of a toolkit app goes).
- **`crates/core`** has the logic with no UI code, so a CLI could reuse it: the three-way compare (`decide.rs`), the SQLite state (`db.rs`), the engine (`engine.rs`), the rclone client (`rclone.rs`), the delta clients (`delta.rs`) and the per-account thread (`runner.rs`). **`crates/app`** is the Slint UI, the tray and `main.rs`.
- **rclone is the transport only.** `rclone rcd` runs on localhost with a random password and its own config file; FastSync calls `config/create` (login), `operations/list|stat|copyfile|deletefile`. Sync decisions are ours.
- **No async runtime.** Work runs in threads and is handed to the UI thread with `slint::invoke_from_event_loop`. HTTP is blocking `ureq`.

More detail for contributors is in [`AGENTS.md`](AGENTS.md).

## First build

Install a Rust toolchain and the libraries it links.

Debian/Ubuntu/Mint:

```bash
sudo apt install libfontconfig-dev libxkbcommon-dev libxkbcommon-x11-dev libx11-dev libxcb1-dev rustup rclone
rustup default stable
```

`rclone` (prefer the official build from rclone.org: Debian/Ubuntu's package lacks the MEGA backend and is old; check with `rclone help backends | grep -i mega`) is only needed to run the app and the one integration test (`crates/core/tests/rclone_local.rs`, skipped without it; `FASTSYNC_RCLONE=/path/to/rclone` points at another binary). SQLite is compiled in.

## How to run

```bash
cargo run --profile fast         # quick iteration build (no LTO)
cargo build --release            # thin LTO
cargo test
```

## From a release (Flatpak bundle)

Download `FastSync-<version>-x86_64.flatpak` from the [Releases](https://github.com/evandro777/FastSync/releases) page, then:

```bash
flatpak install --user ./FastSync-<version>-x86_64.flatpak
flatpak run io.github.evandro777.FastSync
```

For automatic updates use the Flatpak remote described in the [README](README.md#install) instead.

## Build the Flatpak yourself

App id: `io.github.evandro777.FastSync`.

```bash
sudo apt install flatpak-builder
flatpak install --user flathub org.freedesktop.Sdk//25.08 org.freedesktop.Sdk.Extension.rust-stable//25.08
flatpak/build.sh --devel      # installs io.github.evandro777.FastSync.Devel next to a release
flatpak run io.github.evandro777.FastSync.Devel
```

`flatpak/build.sh` without `--devel` builds the real id; `--bundle` also writes `flatpak/fastsync.flatpak`. Crates are vendored
into `flatpak/vendor` (git-ignored) because the sandboxed build has no network, and the build tree goes to
`~/.cache/fastsync-flatpak` (set `FASTSYNC_FLATPAK_CACHE` to move it), so it stays out of the repository. The sandbox only gets the
sockets listed in `finish-args` of the manifest. Settings live in `~/.var/app/io.github.evandro777.FastSync/data/FastSync/`.

How the signed remote is built on GitHub, and how to make its key: [`docs/FLATPAK-SIGNING.md`](docs/FLATPAK-SIGNING.md).

## Build the AppImage yourself

```bash
appimage/build.sh             # needs appimagetool on PATH; writes FastSync-<version>-x86_64.AppImage (+ .zsync)
```

## Settings and data

`$XDG_DATA_HOME/FastSync/settings.toml` (defaults to `~/.local/share/FastSync`; in the Flatpak under `~/.var/app/<app-id>/data/`):

- `language`: `""` follows the system locale; otherwise a code such as `"pt_BR"`. The Language choice in the settings dialog offers `""`, `"en"` and `"pt_BR"`, applies at once and is saved at once.
- `theme`: `""` follows the system light/dark mode; `"light"` or `"dark"` forces one. Set in the settings dialog (burger button), saved at once.
- `width`, `height`: the window size, saved when the app exits.

Next to it:

- `accounts.toml`: the linked accounts (`id`, `kind` = the rclone backend, `local`, `remote_path`, `paused`).
- `rclone.conf` (0600): FastSync's own rclone config with the OAuth tokens (MEGA: email and the obscured, not encrypted, password). Your own `~/.config/rclone/rclone.conf` is never read or written.
- `accounts/<id>/state.db`: the sync state of one account (SQLite): per file how both sides looked after the last sync, the delta cursor and the remote folder ids. Deleting it is safe: the next run compares both sides from scratch, and a file that differs on both sides becomes a conflict copy, never a deletion.
- Inside each synced folder, `.fastsync/` holds temp downloads and `trash/<time>/` (files removed in the cloud). It is never synced; empty it whenever you like.

Providers: Dropbox, Google Drive and OneDrive use their delta APIs (the access token is read from the rclone config and refreshed through rclone); everything else is listed in full every 5 minutes. rclone's shared OAuth client ids are rate-limited (and Google's will be retired in 2026), so Add account has "Use my own client ID": the `client_id` and `client_secret` go into `rclone.conf` through the login parameters. The user guide is [`docs/OWN-CLIENT-ID.md`](docs/OWN-CLIENT-ID.md); the dialog's button opens its GitHub URL (`HELP_URL` in `main.rs`), so the file must stay at that path on `master`.
