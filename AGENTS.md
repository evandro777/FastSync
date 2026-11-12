# AGENTS.md: FastSync

Notes for AI agents and contributors. Read this before changing anything.


## What this is

A **light and fast** Linux desktop app in Rust + Slint: an Insync-style two-way folder sync for Dropbox, Google Drive, OneDrive and MEGA. Priorities: fast start, low memory, simplicity, and above all never losing the user's files. `PLAN.md` has the original design.

**Our own sync engine; rclone only moves the bytes.** `rclone rcd` runs on localhost (random password, our own `rclone.conf`, never the user's) and gives login (`config/create`), `operations/list|stat|copyfile|deletefile`. Everything else is ours:

- `decide.rs`: the pure three-way compare (local, remote, last synced) -> action; conflict names; delete guard; case-collision check. Exhaustively unit-tested. Change it only with a test for the new case.
- `db.rs`: per-account SQLite (`accounts/<id>/state.db`): path -> how both sides looked after the last sync, the remote ids, the remote folder ids and the delta cursor. A row is written only after its transfer succeeded.
- `engine.rs`: `full_scan` (lists both sides; first sync, delta reset, MEGA-style providers) and `sync_paths` (only the paths from inotify or a delta). It talks to the cloud through the `Remote` trait: tests use a plain directory as the "cloud" (no rclone, no network); `rclone.rs` has the real one. The invariants (all tested): local deletes only move to `.fastsync/trash` and only for a file the DB proves we synced; downloads go to `.fastsync/tmp` then rename; a user edit during a download wins; remote names that escape the folder (`..`) are refused; a run deleting more than 50% of 10+ known files stops (`Report.aborted`) until the user presses Sync now.
- `delta.rs`: `RemoteChanges` per provider (Dropbox cursor + long poll, Drive `changes.list`, OneDrive `/delta`) with JSON parsing split from HTTP so it is tested on canned answers. Drive/OneDrive report ids, so `impact()` turns them into paths with the folder ids the DB keeps; a folder created, moved or renamed triggers a full scan because its children's paths change unreported. Expired cursors (`Poll::Reset`) trigger a full scan and a new cursor.
- `runner.rs`: one worker thread per account owns the engine and DB, fed through one channel by the inotify watcher (debounced), the delta poller and timers (a local walk every 2 min, a full scan every 5 min without a feed, a retry after errors). Pause, Sync now, Stop are events.
- Tokens: delta clients read the access token from rclone's config (`Rclone::token` first makes rclone refresh it when close to expiry).
- No rename detection (a rename is an upload plus a delete), no empty folders, no on-demand files: see the README.

## Layout

```txt
Cargo.toml                workspace, release/fast profiles
crates/core/              UI-independent logic: decide, db, engine, rclone, delta, runner, accounts, settings. No Slint here, so a CLI can reuse it
crates/core/tests/rclone_local.rs  the engine through a real rclone with its `local` backend; skipped when rclone is missing
crates/app/               Slint UI + main.rs (shared `STATE` behind a mutex: accounts, runners, statuses) + tray.rs (ksni); build.rs compiles ui/ and bundles translations/
crates/app/src/instance.rs  one window: a second start sends `show` over `$XDG_RUNTIME_DIR/<crate>.sock` and exits; the running one is raised. Identical in every Fast app (this is the master): change it here and copy it to the others
crates/core/src/files.rs  private (0700/0600) atomic writes and `read_or_backup` (a bad file is kept as `<name>.bad.<time>`, never over an older backup). Identical in every Fast app (this is the master): change it here and copy it to the others
crates/app/tray/          the four tray icons (svg + 64px png per status); `build.rs` makes ARGB pixmaps of them for `tray.rs`. Redraw: see docs/ICON-STYLE.md
packaging/                .desktop, SVG icon, 256px PNG icon (the AppImage .DirIcon), metainfo (AppImage, Flatpak and CI validation)
appimage/build.sh         AppImage + .zsync (host libs, not bundled); CI: appimage.yml
flatpak/                  manifest, build.sh (vendors crates, builds outside the repo: `~/.cache/fastsync-flatpak`), new-signing-key.sh, signing-key.b64 (public half, after you run it); CI: flatpak.yml
docs/                     landing page (`site/index.html`, deployed with the Flatpak remote), `screenshots/`, FLATPAK-SIGNING.md, FLATHUB.md, ICON-STYLE.md and UI-STYLE.md (the shared icon and window rules of the Fast apps; the UI values live in `crates/app/ui/family.slint`, and the About card and Preferences row in `about.slint`; both are copied verbatim into every app, together with `controls.slint` (`TabBar`, `TabItem`, `MenuButton`, `CloseButton`, `StepButton`, `PlusIcon`, `AlertIcon`, `Icon`, `NeutralBadge`, `FilterBar`, `SearchBox`; this app uses only `MenuButton`, the rest stay for the kit's sake), and the shared icons in `ui/icons/`, of which each app keeps only those it uses (`search.svg` stays: `controls.slint` needs it))
README.md, BUILD.md       what users read / build, settings and details (keep both in step with behaviour)
dev/gen_po.py             writes the .po files from one table
scripts/ui-snapshot.sh    renders pages on a private Xvfb with canned data (crates/app/src/fixtures.rs, debug builds only, env FASTSYNC_FIXTURES=empty|single|many, FASTSYNC_PAGE=main|missing (rclone banner)|add|add-mega|settings|about) to target/ui-snapshots/. With fixtures nothing is started: no rclone, no runners, no tray
```

## Commands

```bash
cargo build --profile fast    # iteration build, no LTO
cargo build --release         # thin LTO
cargo test                    # unit tests
```

System deps: `libfontconfig-dev libxkbcommon-dev libxkbcommon-x11-dev libx11-dev libxcb1-dev`. No GL/Skia/femtovg.

## Code conventions: Editing code

- Use the Edit tool for code changes. Don't splice files with sed, awk, python or heredocs through Bash.
- After editing Rust, run `cargo fmt && cargo clippy -- -D warnings && cargo test`. Re-read the surrounding block to confirm no braces, lines or whole components (e.g. pages) were deleted.
- After changing dependencies (`Cargo.toml` or `Cargo.lock`), also run `cargo audit`: CI runs it and fails on known vulnerabilities (warnings such as "unmaintained" do not fail). Fix with `cargo update -p <crate>`.
- After any change, also run `cargo build --profile fast` so the binary is built and cached for the owner's manual testing.

## Architecture decisions (inherited from FastMoji, measured there)

- **Software renderer only** (`winit` + `software`, forced in `main.rs` and in `Cargo.toml` features with `default-features = false`): ~27 MB RSS versus ~108 MB with the GL renderer.
- No async runtime. Scans/work in threads, results handed to the UI thread with `slint::invoke_from_event_loop`.
- Never hold a `RefCell` borrow while calling something that makes Slint re-read a model.
- Keep dependencies few and light.
- Slint notes: a `VerticalLayout` inside a `ScrollView` needs `alignment: start`, or a short list spreads its rows over the window; read-only `TextInput` (one per block) makes text selectable; use `content-y` (not `viewport-y`); a callback named `close` shadows a builtin (use `dismiss`); `FocusScope.capture-key-pressed` intercepts keys before `LineEdit`; a wrapped read-only `TextInput` in a nested layout reports one line of height, so give it room with `min-height`, estimated from the available width (a fixed character count leaves a blank band under lines that still fit on one line); `Text` and `TextInput` have no `padding`, so put the space in the parent layout's `spacing` or `padding`.

## Look

- Native Fluent widgets on purpose: apps follow the system accent and light/dark mode. In Slint 1.18 the `Palette` colours are read-only; only `Palette.color-scheme` can be set (the Appearance setting does that). To change a colour you would have to write your own component.
- One page: a header (icon, title, Add account, burger) over a scrolling list of `AccountCard`s (`AccountRow` struct: state 0 up to date, 1 syncing, 2 paused, 3 problem, 4 stopped by the delete guard). A `.po` context is the component name: `Main`, `AccountCard`, `AddDialog`, `SettingsDialog`, `About`, `AboutCard`, `Tr`. A row property called `row` clashes with a built-in Slint property: the card's is `acct`.
- A problem's text is the raw message from rclone, the OS or the provider (not translated); everything FastSync words itself is in `@tr()`.
- Snapshots are 600x450 and cannot scroll, so a list taller than that is only partly visible. To see all of it, render with a taller window: put `height = 800` in a `settings.toml` under a scratch `XDG_DATA_HOME` and run the script with that variable set.
- `LANGUAGE` (often `en_US` on a dev machine) beats `LANG`: to check a translation run the script with `LC_ALL=pt_BR.UTF-8 LANGUAGE=`.

## Dialogs

- **Closing the window hides it to the tray** (`on_close_requested` returns `HideWindow`, the loop is `run_event_loop_until_quit`): syncing must go on. Without a tray host the close button quits instead, or the app would be unreachable. Quit (tray menu) calls `quit()`, which also kills rclone: statics are never dropped, so the child would otherwise outlive the app. This replaces the Fast family rule "closing Main always ends the app".
- Dialogs are in-window overlays (the last children of `Main`), not second windows, and `scripts/ui-snapshot.sh` can capture them (`FASTSYNC_PAGE=add|add-mega|settings|about`). The Add account dialog runs the browser login on a thread (it blocks until the user finishes); `login_gen` makes a cancelled login discard its token. They share one shell, `component Dialog` (dim background, Esc, Tab trap, card capped to the window height); a new dialog inherits it and puts its content inside. `SettingsDialog` (burger button in the header) holds Appearance and Language plus an About button; `About` opens from it and closing About returns to Settings (re-creating Settings gives its keys focus back, so Esc keeps working). The About version comes from `CARGO_PKG_VERSION`, the Website button opens `CARGO_PKG_REPOSITORY` with `xdg-open`. A `ScrollView` does not report its content's height, hence the explicit `preferred-height` in the settings card.
- Theme and language are saved the moment they change (`theme-changed` / `language-changed`, skipped when the value equals the stored one so start-up never writes); window size only on a normal exit. An unknown locale falls back to the English source strings (`select_language` in `main.rs`).

## Translations

- UI strings use `@tr()`; strings built in Rust go through the `Tr` global in `ui/main.slint`. Never `format!` user-visible text in Rust.
- `.po` files are generated by `dev/gen_po.py` (edit its table, run it, commit both). Slint's gettext context is the component name (`Main`, `Tr`). A new `@tr` string needs an entry in every language. A string with a count needs its own singular string for `n == 1` ("1 connection", not "1 connections"), with its own entry.
- Language = `settings.language` (`""` = system locale).
- Appearance = `settings.theme` (`""` = follow the system, `"light"`, `"dark"`). The Appearance box sets `Palette.color-scheme` in the `.slint` (`unknown` hands it back to the desktop). `THEMES` and `theme_index` in `crates/app/src/main.rs` keep the order of that list, and a test checks they agree. `Family.dark` reads the drawn background, so it stays right in every state.

## Conventions

- Rust 2024, `rustfmt` max_width 120, clippy `-D warnings`. Comments explain *why*, not what.
- Settings are serde TOML with `#[serde(default)]`; add a unit test for each new option so old files keep loading.
- Settings path: `$XDG_DATA_HOME/FastSync/settings.toml`.
- Update `README.md` when behaviour changes, and this file when you learn something non-obvious.

## Testing

- `cargo test`. Manual harnesses (if any) go in `dev/`, must never capture the whole screen, and must never click or type at hard-coded coordinates.
- Never `pkill -f` with a pattern that appears in your own command line; kill by PID.

### Verifying UI changes

- After any Slint UI change, run `scripts/ui-snapshot.sh <page>` and read the PNGs (empty, single, many) before saying the change is done. It uses a private Xvfb and canned data, so it never reads or changes the real system.
- Specifically check: rows don't stretch when a list has 1 item, badges and buttons are right-aligned, icons are sharp, items with no icon get a placeholder so headers stay aligned, vertical gaps between rows of one kind are even, counts read right at 1 ("1 items"), a name already in a header is not repeated in every row under it, text is selectable as a block, and empty or error states (e.g. "nothing found") don't throw errors.
- Controls in a row are the same height (32 px); tabs, search boxes and dialog cards use the kit's components (`TabBar`, `SearchBox`, the card in `docs/UI-STYLE.md`).
- If you can't launch it (e.g. the flow needs pkexec or a live X11 session), say so explicitly and list what I should test manually.

## GitHub flow and releases

Work on branches, PRs to `master`. CI: `ci.yml` (fmt, clippy, tests, MSRV, metainfo/desktop validation), `appimage.yml` (AppImage on every push/PR; on a `v*` tag it attaches to the release), `flatpak.yml` (bundle on every push/PR; on a `v*` tag it also publishes the signed remote and the landing page to GitHub Pages, then attaches the bundle), `virustotal.yml` (scans the AppImage and the Flatpak after their workflows; needs the `VIRUSTOTAL_API_KEY` secret). `RELEASING.md` has the one-time setup and the steps. Never commit or push on the owner's behalf.

Release gotchas (each one broke a first release):

- Bump the version in `Cargo.toml`, then run `cargo check` and commit `Cargo.lock`: CI uses `--locked`, so a stale lock fails every job.
- The metainfo needs a `<release version="X">` for the tag, or `flatpak.yml` fails.
- `appimagetool` builds `.DirIcon` from a PNG at the AppDir root, not from the SVG. The icon must follow `docs/ICON-STYLE.md` (the same tile shape for every Fast app, flat colors only). Regenerate `packaging/io.github.evandro777.FastSync.png` when the SVG changes: `convert -background none -density 384 packaging/io.github.evandro777.FastSync.svg -resize 256x256 packaging/io.github.evandro777.FastSync.png`.
- The first tag needs the one-time setup in `RELEASING.md`: the signing key and its secret, Pages, and the `v*` tag rule on the `github-pages` environment.

## Flatpak

App id `io.github.evandro777.FastSync`; `flatpak/build.sh --devel` installs `...FastSync.Devel` next to a release. Needs `flatpak-builder` and `org.freedesktop.Sdk//25.08` + `org.freedesktop.Sdk.Extension.rust-stable//25.08`. The runtime version `25.08` appears in five places (the manifest, the container image tag, the rust-stable install and its `GITHUB_PATH` line in `flatpak.yml`, and the header of `flatpak/build.sh`): bump them together.

- Auto-update = our own signed OSTree remote on GitHub Pages (`flatpak.yml`: `bundle` → `pages` → `release`), deployed on `v*` tags only. Every deploy replaces the whole site, so never deploy Pages from another trigger. The public key is `flatpak/signing-key.b64` (base64 of the binary export, made by `flatpak/new-signing-key.sh`; the template ships none); the private half is the secret `FLATPAK_GPG_PRIVATE_KEY`. See `docs/FLATPAK-SIGNING.md`.
- Keep `finish-args` minimal (the manifest says how for X11-only apps and trays). The build vendors crates (`cargo vendor`), so `flatpak/vendor` must not be in the manifest's `skip:`.
- Patterns to copy when an app needs them (FastClipboard has each one working): inside the sandbox `FLATPAK_ID` is set. A single-instance Unix socket must live in `$XDG_RUNTIME_DIR/app/$FLATPAK_ID/`, the only directory shared between `flatpak run`s. A ksni tray needs `.disable_dbus_name(true)` there. Start at login must go through the Background portal (`org.freedesktop.portal.Background.RequestBackground`, blocking, so from a thread), not a file in `~/.config`. A host command (for example `tesseract`) is reachable only through `flatpak-spawn --host`, which needs the user's own opt-in `flatpak override --talk-name=org.freedesktop.Flatpak`: never put that in the manifest.
- **Flathub policy:** manifests must NOT contain AI-generated content and AI must not open the submission PR. Do not write a Flathub manifest or submission text for the owner; `docs/FLATHUB.md` explains the process.

## Open items

Written without a live cloud account or an rclone binary on the dev machine, so these are **untested against the real services**: the OAuth question flow of `Rclone::login` (follows the documented non-interactive protocol: answers `config_is_local` with true, every other question with its default), the three delta clients' HTTP (only their JSON parsing is tested), token refresh, delta latency, cursor expiry, and `rclone_local.rs` (skipped without rclone). Test each by hand before trusting it.

- Redraw the icon (`packaging/*.svg`, `.png`) following `docs/ICON-STYLE.md`: it is still the template's tile.
- A folder picker for the Add account dialog (the folder is typed in).
- Rename detection (move instead of upload + delete), progress (`core/stats`), start at login (Background portal), FastSync's own verified OAuth client ids (users can already enter theirs: `docs/OWN-CLIENT-ID.md`; an own-id login is untested against rclone 1.76's extra "shared client_id" question, answered by `answer()` in `rclone.rs`), Google Docs export, MEGA's push channel.
- OneDrive work/school accounts with an own app need tenant-specific `auth_url`/`token_url` (rclone docs); not set by FastSync yet.
- `rclone rcd` is killed on a normal quit; after a SIGKILL of the app it is left running (it only listens on localhost with a random password). `PR_SET_PDEATHSIG` would fix it.
- Cancelling a login while the browser is open leaves rclone's auth server busy until its own timeout.

