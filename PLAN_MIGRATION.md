# Plan: migrate FastSync to Slint 1.19

**Priority: low (hygiene).** FastSync is a normal window, not a resident popup that is hidden and shown again, so it
does not have the idle-CPU bug found in FastMoji (Slint 1.18.1 re-arms a 16 ms redraw timer forever after a window is
hidden). Do it so all Fast apps stay on one Slint version (`ui/family.slint` is shared) and for the 1.19 fixes: changes
no longer render twice on X11, faster start, partial-repaint fix after a scale change. FastMoji was migrated on
2026-11-19: copy what worked there.

If FastSync hides its window to the tray or any other way and keeps running while it syncs, treat it like
FastClipboard (high priority): measure idle wakeups after a hide (see Verify).

## Before you start

- `git status` shows **30 uncommitted files**: commit or stash them first, or the migration will be lost in a large diff.
- Rust 1.95 is the new minimum (Slint 1.19). Flatpak extension `rust-stable//25.08` is 1.98.1; local rustc 1.99.1.

## Steps

1. `Cargo.toml`: `rust-version = "1.92"` -> `"1.95"`.
2. `crates/app/Cargo.toml` (lines 17 and 20): `slint` and `slint-build` `"1.18"` -> `"1.19"`.
3. `.github/workflows/ci.yml` line **69** (the file has the toolchain jobs at lines 36 and 69, not 28 and 61):
   `dtolnay/rust-toolchain@1.92` -> `@1.95`. Line 36 is `@stable`: leave.
4. Docs: `grep -rn "1\.92\|Slint 1\.18" --exclude-dir=target --exclude-dir=.git .` and update.
5. `cargo update -p slint -p slint-build`; `cargo fmt && cargo clippy -- -D warnings && cargo test`; `cargo audit`;
   `cargo build --profile fast`.
6. Do not touch `ui/family.slint` or `src/instance.rs` (verbatim copies from FastHello).

## Verify

- `scripts/ui-snapshot.sh` (if present) and read the PNGs: 1.19 changed layout, drop-shadow and gradient details.
- Idle wakeups: read the main thread's `voluntary_ctxt_switches` from `/proc/<pid>/task/<pid>/status` over 3 s while
  the app is idle, and again after hiding or minimising it. Expected: under ~5/s.
- `unstable-winit-030` is used: confirm it still compiles (it did in FastMoji).

## Risks and rollback

- Toolchain older than 1.95 on CI or Flatpak (both are newer today).
- Revert = restore the Cargo.toml lines, `ci.yml` and `Cargo.lock` from git.
- Do not set the 1.19 `edition`: it changes layout rules and is not needed.
