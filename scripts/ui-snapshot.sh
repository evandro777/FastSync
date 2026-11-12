#!/usr/bin/env bash
# Renders pages of the app on a private Xvfb display with canned data (FASTSYNC_FIXTURES) and saves PNGs to
# target/ui-snapshots/<page>-<mode>.png. The real system is never scanned or changed.
# Usage: scripts/ui-snapshot.sh [page ...]   (names or numbers; default all)   MODES="empty single many" to narrow.
set -euo pipefail
cd "$(dirname "$0")/.."

# Add one entry per sidebar page, with the number the app's `page` property uses.
declare -A PAGES=([main]=main [missing]=missing [add]=add [add-own]=add-own [add-mega]=add-mega [settings]=settings [about]=about)
MODES=${MODES:-"empty single many"}
OUT=target/ui-snapshots
mkdir -p "$OUT"

cargo build --quiet

# Xvfb picks a free display and reports its number, so the script can never capture someone else's screen.
DISPLAY_FILE=$(mktemp)
Xvfb -displayfd 3 -screen 0 1280x800x24 3>"$DISPLAY_FILE" >/dev/null 2>&1 &
XVFB=$!
APP=
cleanup() { [ -n "$APP" ] && kill "$APP" 2>/dev/null || true; kill "$XVFB" 2>/dev/null || true; rm -f "$DISPLAY_FILE"; }
trap cleanup EXIT
for _ in $(seq 50); do [ -s "$DISPLAY_FILE" ] && break; sleep 0.1; done
[ -s "$DISPLAY_FILE" ] || { echo "Xvfb did not start" >&2; exit 1; }
DISPLAY_NUM=:$(head -n1 "$DISPLAY_FILE")

[ $# -gt 0 ] && names=("$@") || names=("${!PAGES[@]}")

for name in "${names[@]}"; do
  page=${PAGES[$name]:-$name}
  for mode in $MODES; do
    DISPLAY=$DISPLAY_NUM FASTSYNC_FIXTURES=$mode FASTSYNC_PAGE=$page target/debug/fastsync >/dev/null 2>&1 &
    APP=$!
    for _ in $(seq 50); do
      DISPLAY=$DISPLAY_NUM xdotool search --name FastSync >/dev/null 2>&1 && break
      sleep 0.2
    done
    sleep 1
    DISPLAY=$DISPLAY_NUM import -window root "$OUT/$name-$mode.png"
    kill "$APP" 2>/dev/null || true
    wait "$APP" 2>/dev/null || true
    APP=
    echo "$OUT/$name-$mode.png"
  done
done
