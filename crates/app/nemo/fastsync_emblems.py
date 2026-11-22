# FastSync: per-file sync emblems for Nemo. Installed by FastSync into ~/.local/share/nemo-python/extensions (needs the
# `python-nemo` package; Ubuntu calls it nemo-python). Nothing is stored on the files: Nemo asks the running app, so
# with the app closed there are no emblems. Both Nemo and nemo-desktop load this file. Protocol (text lines, `emblem.rs` is the other half): `hello 1` -> `ok 1 <json list of synced folders>`,
# `q <path>` -> idle|syncing|error|paused|ignored|none, and on a second connection `watch` -> pushed `c <path>` (state changed),
# `all`, `roots <json>`.
import glob
import json
import os
import socket
import time
from collections import OrderedDict

import gi

gi.require_version("Nemo", "3.0")
from gi.repository import GLib, GObject, Nemo  # noqa: E402

STATES = ("idle", "syncing", "error", "paused", "ignored")
ASK_TIMEOUT = 0.15  # Nemo's main thread waits this long at most for one answer
RETRY_SECONDS = 5  # a hung or missing app is not asked again for this long
REMEMBER = 20000  # files kept to refresh when the app says their state changed


def socket_paths():
    run = os.environ.get("XDG_RUNTIME_DIR")
    if not run:
        return []
    # The Flatpak (and its .Devel) can only share app/<id>/ with the host. Only called when (re)connecting.
    # All of them are tried: a dead socket file left by another build must not hide the live one.
    plain = os.path.join(run, "fastsync-emblems.sock")
    return [plain] + sorted(glob.glob(os.path.join(glob.escape(run), "app", "io.github.evandro777.FastSync*", "fastsync-emblems.sock")))


class FastSyncEmblems(GObject.GObject, Nemo.InfoProvider):
    def __init__(self):
        super().__init__()
        self.roots = None  # synced folders (None until the app said); files elsewhere never touch the socket
        self.q = None  # query connection: (socket, reader)
        self.down_until = 0.0
        self.watch = None  # push connection: socket
        self.buf = b""
        self.missed = set()  # files that got no answer; refreshed by tick once the link is back
        self.seen =OrderedDict()  # path -> True, every file Nemo asked about (even while the app was down)
        GLib.timeout_add_seconds(RETRY_SECONDS, self.tick)

    # -- connections -------------------------------------------------------------------------------------------

    def _open(self, first_line):
        for path in socket_paths():
            c = self._open_at(path, first_line)
            if c:
                return c
        return None

    def _open_at(self, path, first_line):
        s = socket.socket(socket.AF_UNIX)
        s.settimeout(ASK_TIMEOUT)
        try:
            s.connect(path)
            r = s.makefile("rb")
            s.sendall(b"hello 1\n")
            line = r.readline(4300).decode("utf-8", "replace").rstrip("\n")
            if not line.startswith("ok 1 "):
                raise OSError("protocol")
            self.roots = [x for x in json.loads(line[5:]) if isinstance(x, str)]
            if first_line:
                s.sendall(first_line)
            return s, r
        except Exception:
            s.close()
            return None

    def ensure(self):
        if self.q:
            return True
        if time.monotonic() < self.down_until:
            return False
        self.q = self._open(None)
        if not self.q:
            self.down_until = time.monotonic() + RETRY_SECONDS
            return False
        return True

    def drop_query(self):
        if self.q:
            self.q[0].close()
        self.q = None
        self.down_until = time.monotonic() + RETRY_SECONDS

    def tick(self):
        # Also what reconnects the push channel after the app was restarted (its answers are asked for again).
        if not self.watch and time.monotonic() >= self.down_until:
            c = self._open(b"watch\n")
            if c:
                c[1].close()
                self.watch = c[0]
                self.watch.setblocking(False)
                self.buf = b""
                GLib.io_add_watch(
                    self.watch.fileno(), GLib.PRIORITY_DEFAULT, GLib.IO_IN | GLib.IO_HUP | GLib.IO_ERR, self.on_push
                )
                self.invalidate_all()
        # A file asked while the query link was down (or slow: 150 ms) got no emblem and nothing would ask again
        # until the user pressed F5, which could hit the same pause. Ask again once the link may be back.
        if self.missed and time.monotonic() >= self.down_until:
            missed, self.missed = self.missed, set()
            for p in missed:
                self.refresh(p)
        return True

    def on_push(self, _fd, cond):
        ok = bool(cond & GLib.IO_IN)
        if ok:
            try:
                data = self.watch.recv(65536)
                ok = bool(data)
                self.buf += data
            except BlockingIOError:
                return True
            except OSError:
                ok = False
        while b"\n" in self.buf:
            line, self.buf = self.buf.split(b"\n", 1)
            self.handle(line.decode("utf-8", "replace"))
        if len(self.buf) > 70000:
            ok = False
        if not ok:
            # The app quit or crashed: the emblems must go, so every file is asked again (and now gets no answer).
            self.watch.close()
            self.watch = None
            self.drop_query()
            self.roots = None
            self.invalidate_all()
            return False
        return True

    def handle(self, line):
        if line.startswith("c "):
            self.invalidate(line[2:])
        elif line == "all":
            self.invalidate_all()
        elif line.startswith("roots "):
            try:
                self.roots = [x for x in json.loads(line[6:]) if isinstance(x, str)]
            except ValueError:
                pass

    # -- Nemo --------------------------------------------------------------------------------------------------

    def inside(self, path):
        return any(path == r or path.startswith(r + "/") for r in self.roots)

    def update_file_info(self, f):
        if f.get_uri_scheme() != "file":
            return
        loc = f.get_location()
        path = loc.get_path() if loc else None
        # A line break would end the request: such a name just gets no emblem.
        if not path or "\n" in path:
            return
        if self.roots is not None and not self.inside(path):
            return
        self.seen[path] = True
        if len(self.seen) > REMEMBER:
            self.seen.popitem(last=False)
        if not self.ensure():
            self.missed.add(path)
            return
        try:
            s, r = self.q
            s.sendall(b"q " + path.encode("utf-8", "surrogateescape") + b"\n")
            word = r.readline(64).decode("utf-8", "replace").strip()
            if not word:
                raise OSError("closed")
        except Exception:
            self.drop_query()
            self.missed.add(path)
            return
        if word in STATES:
            f.add_emblem("emblem-fastsync-" + word)

    def refresh(self, path):
        # lookup, not create: only a file Nemo still holds needs refreshing, and none is built for the rest.
        try:
            f = Nemo.FileInfo.lookup_for_uri(GLib.filename_to_uri(path))
            if f:
                f.invalidate_extension_info()
        except Exception:
            pass

    def invalidate(self, path):
        # A folder's emblem depends on what is inside it: every remembered folder above the file is asked again too.
        while path and path != "/":
            if path in self.seen:
                self.refresh(path)
            path = os.path.dirname(path)

    def invalidate_all(self):
        for p in list(self.seen):
            self.refresh(p)
