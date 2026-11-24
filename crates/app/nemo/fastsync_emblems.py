# FastSync: per-file sync emblems for Nemo. Installed by FastSync into ~/.local/share/nemo-python/extensions (needs the
# `python-nemo` package; Ubuntu calls it nemo-python). Nothing is stored on the files: Nemo asks the running app, so
# with the app closed there are no emblems. Both Nemo and nemo-desktop load this file. Protocol (text lines, `emblem.rs` is the other half): `hello 1` -> `ok 1 <json list of synced folders>`,
# `q <path>` -> idle|syncing|error|paused|ignored|unknown|none, and on a second connection `watch` -> pushed `c <path>` (state changed),
# `all`, `roots <json>`.
# Nemo's main thread never waits for the app: a question is sent without blocking, Nemo gets IN_PROGRESS, and the answer
# is read by a GLib watch, which then tells Nemo (`info_provider_update_complete_invoke`). Answers come back in the
# order of the questions, so `pending` is a plain queue.
import glob
import json
import os
import socket
import sys
import time
from collections import OrderedDict, deque
import gi
gi.require_version("Nemo", "3.0")
from gi.repository import GLib, GObject, Nemo  # noqa: E402
STATES = ("idle", "syncing", "error", "paused", "ignored", "unknown")
UNKNOWN = "emblem-fastsync-unknown"
ASK_TIMEOUT = 2.0  # an answer later than this is given up: the file shows the "unknown" badge and is asked again
RETRY_SECONDS = 5  # an app that is not there is not tried again for this long
RETRY_FAST = 1.0  # after a timeout the app is there but slow: ask again soon
MAX_PENDING = 1000  # questions in flight; a file past this is asked again later
REMEMBER = 20000  # files kept to refresh when the app says their state changed
PATH, FILE, HANDLE, CLOSURE, ASKED, CANCELLED, PROVIDER = range(7)  # fields of a `pending` entry
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
        self.q = None  # query connection (non-blocking socket)
        self.q_src = None  # its GLib watch
        self.q_buf = b""
        self.q_greeted = False  # the `ok 1 ...` line is the first thing on a new connection
        self.pending = deque()  # asked, not answered yet: [path, file, handle, closure, asked at, cancelled, provider]
        self.app_down = False  # the last connect failed: no app, so nothing to retry for the files asked meanwhile
        self.down_until = 0.0
        self.watch = None  # push connection: socket
        self.watch_next = 0.0
        self.buf = b""
        self.missed = set()  # files that got no real answer; asked again by tick
        self.missed_next = 0.0
        self.seen = OrderedDict()  # path -> True, every file Nemo asked about (even while the app was down)
        GLib.timeout_add_seconds(1, self.tick)
    # -- connections -------------------------------------------------------------------------------------------
    def _open(self, first_line):
        for path in socket_paths():
            s = self._open_at(path, first_line)
            if s:
                return s
        return None
    def _open_at(self, path, first_line):
        # Nothing here waits: a Unix socket connects at once or fails at once, and the greeting is read later by the
        # watch (the app answers `hello 1` first, then whatever follows on the same connection).
        s = socket.socket(socket.AF_UNIX)
        s.setblocking(False)
        try:
            s.connect(path)
            s.sendall(b"hello 1\n" + first_line)
            return s
        except OSError:
            s.close()
            return None
    def ensure(self):
        if self.q:
            return True
        if time.monotonic() < self.down_until:
            return False
        s = self._open(b"")
        self.app_down = not s
        if not s:
            self.down_until = time.monotonic() + RETRY_SECONDS
            return False
        self.q, self.q_buf, self.q_greeted = s, b"", False
        self.q_src = GLib.io_add_watch(
            s.fileno(), GLib.PRIORITY_DEFAULT, GLib.IO_IN | GLib.IO_HUP | GLib.IO_ERR, self.on_query
        )
        return True
    def drop_query(self, delay=0.0, in_callback=False):
        # In its own callback the watch ends by returning False; removing it there too would be an error.
        if self.q_src and not in_callback:
            GLib.source_remove(self.q_src)
        self.q_src = None
        if self.q:
            self.q.close()
        self.q = None
        self.down_until = time.monotonic() + delay
    def complete(self, e):
        # The provider is the one Nemo handed to update_file_info_full (its own wrapper of this extension), not `self`:
        # Nemo matches the answer to the job by it and the handle, and drops one that does not fit.
        try:
            Nemo.info_provider_update_complete_invoke(
                e[CLOSURE], e[PROVIDER], e[HANDLE], Nemo.OperationResult.COMPLETE
            )
        except Exception:
            pass
    def fail(self, pend, badge):
        # Every file that was waiting is told to go on, and is asked again soon.
        for e in pend:
            self.missed.add(e[PATH])
            if e[CANCELLED]:
                continue
            if badge:
                e[FILE].add_emblem(UNKNOWN)
            self.complete(e)
    def link_lost(self, in_callback=False):
        # The app closed the link or quit. No badge: with the app gone there are no emblems, and a link only closed
        # (an old app, a restart) is back at the next question.
        pend, self.pending = self.pending, deque()
        self.drop_query(0.0, in_callback)
        self.fail(pend, False)
    def expire(self):
        if self.pending and time.monotonic() - self.pending[0][ASKED] > ASK_TIMEOUT:
            print("FastSync emblems: no answer in %d s, asking again" % ASK_TIMEOUT, file=sys.stderr)
            pend, self.pending = self.pending, deque()
            self.drop_query(RETRY_FAST)
            self.fail(pend, True)
    def tick(self):
        now = time.monotonic()
        self.expire()
        # What reconnects the push channel after the app was restarted (its answers are asked for again).
        if not self.watch and now >= self.watch_next:
            s = self._open(b"watch\n")
            if s:
                self.watch = s
                self.buf = b""
                GLib.io_add_watch(
                    self.watch.fileno(), GLib.PRIORITY_DEFAULT, GLib.IO_IN | GLib.IO_HUP | GLib.IO_ERR, self.on_push
                )
                self.invalidate_all()
            else:
                self.watch_next = now + RETRY_SECONDS
        # A file that got no real answer (slow app, link closed, "unknown") would otherwise wait for F5.
        if self.missed and now >= max(self.down_until, self.missed_next):
            missed, self.missed = self.missed, set()
            self.missed_next = now + 2
            for p in missed:
                self.refresh(p)
        return True
    # -- answers -----------------------------------------------------------------------------------------------
    def set_roots(self, text):
        try:
            self.roots = [x for x in json.loads(text) if isinstance(x, str)]
        except ValueError:
            pass
    def answer(self, e, word):
        if e[CANCELLED]:
            return
        if word in STATES:
            e[FILE].add_emblem("emblem-fastsync-" + word)
        if word == "unknown":
            self.missed.add(e[PATH])
        self.complete(e)
    def query_data(self, data):
        # False: the link is out of step (not our app, or a bug) and must be dropped.
        self.q_buf += data
        while b"\n" in self.q_buf:
            line, self.q_buf = self.q_buf.split(b"\n", 1)
            word = line.decode("utf-8", "replace").strip()
            if not self.q_greeted:
                if not word.startswith("ok 1 "):
                    return False
                self.q_greeted = True
                self.set_roots(word[5:])
            elif self.pending:
                self.answer(self.pending.popleft(), word)
            else:
                return False
        return len(self.q_buf) < 70000
    def on_query(self, _fd, cond):
        ok, data = True, b""
        try:
            data = self.q.recv(65536)
            ok = bool(data)
        except BlockingIOError:
            return True
        except OSError:
            ok = False
        if data and not self.query_data(data):
            ok = False
        if not ok:
            self.link_lost(True)
            return False
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
            self.watch_next = time.monotonic() + RETRY_FAST
            self.link_lost()
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
            self.set_roots(line[6:])
        elif line.startswith("ok 1 "):
            self.set_roots(line[5:])
    # -- Nemo --------------------------------------------------------------------------------------------------
    def inside(self, path):
        return any(path == r or path.startswith(r + "/") for r in self.roots)
    def update_file_info_full(self, provider, handle, closure, f):
        done = Nemo.OperationResult.COMPLETE
        if f.get_uri_scheme() != "file":
            return done
        loc = f.get_location()
        path = loc.get_path() if loc else None
        # A line break would end the request: such a name just gets no emblem.
        if not path or "\n" in path:
            return done
        if self.roots is not None and not self.inside(path):
            return done
        self.seen[path] = True
        if len(self.seen) > REMEMBER:
            self.seen.popitem(last=False)
        if not self.ensure():
            # No app: no emblem, and the push channel asks everything again when it is back. Only a slow one is retried.
            if not self.app_down:
                self.missed.add(path)
            return done
        if len(self.pending) >= MAX_PENDING:
            self.missed.add(path)
            f.add_emblem(UNKNOWN)
            return done
        msg = b"q " + path.encode("utf-8", "surrogateescape") + b"\n"
        try:
            if self.q.send(msg) != len(msg):
                raise OSError("short write")
        except OSError:
            self.missed.add(path)
            self.link_lost()
            return done
        self.pending.append([path, f, handle, closure, time.monotonic(), False, provider])
        return Nemo.OperationResult.IN_PROGRESS
    def cancel_update(self, provider, handle):
        # Nemo no longer wants this one: its answer is still read (to keep the queue in step) but never completed.
        for e in self.pending:
            if e[HANDLE] == handle:
                e[CANCELLED] = True
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
