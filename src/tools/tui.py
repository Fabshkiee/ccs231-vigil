#!/usr/bin/env python3
"""VIGIL demo wall broker pane — optional tooling, outside the graded path."""
import argparse
import os
import re
import shutil
import signal
import subprocess
import sys
import threading
import time
from collections import deque
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "src"
sys.path.insert(0, str(SRC))

import subscriber as client_model  # noqa: E402
from common import storage  # noqa: E402
from common.constants import EVENT_LOG_FILE, OFFSETS_FILE  # noqa: E402
from common.ids import normalise_id  # noqa: E402

BG = "\033[48;2;18;16;14m"
BONE = "\033[38;2;217;205;191m"
STONE = "\033[38;2;168;156;142m"
ASH = "\033[38;2;145;132;118m"
MOSS = "\033[38;2;100;205;130m"
CYAN = "\033[38;2;80;205;225m"
SLATE = "\033[38;2;150;165;245m"
BRASS = "\033[38;2;235;180;70m"
EMBER = "\033[38;2;240;100;55m"
BLOOD = "\033[38;2;245;75;75m"
TAG = "\033[38;2;195;185;170m"
FLASH = "\033[38;2;255;250;240m"
BOLD = "\033[1m"
OFF = "\033[0m"

ANSI = re.compile(r"\033\[[0-9;]*m")
IND = (None, "  ")

SUBSCRIBERS = list(client_model.SUBSCRIBERS)
TOPIC_ORDER = ("sighting", "poltergeist", "readings", "alert")
SHORT_TOPIC = {
    "sighting": "SIGHTING",
    "poltergeist": "POLTERGEIST",
    "readings": "PARANORMAL",
    "alert": "ALERT",
}
ACCENT = {
    "ResponseTeam": MOSS,
    "PublicShield": CYAN,
    "ResearchArchive": SLATE,
    "broker": EMBER,
}
TITLES = {"broker": ["VIGIL"], "ResponseTeam": ["RESPONSE"], "PublicShield": ["SHIELD"]}
COMPACT = {"broker": "VIGIL", "ResponseTeam": "RESPONSE", "PublicShield": "SHIELD"}

GLYPHS = {
    "A": (" █████╗ ", "██╔══██╗", "███████║", "██╔══██║", "██║  ██║", "╚═╝  ╚═╝"),
    "C": (" ██████╗", "██╔════╝", "██║     ", "██║     ", "╚██████╗", " ╚═════╝"),
    "D": ("██████╗ ", "██╔══██╗", "██║  ██║", "██║  ██║", "██████╔╝", "╚═════╝ "),
    "E": ("███████╗", "██╔════╝", "█████╗  ", "██╔══╝  ", "███████╗", "╚══════╝"),
    "G": (" ██████╗ ", "██╔════╝ ", "██║  ███╗", "██║   ██║", "╚██████╔╝", " ╚═════╝ "),
    "H": ("██╗  ██╗", "██║  ██║", "███████║", "██╔══██║", "██║  ██║", "╚═╝  ╚═╝"),
    "I": ("██╗", "██║", "██║", "██║", "██║", "╚═╝"),
    "L": ("██╗     ", "██║     ", "██║     ", "██║     ", "███████╗", "╚══════╝"),
    "M": ("███╗   ███╗", "████╗ ████║", "██╔████╔██║", "██║╚██╔╝██║", "██║ ╚═╝ ██║", "╚═╝     ╚═╝"),
    "N": ("███╗   ██╗", "████╗  ██║", "██╔██╗ ██║", "██║╚██╗██║", "██║ ╚████║", "╚═╝  ╚═══╝"),
    "O": (" ██████╗ ", "██╔═══██╗", "██║   ██║", "██║   ██║", "╚██████╔╝", " ╚═════╝ "),
    "P": ("██████╗ ", "██╔══██╗", "██████╔╝", "██╔═══╝ ", "██║     ", "╚═╝     "),
    "R": ("██████╗ ", "██╔══██╗", "██████╔╝", "██╔══██╗", "██║  ██║", "╚═╝  ╚═╝"),
    "S": ("███████╗", "██╔════╝", "███████╗", "╚════██║", "███████║", "╚══════╝"),
    "T": ("████████╗", "╚══██╔══╝", "   ██║   ", "   ██║   ", "   ██║   ", "   ╚═╝   "),
    "V": ("██╗   ██╗", "██║   ██║", "██║   ██║", "╚██╗ ██╔╝", " ╚████╔╝ ", "  ╚═══╝  "),
}


def plain_len(spans):
    return sum(len(span[-1]) for span in spans)


def paint(spans, limit):
    out, used = [], 0
    for colour, text in spans:
        room = limit - used
        if room <= 0:
            break
        if len(text) > room:
            text = text[:room - 1] + "…"
        out.append(f"{colour}{text}{OFF}" if colour else text)
        used += len(text)
    return "".join(out)


def split(left, right, width):
    gap = max(2, width - 2 - plain_len(left) - plain_len(right))
    return left + [(None, " " * gap)] + right


def join3(left, mid, right, lw, mw, gap=3):
    out = []
    for i in range(max(len(left), len(mid), len(right))):
        l = left[i] if i < len(left) else []
        m = mid[i] if i < len(mid) else []
        r = right[i] if i < len(right) else []
        lp = max(gap, lw - plain_len(l) + gap)
        mp = max(gap, mw - plain_len(m) + gap)
        out.append(l + [(None, " " * lp)] + m + [(None, " " * mp)] + r)
    return out


def glyph_rows(name):
    return ["".join(GLYPHS[c][i] for c in name) for i in range(6)]


def title_width(name):
    return sum(len(GLYPHS[c][0]) for c in name)


def shaded(row, accent):
    spans, buf, solid = [], "", True
    for ch in row:
        kind = solid if ch == " " else (ch == "█")
        if kind != solid and buf:
            spans.append((accent if solid else ASH, buf))
            buf = ""
        solid = kind
        buf += ch
    if buf:
        spans.append((accent if solid else ASH, buf))
    return spans


def title_block(role, width, big):
    accent = ACCENT[role]
    if big:
        for name in TITLES[role]:
            if title_width(name) <= width - 4:
                return [[IND] + shaded(row, accent) for row in glyph_rows(name)]
    return [[IND, (accent, "▌ "), (BOLD + BONE, COMPACT[role])]]


class BrokerState:
    def __init__(self):
        self.events = []
        self.offsets = {}
        self.error = ""
        self._sig = None

    @staticmethod
    def _signature():
        sig = []
        for path in (EVENT_LOG_FILE, OFFSETS_FILE):
            try:
                st = os.stat(path)
            except FileNotFoundError:
                sig.append(None)
            else:
                sig.append((st.st_mtime_ns, st.st_size))
        return tuple(sig)

    def refresh(self):
        sig = self._signature()
        if sig == self._sig:
            return
        self._sig = sig
        try:
            events = storage.load_events()
            offsets = storage.load_offsets()
        except (storage.StoreError, OSError) as exc:
            self.error = str(exc)
            return
        self.error = ""
        flat = []
        for topic, items in events.items():
            if not isinstance(items, list):
                continue
            for ev in items:
                if not isinstance(ev, dict):
                    continue
                payload = ev.get("payload")
                flat.append({
                    "event_id": str(ev.get("event_id", "")),
                    "topic": str(ev.get("topic", topic)),
                    "payload": payload if isinstance(payload, dict) else {},
                    "created_at": float(ev.get("created_at", 0) or 0),
                })
        flat.sort(key=lambda e: e["created_at"])
        self.events = flat
        self.offsets = offsets if isinstance(offsets, dict) else {}

    @property
    def alerts(self):
        return sum(1 for ev in self.events if ev["topic"] == "alert")

    @property
    def plain_events(self):
        return len(self.events) - self.alerts

    def subscriber_counts(self, role):
        sub_id = normalise_id(role)
        offsets = self.offsets.get(sub_id, {})
        received = queued = 0
        for topic in client_model.SUBSCRIBERS[role]:
            total = sum(1 for ev in self.events if ev["topic"] == topic)
            last = offsets.get(topic, -1) if isinstance(offsets, dict) else -1
            if not isinstance(last, int):
                last = -1
            received += min(total, last + 1)
            queued += max(0, total - (last + 1))
        return received, queued


def command_for(role):
    cmd = [sys.executable, "-u", str(SRC / ("broker.py" if role == "broker" else "subscriber.py"))]
    if role != "broker":
        cmd += ["--as", role]
    return cmd


class Child:
    def __init__(self):
        self.proc = None
        self.lines = deque(maxlen=200)
        self.spawn_time = 0.0
        self.stopped_at = None
        self._reader = None

    def start(self, cmd):
        self.lines.clear()
        self.stopped_at = None
        self.spawn_time = time.time()
        self.proc = subprocess.Popen(
            cmd,
            cwd=str(ROOT),
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )
        self._reader = threading.Thread(target=self._read, daemon=True)
        self._reader.start()

    def _read(self):
        try:
            for line in self.proc.stdout:
                self.lines.append(line.rstrip("\n"))
        except (OSError, ValueError):
            pass

    def alive(self):
        return self.proc is not None and self.proc.poll() is None

    def last_line(self):
        for line in reversed(list(self.lines)):
            if line.strip():
                return line.strip()
        return ""

    def stop(self):
        if not self.alive():
            self.stopped_at = time.time()
            return
        if os.name == "posix":
            try:
                self.proc.send_signal(signal.SIGINT)
            except OSError:
                pass
        try:
            self.proc.wait(timeout=2.0)
        except subprocess.TimeoutExpired:
            self.proc.terminate()
            try:
                self.proc.wait(timeout=1.0)
            except subprocess.TimeoutExpired:
                self.proc.kill()
        self.stopped_at = time.time()


def mmss(seconds):
    seconds = max(0, int(seconds))
    return f"{seconds // 60:02d}:{seconds % 60:02d}"


def hhmmss(ts):
    return time.strftime("%H:%M:%S", time.localtime(ts))


def luminance(rank, now, ts):
    if rank == 0 and now - ts < 0.7:
        return BOLD + FLASH
    return (BONE, STONE, ASH, ASH, ASH)[min(rank, 4)]


def status_spans(child):
    if child is None:
        return [(ASH, "○ preview")], ""
    if not child.alive():
        err = child.last_line()
        return [(BLOOD, "○ stopped"), (ASH, " · Enter restarts")], err
    if any("Listening on" in line for line in list(child.lines)):
        return [(MOSS, "● "), (BONE, "listening")], ""
    return [(BRASS, "● "), (BONE, "starting")], ""


def sub_status(child):
    if child is None:
        return [(ASH, "○ preview")]
    if not child.alive():
        return [(BLOOD, "○ stopped"), (ASH, " · Enter restarts")]
    line = ""
    for text in reversed(list(child.lines)):
        if text.strip() and ("[sub]" in text or "retrying in" in text):
            line = text.strip()
            break
    if "warn: cannot_connect" in line or "warn: connection_lost" in line or "retrying in" in line:
        return [(ASH, "◌ reconnecting")]
    if "subscribed to" in line or "connected to" in line:
        return [(MOSS, "● "), (BONE, "subscribed")]
    return [(BRASS, "● "), (BONE, "starting")]


def stats_rows(subs, up, events, alerts):
    # The broker logs no SUBSCRIBE, so a live count would need graded-code changes.
    # "acked" is a past-tense fact that survives a killed subscriber's pane.
    values = (
        ("acked", str(subs), BONE),
        ("uptime", mmss(up) if up is not None else "--:--", BONE),
        ("events", str(events), BONE),
        ("alerts", str(alerts), BLOOD),
    )
    return [
        [IND, (ASH, f"{label:<12}"), (BOLD + colour, f"{value:>6}")]
        for label, value, colour in values
    ]


def route_rows():
    rows = []
    for topic in TOPIC_ORDER:
        subs = [sub for sub in SUBSCRIBERS if topic in client_model.SUBSCRIBERS[sub]]
        spans = [
            IND,
            (BLOOD if topic == "alert" else TAG, f"{SHORT_TOPIC[topic]:<11}"),
            (ASH, " →  "),
        ]
        for i, sub in enumerate(subs):
            if i:
                spans.append((ASH, " · "))
            spans.append((ACCENT[sub], sub))
        rows.append(spans)
    return rows


def bus_feed_rows(events, now, limit, topics=None):
    if topics is not None:
        events = [ev for ev in events if ev["topic"] in topics]
    if not events:
        return [[IND, (ASH, "○ waiting for events")]]
    rows = []
    for rank, ev in enumerate(reversed(events[-limit:])):
        topic = ev["topic"]
        text = client_model.describe(ev["payload"])
        rows.append([
            (BLOOD, "▲ ") if topic == "alert" else (None, "  "),
            (ASH, hhmmss(ev["created_at"])),
            (None, "  "),
            (BLOOD if topic == "alert" else TAG, f"{SHORT_TOPIC.get(topic, topic.upper()):<11}"),
            (None, "  "),
            (ASH, ev["event_id"][:8] + "  "),
            (luminance(rank, now, ev["created_at"]), text),
        ])
    return rows


def build_broker(state, now, width, big, status, err, up):
    gap = 3
    lw = max(30, int(width * 0.26))
    mw = max(40, int(width * 0.28))
    rw = width - (lw + gap) - (mw + gap)

    left = title_block("broker", width, big) + stats_rows(
        len(state.offsets), up, state.plain_events, state.alerts)
    mid = [[IND, (STONE + BOLD, "ROUTING")], []] + route_rows()
    right = [split([IND, (STONE + BOLD, "LIVE BUS FEED")], status, rw),
             [IND, (ASH, err)] if err else []]
    right += bus_feed_rows(state.events, now, 7)

    n = max(len(left), len(mid), len(right))

    def pad(rows):
        return rows + [[] for _ in range(n - len(rows))]

    return [[]] + join3(pad(left), pad(mid), pad(right), lw, mw, gap)


def build_subscriber(role, state, now, width, big, status, received, queued):
    topics = client_model.SUBSCRIBERS[role]
    labels = ", ".join(topic.upper() for topic in topics)
    rows = [[]] + title_block(role, width, big)
    rows.append(split([IND, (ACCENT[role] + BOLD, f"{role} · subscriber")], status, width))
    rows.append([IND, (ASH, f"subscribes: {labels}"),
                 (BONE, f" · received {received} · queued {queued}")])
    rows.append([IND, (ASH, "─" * (width - 3))])
    rows += bus_feed_rows(state.events, now, 5, topics)
    return rows


def lines_for(state, child, now, width, height, role="broker"):
    minimum = 120 if role == "broker" else 70
    if width < minimum or height < 10:
        return [
            "",
            f"  VIGIL — {role} needs a wider window ({width}×{height})",
            "  resize to at least 150×12; 231×13 is the target" if role == "broker"
            else f"  resize to at least {minimum}×12",
        ]
    err = ""
    up = None
    if role == "broker":
        status, err = status_spans(child)
        if child is not None and child.spawn_time:
            up = (child.stopped_at or now) - child.spawn_time
    else:
        status = sub_status(child)
        received, queued = state.subscriber_counts(role)
    for big in (True, False):
        if role == "broker":
            rows = build_broker(state, now, width, big, status, err, up)
        else:
            rows = build_subscriber(role, state, now, width, big, status, received, queued)
        if len(rows) <= height - 1:
            break
    return [paint(row, width - 1) for row in rows][:height - 1]


def frame(lines, height):
    out = ["\033[H"]
    for i in range(height - 1):
        out.append((lines[i] if i < len(lines) else "") + "\033[K\r\n")
    out.append("\033[K")
    return "".join(out)


class Terminal:
    def __init__(self):
        self.fd = None
        self.old = None

    def raw_mode(self):
        if os.name == "posix" and sys.stdin.isatty():
            import termios
            import tty
            self.fd = sys.stdin.fileno()
            if self.old is None:
                self.old = termios.tcgetattr(self.fd)
            tty.setcbreak(self.fd)
        elif os.name == "nt":
            _win_echo(False)

    def line_mode(self):
        if os.name == "posix" and self.old is not None:
            import termios
            termios.tcsetattr(self.fd, termios.TCSADRAIN, self.old)
        elif os.name == "nt":
            _win_echo(True)


def _win_echo(enabled):
    if os.name != "nt":
        return
    import ctypes
    kernel = ctypes.windll.kernel32
    handle = kernel.GetStdHandle(-10)
    mode = ctypes.c_uint(0)
    if kernel.GetConsoleMode(handle, ctypes.byref(mode)):
        flag = 0x0004
        kernel.SetConsoleMode(handle, mode.value | flag if enabled else mode.value & ~flag)


def _win_vt():
    if os.name != "nt":
        return
    import ctypes
    kernel = ctypes.windll.kernel32
    handle = kernel.GetStdHandle(-11)
    mode = ctypes.c_uint(0)
    if kernel.GetConsoleMode(handle, ctypes.byref(mode)):
        kernel.SetConsoleMode(handle, mode.value | 0x0004)


def enter_screen(title):
    sys.stdout.write(f"\033]0;{title}\007\033[?1049h\033[?25l" + BG + "\033[2J")
    sys.stdout.flush()


def leave_screen():
    sys.stdout.write(OFF + "\033[?25h\033[?1049l")
    sys.stdout.flush()


def draw(state, child, role="broker"):
    width, height = shutil.get_terminal_size((231, 13))
    lines = lines_for(state, child, time.time(), width, height, role)
    sys.stdout.write(frame(lines, height))
    sys.stdout.flush()


def restart_prompt(term):
    height = shutil.get_terminal_size((231, 13)).lines
    row = max(1, height - 1)
    term.line_mode()
    sys.stdout.write(f"\033[?25h\033[{row};1H\033[K[Enter] restart  ·  Ctrl+C quit ")
    sys.stdout.flush()
    try:
        input()
        return True
    except (KeyboardInterrupt, EOFError):
        return False
    finally:
        sys.stdout.write("\033[?25l")
        sys.stdout.flush()
        term.raw_mode()


def main(argv=None):
    parser = argparse.ArgumentParser(description="VIGIL demo wall, broker pane")
    parser.add_argument("--role", default="broker", choices=["broker", "ResponseTeam", "PublicShield"])
    parser.add_argument("--once", action="store_true", help="render one frame and exit")
    parser.add_argument("--plain", action="store_true", help="strip colour in --once")
    parser.add_argument("--width", type=int, help="width for --once")
    parser.add_argument("--height", type=int, help="height for --once")
    args = parser.parse_args(argv)

    state = BrokerState()
    state.refresh()

    if args.once:
        size = shutil.get_terminal_size((231, 13))
        width = args.width or size.columns
        height = args.height or size.lines
        lines = lines_for(state, None, time.time(), width, height, args.role)
        text = "\n".join(lines)
        print(ANSI.sub("", text) if (args.plain or not sys.stdout.isatty()) else text)
        return 0

    if not sys.stdout.isatty():
        print("tui: run this in a terminal (or use --once --plain)")
        return 2

    _win_vt()
    term = Terminal()
    term.raw_mode()
    enter_screen("VIGIL — Broker" if args.role == "broker" else f"{args.role} — Subscriber")
    child = Child()
    child.start(command_for(args.role))
    try:
        while True:
            try:
                while child.alive():
                    state.refresh()
                    draw(state, child, args.role)
                    time.sleep(0.2)
            except KeyboardInterrupt:
                child.stop()
            state.refresh()
            draw(state, child, args.role)
            if not restart_prompt(term):
                break
            child.start(command_for(args.role))
    finally:
        child.stop()
        term.line_mode()
        leave_screen()
    return 0


if __name__ == "__main__":
    sys.exit(main())
