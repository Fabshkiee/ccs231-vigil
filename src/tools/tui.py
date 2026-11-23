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
TITLES = {"broker": ["VIGIL"]}
COMPACT = {"broker": "VIGIL"}

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


