# The wire format. One format_* and one parse_* per command, nothing else.
# No sockets and no files in here — text in, text out — so the tests can run
# without starting a broker. Whatever you add has to round-trip in
# tests/test_protocol.py, or it doesn't go on the wire.
import json

from dataclasses import dataclass
from common.constants import MAX_LINE_BYTES
from common.ids import normalise_id

@dataclass(frozen=True)
class Ok:
    cmd: str
    fields: dict

@dataclass(frozen=True)
class Err:
    code: int
    reason: str
    close: bool = False

def split_lines(buffer, data):
    buf = buffer + data
    *complete, rest = buf.split(b"\n")
    if any(len(line) > MAX_LINE_BYTES for line in complete) or len(rest) > MAX_LINE_BYTES:
        return [], b"", Err(413, "line too long", close=True)
    return complete, rest, None

def _bad(reason):
    return Err(400, reason)

def _field(value):
    if "|" in value or "\n" in value:
        raise ValueError(f"field contains '|' or newline: {value[:80]!r}")
    return value

def parse_sub(line):
    parts = line.split("|")
    if len(parts) != 3 :
        return _bad("bad_field_count")
    topic, sub_id = parts[1].strip(), normalise_id(parts[2])
    if not topic:
        return _bad("empty_topic")
    if not sub_id:
        return _bad("empty_sub_id")
    return Ok("SUB", {"topic": topic, "sub_id": sub_id})

def format_sub(topic, sub_id):
    return f"SUB|{_field(topic)}|{_field(sub_id)}"