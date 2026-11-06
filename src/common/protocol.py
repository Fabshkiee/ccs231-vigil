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
