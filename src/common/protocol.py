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
        return [], b"", Err(413, "line_too_long", close=True)
    return complete, rest, None

#_helpers
def _bad(reason):
    return Err(400, reason)

def _field(value):
    if "|" in value or "\n" in value:
        raise ValueError(f"field contains '|' or newline: {value[:80]!r}")
    return value

def _json_object(text):
    try:
        value = json.loads(text)
    except (ValueError, RecursionError):
        return None, _bad("bad_json")
    if not isinstance(value, dict):
        return None, _bad("payload_not_object")
    return value, None



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

#topic_only
def _topic_only(cmd, line):
    parts = line.split("|")
    if len(parts) != 2:
        return _bad("bad_field_count")
    topic = parts[1].strip()
    if not topic:
        return _bad("empty_topic")
    return Ok(cmd, {"topic": topic})

def parse_sub_ack(line):
    return _topic_only("SUB_ACK", line)
def format_sub_ack(topic):
    return f"SUB_ACK|{_field(topic)}"
    
def parse_pub_ack(line):
    parts = line.split("|")
    if len(parts) != 2:
        return _bad("bad_field_count")
    event_id = parts[1].strip()
    if not event_id:
        return _bad("empty_event_id")
    return Ok("PUB_ACK", {"event_id": event_id})
def format_pub_ack(event_id):
    return f"PUB_ACK|{_field(event_id)}"
#parse
def parse_pub(line):
    parts = line.split("|", 2)
    if len(parts) != 3:
        return _bad("bad_field_count")
    topic=parts[1].strip()
    if not topic:
        return _bad("empty_topic")
    payload, err = _json_object(parts[2])
    if err:
        return err
    return Ok("PUB", {"topic": topic, "payload": payload})

def parse_ack(line):
    parts = line.split("|")
    if len(parts) != 4:
        return _bad("bad_field_count")
    topic, sub_id, event_id = parts[1].strip(), normalise_id(parts[2]), parts[3].strip()
    if not topic:
        return _bad("empty_topic")
    if not sub_id:
        return _bad("empty_sub_id")
    if not event_id:
        return _bad("empty_event_id")
    return Ok("ACK", {"topic": topic, "sub_id": sub_id, "event_id": event_id})

def parse_event(line):
    parts = line.split("|", 1)
    if len(parts) != 2:
        return _bad("bad_field_count")
    event, err = _json_object(parts[1])
    if err:
        return err
    return Ok("EVENT", {"event": event})

def parse_get_stats(line):
    return _topic_only("GET_STATS", line)

def parse_err(line):
    parts = line.split("|", 2)
    if len(parts) != 3:
        return _bad("bad_field_count")
    if not (parts[1].isascii() and parts[1].isdigit()):
        return _bad("bad_code")
    return Ok("ERR", {"code": int(parts[1]), "message": parts[2]})


#format
def format_ack(topic, sub_id, event_id):
    return f"ACK|{_field(topic)}|{_field(sub_id)}|{_field(event_id)}"

def format_get_stats(topic):
    return f"GET_STATS|{_field(topic)}"

def format_pub(topic, payload):
    return f"PUB|{_field(topic)}|{json.dumps(payload, separators=(',', ':'))}"

def format_event(event):
    return f"EVENT|{json.dumps(event, separators=(',', ':'))}"

def format_err(code, message):
    return f"ERR|{int(code)}|{message}"



PARSERS = {
    "SUB": parse_sub, "SUB_ACK": parse_sub_ack, "ACK": parse_ack,
    "GET_STATS": parse_get_stats, "PUB": parse_pub,
    "EVENT": parse_event, "ERR": parse_err,
}

def parse_line(line):
    if isinstance(line, bytes):
        try:
            line = line.decode("utf-8")
        except UnicodeDecodeError:
            return _bad("bad_encoding")
    line = line.rstrip("\r")
    if not line.strip():
        return _bad("empty_line")
    parser = PARSERS.get(line.split("|", 1)[0])
    if parser is None:
        return _bad("unknown_command")
    return parser(line)