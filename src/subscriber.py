# The listener. Connects, subscribes, prints whatever arrives, ACKs (Acknowledge) it.
# If the broker dies it has to reconnect and subscribe again on its own

import argparse, os, socket, sys

from common.constants import ALERT_MIN_SEV, PORT, SUB_ACK_TIMEOUT_S
from common.ids import normalise_id
from common.log import info, warn
from common.protocol import Err, format_sub, parse_line, split_lines

# --- TEMPORARY constants -------------------------
HOST = "127.0.0.1"
LIVE_POLL_S = 1 # while live, wake up this often so Ctrl+C is noticed
# -------------------------------------------------

SUBSCRIBERS = {
    "ResponseTeam": ("sighting", "poltergeist"),
    "PublicShield": ("alert",),
    "ResearchArchive": ("readings", "sighting", "poltergeist"),
}

RECV_BYTES = 4096       
SHORT_ID_LEN = 8
WARN_MIN_SEV = 2 # severity scale: 1 faint, 2-3 yellow, 4-5 red

# ansi color codes
RED, YELLOW, RESET = "\033[31m", "\033[33m", "\033[0m"


class LinkError(Exception):
    """the connection to the broker is unusable (closed, reset, no SUB_ACK)"""

class ReadTimeout(Exception):
    """no complete line arrived in time. not an error once we're live: a quiet
    broker just means nothing was published. kept separate from LinkError"""

class BrokerLink:
    """one tcp connection to the broker, with line framing and error wrapping"""

    def __init__(self, sock):
        self.sock = sock
        self._buffer = b""
        self._lines = []

    def send(self, line):
        """send one protocol line"""
        try:
            self.sock.sendall(line.encode("utf-8") + b"\n")
        except OSError as exc:
            # if cable unplugged/broker crashed
            raise LinkError(f"send_failed: {exc}") from exc

    def read_message(self):
        """block until one full line arrives; return Ok/Err result"""
        while not self._lines:
            try:
                data = self.sock.recv(RECV_BYTES)
            except TimeoutError:
                # seperate from broken connection, broker is just quiet
                raise ReadTimeout from None
            except OSError as exc:
                # actual OS socket error, broken connection
                raise LinkError(f"recv_failed: {exc}") from exc
            if not data:
                # if recv() returns 0kb, socket is closed
                raise LinkError("connection_closed")
            # organize bytes received, glue leftover bytes + new bytes,
            # split on \n, put leftover back into self._buffer
            complete, self._buffer, err = split_lines(self._buffer, data)
            if err:
                raise LinkError(err.reason)
            # add real lines to back of self._lines queue
            self._lines.extend(line for line in complete if line.strip())
        return parse_line(self._lines.pop(0))

    def set_timeout(self, seconds):
        """change ReadTimeout: long wait on initial connection(hist replay), 1s on live"""
        self.sock.settimeout(seconds)

def connect(host, port, timeout):
    """open tcp connection, perform 3-way tcp handshake, handover brokerlink"""
    return BrokerLink(socket.create_connection((host, port), timeout=timeout))

def short_id(event_id):
    """first few chars of uuid"""
    return event_id[:SHORT_ID_LEN]

def event_problem(event):
    """return why an event can't be shown, or none if it is fine."""
    event_id = event.get("event_id")
    # check if event_id is real string and not empty
    if not isinstance(event_id, str) or not event_id:
        return "bad_event_id"
    # ensure topic is a string
    if not isinstance(event.get("topic"), str):
        return "bad_topic"
    # ensure payload is a dict
    if not isinstance(event.get("payload"), dict):
        return "bad_payload"
    # event is valid
    return None

def severity_of(payload):
    """the payload's severity as an int, or none (readings have none)"""
    sev = payload.get("sev")
    # check if sev is an int
    if isinstance(sev, int) and not isinstance(sev, bool):
        return sev
    return None

def describe(payload):
    """turn a payload into readable text, whatever fields it happens to have"""
    # start list with event's "kind"
    parts = [str(payload.get("kind", "unknown"))]
    # append extra existing details in payload
    if "zone" in payload:
        parts.append(str(payload["zone"]))
    if "level" in payload:
        parts.append(f"level {payload['level']}")
    if "sev" in payload:
        parts.append(f"severity {payload['sev']}")
    # split()/join collapses newlines and tabs, so one event is always one line
    return " ".join(", ".join(parts).split())


def format_event_line(event):
    """the plain-text line for one event (no color)"""
    return f"Event {short_id(event['event_id'])} — {event['topic']}: {describe(event['payload'])}"

def color_for(sev):
    """red for sev >= 4, yellow for 2-3, no color otherwise"""
    if sev is None:
        return ""
    if sev >= ALERT_MIN_SEV:
        return RED
    if sev >= WARN_MIN_SEV:
        return YELLOW
    return ""


def color_enabled():
    """color only on a real terminal, and never when NO_COLOR is set"""
    return not os.environ.get("NO_COLOR") and sys.stdout.isatty()


def show_event(event):
    """print one event, returns true if shown"""
    # validate field types
    problem = event_problem(event)
    if problem:
        # never crash on a bad event, warn and carry on
        warn("sub", problem, event)
        return False
    # build the plain-text summary line
    line = format_event_line(event)
    # look up sev color and apply if applicable
    color = color_for(severity_of(event["payload"]))
    if color and color_enabled():
        line = f"{color}{line}{RESET}"
    # flush=True: when output is piped (tail -f during the demo), Python would
    # otherwise hold lines in a buffer and the audience would see nothing.
    # print
    print(line, flush=True)
    return True
