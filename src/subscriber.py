# The listener. Connects, subscribes, prints whatever arrives, ACKs (Acknowledge) it.
# If the broker dies it has to reconnect and subscribe again on its own

import argparse, os, socket, sys, time
from collections import deque

from common.constants import (
    ALERT_MIN_SEV, PORT, SUB_ACK_TIMEOUT_S, DEDUPE_WINDOW,
    BACKOFF_STEPS_S
    )
from common.ids import normalise_id
from common.log import info, warn
from common.protocol import Err, format_sub, parse_line, split_lines, format_ack

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

# error code broker uses when an ACK names an event it doesn't know
# answers an ACK not a SUB, must not abort subsrcibing
ERR_UNKNOWN_EVENT = 422

# ansi color codes
RED, YELLOW, RESET = "\033[31m", "\033[33m", "\033[0m"


class LinkError(Exception):
    """the connection to the broker is unusable (closed, reset, no SUB_ACK)
    still worth retrying(new connection may work)"""

class ReadTimeout(Exception):
    """no complete line arrived in time. not an error once we're live: a quiet
    broker just means nothing was published. kept separate from LinkError"""

class SubscribeRejected(Exception):
    """broker answered the SUB with an Err. not worth retrying
    (req itself is invalid)"""

class SeenIds:
    """remembers the last 'limit' event ids so duplicates can be recognised

    duplicates are expected with at-least-once delivery: if the ACK is lost,
    the broker replays the event
    """

    def __init__(self, limit=DEDUPE_WINDOW):
        if limit < 1:
            raise ValueError("limit must be at least 1")
        self._limit = limit
        # deque records the order ids arrived in so we can remove the oldest
        # one from the front
        self._order = deque()
        # checks if id has already been seen, manage eviction manually to
        # delete from both "_order" and "_ids" at the same time, otherwise
        # "_ids" would grow forever and leak memory
        self._ids = set()

    def __contains__(self, event_id):
        """lets us write 'event_id in seen'"""
        return event_id in self._ids

    def add(self, event_id):
        """remember an id, forgetting the oldest one if the window is full"""
        # if id is already remembered, do nothing and exit early
        if event_id in self._ids:
            return
        # if memory window is full, pop oldest id and remove it from set also
        if len(self._order) >= self._limit:
            # forget the oldest id (leftmost in the deque) in both structures
            self._ids.discard(self._order.popleft())
        # place new id at the back and register it in the fast-lookup set
        self._order.append(event_id)
        self._ids.add(event_id)


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

    def close(self):
        """Close the socket."""
        self.sock.close()

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

def print_event(event):
    """print one event. The caller has already checked it with event_problem()."""
    # build the plain-text summary line
    line = format_event_line(event)
    # look up sev color and apply if applicable
    color = color_for(severity_of(event["payload"]))
    if color and color_enabled():
        line = f"{color}{line}{RESET}"
    # flush=True: the line must really be out before we send the ACK, and when
    # output is piped (tail -f during the demo) Python would otherwise buffer it
    print(line, flush=True)

def handle_event(link, sub_id, seen, event):
    """check, dedupe, print, then ACK one event. the heart of at-least-once"""
    # validate field types first
    problem = event_problem(event)
    if problem:
        # never crash on a bad event. do not ACK it, we never showed it
        warn("sub", problem, event)
        return
    # check if event_id is alreay in our SeenIds memory
    event_id = event["event_id"]
    if event_id not in seen:
        # new event, show to user right now
        print_event(event)
        # remember it before the ACK is sent. if the send fails, a replay of
        # this event is then recognised as a duplicate and not printed twice
        seen.add(event_id)
    # duplicates skip the print but not the ACK. If earlier ACK was lost,
    # the broker's offset is stuck behind this event, re-ACKing moves it on
    try:
        # build protocol ACK string
        ack_line = format_ack(event["topic"], sub_id, event_id)
    except ValueError:
        # warn and skip sending if a broken field would corrupt the protocol line
        warn("sub", "unackable_event", event_id)
        return
    # send ACK receipt (after print and before next read)
    link.send(ack_line)


def subscribe(link, sub_id, topics, seen):
    """send one SUB per topic and wait until every topic has been SUB_ACKed"""
    # one SUB per topic, the broker keeps a separate offset per (topic, sub_id).
    # fire off all SUB reqs up front
    for topic in topics:
        link.send(format_sub(topic, sub_id))
    # track which topics still haven't received their SUB_ACK receipt
    pending = set(topics)
    while pending:
        try:
            # wait for next line from the broker
            message = link.read_message()
        except ReadTimeout:
            # timeout on handshake, raise LinkError and list which topics hung
            raise LinkError(f"no_sub_ack_within_timeout: {sorted(pending)}") from None
        if isinstance(message, Err):
            # a malformed line, warn and keep waiting, the timeout catches a
            # broker that never answers properly
            warn("sub", "bad_line_from_broker", message.reason)
        elif message.cmd == "SUB_ACK":
            # broker confirmed one topic, cross it off the pending list
            topic = message.fields["topic"]
            if topic in pending:
                pending.remove(topic)
            else:
                # warn if broker ACKed a topic we didn't ask for
                warn("sub", "unexpected_sub_ack", topic)
        elif message.cmd == "EVENT":
            # the broker sends a topic's replay right after its SUB_ACK but
            # before it reads our next SUB, so events can arrive while we are
            # still waiting for the other topics' SUB_ACKs, show them now
            handle_event(link, sub_id, seen, message.fields["event"])
        elif message.cmd == "ERR":
            if message.fields["code"] == ERR_UNKNOWN_EVENT:
                warn("sub", "broker_error",
                    f"{message.fields['code']} {message.fields['message']}")
            else:
            # the broker refused our SUB, so a SUB_ACK will never come
                raise SubscribeRejected(f"{message.fields['code']} {message.fields['message']}")
        else:
            warn("sub", "unexpected_reply", message.cmd)


def listen(link, sub_id, seen):
    """live loop: print every event until the connection ends (raises LinkError)"""
    # while live, silence is normal (nothing published)
    link.set_timeout(LIVE_POLL_S)
    while True:
        try:
            message = link.read_message()
        except ReadTimeout:
            continue # nothing arrived; wait again
        if isinstance(message, Err):
            # broker sent garbled text that failed protocol parsing
            # log a warning and stay connected
            warn("sub", "bad_line_from_broker", message.reason)
        elif message.cmd == "EVENT":
            # real event arrived, validate, format, color, and print it
            handle_event(link, sub_id, seen, message.fields["event"])
        elif message.cmd == "ERR":
            # bad ACK, The connection is fine
            warn("sub", "broker_error",
                 f"{message.fields['code']} {message.fields['message']}")
        else:
            # else other non-event reply gets logged without killing the loop
            warn("sub", "unexpected_message", message.cmd)


def parse_args(argv=None):
    """read --as and --host from the command line"""
    parser = argparse.ArgumentParser(description="Paranormal Office subscriber")
    parser.add_argument("--as", dest="role", required=True, choices=sorted(SUBSCRIBERS))
    parser.add_argument("--host", default=HOST, help=f"broker address (default {HOST})")
    return parser.parse_args(argv)


def main(argv=None):
    """connect, subscribe, listen. return an exit code"""
    args = parse_args(argv)
    # normalise_id gives the same identity the broker computes ("ResponseTeam"
    # -> "responseteam"), so our offsets belong to us across restarts
    sub_id = normalise_id(args.role)
    # look up which tuple of topics the role should subscribe to
    topics = SUBSCRIBERS[args.role]
    try:
        # dial broker with startup timeout for the handshake
        link = connect(args.host, PORT, SUB_ACK_TIMEOUT_S)
    except OSError as exc:
        # broker isn't running or network is unreachable, warn and fail cleanly
        warn("sub", "cannot_connect", f"{args.host}:{PORT} {exc}")
        return 1
    info("sub", f"{sub_id} connected to {args.host}:{PORT}")
    try:
        # send SUB commands and wait for all SUB_ACKs (plus replay)
        subscribe(link, sub_id, topics)
        info("sub", f"subscribed to {', '.join(topics)}")
        # stream live events until disconnected or Ctrl + C
        listen(link)
    except LinkError as exc:
        # reconnect loop ----
        warn("sub", "link_failed", str(exc))
        return 1
    except KeyboardInterrupt:
        # print newline so returning shell isn't stuck after "^C"
        print()
    finally:
        # close tcp socket
        link.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())