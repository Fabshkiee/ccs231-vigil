# The thing we type into during the demo. Reads one event, sends it, prints
# "saved". If severity is 4 or 5 it also sends the same event to alert
# that decision belongs here, not in the broker, because the broker is only
# allowed to route by topic.
import argparse, random, socket, sys, time, re

from common.constants import SEV_MIN, SEV_MAX, ALERT_MIN_SEV
from common.log import warn, info
from common.protocol import Err, format_pub, parse_line, split_lines


# --- TEMP CONSTANTS (To be moved to constants.py) ----
HOST = "127.0.0.1"
PUB_ACK_TIMEOUT_S = 2
READINGS_INTERVAL_S = 3 # paranormal readings
# -----------------------------------------------------


# roles, only two receives alert
ROLES = {
    "ParanormalReadings": ("readings", False),
    "SightingReports": ("sighting", True),
    "PoltergeistActivity": ("poltergeist", True),
}

SEV_WORD = "severity"
AUTO_ROLE = "ParanormalReadings"
SEV_WORD = "severity"
RECV_BYTES = 4096
SHORT_ID_LEN = 8
ALERT_TOPIC = "alert"

# scenario data for the automatic readings stream
READING_SEED = 231 # seeded so demo run is repeatable
ZONES = ("zone-a", "zone-b", "zone-c")
READING_MIN, READING_MAX = 0, 100

class LinkError(Exception):
    """Connection to the broker is unusable (timeout, reset, bad reply)"""

class BrokerRejected(Exception):
    """broker answered with an ERR line, connection is fine"""

# allow only valid nodes to connect to system
def parse_args(argv=None):
    """Read --as <role> from the command line"""
    parser = argparse.ArgumentParser(description="Paranormal Office publisher")
    # dest="role" because 'as' is a Python keyword and can't be an attribute name
    parser.add_argument("--as", dest="role", required=True, choices=sorted(ROLES))
    return parser.parse_args(argv)

# extract text payload and severity integer
def parse_typed_line(raw):
    """Splits 'chanak sighting, severity 5' into ((kind, sev), None) or (None, reason)"""
    cleaned = raw.strip()
    # validate text
    matches = list(re.finditer(rf"\b{SEV_WORD}\b", cleaned, re.IGNORECASE))
    if not matches:
        return None, "missing_severity"
    last_match = matches[-1]
    kind = cleaned[:last_match.start()].strip().rstrip(",").strip()
    if not kind:
        return None, "empty_text"
    sev_text = cleaned[last_match.end():].strip()
    # validate severity
    if not (sev_text.isascii() and sev_text.isdigit()):
        return None, "bad_severity"
    sev = int(sev_text)
    if not SEV_MIN <= sev <= SEV_MAX:
        return None, "severity_out_of_range"
    return (kind, sev), None

# fan-out alert from valid roles only
def needs_alert(role, sev):
    """true when a second PUB on the 'alert' topic must be sent"""
    may_alert = ROLES.get(role, (None, False))[1]
    return may_alert and sev >= ALERT_MIN_SEV

class BrokerLink:
    """one TCP connection to the broker, with line framing and error wrapping"""

    def __init__(self, sock):
        # accept existing socket for unit testing
        # tests hand in socketpair() instead of real network connection
        self.sock = sock
        self._buffer = b""
        self._lines = []

    def send(self, line):
        """send one protocol line, add in \n for boundary"""
        try:
            # keeps writing until every byte is accepted by the OS
            self.sock.sendall(line.encode("utf-8") + b"\n")
        except OSError as exc:
            raise LinkError(f"send_failed: {exc}") from exc

    def read_reply(self):
        """block until one full reply line arrives, retun Ok/Err result"""
        while not self._lines:
            try:
                data = self.sock.recv(RECV_BYTES)
            except TimeoutError:
                # catch TimeoutError for silent broker raise
                raise LinkError("no_reply_within_timeout") from None
            except OSError as exc:
                raise LinkError(f"recv_failed: {exc}") from exc
            if not data:
                # recv() return b"" means broker closed the connection
                raise LinkError("connection_closed")
            complete, self._buffer, err = split_lines(self._buffer, data)
            if err:
                raise LinkError(err.reason)
            # skip blank lines
            self._lines.extend(line for line in complete if line.strip())
        return parse_line(self._lines.pop(0))

    def close(self):
        """close socket"""
        self.sock.close()

def connect(host, port, timeout):
    """open TCP connection; timeout applies to later send/recv"""
    return BrokerLink(socket.create_connection(host, port), timeout=timeout)

def short_id(event_id):
    """First few chars of uuid for readable demo"""
    return event_id[:SHORT_ID_LEN]

def build_payload(kind, sev, role):
    """JSON object for typed event"""
    return {"kind": kind, "sev": sev, "source": role}

def publish(link, topic, payload):
    """send one PUB, wait for its PUB_ACK, and return the new event_id"""
    link.send(format_pub(topic, payload))
    # require PUB_ACK before sending the next PUB
    reply = link.read_reply()
    if isinstance(reply, Err):
        # broker sent invalid reply/syntax,
        # byte stream is corrupted, raise LinkError to abort session
        raise LinkError(f"bad__reply: {reply.reason}")
    if reply.cmd == "PUB_ACK":
        # broker confirmed event is saved and fsynced, return its new id
        return reply.fields["event_id"]
    if reply.cmd == "ERR":
        # valid wire format. if broker refused event,
        # raise BrokerRej so caller knows nothing was saved and published
        raise BrokerRejected(f"{reply.fields['code']} {reply.fields['message']}")
    raise LinkError(f"unexpected_reply: {reply.cmd}")

def publish_and_print(link, topic, payload):
    """publish one event and print the one line cofirmation"""
    # block until broker confirms event is saved
    event_id = publish(link, topic, payload)
    print(f"Event {short_id(event_id)} saved ({topic})")

def handle_line(link, role, raw):
    """validate one typed line, then publish it(and its alert if due)"""
    parsed, reason = parse_typed_line(raw) # validate
    if reason:
        # validate before touching the network
        warn("pub", reason, raw)
        return
    kind, sev = parsed
    # package validated data and pub role into the JSON payload
    payload = build_payload(kind, sev, role)
    try:
        # send primary event to this role's main topic
        publish_and_print(link, ROLES[role][0], payload)
        # check if role is allowed to alert
        if needs_alert(role, sev): 
            # alert is second PUB with same payload, broker gives it its own id
            # and is only sent after the first succeeded   
            publish_and_print(link, ALERT_TOPIC, payload)
    except BrokerRejected as exc:
        # connection is healthy, if first PUB was rejected,
        # skip the alert, since nothing was saved 
        warn("pub", "broker_rejected", str(exc))

def make_reading(rng, role):
    """one ambient reading"""
    # no 'sev' key, readings are routine nums, not incidents, they never alert
    return {
        "kind": "ambient reading",
        "zone": rng.choice(ZONES),
        "level": rng.randint(READING_MIN, READING_MAX),
        "source": role,
    }

def run_auto(link, role, interval):
    """ParanormalReadings mode, publish a reading every 'interval' seconds."""
    topic = ROLES[role][0]
    rng = random(READING_SEED)   # own generator, not the global one
    # print status startup status line showing target topic and publish interval
    info("pub", f"{role} streaming to '{topic}' every {interval}s (Ctrl+C to stop)")
    # run continuously
    while True:
        # build one reading dictionary, sends json over the socket,
        # blocks for PUB_ACK, and prints line confirmation
        publish_and_print(link, make_reading(rng, role))
        # pause loop to follow interval
        time.sleep(interval)

def run_typed(link, role):
    """SightingReports / PoltergeistActivity mode, one typed line, one event."""
    # run continuously, read a typed report one at a time
    while True:
        try:
            raw = input(f"{role}: ") # prompt input
        except EOFError:
            return
        handle_line(link, role, raw)


def main(argv=None):
    """connect, run the mode for this role, and return an exit code"""
    args = parse_args(argv)
    role = args.role
    try:
        # open tcp connection to the broker
        link = connect(args.host, PORT, PUB_ACK_TIMEOUT_S)
    except OSError as exc:
        # if broker isn't running/unreachable
        warn("pub", "cannot_connect", f"{args.host}:{PORT} {exc}")
        return 1
    # confirm connection
    info("pub", f"{role} connected to {args.host}:{PORT}")
    try:
        # route execution based on role
        if role == AUTO_ROLE:
            run_auto(link, role, args.interval)
        else:
            run_typed(link, role)
    except LinkError as exc:
        # no silent retry (Flows §8): a retry after a timeout could save the
        # event twice, print the problem and exit so the operator decides
        warn("pub", "link_failed", str(exc))
        return 1
    except KeyboardInterrupt:
        print()
    finally:
        link.close() # cleanup, close tcp socket
    return 0


if __name__ == "__main__":
    sys.exit(main())