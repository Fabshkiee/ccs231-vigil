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
        self.sock = socket 
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


# Prompt loop
def main(argv=None):
    """demo loop: validate typed lines and show what would be sent"""
    role = parse_args(argv).role
    topic = ROLES[role][0]
    info("pub", f"{role} -> topic '{topic}' (validation only, nothing is sent yet)")
    while True:
        try:
            raw = input(f"{role}> ")
        except (EOFError, KeyboardInterrupt):
            print()
            break
        parsed, reason = parse_typed_line(raw)
        if reason:
            # Validate before any socket, rejected line must cause zero traffic.
            warn("pub", reason, raw)
            continue
        kind, sev = parsed
        info("pub", f"would send {topic}: kind={kind!r} sev={sev} alert={needs_alert(role, sev)}")


if __name__ == "__main__":
    main()