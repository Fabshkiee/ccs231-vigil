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

# ansi colour codes
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