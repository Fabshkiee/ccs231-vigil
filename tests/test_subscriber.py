import contextlib, io, os, socket, sys, unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from common.protocol import Err, Ok
from subscriber import (
    LIVE_POLL_S, RED, SUBSCRIBERS, YELLOW, BrokerLink, LinkError, ReadTimeout
)

LONG_ID = "a" * 32