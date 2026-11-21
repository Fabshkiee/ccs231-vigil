import contextlib, io, os, socket, sys, unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from common.protocol import Err, Ok
from subscriber import (
    LIVE_POLL_S, RED, SUBSCRIBERS, YELLOW, BrokerLink, LinkError, ReadTimeout
)

LONG_ID = "a" * 32

class BrokerLinkTests(unittest.TestCase):
    def setUp(self):
        self.mine, self.broker_side = socket.socketpair()
        self.mine.settimeout(1)
        self.link = BrokerLink(self.mine)

    def tearDown(self):
        self.mine.close()
        self.broker_side.close()

    def test_reads_an_event_line(self):
        self.broker_side.sendall(
            b'EVENT|{"event_id":"a","topic":"t","payload":{},"created_at":1}\n')
        self.assertEqual(self.link.read_message().cmd, "EVENT")


if __name__ == "__main__":
    unittest.main()