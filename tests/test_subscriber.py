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

    def test_two_lines_in_one_chunk_come_out_one_at_a_time(self):
        self.broker_side.sendall(b"SUB_ACK|one\nSUB_ACK|two\n")
        self.assertEqual(self.link.read_message().fields["topic"], "one")
        self.assertEqual(self.link.read_message().fields["topic"], "two")

    def test_timeout_is_read_timeout_not_link_error(self):
        self.mine.settimeout(0.2)
        with self.assertRaises(ReadTimeout):
            self.link.read_message()

    def test_closed_connection_raises_link_error(self):
        self.broker_side.close()
        with self.assertRaises(LinkError):
            self.link.read_message()

    def test_send_appends_newline(self):
        self.link.send("SUB|sighting|responseteam")
        self.assertEqual(self.broker_side.recv(100), b"SUB|sighting|responseteam\n")


if __name__ == "__main__":
    unittest.main()