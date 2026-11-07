import os
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from common import protocol as p
from common.constants import MAX_LINE_BYTES
from common.ids import new_event_id, normalise_id


class TestIds(unittest.TestCase):
    def test_messy_names_match(self):
        self.assertEqual(normalise_id("  Response   TEAM "), "response team")

    def test_event_id_unique_hex(self):
        a, b = new_event_id(), new_event_id()
        self.assertNotEqual(a, b)
        int(a, 16)


class TestBuffering(unittest.TestCase):
    def test_half_line_kept(self):
        self.assertEqual(p.split_lines(b"", b"SUB|sig"), ([], b"SUB|sig", None))

    def test_three_lines_one_chunk(self):
        lines, rest, _ = p.split_lines(b"", b"A\nB\nC\n")
        self.assertEqual((lines, rest), ([b"A", b"B", b"C"], b""))

    def test_line_across_two_chunks(self):
        _, rest, _ = p.split_lines(b"", b"SUB|sighting|")
        lines, rest, _ = p.split_lines(rest, b"team\n")
        self.assertEqual(lines, [b"SUB|sighting|team"])

    def test_oversize_closes(self):
        _, _, err = p.split_lines(b"", b"x" * (MAX_LINE_BYTES + 1))
        self.assertEqual((err.code, err.close), (413, True, "line_too_long"))

class TestRoundTrip(unittest.TestCase):
    def test_sub(self):
        r=p.parse_sub(p.format_sub("sighting", "responseteam"))
        self.assertEqual(r, p.Ok("SUB", {"topic": "sighting", "sub_id": "responseteam"}))

    def test_sub_ack(self):
        self.assertEqual(p.parse_sub_ack(p.format_sub_ack("alert")).fields["topic"], "alert")

    def test_get_stats(self):
        self.assertEqual(p.parse_get_stats(p.format_get_stats("alert")).cmd, "GET_STATS")

    def test_pub_pipe_in_payload(self):
        payload = {"note": "a|b", "sev": 5}
        r = p.parse_pub(p.format_pub("sighting", payload))
        self.assertEqual(r.fields["payload"], payload)

    def test_event(self):
        ev = {"event_id": "x", "topic": "alert", "payload": {}, "created_at": 1.0}
        self.assertEqual(p.parse_event(p.format_event(ev)).fields["event"], ev)

    def test_ack_rejects(self):
        r = p.parse_ack("ACK|a|b|")
        self.assertEqual(r.reason, "empty_event_id")

class TestRejects(unittest.TestCase):
    def test_pub_rejects(self):
        cases = [
            ("PUB|sighting", "bad_field_count"),
            ('PUB|sighting|"str"', "payload_not_object"),
            ("PUB|sighting|[1]", "payload_not_object"),
            ("PUB|sighting|{broken", "bad_json"),
        ]
        for line, reason in cases:
            with self.subTest(line=line):
                r=p.parse_pub(line)
                self.assertIsInstance(r, p.Err)
                self.assertFalse(r.close)
                self.assertEqual(r.reason, reason)

if __name__ == "__main__":
    unittest.main()