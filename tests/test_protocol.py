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
        self.assertEqual((err.code, err.close, err.reason), (413, True, "line_too_long"))

class TestRoundTrip(unittest.TestCase):
    def test_sub(self):
        r=p.parse_sub(p.format_sub("sighting", "responseteam"))
        self.assertEqual(r, p.Ok("SUB", {"topic": "sighting", "sub_id": "responseteam"}))

    def test_sub_ack(self):
        self.assertEqual(p.parse_sub_ack(p.format_sub_ack("alert")).fields["topic"], "alert")

    def test_pub_ack(self):
        self.assertEqual(p.parse_pub_ack(p.format_pub_ack("e-8")).fields["event_id"], "e-8")

    def test_get_stats(self):
        self.assertEqual(p.parse_get_stats(p.format_get_stats("alert")).cmd, "GET_STATS")

    def test_pub_pipe_in_payload(self):
        payload = {"note": "a|b", "sev": 5}
        r=p.parse_pub(p.format_pub("sighting", payload))
        self.assertEqual(r.fields["payload"], payload)

    def test_event(self):
        ev= {"event_id": "x", "topic": "alert", "payload": {}, "created_at": 1.0}
        self.assertEqual(p.parse_event(p.format_event(ev)).fields["event"], ev)

    def test_ack(self):
        r = p.parse_ack(p.format_ack("sighting", "responseteam", "e-8"))
        self.assertEqual(r.fields["event_id"], "e-8")

    def test_err(self):
        r = p.parse_err(p.format_err(400, "empty_topic"))
        self.assertEqual((r.fields["code"], r.fields["message"]), (400, "empty_topic"))

    def test_dispatcher_matches_direct_parser(self):
        line = p.format_sub("sighting", "team")
        self.assertEqual(p.parse_line(line), p.parse_sub(line))


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

    def reason(self, line):
        r = p.parse_line(line)
        self.assertIsInstance(r, p.Err)
        self.assertFalse(r.close)
        return r.reason

    def test_dispatcher_rejects(self):
        cases = [
            ("", "empty_line"),
            ("|", "unknown_command"),
            ("HELLO|x", "unknown_command"),
            ("SUB", "bad_field_count"),
            (b"\xff\xfe", "bad_encoding"),
        ]
        for line, reason in cases:
            with self.subTest(line=line):
                self.assertEqual(self.reason(line), reason)
    
    def test_ack_rejects(self):
        r=p.parse_ack("ACK|a|b|")
        self.assertEqual(r.reason, "empty_event_id")

    def test_pub_ack_rejects(self):
        self.assertEqual(p.parse_pub_ack("PUB_ACK|").reason, "empty_event_id")      
    
    def test_event_rejects(self):
        self.assertEqual(self.reason("EVENT|{}"), "missing_event_id")

    def test_err_rejects_newline(self):
        with self.assertRaises(ValueError):
            p.format_err(400, "a\nb")

    def test_err_bad_code(self):
        for line in ["ERR|abc|x", "ERR|²|x"]:
            with self.subTest(line=line):
                self.assertEqual(p.parse_err(line).reason, "bad_code")

if __name__ == "__main__":
    unittest.main()