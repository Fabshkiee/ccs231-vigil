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
        self.assertEqual((err.code, err.close), (413, True))

class TestRoundTrip(unittest.TestCase):
    def test_sub(self):
        r=p.parse_sub(p.format_sub("sighting", "responseteam"))
        self.assertEqual(r, p.Ok("SUB", {"topic": "sighting", "sub_id": "responseteam"}))

    def test_sub_ack(self):
        self.assertEqual(p.parse_sub_ack(p.format_sub_ack("alert")).fields["topic"], "alert")

if __name__ == "__main__":
    unittest.main()