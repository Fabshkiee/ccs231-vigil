import os
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from common.ids import new_event_id, normalise_id


class TestIds(unittest.TestCase):
    def test_messy_names_match(self):
        self.assertEqual(normalise_id("  Response   TEAM "), "response team")

    def test_event_id_unique_hex(self):
        a, b = new_event_id(), new_event_id()
        self.assertNotEqual(a, b)
        int(a, 16)


if __name__ == "__main__":
    unittest.main()