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

    def test_complete_line_then_unfinished_line_in_one_chunk(self):             #P-07 NEW test
        lines, rest, err = p.split_lines(b"", b"SUB|a|b\nPUB|sig")              #one complete line + an unfinished one in the same chunk
        self.assertEqual((lines, rest, err), ([b"SUB|a|b"], b"PUB|sig", None))  #P-07 FIX: complete line returned, unfinished part stays buffered, no error
        lines, rest, err = p.split_lines(rest, b'hting|{}\n')                   #P-07 FIX: next chunk finishes the kept tail
        self.assertEqual((lines, rest, err), ([b"PUB|sighting|{}"], b"", None)) #P-07 FIX: tail joins the new bytes into one full line

    def test_oversize_closes(self):
        _, _, err = p.split_lines(b"", b"x" * (MAX_LINE_BYTES + 1))
        self.assertEqual((err.code, err.close, err.reason), (413, True, "line_too_long"))

class TestRoundTrip(unittest.TestCase):
    def test_sub(self):
        r=p.parse_sub(p.format_sub("sighting", "responseteam"))
        self.assertEqual(r, p.Ok("SUB", {"topic": "sighting", "sub_id": "responseteam"}))

    def test_sub_ack(self):
        r = p.parse_sub_ack(p.format_sub_ack("alert")) 
        self.assertEqual(r, p.Ok("SUB_ACK", {"topic": "alert"})) #P-01 FIX: was only fields["topic"]; now checks command name + all fields

    def test_pub_ack(self):
        r = p.parse_pub_ack(p.format_pub_ack("e-8"))
        self.assertEqual(r, p.Ok("PUB_ACK", {"event_id": "e-8"})) #P-01 FIX: was only fields["event_id"]; now also checks the command name

    def test_get_stats(self):
        r = p.parse_get_stats(p.format_get_stats("alert"))
        self.assertEqual(r, p.Ok("GET_STATS", {"topic": "alert"})) #P-01 FIX: was only .cmd; now also checks the topic
    def test_pub_pipe_in_payload(self):
        payload = {"note": "a|b", "sev": 5}
        r = p.parse_pub(p.format_pub("sighting", payload))
        self.assertEqual(r, p.Ok("PUB", {"topic": "sighting", "payload": payload})) #P-01 FIX: was only payload; now also command + topic

    def test_event(self):
        ev = {"event_id": "x", "topic": "alert", "payload": {}, "created_at": 1.0}
        r = p.parse_event(p.format_event(ev))
        self.assertEqual(r, p.Ok("EVENT", {"event": ev})) #P-01 FIX: was only fields["event"]; now also the command name

    def test_ack(self):
        r = p.parse_ack(p.format_ack("sighting", "responseteam", "e-8"))
        self.assertEqual(
            r, p.Ok("ACK", {"topic": "sighting", "sub_id": "responseteam", "event_id": "e-8"}) #P-01 FIX: was only event_id; now command, topic, sub_id and event_id
        )

    def test_err(self):
        r = p.parse_err(p.format_err(400, "empty_topic"))
        self.assertEqual(r, p.Ok("ERR", {"code": 400, "message": "empty_topic"})) #P-01 FIX: was only code + message; now also the command name

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

    def test_missing_and_extra_fields(self):           #P-02 NEW test: every command rejects too few AND too many fields
        cases = [
            "SUB", "SUB|a", "SUB|a|b|c",               #P-02 FIX: SUB needs exactly 3 fields (missing + extra)
            "SUB_ACK", "SUB_ACK|a|b",                  #P-02 FIX: SUB_ACK needs exactly 2 fields
            "PUB_ACK", "PUB_ACK|a|b",                  #P-02 FIX: PUB_ACK needs exactly 2 fields
            "GET_STATS", "GET_STATS|a|b",              #P-02 FIX: GET_STATS needs exactly 2 fields
            "ACK", "ACK|a", "ACK|a|b", "ACK|a|b|c|d",  #P-02 FIX: ACK needs exactly 4 fields
            "PUB",                                     #P-02 FIX: PUB with no topic and no payload
            "EVENT",                                   #P-02 FIX: EVENT with no payload
            "ERR", "ERR|400",                          #P-02 FIX: ERR needs a code and a message
        ]
        for line in cases:
            with self.subTest(line=line):
                self.assertEqual(self.reason(line), "bad_field_count") #P-02 FIX: rejected, never repaired

    def test_extra_pipe_in_pub_and_event_is_payload(self):             #P-02 NEW test: PUB/EVENT allow '|' in the last field
        self.assertEqual(self.reason("PUB|sighting|{}|x"), "bad_json") #P-02 FIX: extra '|' becomes part of the JSON, so bad_json not bad_field_count
        self.assertEqual(self.reason("EVENT|{}|x"), "bad_json")        #P-02 FIX: same rule for EVENT

    def test_empty_and_whitespace_topic(self): #P-03 NEW test: empty or blank topic gives empty_topic
        cases = [
            "SUB||team", "SUB|   |team", "SUB|\t|team",    #P-03 FIX: SUB with empty, spaces-only and tab-only topic
            "SUB_ACK|", "SUB_ACK|   ", "SUB_ACK|\t",       #P-03 FIX: SUB_ACK
            "PUB||{}", "PUB|   |{}", "PUB|\t|{}",          #P-03 FIX: PUB
            "ACK||s|e", "ACK|   |s|e", "ACK|\t|s|e",       #P-03 FIX: ACK
            "GET_STATS|", "GET_STATS|   ", "GET_STATS|\t", #P-03 FIX: GET_STATS
        ]
        for line in cases:
            with self.subTest(line=line):
                self.assertEqual(self.reason(line), "empty_topic") #P-03 FIX: named empty_topic, not a generic error

if __name__ == "__main__":
    unittest.main()