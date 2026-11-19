import os, sys, unittest, contextlib, io, random, socket, threading, unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from publisher import ( 
    parse_typed_line, needs_alert, parse_args, BrokerLink, BrokerRejected,
    LinkError, READING_MAX, READING_MIN, build_payload, handle_line, make_reading,
    publish, short_id
)
from common.protocol import Ok, Err, format_pub

LONG_ID = "a" * 32 # what real uuid2 looks like

class ParseTypedLineTests(unittest.TestCase):
    def test_valid_lines(self):
        """verify successful parsing of correctly formmated inputs"""
        cases = {
            # standard valid inputs
            "chanak sighting, severity 5": ("chanak sighting", 5),
            "fridge opened itself, severity 2": ("fridge opened itself", 2),
            # whitespaces and weird casings handled
            "  Chants in walls ,  SEVERITY   1  ": ("Chants in walls", 1),
            # ensure rfind works, take only the last instance of severity
            "severity of the haunting, severity 3": ("severity of the haunting", 3),
        }
        for raw, expected in cases.items():
            with self.subTest(raw=raw): # isolate each failing case
                self.assertEqual(parse_typed_line(raw), (expected, None))

    def test_rejected_lines(self):
        """verify block of invalid inputs"""
        cases = {
            # missing required keyword
            "": "missing_severity",
            "chanak sighting": "missing_severity",
            #keyword must be whole
            "myseverity 5": "missing_severity",
            "severityseverity 5": "missing_severity",
            # missing/malformed integer value
            "chanak, severity": "bad_severity",
            "chanak, severity abc": "bad_severity",
            "chanak, severity 5.0": "bad_severity",
            "chanak, severity -1": "bad_severity",
            # out of range sev values
            "chanak, severity 0": "severity_out_of_range",
            "chanak, severity 6": "severity_out_of_range",
            # missing actual event description
            ", severity 3": "empty_text",
        }
        for raw, reason in cases.items():
            with self.subTest(raw=raw):
                self.assertEqual(parse_typed_line(raw), (None, reason))


class NeedsAlertTests(unittest.TestCase):
    def test_threshold(self):
        """verify that only sev 4 and 5 trigger the secondary alert"""
        # no alerts needed
        self.assertFalse(needs_alert("SightingReports", 3))
        # triggers alert
        self.assertTrue(needs_alert("SightingReports", 4))
        self.assertTrue(needs_alert("PoltergeistActivity", 5))

    def test_readings_never_alert(self):
        """verify background sensors don't trigger alerts"""
        self.assertFalse(needs_alert("ParanormalReadings", 5))


class ParseArgsTests(unittest.TestCase):
    def test_valid_role(self):
        """follow --as <role> format"""
        args = parse_args(["--as", "SightingReports"])
        self.assertEqual(args.role, "SightingReports")

    def test_unknown_role_exits(self):
        """verify successful syst exits on invalid roles"""
        # argparse raises SystemExit, assertRaises catches it to prevent test run kill
        # redirect hides usage text for clean output
        with contextlib.redirect_stderr(io.StringIO()):
            with self.assertRaises(SystemExit):
                parse_args(["--as", "Nobody"])

class FakeLink:
    """stands in for BrokerLink, records sends, replays scripted replies"""

    # create lists in memory to record pub sends and broker answers
    def __init__(self, replies=()):
        self.sent = []
        self._replies = list(replies)

    def send(self, line):
        self.sent.append(line)

    def read_reply(self):
        return self._replies.pop(0)


def run_quietly(func, *args):
    """call func while capturing what it prints, return the printed text"""
    with contextlib.redirect_stdout(io.StringIO()) as out:
        func(*args)
    return out.getvalue()

class PayloadTests(unittest.TestCase):
    def test_short_id(self):
        self.assertEqual(short_id(LONG_ID), "aaaaaaaa")

    def test_build_payload(self):
        self.assertEqual(
            build_payload("chanak sighting", 5, "SightingReports"),
            {"kind": "chanak sighting", "sev": 5, "source": "SightingReports"},
        )

    def test_readings_are_repeatable_and_in_range(self):
        first = [make_reading(random.Random(7), "ParanormalReadings") for _ in range(1)]
        again = [make_reading(random.Random(7), "ParanormalReadings") for _ in range(1)]
        self.assertEqual(first, again)          # same seed -> same reading
        rng = random.Random(1)
        for _ in range(50):
            reading = make_reading(rng, "ParanormalReadings")
            self.assertEqual(set(reading), {"kind", "zone", "level", "source"})
            self.assertTrue(READING_MIN <= reading["level"] <= READING_MAX)
            self.assertNotIn("sev", reading)    # readings never carry severity

class PublishTests(unittest.TestCase):
    # success case
    def test_returns_event_id_and_sends_pub_line(self):
        link = FakeLink([Ok("PUB_ACK", {"event_id": LONG_ID})])
        payload = {"kind": "x", "sev": 1, "source": "SightingReports"}
        self.assertEqual(publish(link, "sighting", payload), LONG_ID)
        self.assertEqual(link.sent, [format_pub("sighting", payload)])
    # valid input but broker replies ERR
    def test_err_reply_raises_broker_rejected(self):
        link = FakeLink([Ok("ERR", {"code": 503, "message": "store_failed"})])
        with self.assertRaises(BrokerRejected):
            publish(link, "sighting", {"kind": "x"})
    # invalid reply or wrong command
    def test_malformed_or_unexpected_reply_raises_link_error(self):
        for reply in (Err(400, "bad_field_count"), Ok("SUB_ACK", {"topic": "t"})):
            with self.subTest(reply=reply):
                with self.assertRaises(LinkError):
                    publish(FakeLink([reply]), "sighting", {"kind": "x"})

class HandleLineTests(unittest.TestCase):
    ACK = Ok("PUB_ACK", {"event_id": LONG_ID}) # fake ACK

    def test_high_severity_sends_event_then_alert_with_same_payload(self):
        link = FakeLink([self.ACK, self.ACK])
        out = run_quietly(handle_line, link, "SightingReports", "chanak, severity 5")
        self.assertEqual(len(link.sent), 2) # 
        self.assertTrue(link.sent[0].startswith("PUB|sighting|"))
        self.assertTrue(link.sent[1].startswith("PUB|alert|"))
        # same payload text after the second '|' on both lines.
        self.assertEqual(link.sent[0].split("|", 2)[2], link.sent[1].split("|", 2)[2])
        self.assertIn("saved (sighting)", out)
        self.assertIn("saved (alert)", out)

    def test_low_severity_sends_only_the_event(self):
        link = FakeLink([self.ACK])
        run_quietly(handle_line, link, "SightingReports", "footsteps, severity 3")
        self.assertEqual(len(link.sent), 1)

if __name__ == "__main__":
    unittest.main()