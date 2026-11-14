import os, sys, unittest, contextlib, io

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from publisher import parse_typed_line, needs_alert, parse_args


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

if __name__ == "__main__":
    unittest.main()