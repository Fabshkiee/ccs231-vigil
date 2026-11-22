import contextlib, io, os, socket, sys, unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from common.protocol import Err, Ok, format_ack
from subscriber import (
    LIVE_POLL_S, RED, SUBSCRIBERS, YELLOW, BrokerLink, LinkError, ReadTimeout,
    describe, format_event_line, event_problem, severity_of, color_for, listen,
    parse_args,  subscribe, SeenIds, SubscribeRejected, backoff_delay, handle_event,
    run_forever
)

LONG_ID = "a" * 32
OTHER_ID = "b" * 32

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

    def test_set_timeout_changes_the_socket_timeout(self):
        self.link.set_timeout(0.5)
        self.assertEqual(self.mine.gettimeout(), 0.5)

def make_event(event_id=LONG_ID, topic="sighting", payload=None):
    """build an event dict like the ones the broker sends"""
    if payload is None:
        payload = {"kind": "chanak sighting", "sev": 5, "source": "SightingReports"}
    return {"event_id": event_id, "topic": topic, "payload": payload, "created_at": 1.0}

class FormattingTests(unittest.TestCase):
    def test_describe(self): 
        cases = [
            ({"kind": "chanak sighting", "sev": 5}, "chanak sighting, severity 5"),
            ({"kind": "ambient reading", "zone": "zone-b", "level": 42},
             "ambient reading, zone-b, level 42"),
            ({}, "unknown"), # empty payload fallback, not raise KeyError
            ({"kind": "a\nb\tc"}, "a b c"),
        ]
        for payload, expected in cases:
            with self.subTest(payload=payload):
                self.assertEqual(describe(payload), expected)

    def test_format_event_line(self):
            self.assertEqual(
                format_event_line(make_event()),
                "Event aaaaaaaa — sighting: chanak sighting, severity 5",
            )

    def test_event_problem(self):
        self.assertIsNone(event_problem(make_event()))
        cases = {
            "bad_event_id": [make_event(event_id=""), make_event(event_id=7),
                             make_event(event_id=["x"])],
            "bad_topic": [make_event(topic=3)],
            "bad_payload": [make_event(payload=["not", "a", "dict"])],
        }
        for reason, events in cases.items():
            for event in events:
                with self.subTest(reason=reason, event=event):
                    self.assertEqual(event_problem(event), reason)

    def test_severity_of(self):
        self.assertEqual(severity_of({"sev": 5}), 5)
        for bad in ({}, {"sev": "5"}, {"sev": True}, {"sev": 2.5}):
            with self.subTest(payload=bad):
                self.assertIsNone(severity_of(bad))

class FakeLink:
    """stands in for BrokerLink: records sends, replays a script of replies
    a script item that is an exception is raised instead of returned"""

    def __init__(self, script=()):
        self.sent = []
        self.timeout = None
        self._script = list(script)
        self.closed = False

    def send(self, line):
        self.sent.append(line)

    def set_timeout(self, seconds):
        self.timeout = seconds

    def read_message(self):
        item = self._script.pop(0)
        if isinstance(item, Exception):
            raise item
        return item

    def close(self):
        self.closed = True

class OrderLink(FakeLink):
    """records how much text had been printed at the moment each line is sent"""

    def __init__(self, out):
        super().__init__()
        self.out = out
        self.printed_at_send = []

    def send(self, line):
        self.printed_at_send.append(self.out.getvalue())
        super().send(line)

class FailingLink(FakeLink):
    """a link whose sends always fail, like a connection that just died"""

    def send(self, line):
        raise LinkError("send_failed")
    

class ParseArgsTests(unittest.TestCase):
    def test_valid_role_and_default_host(self):
        args = parse_args(["--as", "ResponseTeam"])
        self.assertEqual((args.role, args.host), ("ResponseTeam", "127.0.0.1"))

    def test_unknown_role_exits(self):
        with contextlib.redirect_stderr(io.StringIO()):
            with self.assertRaises(SystemExit):
                parse_args(["--as", "Nobody"])

    def test_topic_table_matches_the_matrix(self):
        self.assertEqual(SUBSCRIBERS["ResponseTeam"], ("sighting", "poltergeist"))
        self.assertEqual(SUBSCRIBERS["PublicShield"], ("alert",))
        self.assertIn("readings", SUBSCRIBERS["ResearchArchive"])
        self.assertNotIn("readings", SUBSCRIBERS["ResponseTeam"]) # ignores ambient

class SubscribeTests(unittest.TestCase):
    def test_sends_one_sub_per_topic_and_waits_for_every_ack(self):
        link = FakeLink([Ok("SUB_ACK", {"topic": "sighting"}),
                         Ok("SUB_ACK", {"topic": "poltergeist"})])
        with contextlib.redirect_stdout(io.StringIO()):
            subscribe(link, "responseteam", ("sighting", "poltergeist"), SeenIds)
        self.assertEqual(link.sent, ["SUB|sighting|responseteam",
                                     "SUB|poltergeist|responseteam"])

    def test_unexpected_ack_is_warned_not_fatal(self):
        link = FakeLink([Ok("SUB_ACK", {"topic": "other"}),
                         Ok("SUB_ACK", {"topic": "sighting"})])
        with contextlib.redirect_stdout(io.StringIO()) as out:
            subscribe(link, "responseteam", ("sighting",), SeenIds)
        self.assertIn("unexpected_sub_ack", out.getvalue())

class ListenTests(unittest.TestCase):
    def run_listen(self, script):
        """Run listen() until the script ends in a LinkError; return the output."""
        link = FakeLink(script + [LinkError("connection_closed")])
        with contextlib.redirect_stdout(io.StringIO()) as out:
            with self.assertRaises(LinkError):
                listen(link, "responseteam", SeenIds())
        self.assertEqual(link.timeout, LIVE_POLL_S)
        return out.getvalue()

    def test_prints_events_and_skips_timeouts(self):
        out = self.run_listen([ReadTimeout(), Ok("EVENT", {"event": make_event()}),
                               ReadTimeout()])
        self.assertIn("chanak sighting, severity 5", out)

    def test_malformed_line_and_err_are_warnings(self):
        out = self.run_listen([Err(400, "bad_json"),
                               Ok("ERR", {"code": 422, "message": "unknown_event"})])
        self.assertIn("bad_line_from_broker", out)
        self.assertIn("broker_error", out)

class SeenIdsTests(unittest.TestCase):
    def test_remembers_ids(self):
        seen = SeenIds(limit=3)
        self.assertNotIn("a", seen)
        seen.add("a")
        self.assertIn("a", seen)

    def test_oldest_id_is_forgotten_when_full(self):
        seen = SeenIds(limit=3)
        for event_id in "abcd":      # iterating a string gives "a","b","c","d"
            seen.add(event_id)
        self.assertNotIn("a", seen)  # pushed out by "d"
        for event_id in "bcd":
            self.assertIn(event_id, seen)

    def test_adding_a_known_id_does_not_use_up_space(self):
        seen = SeenIds(limit=2)
        seen.add("a")
        seen.add("a")
        seen.add("b")
        self.assertIn("a", seen)

class HandleEventTests(unittest.TestCase):
    ACK_LINE = f"ACK|sighting|responseteam|{LONG_ID}"

    def handle(self, link, event, seen=None):
        """Run handle_event quietly and return what it printed."""
        seen = SeenIds() if seen is None else seen
        with contextlib.redirect_stdout(io.StringIO()) as out:
            handle_event(link, "responseteam", seen, event)
        return out.getvalue()

    def test_ack_line_matches_the_protocol_formatter(self):
        self.assertEqual(self.ACK_LINE, format_ack("sighting", "responseteam", LONG_ID))

    def test_prints_and_then_acks(self):
        link = FakeLink()
        out = self.handle(link, make_event())
        self.assertIn("chanak sighting, severity 5", out)
        self.assertEqual(link.sent, [self.ACK_LINE])

    def test_ack_is_sent_only_after_the_print(self):
        out = io.StringIO()
        link = OrderLink(out)
        with contextlib.redirect_stdout(out):
            handle_event(link, "responseteam", SeenIds(), make_event())
        # at the moment the ACK went out, the event text was already printed
        self.assertIn("chanak sighting", link.printed_at_send[0])

    def test_duplicate_is_not_printed_but_is_acked_again(self):
        seen = SeenIds()
        self.handle(FakeLink(), make_event(), seen)
        link = FakeLink()
        out = self.handle(link, make_event(), seen)
        self.assertEqual(out, "")
        self.assertEqual(link.sent, [self.ACK_LINE])

class BackoffTests(unittest.TestCase):
    def test_sequence_grows_then_stays_capped(self):
        self.assertEqual([backoff_delay(n) for n in range(7)], [1, 2, 4, 5, 5, 5, 5])

    def test_negative_attempt_counts_as_the_first(self):
        self.assertEqual(backoff_delay(-3), 1)

class Connector:
    """a fake connect function: each call consumes the next scripted outcome.
    An Exception is raised (a failed connect); anything else is returned"""

    def __init__(self, outcomes):
        self.outcomes = list(outcomes)
        self.calls = 0

    def __call__(self, host, port, timeout):
        self.calls += 1
        outcome = self.outcomes.pop(0)
        if isinstance(outcome, Exception):
            raise outcome
        return outcome


class FakeSleep:
    """records every delay instead of waiting; stops the loop after `limit`
    sleeps by raising KeyboardInterrupt, exactly like a Ctrl+C would"""

    def __init__(self, limit):
        self.limit = limit
        self.delays = []

    def __call__(self, seconds):
        self.delays.append(seconds)
        if len(self.delays) >= self.limit:
            raise KeyboardInterrupt


REFUSED = ConnectionRefusedError("refused")
TOPICS = ("sighting",)
SUB_ACK = Ok("SUB_ACK", {"topic": "sighting"})
DROPPED = LinkError("connection_closed")


class RunForeverTests(unittest.TestCase):
    def run_loop(self, outcomes, sleeps, seen=None):
        """run the loop until FakeSleep stops it. Returns (connector, sleep, output)"""
        connector = Connector(outcomes)
        sleep = FakeSleep(sleeps)
        seen = SeenIds() if seen is None else seen
        with contextlib.redirect_stdout(io.StringIO()) as out:
            with self.assertRaises(KeyboardInterrupt):
                run_forever("127.0.0.1", "responseteam", TOPICS, seen,
                            connect_fn=connector, sleep_fn=sleep)
        return connector, sleep, out.getvalue()

    def test_backoff_grows_while_the_broker_is_down(self):
        _, sleep, out = self.run_loop([REFUSED] * 5, sleeps=5)
        self.assertEqual(sleep.delays, [1, 2, 4, 5, 5])
        self.assertIn("cannot_connect", out)

    def test_every_reconnect_subscribes_again_and_closes_the_old_link(self):
        first = FakeLink([SUB_ACK, DROPPED])
        second = FakeLink([SUB_ACK, DROPPED])
        self.run_loop([first, second, REFUSED], sleeps=3)
        for link in (first, second):
            self.assertEqual(link.sent, ["SUB|sighting|responseteam"])
            self.assertTrue(link.closed)


if __name__ == "__main__":
    unittest.main()