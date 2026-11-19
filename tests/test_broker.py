import json
import socket as sck
import threading as thr
import time as t
import unittest as ut
from unittest.mock import MagicMock, patch
import os, sys, io

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

import broker
import common.constants as consts
import common.protocol as prot

ADDRESS = (broker.BROKER_HOST, consts.PORT)

class TestBrokerConnection(ut.TestCase):
	@classmethod
	def setUpClass(cls):
		cls.logger_patcher = patch("broker.logger")
		cls.logger_patcher.start()

		cls.server_thread = thr.Thread(
			target=broker.start_broker,
			daemon=True,
		)
		cls.server_thread.start()
		t.sleep(0.1)

		cls.pub_sock = sck.socket(sck.AF_INET, sck.SOCK_STREAM)
		cls.pub_sock.settimeout(2.0)
		cls.pub_sock.connect(ADDRESS)
		cls.pub_file = cls.pub_sock.makefile("r", encoding="utf-8")

	@classmethod
	def tearDownClass(cls):
		cls.pub_file.close()
		cls.pub_sock.close()
		t.sleep(0.05)
		cls.logger_patcher.stop()

	def setUp(self):
		with broker.sub_lock:
			broker.subscriptions.clear()
			broker.clients.clear()
			broker.client_send_locks.clear()

	def _assert_log_written(self, filepath: str, topic: str):
		with open(filepath, "r", encoding="utf-8") as log_f:
			contents = log_f.read()
		self.assertIn(topic, contents)
	
	def _acked_event_id(self, response: str) -> str:
		parsed = prot.parse_pub_ack(response)
		self.assertIsInstance(parsed, prot.Ok)
		return parsed.fields["event_id"]

	def _get_test_pub_payload(self) -> dict:
		return {"text": "chants", "severity": 2}

	def _connect_client(self) -> tuple[sck.socket, io.TextIOWrapper]:
		sock = sck.socket(sck.AF_INET, sck.SOCK_STREAM)
		sock.settimeout(2.0)
		sock.connect(ADDRESS)
		f = sock.makefile("r", encoding="utf-8")
		self.addCleanup(sock.close)
		self.addCleanup(f.close)
		return sock, f

	def test_publisher_can_connect(self):		
		"""Tests if publisher can establish basic connection with broker"""
		ip, port = self.pub_sock.getpeername()
		self.assertEqual(port, consts.PORT, f"{[port]} is not {consts.PORT}")

	def test_broker_save_event_and_returns_pub_ack(self):
		topic = "sightings"
		pub_line = prot.format_pub(topic, self._get_test_pub_payload()) + "\n"
		self.pub_sock.sendall(pub_line.encode("utf-8"))

		# assert 1: broker replied with pub_ack
		response = self.pub_file.readline().strip()
		self._acked_event_id(response)

		# assert 2: verify event_log.json was written
		self._assert_log_written(consts.EVENT_LOG_FILE, topic)

	def test_broker_broadcasts_to_subscribers_and_drops_dead(self):
		"""
		Tests the broker's routing loop.  
		
		Tests if it safely delivers message to subscribers while droppping dead sockets.
		Simulates subscriber while broker._handle_sub() is not implemented yet.
		"""
		topic = "readings"

		# 2 live sockets
		broker_sub_sock, client_sub_sock = sck.socketpair()
		client_sub_sock.settimeout(2.0)
		self.addCleanup(broker_sub_sock.close)
		self.addCleanup(client_sub_sock.close)

		# 2 dead sockets
		dead_broker_sock, dead_client_sock = sck.socketpair()
		dead_broker_sock.close()
		dead_client_sock.close()

		# register broker sockets under subscriptions and clients
		with broker.sub_lock:
			broker.subscriptions[topic] = [
				(broker_sub_sock, "sub_live"),
				(dead_broker_sock, "sub_dead")
			]
			broker.clients[broker_sub_sock] = "sub_live"
			broker.clients[dead_broker_sock] = "sub_dead"

		pub_line = prot.format_pub(topic, self._get_test_pub_payload()) + "\n"
		self.pub_sock.sendall(pub_line.encode("utf-8"))

		# assert 1: broker replied with pub_ack
		pub_response = self.pub_file.readline().strip()
		event_id = self._acked_event_id(pub_response)

		# assert 2: live subscriber received the formatted event
		with client_sub_sock.makefile("r", encoding="utf-8") as sub_f:
			received_event = sub_f.readline().strip()
		self.assertIn(event_id, received_event)
		self.assertIn("chants", received_event)

		# assert 3: dead subscriber was removed from both subscriptions and clients
		with broker.sub_lock:
			remaining_subs = [s_id for _, s_id in broker.subscriptions.get(topic, [])]
			self.assertEqual(remaining_subs, ["sub_live"])
			self.assertNotIn(dead_broker_sock, broker.clients)
			self.assertIn(broker_sub_sock, broker.clients)

	def test_publishes_keep_log_order(self):
		"""Two publishers on one topic must reach the subscriber in log order."""
		topic = "sightings"
		sent: list[str] = []
		appended: list[str] = []
		first_appended = thr.Event()
		real_append = broker.storage.append_event

		def slow_first_append(event):
			real_append(event)
			appended.append(event["event_id"])
			if len(appended) == 1:
				first_appended.set()
				t.sleep(0.2)

		def record_send(_client, wire):
			sent.append(wire)

		with broker.sub_lock:
			broker.subscriptions[topic] = [(object(), "sub_order")]

		with patch("broker.storage.append_event", side_effect=slow_first_append), \
				patch("broker._send_line", side_effect=record_send):
			first = thr.Thread(target=broker._handle_pub, args=(object(), topic, {"kind": "A"}))
			first.start()
			first_appended.wait(timeout=2)
			broker._handle_pub(object(), topic, {"kind": "B"})
			first.join(timeout=2)

		self.assertFalse(first.is_alive())
		kinds = [
			json.loads(wire.split("|", 1)[1])["payload"]["kind"]
			for wire in sent if wire.startswith("EVENT|")
		]
		self.assertEqual(kinds, ["A", "B"])

	@patch("broker.storage.append_event", side_effect=OSError("Disk full"))
	def test_broker_fails_to_save_event_returns_503_and_aborts_broadcast(self, mock_append):
		"""Test broker replies with ERR 503 and does not forward event to subscribers on storage failure"""
		topic = "sightings"

		broker_sub_sock, client_sub_sock = sck.socketpair()
		client_sub_sock.settimeout(0.2)  # Short timeout since we expect no message
		self.addCleanup(broker_sub_sock.close)
		self.addCleanup(client_sub_sock.close)

		with broker.sub_lock:
			broker.subscriptions[topic] = [(broker_sub_sock, "sub_live")]
			broker.clients[broker_sub_sock] = "sub_live"

		# publish an event while storage.append_event is rigged to fail
		pub_line = prot.format_pub(topic, self._get_test_pub_payload()) + "\n"
		self.pub_sock.sendall(pub_line.encode("utf-8"))

		# assert 1: publisher receives a 503 error message
		pub_response = self.pub_file.readline().strip()
		self.assertIn("503", pub_response)
		self.assertIn("store failure", pub_response)

		# assert 2: subscriber never received the unsaved event
		with self.assertRaises(TimeoutError):
			client_sub_sock.recv(1024)

	@patch("broker.storage.append_event")
	@patch("broker.storage.load_events")
	@patch("broker.storage.load_offsets")
	def test_first_time_subscriber(self, mock_offsets, mock_events, _mock_append):
		"""Test Case #1: First time subscriber"""
		ev_pld = {
			"event_id": "ev_replay",
			"topic": "sightings",
			"payload": {"text": "chants", "severity": 2},
			"created_at": 0.0,
		}
		mock_offsets.return_value = {}
		mock_events.return_value = {"sightings": [ev_pld]}
		sub_sock, sub_file = self._connect_client()

		# subscribe -> expects SUB_ACK + replayed ev_pld["event_id"]
		sub_sock.sendall(b"SUB|sightings|sub_new\n")
		self.assertEqual(sub_file.readline().strip(), prot.format_sub_ack("sightings"))
		self.assertIn(ev_pld["event_id"], sub_file.readline())

		# publish live -> expects the newly acked id delivered
		self.pub_sock.sendall((prot.format_pub("sightings", self._get_test_pub_payload()) + "\n").encode("utf-8"))
		event_id = self._acked_event_id(self.pub_file.readline().strip())
		self.assertIn(event_id, sub_file.readline())

	@patch("broker.storage.load_events")
	@patch("broker.storage.load_offsets")
	def test_reconnecting_subscriber_replays_only_missed_events(self, mock_offsets, mock_events):
		"""Test Case #2: Skips already-acked events (0, 1) and replays only missed events (2, 3)."""
		mock_offsets.return_value = {"sub_alice": {"sightings": 1}}
		mock_events.return_value = {
		"sightings": [
				{"event_id": "ev0", "topic": "sightings", "payload": {}},
				{"event_id": "ev1", "topic": "sightings", "payload": {}},
				{"event_id": "ev2", "topic": "sightings", "payload": {}},
				{"event_id": "ev3", "topic": "sightings", "payload": {}},
			]
		}
		fake_client = MagicMock()
		broker._handle_sub(fake_client, "sightings", "sub_alice")

		sent = [call[0][0] for call in fake_client.sendall.call_args_list]
		self.assertEqual(len(sent), 3)  # SUB_ACK + ev2 + ev3
		self.assertIn(b"ev2", sent[1])
		self.assertIn(b"ev3", sent[2])

	@patch("broker.storage.load_offsets", side_effect=OSError("Disk read failed"))
	def test_sub_storage_failure_returns_503_and_aborts(self, _mock_offsets):
		"""Test Case #3: Storage error replies with 503 and does not register the subscriber."""
		fake_client = MagicMock()

		broker._handle_sub(fake_client, "sightings", "sub_fail")

		sent_bytes = fake_client.sendall.call_args[0][0]
		self.assertIn(b"503", sent_bytes)
		self.assertEqual(broker.subscriptions.get("sightings", []), [])
		self.assertNotIn(fake_client, broker.clients)

	def test_corrupt_log_blocks_startup(self):
		"""A corrupt event log must stop the broker before it listens."""
		with patch("broker.storage.load_events", side_effect=broker.storage.StoreError("bad json")), \
				patch("broker.sck.socket") as fake_socket:
			broker.start_broker()
		fake_socket.assert_not_called()

	def test_resub_replaces_the_old_socket(self):
		"""A second SUB with the same sub_id must replace the first socket."""
		topic = "sightings"
		old_sock, old_peer = sck.socketpair()
		new_sock, new_peer = sck.socketpair()
		self.addCleanup(old_peer.close)
		self.addCleanup(new_sock.close)
		self.addCleanup(new_peer.close)

		with patch("broker.storage.load_offsets", return_value={}), \
				patch("broker.storage.load_events", return_value={}):
			broker._handle_sub(old_sock, topic, "dupe")
			broker._handle_sub(new_sock, topic, "dupe")

		with broker.sub_lock:
			subs = broker.subscriptions.get(topic, [])
		self.assertEqual([c for c, _ in subs], [new_sock])
		self.assertNotIn(old_sock, broker.clients)

	# Test broker._handle_ack
	def _pub_event(self, topic: str = "sightings") -> str:
		self.pub_sock.sendall((prot.format_pub(topic, self._get_test_pub_payload()) + "\n").encode("utf-8"))
		ack_line = self.pub_file.readline().strip()
		return self._acked_event_id(ack_line)

	def test_ack_creates_and_updates_offsets_json(self):
		ev1, ev2 = self._pub_event(), self._pub_event()
		client = MagicMock()

		broker._handle_ack(client, "sightings", "sub_ack_1", ev1)
		first_idx = broker.storage.load_offsets()["sub_ack_1"]["sightings"]

		broker._handle_ack(client, "sightings", "sub_ack_1", ev2)
		second_idx = broker.storage.load_offsets()["sub_ack_1"]["sightings"]

		self.assertGreater(second_idx, first_idx)
		self.assertTrue(os.path.exists(consts.OFFSETS_FILE))
		self._assert_log_written(consts.OFFSETS_FILE, "sightings")

	def test_ack_ignores_duplicate_and_stale_ack(self):
		ev1, ev2 = self._pub_event(), self._pub_event()
		client = MagicMock()

		broker._handle_ack(client, "sightings", "sub_ack_2", ev2)
		saved_idx = broker.storage.load_offsets()["sub_ack_2"]["sightings"]

		broker._handle_ack(client, "sightings", "sub_ack_2", ev2) # duplicate
		broker._handle_ack(client, "sightings", "sub_ack_2", ev1) # stale

		self.assertEqual(broker.storage.load_offsets()["sub_ack_2"]["sightings"], saved_idx)

	def test_ack_unknown_event_returns_442(self):
		other_ev = self._pub_event("readings")
		client = MagicMock()

		broker._handle_ack(client, "sightings", "sub_ack_3", other_ev)

		self.assertIn(b"422", client.sendall.call_args[0][0])
		self.assertNotIn("sub_ack_3", broker.storage.load_offsets())

if __name__ == "__main__":
	ut.main()
