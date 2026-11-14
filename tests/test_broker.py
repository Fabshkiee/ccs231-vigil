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
import common.ids as id

ADDRESS = (broker.BROKER_HOST, consts.PORT)

class TestBrokerConnection(ut.TestCase):
	@classmethod
	def setUpClass(cls):
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
		t.sleep(0.05)
		cls.pub_file.close()
		cls.pub_sock.close()

	def setUp(self):
		with broker.sub_lock:
			broker.subscriptions.clear()
			broker.clients.clear()

	def _assert_log_written(self, topic: str):
		with open(consts.EVENT_LOG_FILE, "r", encoding="utf-8") as log_f:
			contents = log_f.read()
		self.assertIn(topic, contents)
	
	def _assert_broker_replied_pub_ack(self, event_id: str, response: str):
		self.assertIn(prot.format_pub_ack(event_id), response)

	def _get_test_pub_payload_copy(self) -> dict:
		return {
			"event_id": id.new_event_id(), 
			"topic": "sightings", 
			"payload": {"text": "chants", "severity": 2}, 
			"created_at": t.time()
		}

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
		pub_payload: dict = self._get_test_pub_payload_copy()
		pub_line = prot.format_pub(topic, pub_payload) + "\n"
		self.pub_sock.sendall(pub_line.encode("utf-8"))

		# assert 1: broker replied with pub_ack
		response = self.pub_file.readline().strip()
		self._assert_broker_replied_pub_ack(pub_payload["event_id"], response)

		# assert 2: verify event_log.json was written
		self._assert_log_written(topic)

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

		pub_payload = self._get_test_pub_payload_copy()
		pub_payload["topic"] = topic
		pub_line = prot.format_pub(topic, pub_payload) + "\n"
		self.pub_sock.sendall(pub_line.encode("utf-8"))

		# assert 1: broker replied with pub_ack
		pub_response = self.pub_file.readline().strip()
		self._assert_broker_replied_pub_ack(pub_payload["event_id"], pub_response)

		# assert 2: live subscriber received the formatted event
		with client_sub_sock.makefile("r", encoding="utf-8") as sub_f:
			received_event = sub_f.readline().strip()
		self.assertIn(pub_payload["event_id"], received_event)
		self.assertIn("chants", received_event)

		# assert 3: dead subscriber was removed from both subscriptions and clients
		with broker.sub_lock:
			remaining_subs = [s_id for _, s_id in broker.subscriptions.get(topic, [])]
			self.assertEqual(remaining_subs, ["sub_live"])
			self.assertNotIn(dead_broker_sock, broker.clients)
			self.assertIn(broker_sub_sock, broker.clients)

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
		pub_payload = self._get_test_pub_payload_copy()
		pub_line = prot.format_pub(topic, pub_payload) + "\n"
		self.pub_sock.sendall(pub_line.encode("utf-8"))

		# assert 1: publisher receives a 503 error message
		pub_response = self.pub_file.readline().strip()
		self.assertIn("503", pub_response)
		self.assertIn("Disk full", pub_response)

		# assert 2: subscriber never received the unsaved event
		with self.assertRaises(TimeoutError):
			client_sub_sock.recv(1024)

	@patch("broker.storage.append_event")
	@patch("broker.storage.load_events")
	@patch("broker.storage.load_offsets")
	def test_first_time_subscriber(self, mock_offsets, mock_events, _mock_append):
		"""Test Case #1: First time subscriber"""
		ev_pld = self._get_test_pub_payload_copy()
		mock_offsets.return_value = {}
		mock_events.return_value = {"sightings": [ev_pld]}
		sub_sock, sub_file = self._connect_client()

		# 1. Subscribe -> expects SUB_ACK + replayed ev_pld["event_id"]
		sub_sock.sendall(b"SUB|sightings|sub_new\n")
		self.assertEqual(sub_file.readline().strip(), prot.format_sub_ack("sightings"))
		self.assertIn(ev_pld["event_id"], sub_file.readline())

		# 2. Publish live -> expects live_2 delivered
		payload = self._get_test_pub_payload_copy()
		self.pub_sock.sendall((prot.format_pub("sightings", payload) + "\n").encode("utf-8"))
		self.pub_file.readline() 
		self.assertIn(payload["event_id"], sub_file.readline())

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

if __name__ == "__main__":
	ut.main()