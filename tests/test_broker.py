import socket as sck
import threading as thr
import time as t
import unittest as ut
import os, sys

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
		cls.pub_file.close()
		cls.pub_sock.close()

	def tearDown(self) -> None:
		t.sleep(0.65)

	def setUp(self):
		with broker.sub_lock:
			broker.subscriptions.clear()
			broker.clients.clear()

	def _assert_verify_log_was_written(self, topic: str):
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
		}.copy()

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
		self._assert_verify_log_was_written(topic)


if __name__ == "__main__":
	ut.main()