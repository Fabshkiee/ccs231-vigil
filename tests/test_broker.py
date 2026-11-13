import socket as sck
import threading as thr
import time as t
import unittest as ut
from unittest.mock import patch
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

	def test_publisher_can_connect(self):		
		ip, port = self.pub_sock.getpeername()
		self.assertEqual(port, consts.PORT, f"{[port]} is not {consts.PORT}")

		try:
			pub_sock.connect(ADDRESS)
			ip, port = pub_sock.getpeername()
			self.assertEqual(port, consts.PORT, f"{[port]} is not {consts.PORT}")
		finally:
			pub_sock.close()

