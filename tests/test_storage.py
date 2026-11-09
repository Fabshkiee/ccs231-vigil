import json
import os
import unittest
from tempfile import TemporaryDirectory

from src.common.storage import atomic_write

class TestAtomicWrite(unittest.TestCase):

	def setUp(self):
		# each test gets its own temp dir
		self.test_dir = TemporaryDirectory()
		self.file_path = os.path.join(self.test_dir.name, "test_event_log.json")

	def tearDown(self):
		# clean up after each test
		self.test_dir.cleanup()

	def test_atomic_write_success(self):
		data = [{"event_id": "1", "topic": "sighting"}]

		atomic_write(self.file_path, data)

		self.assertTrue(os.path.exists(self.file_path))
		with open(self.file_path, "r", encoding="utf-8") as f:
			loaded_data = json.load(f)

		self.assertEqual(loaded_data, data)