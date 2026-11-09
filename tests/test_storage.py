import json
import os
import unittest
from unittest.mock import patch
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

	# aw = atomic_write
	def test_aw_success(self):
		data = [{"event_id": "1", "topic": "sighting"}]

		atomic_write(self.file_path, data)

		self.assertTrue(os.path.exists(self.file_path))
		with open(self.file_path, "r", encoding="utf-8") as f:
			loaded_data = json.load(f)

		self.assertEqual(loaded_data, data)

	def test_aw_unix_directory_sync(self):
		# patch os from windows to unix
		with patch("os.name", "posix"):
			with patch("os.open") as mock_os_open, \
				patch("os.fsync") as mock_fsync, \
				patch("os.close") as mock_os_close:

				mock_os_open.return_value = 99 # replacement after patching os.open() 

				atomic_write(self.file_path, {"test": "data"})

				# verify that the directory was opened and fsynced on Unix
				mock_os_open.assert_called()
				mock_fsync.assert_called()
				mock_os_close.assert_called_with(99)