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

	def _read_file(self, path=None):
		"""Helper to quickly read and load JSON from the target file."""
		target = path or self.file_path
		with open(target, "r", encoding="utf-8") as f:
			return json.load(f)

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

	def test_aw_overwrite(self):
		atomic_write(self.file_path, [1, 2, 3]) # initial data
		atomic_write(self.file_path, [4, 5, 6]) # overwrite

		with open(self.file_path, 'r', encoding='utf-8') as f:
			self.assertEqual(json.load(f), [4, 5, 6])

		# check: a failed write must delete the .tmp file
		tmp_files = [f for f in os.listdir(self.test_dir.name) if f.endswith(".tmp")]
		self.assertEqual(len(tmp_files), 0)
