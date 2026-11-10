import json
import os
import unittest
from unittest.mock import patch
from tempfile import TemporaryDirectory

from src.common.storage import atomic_write, load_events, append_event, StoreError

class BaseStorageTest(unittest.TestCase):
	def setUp(self):
		# each test gets its own temp dir
		self.test_dir = TemporaryDirectory()
		self.file_path = os.path.join(self.test_dir.name, "test_file.json")

	def tearDown(self):
		self.test_dir.cleanup() # clean up after each test

	def _read_file(self, path=None):
		"""Helper to quickly read and load JSON from the target file."""
		target = path or self.file_path
		with open(target, "r", encoding="utf-8") as f:
			return json.load(f)

	def _write_file(self, content: str):
		"""Helper to quickly write content to the target file."""
		with open(self.file_path, "w", encoding="utf-8") as f:
			f.write(content)

class TestAtomicWrite(BaseStorageTest):
	def _assert_no_tmp_files(self):
		"""Helper to test if no temporary .tmp files were left behind."""
		tmp_files = [f for f in os.listdir(self.test_dir.name) if f.endswith(".tmp")]
		self.assertEqual(len(tmp_files), 0, f"Lingering .tmp files found: {tmp_files}")

	# aw = atomic_write
	def test_aw_success(self):
		data = [{"event_id": "1", "topic": "sighting"}]

		atomic_write(self.file_path, data)

		self.assertTrue(os.path.exists(self.file_path))
		self.assertEqual(self._read_file(), data)

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
		self.assertEqual(self._read_file(), [4, 5, 6]) # check
		self._assert_no_tmp_files() # check: a failed write must delete the .tmp file

	def test_aw_failure_cleanup(self):
		init_data = ["old_data"]
		atomic_write(self.file_path, init_data)

		# simulate disk full
		with patch("os.replace", side_effect=OSError("Disk full")):
			with self.assertRaises(OSError):
				atomic_write(self.file_path, ["new_data"])

		self.assertEqual(self._read_file(), init_data)
		self._assert_no_tmp_files()

class TestLoadEvents(BaseStorageTest):
	def test_load_missing_file(self):
		# case: program runs the first time, data/ and it's files won't exist yet 
		# so load_events returns an empty string as placeholder
		result = load_events(self.file_path)
		self.assertEqual(result, [])

	def test_load_valid_file(self):
		expected_data = [
			{
				"sighting": {
					"event_id": "4be5feca1c964528a343661b336d8c09", 
					"topic": "sighting", 
					"payload": {
						"text": "chants",
						"severity": 2
					},
					"created_at": 1730
				},
				"alert": []
			}
		]

		self._write_file(json.dumps(expected_data))
		result = load_events(self.file_path)
		self.assertEqual(result, expected_data)

	def test_load_corrupted_file(self):
		self._write_file("[{bad json content or smth]")
		with self.assertRaises(StoreError):
			load_events(self.file_path)

class TestAppendEvents(BaseStorageTest):
	# ae = append_event
	def test_ae_new_file(self):
		event = {
			"sighting": {
				"event_id": 12, 
				"topic": "sighting", 
				"payload": {},
				"created_at": 2400
			},
			"alert": []
		}

		append_event(event, self.file_path)
		self.assertEqual(self._read_file(), [event])

	def test_ae_to_existing_file(self):
		...

	def test_ae_disk_error(self):
		...
	
