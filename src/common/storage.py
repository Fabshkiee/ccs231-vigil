# Saving and loading the two json files under data/.
# Rule: write to a .tmp file, fsync it, then rename it over the real one.
# Never open the real file and start typing into it — if we crash halfway,
# the log still has to be readable when we come back.
import threading as thr
import os 
import json
from common.constants import EVENT_LOG_FILE, OFFSETS_FILE
from typing import Callable, TypeVar

log_lock = thr.Lock()


class StoreError(Exception):
	"""Raised when file is corrupted"""
	pass


def atomic_write(filepath: str, data: dict | list):
	# Create the parent directories of filepath if it doesn't exist
	dir_name = os.path.dirname(os.path.abspath(filepath))
	if dir_name:
		os.makedirs(dir_name, exist_ok=True)

	tmp_filepath = f"{filepath}.tmp"

	try:
		with open(tmp_filepath, 'w', encoding='utf-8') as f:
			f.write(json.dumps(data))
			f.flush()

			os.fsync(f.fileno())

		os.replace(tmp_filepath, filepath)

		# For non-windows
		if os.name != 'nt' and dir_name:
			try:
				dir_fd =  os.open(dir_name, os.O_RDONLY)	
				try:	
					os.fsync(dir_fd)
				finally:
					os.close(dir_fd)
			except OSError:
				pass
			
	except OSError as e:
		if os.path.exists(tmp_filepath):
			os.remove(tmp_filepath)
		raise e # the broker handles this and replies with 503

T = TypeVar('T', list, dict) # this specifies what default_factory expects
def _load_json(filepath: str, default_factory: Callable[[], T]) -> T:
	"""
	Internal helper to load JSON and handle missing/corrupted files.

	Args:
		filepath - str
		default_factory - pass either list or dict type
	
	Raises:
		StoreError - bad json (corrupted)
	"""
	if not os.path.exists(filepath):
		return default_factory()

	try:
		with open(filepath, "r", encoding="utf-8") as f:
			return json.load(f)
	# check if the file is corrupted
	except json.JSONDecodeError as e:
		raise StoreError(f"Corrupted log file at {filepath}: {e}")

def load_events(filepath: str=EVENT_LOG_FILE) -> dict:
	return _load_json(filepath, dict)

def load_offsets(filepath: str=OFFSETS_FILE) -> dict:
	try:
		return _load_json(filepath, dict)
	except StoreError:
		return {}

def append_event(event: dict, filepath: str=EVENT_LOG_FILE):
	with log_lock:
		events = load_events(filepath)
		events.setdefault(event["topic"], []).append(event)
		atomic_write(filepath, events)

def save_offset(subscriber_id: str, topic: str, index: int, filepath: str=OFFSETS_FILE):
	with log_lock:
		offsets = load_offsets(filepath)
		offsets.setdefault(subscriber_id, {})[topic] = index  
		atomic_write(filepath, offsets)

