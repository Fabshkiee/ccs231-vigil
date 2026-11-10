# Saving and loading the two json files under data/.
# Rule: write to a .tmp file, fsync it, then rename it over the real one.
# Never open the real file and start typing into it — if we crash halfway,
# the log still has to be readable when we come back.
import threading as thr
import os 
import json

LOG_LOCK = thr.RLock()
EVENT_LOG_PATH = "data/event_log.json"
OFFSETS_PATH = "data/offsets.json" 

class StoreError(Exception):
	"""Raised when event_log.json is corrupted"""
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

def load_events(filepath: str=EVENT_LOG_PATH) -> list:
	with LOG_LOCK:
		if not os.path.exists(filepath):
			return []

		try:
			with open(filepath, "r", encoding="utf-8") as f:
				data = json.load(f)
				return data if isinstance(data, list) else []
		# check if event_log.json is corrupted
		except json.JSONDecodeError as e:
			raise StoreError(f"Corrupted log file at {filepath}: {e}")

def append_event(event: dict, filepath: str=EVENT_LOG_PATH):
	with LOG_LOCK:
		events = load_events(filepath)
		events.append(event)
		atomic_write(filepath, events)

def load_offsets(filepath: str=OFFSETS_PATH) -> dict:
	with LOG_LOCK:
		if not os.path.exists(filepath):
			return {}

		try:
			with open(filepath, "r", encoding="utf-8") as f:
				data = json.load(f)
				return data if isinstance(data, dict) else {}
		# check if offsets.json is corrupted
		except json.JSONDecodeError as e:
			raise StoreError(f"Corrupted offsets file at {filepath}: {e}")

