# Saving and loading the two json files under data/.
# Rule: write to a .tmp file, fsync it, then rename it over the real one.
# Never open the real file and start typing into it — if we crash halfway,
# the log still has to be readable when we come back.
import os 
import json
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
		raise e
