# Ports, size caps, timeouts and file paths. No logic lives here
import os

PORT = 5000
MAX_LINE_BYTES = 1024 * 1024
SUB_ACK_TIMEOUT_S = 2
BACKOFF_STEPS_S = (1, 2, 4, 5)
DEDUPE_WINDOW = 1000
DRAIN_S = 2
SEV_MIN = 1
SEV_MAX = 5
ALERT_MIN_SEV = 4

_BASE_DIR = os.path.dirname(os.path.abspath(__file__))
EVENT_LOG_FILE = os.path.join(_BASE_DIR, "data", "event_log.json")
OFFSETS_FILE = os.path.join(_BASE_DIR, "data", "event_log.json")