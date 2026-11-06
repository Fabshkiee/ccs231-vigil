# Two small helpers everyone needs: making an event id (uuid4) and cleaning up
# a sub_id (strip, lowercase, squash the spaces).
# Keep the cleanup in this one function. If two places normalise differently,
# the broker ends up seeing two subscribers where there is only one person.
import re
import uuid

def normalise_id(raw):
    """strip, lowercase, collapse runs of whitespace"""
    return re.sub(r"\s+", " ", raw.strip().lower())

def new_event_id():
    """return a new event id as a hex string"""
    return uuid.uuid4().hex