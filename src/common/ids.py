# Two small helpers everyone needs: making an event id (uuid4) and cleaning up
# a sub_id (strip, lowercase, squash the spaces).
# Keep the cleanup in this one function. If two places normalise differently,
# the broker ends up seeing two subscribers where there is only one person.
