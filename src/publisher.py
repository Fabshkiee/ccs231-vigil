# The thing we type into during the demo. Reads one event, sends it, prints
# "saved". If severity is 4 or 5 it also sends the same event to alert
# that decision belongs here, not in the broker, because the broker is only
# allowed to route by topic.
