# The broker. One listener on port 5000 or 5001 if ur in mac 
# one thread per connection.
# The order that matters: save the event first, then send it out. And never
# hold a lock while sending — copy what you need, let go, then send.
import socket as sck
import threading as thr
from collections.abc import Iterable

import common.constants as consts
import common.log as logger
import common.protocol as prot
import common.storage as storage


BROKER_HOST = "localhost" # change to "0.0.0.0" if receiving external network instead
sub_lock = thr.Lock()
client_send_locks: dict[sck.socket, thr.Lock] = {}
subscriptions: dict[str, list[tuple]] = {} # topic -> [(client, sub_id)]
clients: dict[sck.socket, str] = {} # client -> sub_id

def _encode_msg(msg: str) -> bytes:
	return msg.encode("utf-8")

def _get_send_lock(client: sck.socket) -> thr.Lock:
	"""Get a client specific lock. Give a lock to a client if they don't have one"""
	with sub_lock:
		lock = client_send_locks.get(client)
		if lock is None:
			lock = thr.Lock()
			client_send_locks[client] = lock
		return lock

def _send_line(client: sck.socket, wire_msg: str) -> None:
	"""Use this when sending messages sent over network"""
	data = _encode_msg(wire_msg + "\n")
	with _get_send_lock(client):
		client.sendall(data)

def _send_err(client: sck.socket, err_code: int, reason: str) -> None:
	"""Helper function that encodes and sends error message to client"""
	clean_reason = reason.replace("|", "/").replace("\n", " ").replace("\r", "")
	try:
		_send_line(client, prot.format_err(err_code, clean_reason))
	except (ConnectionError, OSError):
		pass # there is nobody on the other end

def _drop_dead_sockets(dead_sockets: Iterable[sck.socket]) -> None:
	dead_set = set(dead_sockets)
	if not dead_set:
		return

	with sub_lock:
		# remove dead sockets from all subscribed topics
		for topic_name in list(subscriptions.keys()):
			subscriptions[topic_name] = [
				(sub, s_id) for sub, s_id in subscriptions[topic_name] if sub not in dead_set
			]

		# remove dead sockets in clients as well
		for dead_sub in dead_set:
			clients.pop(dead_sub, None)
			client_send_locks.pop(dead_sub, None) # remove unused locks

	# close dead sockets (outside the lock so I/O it doesn't block other threads)
	for dead_sub in dead_set:
		try:
			dead_sub.close()
		except OSError:
			pass
	
	logger.info("broker", f"dropped {len(dead_set)} dead sockets")

def _handle_pub(client: sck.socket, topic: str, payload: dict) -> None:
	# validate the payload has the correct keys
	for key in ("event_id", "topic", "payload", "created_at"):
		if key not in payload:
			_send_err(client, 400, f"missing_{key}")
			return
	
	# payload[topic] must equal topic
	if payload["topic"] != topic:
		_send_err(client, 400, "topic mismatch")
		return

	# try to save the event in log file
	try:
		storage.append_event(payload)
	except (storage.StoreError, OSError) as e:
		_send_err(client, 503, str(e))
		return

	# save successful: copy current subscription list before operation
	with sub_lock:
		subscriptions_copy = subscriptions.get(topic, []).copy()

	# track empty sockets
	dead_subscribers: list[sck.socket] = []
	event_msg: str = prot.format_event(payload)
	for subscriber, sub_id in subscriptions_copy:
		try:
			_send_line(subscriber, event_msg)
			logger.info("broker", f"sent {payload["topic"]} | {payload["event_id"]}")
		except Exception:
			logger.warn("broker", f"{sub_id} is unresponsive")
			logger.info("broker", f"dropped {sub_id} from {topic}")
			dead_subscribers.append(subscriber)

	if dead_subscribers:
		_drop_dead_sockets(dead_subscribers)

	# reply pub_ack with event_id
	_send_line(client, prot.format_pub_ack(payload["event_id"]))

def _handle_sub(client: sck.socket, topic: str, sub_id: str) -> None:
	try:
		offsets = storage.load_offsets()
		topic_events = storage.load_events().get(topic, [])
	except (storage.StoreError, OSError) as e:
		_send_err(client, 503, str(e))
		return

	# proceed if offsets loaded
	subscriber_offset = offsets.get(sub_id, {})
	last_acked_idx = subscriber_offset.get(topic, -1)
	next_idx = last_acked_idx + 1

	# acknowledge subscriber
	_send_line(client, prot.format_sub_ack(topic))

	# replay events
	while True:
		missed_events = topic_events[next_idx:]
		for event in missed_events:
			_send_line(client, prot.format_event(event))
			next_idx += 1

		# briefly acquire lock to make sure no clients arrived
		with sub_lock:
			try:
				topic_events = storage.load_events().get(topic, [])
			except (storage.StoreError, OSError) as e:
				_send_err(client, 503, str(e))
				return
				
			# get the latest total count of events[topic] 
			if next_idx >= len(topic_events):
				clients[client] = sub_id
				# add subscriber to subscriptions[topic] via in-place mutation (no rebuilding)
				topic_subs = subscriptions.setdefault(topic, [])
				if not any(c is client for c, _ in topic_subs):
					topic_subs.append((client, sub_id))
				break
		
def _handle_ack(client: sck.socket, topic: str, sub_id: str, event_id: str) -> None:
	try:
		offsets = storage.load_offsets()
		events = storage.load_events()
	except (storage.StoreError, OSError) as e:
		_send_err(client, 503, str(e))
		return

	topic_events = events.get(topic, [])
	matched_idx = None
	for idx, event in enumerate(topic_events):
		if event["event_id"] == event_id:
			matched_idx = idx
			break

	# event doesn't exist
	if matched_idx is None:
		err_msg = "unknown event"
		logger.warn("broker", err_msg)
		_send_err(client, 422, err_msg)
		return

	client_curr_offset = offsets.get(sub_id, {}).get(topic, -1)
	try:
		# ignore matched_idx < client_curr_offset
		# update outdated offset
		if matched_idx > client_curr_offset:
			storage.save_offset(sub_id, topic, matched_idx)
			logger.info("broker", f"ack {sub_id} on {topic} -> offset {matched_idx}")
	except (storage.StoreError, OSError) as e:
		_send_err(client, 503, str(e))
		return	

# def _handle_get_stats(client: sck.socket, topic: str) -> None:
# 	raise NotImplementedError("GET_STATS handler not yet implemented")

def _handle_no_command(client: sck.socket, cmd: str) -> None:
	logger.info("broker", f"ignored unknown command from {client.getpeername()}: {cmd}")

def handle_client(client: sck.socket, addr):
	logger.info("broker", f"new connection from {addr}")
	
	try:
		with client, client.makefile("r", encoding="utf-8") as client_file:
			while True:
				line = client_file.readline(consts.MAX_LINE_BYTES)
				if not line:
					logger.info("broker", f"connection closed by {addr}")
					break
				
				result = prot.parse_line(line)
				if isinstance(result, prot.Err):
					logger.warn("broker", f"error parsing line from {addr}: {result.reason}", line)
					_send_err(client, result.code, f"error parsing line: {result.reason}")
					if result.close:
						break
					continue

				# proceed if result is Ok
				command: str = result.cmd
				fields: dict = result.fields
				match command:
					case "PUB": _handle_pub(client, fields["topic"], fields["payload"])
					case "SUB": _handle_sub(client, fields["topic"], fields["sub_id"])
					case "ACK": _handle_ack(client, fields["topic"], fields["sub_id"], fields["event_id"])
					# case "GET_STATS": _handle_get_stats(client, fields["topic"])
					case _: _handle_no_command(client, command)
	except (ConnectionError, OSError) as e:
		err_msg = str(e)
		logger.warn("broker", err_msg)
	finally:
		_drop_dead_sockets([client])
		logger.info("broker", f"{addr} disconnected")
					
def start_broker():
	with sck.socket(sck.AF_INET, sck.SOCK_STREAM) as server:
		server.setsockopt(sck.SOL_SOCKET, sck.SO_REUSEADDR, 1) 
		server.bind((BROKER_HOST, consts.PORT))
		server.listen()
		server.settimeout(1.0) # wake up socket to allow keyboard interrupt
		logger.info("broker", f"Listening on {BROKER_HOST}:{consts.PORT}")

		try:
			while True:
				try:
					client, addr = server.accept()
				except TimeoutError:
					continue

				# offload current client to accept next client
				thr.Thread(
					target=handle_client, 
					args=(client, addr),
					name=f"client-{addr[1]}", 
					daemon=True
				).start()
				active_clients = sum(1 for t in thr.enumerate() if t.name.startswith("client-"))
				logger.info("broker", f"Connected clients: {active_clients}")
		except KeyboardInterrupt:
			logger.info("broker", "Shutting down..")
	logger.info("broker", "Socket closed.")


if __name__ == "__main__":
	start_broker()