# The broker. One listener on port 5000 or 5001 if ur in mac 
# one thread per connection.
# The order that matters: save the event first, then send it out. And never
# hold a lock while sending — copy what you need, let go, then send.
import socket as sck
import threading as thr
import time as t
from collections.abc import Iterable

import common.constants as consts
import common.log as logger
import common.protocol as prot
import common.storage as storage
import common.ids as id


BROKER_HOST = "localhost" # change to "0.0.0.0" if receiving external network instead
sub_lock = thr.RLock()
send_lock = thr.RLock()
subscriptions: dict[str, list[tuple]] = {} # topic -> [(client, sub_id)]
clients: dict[sck.socket, str] = {} # client -> sub_id


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

	# close dead sockets (outside the lock so I/O it doesn't block other threads)
	for dead_sub in dead_set:
		try:
			dead_sub.close()
		except OSError:
			pass
	
	logger.info("broker", f"dropped {len(dead_set)} dead sockets")

def _encode_msg(msg: str) -> bytes:
	return msg.encode("utf-8")

def _handle_pub(client: sck.socket, topic: str, payload: dict) -> None:
	try:
		storage.append_event(payload)
	except Exception as e:
		err_msg: bytes = _encode_msg(prot.format_err(503, str(e)) + "\n")
		client.sendall(err_msg)
		return

	# save successful: copy current subscription list before operation
	with sub_lock:
		subscriptions_copy = subscriptions.get(topic, []).copy()

	# track empty sockets
	dead_subscribers: list[sck.socket] = []
	event_msg: str = prot.format_event(payload) + "\n"
	for subscriber, sub_id in subscriptions_copy:
		try:
			subscriber.sendall(_encode_msg(event_msg))
			logger.info("broker", f"sent {payload["topic"]} | {payload["event_id"]}")
		except Exception:
			logger.warn("broker", f"{sub_id} is unresponsive")
			logger.info("broker", f"dropped {sub_id} from {topic}")
			dead_subscribers.append(subscriber)

	if dead_subscribers:
		_drop_dead_sockets(dead_subscribers)

	# reply pub_ack with event_id
	pub_ack_msg: bytes = _encode_msg(prot.format_pub_ack(payload["event_id"]) + "\n")
	client.sendall(pub_ack_msg)

def _handle_sub(client: sck.socket, topic: str, sub_id: str) -> None:
	offsets = storage.load_offsets()
	subscriber_offset = offsets.get(sub_id, {})
	last_acked_idx = subscriber_offset.get(topic, -1)
	next_idx = last_acked_idx + 1

	# acknowledge subscriber
	sub_ack_msg = _encode_msg(prot.format_pub_ack(topic) + "\n")
	client.sendall(sub_ack_msg)

	# replay events
	while True:
		topic_events = storage.load_events().get(topic, [])
		missed_events = topic_events[next_idx:]

		for event in missed_events:
			event_msg = _encode_msg(prot.format_event(event) + "\n")
			client.sendall(event_msg)
			next_idx += 1

		# briefly acquire lock to make sure no clients arrived
		with sub_lock:
			latest_len = len(storage.load_events().get(topic, []))
			if next_idx >= latest_len:
				clients[client] = sub_id
				# add subscriber to subscriptions[topic] via in-place mutation (no rebuilding)
				topic_subs = subscriptions.setdefault(topic, [])
				if not any(c is client for c, _ in topic_subs):
					topic_subs.append((client, sub_id))
				break
		
def _handle_ack(client: sck.socket, topic: str, sub_id: str, event_id: str):
	raise NotImplementedError("ACK handler not yet implemented")

def _handle_get_stats(client: sck.socket, topic: str):
	raise NotImplementedError("GET_STATS handler not yet implemented")

def _handle_no_command(cmd: str):
	raise NotImplementedError("default handler not yet implemented")

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
					case "GET_STATS": _handle_get_stats(client, fields["topic"])
					case _: _handle_no_command(command)
	except (ConnectionError, OSError) as e:
		logger.warn("broker", str(e))
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