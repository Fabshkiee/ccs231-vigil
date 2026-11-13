# The broker. One listener on port 5000 or 5001 if ur in mac 
# one thread per connection.
# The order that matters: save the event first, then send it out. And never
# hold a lock while sending — copy what you need, let go, then send.
import socket as sck
import threading as thr

import common.constants as consts
import common.log as logger
import common.protocol as prot

BROKER_HOST = "localhost" # change to "0.0.0.0" if receiving external network instead
SUB_LOCK = thr.RLock()
SEND_LOCK = thr.RLock()
SUBSCRIPTIONS: dict[str, list[tuple]] = {}
CLIENTS: dict = {}

def _handle_pub(client: sck.socket, topic: str, payload: dict):
	raise NotImplementedError("PUB handler not yet implemented")

def _handle_sub(client: sck.socket, topic: str, sub_id: str):
	raise NotImplementedError("SUB handler not yet implemented")

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
					logger.warn(f"broker", "error parsing line from {addr}: {result.reason}", line)
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

	except Exception as e:
		logger.warn("broker", str(e))
	finally:
		with SUB_LOCK:
			for topic_key in list(SUBSCRIPTIONS.keys()):
				# rebuild the list of subcribers of this particular topic without the disconnected client
				SUBSCRIPTIONS[topic_key] = [ (c, s_id) for c, s_id in SUBSCRIPTIONS[topic_key] if c != client]
			CLIENTS.pop(client, None)
		client.close()
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
				thr.Thread(target=handle_client, args=(client, addr), daemon=True).start()
				logger.info("broker", f"Connected clients: {thr.active_count() - 1}")
		except KeyboardInterrupt:
			logger.info("broker", "Shutting down..")
	logger.info("broker", "Socket closed.")


if __name__ == "__main__":
	start_broker()