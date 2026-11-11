# The broker. One listener on port 5000 or 5001 if ur in mac 
# one thread per connection.
# The order that matters: save the event first, then send it out. And never
# hold a lock while sending — copy what you need, let go, then send.
import socket as sck
import threading as thr

import common.constants as consts
import common.log as logger

BROKER_HOST = "localhost" # change to "0.0.0.0" if receiving external network instead


def handle_client(client: sck.socket, addr):
	logger.info("broker", f"new connection from {addr}")
		
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