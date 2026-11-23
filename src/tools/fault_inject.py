# Test harness for the failure cases. Starts its own broker on a
# spare port, breaks exactly one thing, checks what actually happened, and
# writes the row in the report.
# A case that still needs you to kill a window by hand isn't a case yet.




import json

from pathlib import Path
import argparse
import tempfile
import time
import os
import socket
import sys
import multiprocessing
import signal



def run_test_broker(src_dir, test_port, event_log, offsets_file):
    sys.path.insert(0, str(src_dir))

    import common.constants as constants



    constants.PORT = test_port

    constants.EVENT_LOG_FILE = str(event_log)
    constants.OFFSETS_FILE = str(offsets_file)

    import broker
    broker.BROKER_HOST = "127.0.0.1"
    broker.start_broker()

#starts test broker
def wait_for_broker(process, test_port):
    deadline = time.monotonic() + 5

    while time.monotonic() < deadline:
        if not process.is_alive():
            raise RuntimeError("Test broker exited before it was ready")

        try:
            with socket.create_connection(
                ("127.0.0.1", test_port), timeout=0.2
            ):
                return
        except OSError:
            time.sleep(0.05)

    raise TimeoutError("Test broker did not start within 5 seconds")


def read_reply(connection):
    deadline = time.monotonic() + 5
    reply = bytearray()

    while not reply.endswith(b"\n"):
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError("Broker reply took too long")

        connection.settimeout(remaining)
        chunk = connection.recv(1)

        if not chunk:
            raise ConnectionError("Broker closed before replying")

        reply.extend(chunk)

        if len(reply) > 1024 * 1024:
            raise ValueError("Broker reply is too large")

    return reply.decode("utf-8").rstrip("\r\n")



def main():
    #This reads the selected fault case and displays it
    parser = argparse.ArgumentParser(description="VIGIL QA Tests")
    parser.add_argument("--case", required=True, choices=["F-01"])


   

   
    args = parser.parse_args()
    print("Selected case:", args.case)

    src_dir = Path(__file__).resolve().parents[1]
    print("Source folder:", src_dir)



    #Create temp folder for fault testss
   
   
    with tempfile.TemporaryDirectory(prefix="vigil-f01-") as test_dir:
        print("Test folder:", test_dir)

        # Ask the operating system for an available local porrrt
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as test_socket:
            test_socket.bind(("127.0.0.1", 0))
            test_port = test_socket.getsockname()[1]


        print("Test port:", test_port)



        # Set up the files used by the test broker.
        data_dir = Path(test_dir) / "data"
        data_dir.mkdir()

        event_log = data_dir / "event_log.json"
        offsets_file = data_dir / "offsets.json"

        print("Event log:", event_log)
        print("Offsets:", offsets_file)

        context = multiprocessing.get_context("spawn")
        broker_process = context.Process(
            target=run_test_broker,
            args=(src_dir, test_port, event_log, offsets_file),
        )

        broker_process.start()

        try:



            wait_for_broker(broker_process, test_port)
            print("Test broker is ready")
            
            
            payload = {
                "kind": "F-01 baseline",
                "sev": 2,
                "source": "fault_inject",
            }

            with socket.create_connection(
                ("127.0.0.1", test_port), timeout=5
            ) as publisher:
                message = f"PUB|sighting|{json.dumps(payload)}\n"
                publisher.sendall(message.encode("utf-8"))
                reply = read_reply(publisher)

            command, separator, event_id = reply.partition("|")

            if command != "PUB_ACK" or not separator or not event_id:
                raise RuntimeError(f"Expected PUB_ACK, received {reply}")

            print("Baseline acknowledged:", event_id)

            baseline_bytes = event_log.read_bytes()
            saved_log = json.loads(baseline_bytes)
            saved_events = saved_log.get("sighting", [])

            if len(saved_events) != 1:
                raise RuntimeError("Expected one baseline event")

            baseline_event = saved_events[0]



            if (
                baseline_event.get("event_id") != event_id
                or baseline_event.get("topic") != "sighting"
                or baseline_event.get("payload") != payload
            ):
                raise RuntimeError("Saved event does not match the publish")



            print("Baseline event verified on disk")
   
   
        finally:
            if broker_process.is_alive():
                os.kill(broker_process.pid, signal.SIGINT)
            
                broker_process.join(timeout=3)


            if broker_process.is_alive():
                broker_process.kill()
                broker_process.join(timeout=3)

            print("Test broker stopped")


if __name__ == "__main__":
    main()