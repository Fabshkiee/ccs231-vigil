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



#starts test broker
def run_test_broker(src_dir, test_port, event_log, offsets_file, armed, checkpoint):
    sys.path.insert(0, str(src_dir))

    import common.constants as constants



    constants.PORT = test_port

    constants.EVENT_LOG_FILE = str(event_log)
    constants.OFFSETS_FILE = str(offsets_file)

    import common.storage as storage

    original_fsync = storage.os.fsync
    temporary_log = Path(str(event_log) + ".tmp")

    def pause_after_fsync(file_descriptor):
    
    
    
        #This pauses the broker after syncing the temporary event log
        original_fsync(file_descriptor)

        if not armed.is_set():
            return

        try:
            temporary_file = temporary_log.stat()
        except FileNotFoundError:
            return

        opened_file = os.fstat(file_descriptor)

        if (
 
 
 
            opened_file.st_dev != temporary_file.st_dev
            or opened_file.st_ino != temporary_file.st_ino
        ):
            return

        checkpoint.set()

        #The parent must kill this broker before this timeout expires
        time.sleep(15)
        os._exit(90)

    storage.os.fsync = pause_after_fsync

    import broker
    broker.BROKER_HOST = "127.0.0.1"
    broker.start_broker()


#This waits up to five seconds for the test broker to accept connections

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


#Reads one reply from broker
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
        #These signals control when the broker pauses for the crash test
        armed = context.Event()
        checkpoint = context.Event()
        broker_process = context.Process(

            target=run_test_broker,
            args=(
                src_dir,
                test_port,
                event_log,
                offsets_file,
                armed,
                checkpoint,
            ),
        
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

        #This enables the pause during the next event save
            armed.set()

            interrupted_payload = {
                "kind": "F-01 interrupted event",
                "sev": 2,
                "source": "fault_inject",
            }

            with socket.create_connection(
                ("127.0.0.1", test_port), timeout=5
            ) as publisher:
             
                message = (
               
               
               
                    f"PUB|sighting|{json.dumps(interrupted_payload)}\n"
                )
                publisher.sendall(message.encode("utf-8"))

                #This waits until the temporary event log has been synced
                if not checkpoint.wait(timeout=5):
                    raise TimeoutError("Broker did not reach the crash checkpoint")

                print("Crash checkpoint reached")

                #This kills the test broker while the save is paused
               
               
                broker_process.kill()
                broker_process.join(timeout=5)

                if broker_process.exitcode != -signal.SIGKILL:
                    raise RuntimeError("Test broker did not exit through SIGKILL")

                print("Test broker killed with SIGKILL")





            #This checks that the committed log survived the interrupted save
            after_crash_bytes = event_log.read_bytes()
            json.loads(after_crash_bytes)

            if after_crash_bytes != baseline_bytes:
                raise RuntimeError("Committed log changed before replacement")

            print("Committed log is valid JSON")
            print("Acknowledged baseline event preserved")

                        #This disables the crash checkpoint before restarting the broker
            armed.clear()
            checkpoint.clear()




            #This restarts sssthe broker using the same saved data
            broker_process = context.Process(
                target=run_test_broker,
                args=(
                    src_dir,
                    test_port,
                    event_log,
                    offsets_file,
                    armed,
                    checkpoint,
                ),
            )
            broker_process.start()
            wait_for_broker(broker_process, test_port)

            print("Test broker restarted")

            # subscribe test client to receive the saved baseline event
            with socket.create_connection(
                ("127.0.0.1", test_port), timeout=5
            ) as subscriber:
                subscriber.sendall(b"SUB|sighting|f01-check\n")

                subscription_reply = read_reply(subscriber)

                if subscription_reply != "SUB_ACK|sighting":
                    raise RuntimeError(
                        f"Unexpected subscription reply: {subscription_reply}"
                    )

                replay_reply = read_reply(subscriber)
                command, separator, event_json = replay_reply.partition("|")

                if command != "EVENT" or not separator:
                    raise RuntimeError(
                        f"Expected replay EVENT, received {replay_reply}"
                    )

                replayed_event = json.loads(event_json)

                #This compares the replayed event the original saved eventtt
                if replayed_event != baseline_event:
                    raise RuntimeError("Replayed event differs from the baseline")

                print("Replayed event:", replayed_event["event_id"])
                print("Baseline replay verified after broker restart")
   
   
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