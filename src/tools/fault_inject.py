# Test harness for the failure cases. Starts its own broker on a
# spare port, breaks exactly one thing, checks what actually happened, and
# writes the row in the report.
# A case that still needs you to kill a window by hand isn't a case yet.
import json

from pathlib import Path
import contextlib
import argparse
import tempfile
import time
import os
import socket
import sys
import multiprocessing
from concurrent.futures import ThreadPoolExecutor
import signal
from datetime import datetime
import threading


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



def run_f01(observed):


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

            observed["baseline_event"] = baseline_event
            observed["baseline_saved"] = True
            observed["test_port"] = test_port

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

                
                
                observed["crash_point"] = "After temporary-file fsync, before replacement"




                #This kills the test broker while the save is paused
               
               
                broker_process.kill()
                broker_process.join(timeout=5)

                if broker_process.exitcode != -signal.SIGKILL:
                    raise RuntimeError("Test broker did not exit through SIGKILL")

                print("Test broker killed with SIGKILL")

                observed["crashed_broker_exit_code"] = broker_process.exitcode





            #This checks that the committed log survived the interrupted save
            after_crash_bytes = event_log.read_bytes()
            json.loads(after_crash_bytes)

            if after_crash_bytes != baseline_bytes:
                raise RuntimeError("Committed log changed before replacement")

            print("Committed log is valid JSON")
            print("Acknowledged baseline event preserved")

            observed["committed_log_valid"] = True
            observed["committed_log_unchanged"] = True

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



                observed["replayed_event"] = replayed_event
                observed["replay_matches_baseline"] = True
   
   
        finally:
            if broker_process.is_alive():
                os.kill(broker_process.pid, signal.SIGINT)
            
                broker_process.join(timeout=3)


            if broker_process.is_alive():
                broker_process.kill()
                broker_process.join(timeout=3)
#confirmation before succesful cleanup
           
            if broker_process.is_alive():
                raise RuntimeError("Test broker did not stop during cleanup")

          
          
          
          
            observed["broker_stopped"] = True

            print("Test broker stopped")


def stop_test_process(process):
    #This stops a test process and checks that it if  finishedddd
    if process is None:
        return

    if process.is_alive():
        os.kill(process.pid, signal.SIGINT)
        process.join(timeout=3)

    if process.is_alive():
        process.kill()
        process.join(timeout=3)

    if process.is_alive():
        raise RuntimeError("Test process did not stop")


    
def read_offset(offsets_file, subscriber_id, topic):
    #This reads the subscriber's last acknowledged position for a topic
    if not offsets_file.exists():
        return -1

    offsets = json.loads(offsets_file.read_text(encoding="utf-8"))
    return offsets.get(subscriber_id, {}).get(topic, -1)

def run_f02_subscriber(src_dir, test_port, ack_checkpoint):
    #This starts a subscriber and pauses it before sending its first ACK
    sys.path.insert(0, str(src_dir))

    import common.constants as constants

    constants.PORT = test_port

    import subscriber

    original_send = subscriber.BrokerLink.send

    def pause_before_ack(connection, line):
        #This signals that the subscriber reached an ACK without sending it
        if line.startswith("ACK|"):
            ack_checkpoint.set()

            #The parent must kill this subscriber before the timeout expires
            time.sleep(15)
            os._exit(90)

        original_send(connection, line)

    subscriber.BrokerLink.send = pause_before_ack
    subscriber.main(["--as", "ResponseTeam"])


def run_f02(observed):
    #This checks that an event replays after its subscriber dies before ACK
    src_dir = Path(__file__).resolve().parents[1]
    subscriber_id = "responseteam"
    topic = "sighting"

    with tempfile.TemporaryDirectory(prefix="vigil-f02-") as test_dir:
        data_dir = Path(test_dir) / "data"
        data_dir.mkdir()

        event_log = data_dir / "event_log.json"
        offsets_file = data_dir / "offsets.json"

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as test_socket:
            test_socket.bind(("127.0.0.1", 0))
            test_port = test_socket.getsockname()[1]

        observed["test_port"] = test_port
        observed["subscriber_id"] = subscriber_id

        context = multiprocessing.get_context("spawn")
        armed = context.Event()
        broker_checkpoint = context.Event()
        ack_checkpoint = context.Event()

        broker_process = context.Process(
            target=run_test_broker,
            args=(
                src_dir,
                test_port,
                event_log,
                offsets_file,
                armed,
                broker_checkpoint,
            ),
        )
        subscriber_process = None

        try:
            #This starts the broker with its crash checkpoint disabled
            broker_process.start()
            wait_for_broker(broker_process, test_port)
            print("F-02 test broker is ready")

            payload = {
                "kind": "F-02 unacknowledged event",
                "sev": 2,
                "source": "fault_inject",
            }

            with socket.create_connection(
                ("127.0.0.1", test_port), timeout=5
            ) as publisher:
                message = f"PUB|{topic}|{json.dumps(payload)}\n"
                publisher.sendall(message.encode("utf-8"))
                reply = read_reply(publisher)

            command, separator, event_id = reply.partition("|")

            if command != "PUB_ACK" or not separator or not event_id:
                raise RuntimeError(f"Expected PUB_ACK, received {reply}")

            saved_log = json.loads(event_log.read_text(encoding="utf-8"))
            saved_events = saved_log.get(topic, [])

            if len(saved_events) != 1:
                raise RuntimeError("Expected exactly one saved event")

            saved_event = saved_events[0]

            if (
                saved_event.get("event_id") != event_id
                or saved_event.get("topic") != topic
                or saved_event.get("payload") != payload
            ):
                raise RuntimeError("Saved event does not match the publish")

            observed["saved_event"] = saved_event
            print("F-02 event saved:", event_id)

            #This records the offset before the subscriber receives the event
            before_offset = read_offset(offsets_file, subscriber_id, topic)
            observed["offset_before_delivery"] = before_offset

            if before_offset != -1:
                raise RuntimeError("Fresh subscriber already has an ACK offset")

            subscriber_process = context.Process(
                target=run_f02_subscriber,
                args=(src_dir, test_port, ack_checkpoint),
            )
            subscriber_process.start()

            #This waits until the real subscriber pauses before sending its ACK
            if not ack_checkpoint.wait(timeout=5):
                raise TimeoutError("Subscriber did not reach the ACK checkpoint")

            paused_offset = read_offset(offsets_file, subscriber_id, topic)
            observed["offset_at_ack_checkpoint"] = paused_offset

            if paused_offset != before_offset:
                raise RuntimeError("Offset advanced before the ACK was sent")

            print("Subscriber paused before ACK")

            #This kills the subscriber while its ACK is still unsent
            subscriber_process.kill()
            subscriber_process.join(timeout=5)

            if subscriber_process.exitcode != -signal.SIGKILL:
                raise RuntimeError("Subscriber did not exit through SIGKILL")

            observed["subscriber_exit_code"] = subscriber_process.exitcode
            print("Subscriber killed with SIGKILL")

            after_kill_offset = read_offset(offsets_file, subscriber_id, topic)
            observed["offset_after_subscriber_kill"] = after_kill_offset

            if after_kill_offset != before_offset:
                raise RuntimeError("Offset changed despite the missing ACK")

            #This reconnects a harness client using the same subscriber ID
            with socket.create_connection(
                ("127.0.0.1", test_port), timeout=5
            ) as subscriber:
                message = f"SUB|{topic}|{subscriber_id}\n"
                subscriber.sendall(message.encode("utf-8"))

                reply = read_reply(subscriber)

                if reply != f"SUB_ACK|{topic}":
                    raise RuntimeError(f"Unexpected subscription reply: {reply}")

                reply = read_reply(subscriber)
                command, separator, event_json = reply.partition("|")

                if command != "EVENT" or not separator:
                    raise RuntimeError(f"Expected replay EVENT, received {reply}")

                replayed_event = json.loads(event_json)
                observed["replayed_event"] = replayed_event

                if replayed_event != saved_event:
                    raise RuntimeError("Replayed event differs from the saved event")

                print("Unacknowledged event replayed:", event_id)

                #This acknowledges the replay and waits for the saved offset
                message = f"ACK|{topic}|{subscriber_id}|{event_id}\n"
                subscriber.sendall(message.encode("utf-8"))

                deadline = time.monotonic() + 5

                while read_offset(offsets_file, subscriber_id, topic) != 0:
                    if time.monotonic() >= deadline:
                        raise TimeoutError("Replay ACK did not advance the offset")
                    time.sleep(0.05)

            observed["offset_after_replay_ack"] = 0
            observed["replay_matches_saved_event"] = True
            print("Replay ACK advanced the offset to 0")

        finally:
            #This attempts cleanup of both processes even if one cleanup fails
            try:
                stop_test_process(subscriber_process)
                observed["subscriber_stopped"] = True
            finally:
                stop_test_process(broker_process)
                observed["broker_stopped"] = True

            print("F-02 test processes stopped")

def run_f03_subscriber(
    src_dir,
    test_port,
    output_file,
    deliveries_file,
    ack_dropped,
    allow_reconnect,
):
    #This runs a subscriber that loses its first ACK and then reconnects
    sys.path.insert(0, str(src_dir))

    import common.constants as constants

    constants.PORT = test_port

    import subscriber

    original_send = subscriber.BrokerLink.send
    original_handle_event = subscriber.handle_event
    first_ack_dropped = False

    def record_delivery(link, subscriber_id, seen, event):
        #recordssss each receiv evenre duplicate suppressionnnn
        with deliveries_file.open("a", encoding="utf-8") as evidence:
            evidence.write(json.dumps(event) + "\n")

        original_handle_event(link, subscriber_id, seen, event)

    def drop_first_ack(connection, line):
        #suppresses the first ACK and reconnectssss with the same eseen IDs
        nonlocal first_ack_dropped

        if line.startswith("ACK|") and not first_ack_dropped:
            first_ack_dropped = True
            ack_dropped.set()

            #lets the parent check the offset before reconnecting
            if not allow_reconnect.wait(timeout=5):
                raise RuntimeError("Parent did not release the reconnect")

            connection.close()
            raise subscriber.LinkError("F-03 injected lost ACK and disconnect")

        original_send(connection, line)

    subscriber.handle_event = record_delivery
    subscriber.BrokerLink.send = drop_first_ack

    #this will  captures printed output so the parent can check for duplicates
    with output_file.open("w", encoding="utf-8") as output:
        with contextlib.redirect_stdout(output):
            subscriber.main(["--as", "ResponseTeam"])


def run_f03(observed):
    #Check replay and duplicate suppression after a lost ACK
    src_dir = Path(__file__).resolve().parents[1]
    subscriber_id = "responseteam"
    topic = "sighting"

    with tempfile.TemporaryDirectory(prefix="vigil-f03-") as test_dir:
        data_dir = Path(test_dir) / "data"
        data_dir.mkdir()

        event_log = data_dir / "event_log.json"
        offsets_file = data_dir / "offsets.json"
        output_file = Path(test_dir) / "subscriber-output.txt"
        deliveries_file = Path(test_dir) / "deliveries.jsonl"

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as test_socket:
            test_socket.bind(("127.0.0.1", 0))
            test_port = test_socket.getsockname()[1]

        context = multiprocessing.get_context("spawn")
        armed = context.Event()
        broker_checkpoint = context.Event()
        ack_dropped = context.Event()
        allow_reconnect = context.Event()

        broker_process = context.Process(
            target=run_test_broker,
            args=(
                src_dir,
                test_port,
                event_log,
                offsets_file,
                armed,
                broker_checkpoint,
            ),
        )
        subscriber_process = None

        observed["test_port"] = test_port
        observed["subscriber_id"] = subscriber_id

        try:
            #Start the isolated broker
            broker_process.start()
            wait_for_broker(broker_process, test_port)
            print("F-03 test broker is ready")

            payload = {
                "kind": "F-03 lost ACK",
                "sev": 2,
                "source": "fault_inject",
            }

            with socket.create_connection(
                ("127.0.0.1", test_port), timeout=5
            ) as publisher:
                message = f"PUB|{topic}|{json.dumps(payload)}\n"
                publisher.sendall(message.encode("utf-8"))
                reply = read_reply(publisher)

            command, separator, event_id = reply.partition("|")

            if command != "PUB_ACK" or not separator or not event_id:
                raise RuntimeError(f"Expected PUB_ACK, received {reply}")

            saved_log = json.loads(event_log.read_text(encoding="utf-8"))
            saved_events = saved_log.get(topic, [])

            if len(saved_events) != 1:
                raise RuntimeError("Expected exactly one saved event")

            saved_event = saved_events[0]

            if (
                saved_event.get("event_id") != event_id
                or saved_event.get("topic") != topic
                or saved_event.get("payload") != payload
            ):
                raise RuntimeError("Saved event does not match the publish")

            observed["saved_event"] = saved_event
            print("F-03 event saved:", event_id)

            before_offset = read_offset(offsets_file, subscriber_id, topic)
            observed["offset_before_delivery"] = before_offset

            if before_offset != -1:
                raise RuntimeError("Fresh subscriber already has an ACK offset")

            #Start the subscriber that suppresses its first ACK
            subscriber_process = context.Process(
                target=run_f03_subscriber,
                args=(
                    src_dir,
                    test_port,
                    output_file,
                    deliveries_file,
                    ack_dropped,
                    allow_reconnect,
                ),
            )
            subscriber_process.start()

            if not ack_dropped.wait(timeout=5):
                raise TimeoutError("Subscriber did not reach the lost-ACK checkpoint")

            dropped_offset = read_offset(offsets_file, subscriber_id, topic)
            observed["offset_after_dropped_ack"] = dropped_offset

            if dropped_offset != before_offset:
                raise RuntimeError("Offset advanced despite the suppressed ACK")

            print("First ACK suppressed; offset remains -1")

            #Allow the same subscriber process to reconnect
            allow_reconnect.set()
            deadline = time.monotonic() + 5

            while read_offset(offsets_file, subscriber_id, topic) != 0:
                if not broker_process.is_alive():
                    raise RuntimeError("Broker exited during replay")

                if not subscriber_process.is_alive():
                    raise RuntimeError("Subscriber exited during replay")

                if time.monotonic() >= deadline:
                    raise TimeoutError("Replay ACK did not advance the offset")

                time.sleep(0.05)

            observed["offset_after_replay_ack"] = 0
            print("Replay acknowledged; offset advanced to 0")

            #Stop the subscriber and flush its captured output
            stop_test_process(subscriber_process)
            observed["subscriber_exit_code"] = subscriber_process.exitcode

            if subscriber_process.exitcode != 0:
                raise RuntimeError("Subscriber did not shut down cleanly")

            delivered_events = [
                json.loads(line)
                for line in deliveries_file.read_text(
                    encoding="utf-8"
                ).splitlines()
                if line.strip()
            ]

            observed["received_events"] = delivered_events

            if delivered_events != [saved_event, saved_event]:
                raise RuntimeError("Expected the same saved event to arrive twice")

            #Compare received deliveries with displayed events
            output = output_file.read_text(encoding="utf-8")
            printed_events = [
                line
                for line in output.splitlines()
                if line.startswith("Event ")
            ]

            observed["subscriber_output"] = output
            observed["printed_events"] = printed_events
            observed["delivery_count"] = len(delivered_events)
            observed["print_count"] = len(printed_events)

            if (
                len(printed_events) != 1
                or event_id[:8] not in printed_events[0]
                or payload["kind"] not in printed_events[0]
            ):
                raise RuntimeError("Expected the target event to be printed once")

            print("Same event received twice")
            print("Same event printed once")
            print("Duplicate suppression verified")

        finally:
            #Clean up both processes even if one cleanup fails
            try:
                stop_test_process(subscriber_process)
                observed["subscriber_stopped"] = True
            finally:
                stop_test_process(broker_process)
                observed["broker_stopped"] = True

            print("F-03 test processes stopped")



def publish_f04_events(test_port, publisher_name, start_barrier):
    #Publish identifiable events after both publishers are connected
    accepted_events = []

    with socket.create_connection(
        ("127.0.0.1", test_port), timeout=5
    ) as publisher:
        start_barrier.wait(timeout=5)

        for number in range(10):
            payload = {
                "kind": f"F-04 {publisher_name} event {number}",
                "sev": 2,
                "source": publisher_name,
                "note": "a|b " * 100,
            }

            message = f"PUB|sighting|{json.dumps(payload)}\n"
            publisher.sendall(message.encode("utf-8"))
            reply = read_reply(publisher)

            command, separator, event_id = reply.partition("|")

            if command != "PUB_ACK" or not separator or not event_id:
                raise RuntimeError(f"Expected PUB_ACK, received {reply}")

            accepted_events.append({
                "event_id": event_id,
                "payload": payload,
            })

    return accepted_events


def run_f04(observed):
    #Check concurrent publishes against the saved event order
    src_dir = Path(__file__).resolve().parents[1]
    topic = "sighting"
    subscriber_id = "f04-check"

    with tempfile.TemporaryDirectory(prefix="vigil-f04-") as test_dir:
        data_dir = Path(test_dir) / "data"
        data_dir.mkdir()

        event_log = data_dir / "event_log.json"
        offsets_file = data_dir / "offsets.json"

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as test_socket:
            test_socket.bind(("127.0.0.1", 0))
            test_port = test_socket.getsockname()[1]

        context = multiprocessing.get_context("spawn")
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

        observed["test_port"] = test_port
        observed["events_per_publisher"] = 10

        try:
            broker_process.start()
            wait_for_broker(broker_process, test_port)
            print("F-04 test broker is ready")

            #Save a warmup event to confirm the subscriber becomes ready
            warmup_payload = {
                "kind": "F-04 warmup",
                "sev": 2,
                "source": "fault_inject",
            }

            with socket.create_connection(
                ("127.0.0.1", test_port), timeout=5
            ) as publisher:
                message = f"PUB|{topic}|{json.dumps(warmup_payload)}\n"
                publisher.sendall(message.encode("utf-8"))
                reply = read_reply(publisher)

            command, separator, warmup_id = reply.partition("|")

            if command != "PUB_ACK" or not separator or not warmup_id:
                raise RuntimeError(f"Expected warmup PUB_ACK, received {reply}")

            observed["warmup_event_id"] = warmup_id

            with socket.create_connection(
                ("127.0.0.1", test_port), timeout=5
            ) as subscriber:
                message = f"SUB|{topic}|{subscriber_id}\n"
                subscriber.sendall(message.encode("utf-8"))

                reply = read_reply(subscriber)

                if reply != f"SUB_ACK|{topic}":
                    raise RuntimeError(f"Unexpected subscription reply: {reply}")

                reply = read_reply(subscriber)
                command, separator, event_json = reply.partition("|")

                if command != "EVENT" or not separator:
                    raise RuntimeError("Expected the warmup event")

                warmup_event = json.loads(event_json)

                if (
                    warmup_event.get("event_id") != warmup_id
                    or warmup_event.get("topic") != topic
                    or warmup_event.get("payload") != warmup_payload
                ):
                    raise RuntimeError("Warmup event does not match the publish")

                #Wait for the warmup ACK to confirm subscription processing finished
                message = f"ACK|{topic}|{subscriber_id}|{warmup_id}\n"
                subscriber.sendall(message.encode("utf-8"))

                deadline = time.monotonic() + 5

                while read_offset(offsets_file, subscriber_id, topic) != 0:
                    if time.monotonic() >= deadline:
                        raise TimeoutError("Warmup ACK was not saved")
                    time.sleep(0.05)

                print("F-04 subscriber is ready")

                #Start both publishers and receive their events concurrently
                start_barrier = threading.Barrier(3)
                received_events = []

                with ThreadPoolExecutor(max_workers=2) as workers:
                    first = workers.submit(
                        publish_f04_events,
                        test_port,
                        "publisher-a",
                        start_barrier,
                    )
                    second = workers.submit(
                        publish_f04_events,
                        test_port,
                        "publisher-b",
                        start_barrier,
                    )

                    start_barrier.wait(timeout=5)

                    for number in range(20):
                        reply = read_reply(subscriber)
                        command, separator, event_json = reply.partition("|")

                        if command != "EVENT" or not separator:
                            raise RuntimeError(
                                f"Expected EVENT {number + 1}, received {reply}"
                            )

                        event = json.loads(event_json)

                        if not isinstance(event, dict):
                            raise RuntimeError("Received event is not an object")

                        received_events.append(event)

                    first_events = first.result(timeout=5)
                    second_events = second.result(timeout=5)

                #Check for unexpected additional data after both publishers finish
                subscriber.settimeout(0.3)

                try:
                    extra = subscriber.recv(1)
                except socket.timeout:
                    pass
                else:
                    if extra:
                        raise RuntimeError("Unexpected extra event data")
                    raise RuntimeError("Broker closed the subscriber connection")

            observed["publisher_a_event_ids"] = [
                event["event_id"] for event in first_events
            ]
            observed["publisher_b_event_ids"] = [
                event["event_id"] for event in second_events
            ]
            observed["received_event_ids"] = [
                event.get("event_id") for event in received_events
            ]

            #Compare the complete received events with their committed log entries
            saved_log = json.loads(event_log.read_text(encoding="utf-8"))
            stored_events = saved_log.get(topic, [])

            if len(stored_events) != 21:
                raise RuntimeError("Expected one warmup and 20 concurrent events")

            if stored_events[0] != warmup_event:
                raise RuntimeError("Warmup event changed in the log")

            concurrent_events = stored_events[1:]
            observed["stored_event_ids"] = [
                event.get("event_id") for event in concurrent_events
            ]

            if received_events != concurrent_events:
                raise RuntimeError("Subscriber events differ from the saved log order")

            accepted_events = first_events + second_events
            accepted_by_id = {
                event["event_id"]: event["payload"]
                for event in accepted_events
            }

            if len(accepted_by_id) != 20:
                raise RuntimeError("Expected 20 unique acknowledged event IDs")

            for event in concurrent_events:
                event_id = event.get("event_id")

                if event_id not in accepted_by_id:
                    raise RuntimeError("Log contains an unrecognized event ID")

                if (
                    event.get("topic") != topic
                    or event.get("payload") != accepted_by_id[event_id]
                ):
                    raise RuntimeError("Stored event differs from its published payload")

            observed["received_count"] = len(received_events)
            observed["stored_concurrent_count"] = len(concurrent_events)
            observed["delivery_matches_log_order"] = True
            observed["all_payloads_preserved"] = True

            print("Both publishers completed 10 events")
            print("All 20 events received and stored")
            print("Delivery order matches the saved log")
            print("All payloads preserved")

        finally:
            #Stop the owned broker on success or failure
            stop_test_process(broker_process)
            observed["broker_stopped"] = True
            print("F-04 test broker stopped")



def run_f05(observed):
    #Check saved events and ACK offsets after a broker restart
    src_dir = Path(__file__).resolve().parents[1]
    subscriber_id = "f05-check"
    topic = "sighting"

    with tempfile.TemporaryDirectory(prefix="vigil-f05-") as test_dir:
        data_dir = Path(test_dir) / "data"
        data_dir.mkdir()

        event_log = data_dir / "event_log.json"
        offsets_file = data_dir / "offsets.json"

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as test_socket:
            test_socket.bind(("127.0.0.1", 0))
            test_port = test_socket.getsockname()[1]

        context = multiprocessing.get_context("spawn")
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

        observed["test_port"] = test_port
        observed["subscriber_id"] = subscriber_id

        def receive_event(connection):
            #Read one replayed event from the broker
            reply = read_reply(connection)
            command, separator, event_json = reply.partition("|")

            if command != "EVENT" or not separator:
                raise RuntimeError(f"Expected EVENT, received {reply}")

            event = json.loads(event_json)

            if not isinstance(event, dict):
                raise RuntimeError("Received event is not an object")

            return event

        def acknowledge_event(connection, event, expected_offset):
            #Send an ACK and wait for its stored offset
            message = f"ACK|{topic}|{subscriber_id}|{event['event_id']}\n"
            connection.sendall(message.encode("utf-8"))
            deadline = time.monotonic() + 5

            while read_offset(
                offsets_file, subscriber_id, topic
            ) != expected_offset:
                if not broker_process.is_alive():
                    raise RuntimeError("Broker exited while saving the ACK")

                if time.monotonic() >= deadline:
                    raise TimeoutError("ACK offset was not saved")

                time.sleep(0.05)

        try:
            broker_process.start()
            wait_for_broker(broker_process, test_port)
            print("F-05 test broker is ready")

            event_ids = []
            payloads = []

            #Save three acknowledged publishes before crashing the broker
            with socket.create_connection(
                ("127.0.0.1", test_port), timeout=5
            ) as publisher:
                for number in range(1, 4):
                    payload = {
                        "kind": f"F-05 restart event {number}",
                        "sev": 2,
                        "source": "fault_inject",
                    }
                    payloads.append(payload)

                    message = f"PUB|{topic}|{json.dumps(payload)}\n"
                    publisher.sendall(message.encode("utf-8"))
                    reply = read_reply(publisher)

                    command, separator, event_id = reply.partition("|")

                    if command != "PUB_ACK" or not separator or not event_id:
                        raise RuntimeError(f"Expected PUB_ACK, received {reply}")

                    event_ids.append(event_id)

            saved_log = json.loads(event_log.read_text(encoding="utf-8"))
            saved_events = saved_log.get(topic, [])

            if len(saved_events) != 3 or len(set(event_ids)) != 3:
                raise RuntimeError("Expected three unique saved events")

            for event, event_id, payload in zip(
                saved_events, event_ids, payloads
            ):
                if (
                    event.get("event_id") != event_id
                    or event.get("topic") != topic
                    or event.get("payload") != payload
                ):
                    raise RuntimeError("Saved events differ from the publishes")

            observed["saved_events"] = saved_events
            print("Three events saved:", event_ids)

            #Acknowledge only the first event before the broker crash
            with socket.create_connection(
                ("127.0.0.1", test_port), timeout=5
            ) as subscriber:
                subscriber.sendall(
                    f"SUB|{topic}|{subscriber_id}\n".encode("utf-8")
                )

                if read_reply(subscriber) != f"SUB_ACK|{topic}":
                    raise RuntimeError("Initial subscription was not acknowledged")

                first_event = receive_event(subscriber)

                if first_event != saved_events[0]:
                    raise RuntimeError("Initial replay returned the wrong event")

                acknowledge_event(subscriber, first_event, 0)

            observed["offset_before_crash"] = 0
            log_before = event_log.read_bytes()
            offsets_before = json.loads(
                offsets_file.read_text(encoding="utf-8")
            )
            observed["offsets_before_crash"] = offsets_before
            print("First event acknowledged; offset is 0")

            #Kill the broker after events and offsets have been saved
            broker_process.kill()
            broker_process.join(timeout=5)

            if broker_process.exitcode != -signal.SIGKILL:
                raise RuntimeError("Broker did not exit through SIGKILL")

            observed["crashed_broker_exit_code"] = broker_process.exitcode
            print("Broker killed with SIGKILL")

            #Restart using the same event log and offsets
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

            if event_log.read_bytes() != log_before:
                raise RuntimeError("Saved event log changed across restart")

            offsets_after = json.loads(
                offsets_file.read_text(encoding="utf-8")
            )
            observed["offsets_after_restart"] = offsets_after

            if offsets_after != offsets_before:
                raise RuntimeError("Saved offsets changed across restart")

            print("Broker restarted with saved events and offsets")

            #Reconnect with the same ID and read only the two pending events
            with socket.create_connection(
                ("127.0.0.1", test_port), timeout=5
            ) as subscriber:
                subscriber.sendall(
                    f"SUB|{topic}|{subscriber_id}\n".encode("utf-8")
                )

                if read_reply(subscriber) != f"SUB_ACK|{topic}":
                    raise RuntimeError("Reconnect was not acknowledged")

                replayed_events = [
                    receive_event(subscriber),
                    receive_event(subscriber),
                ]
                observed["replayed_events"] = replayed_events

                if replayed_events != saved_events[1:]:
                    raise RuntimeError("Replay did not match the pending events")

                acknowledge_event(subscriber, replayed_events[0], 1)
                acknowledge_event(subscriber, replayed_events[1], 2)

                #Check that the acknowledged first event was not replayed again
                subscriber.settimeout(0.3)

                try:
                    extra = subscriber.recv(1)
                except socket.timeout:
                    pass
                else:
                    if extra:
                        raise RuntimeError("Unexpected extra replay data")
                    raise RuntimeError("Broker closed the subscriber connection")

            observed["offset_after_replay_ack"] = 2
            observed["event_log_preserved"] = True
            observed["saved_offsets_preserved"] = True
            observed["replay_matches_pending_events"] = True

            print("Only the two pending events replayed in order")
            print("Replay ACKs advanced the offset to 2")

        finally:
            #Stop the test broker before removing temporary data
            stop_test_process(broker_process)
            observed["broker_stopped"] = True
            print("F-05 test broker stopped")

def run_f06(observed):
    #Check unread events remain available while all subscribers are offline
    src_dir = Path(__file__).resolve().parents[1]
    topic = "sighting"
    slow_id = "f06-slow"
    fast_id = "f06-fast"

    observed["retention_policy"] = "Keep all events; no pruning"

    with tempfile.TemporaryDirectory(prefix="vigil-f06-") as test_dir:
        data_dir = Path(test_dir) / "data"
        data_dir.mkdir()

        event_log = data_dir / "event_log.json"
        offsets_file = data_dir / "offsets.json"

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as test_socket:
            test_socket.bind(("127.0.0.1", 0))
            test_port = test_socket.getsockname()[1]

        context = multiprocessing.get_context("spawn")
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
        observed["test_port"] = test_port

        def publish_batch(first_number, count):
            #Publish a batch and verify each acknowledged event on disk
            accepted = []

            with socket.create_connection(
                ("127.0.0.1", test_port), timeout=5
            ) as publisher:
                for number in range(first_number, first_number + count):
                    payload = {
                        "kind": f"F-06 retained event {number}",
                        "sev": 2,
                        "source": "fault_inject",
                    }
                    message = f"PUB|{topic}|{json.dumps(payload)}\n"
                    publisher.sendall(message.encode("utf-8"))
                    reply = read_reply(publisher)

                    command, separator, event_id = reply.partition("|")

                    if command != "PUB_ACK" or not separator or not event_id:
                        raise RuntimeError(f"Expected PUB_ACK, received {reply}")

                    accepted.append({
                        "event_id": event_id,
                        "payload": payload,
                    })

            saved_log = json.loads(event_log.read_text(encoding="utf-8"))
            batch = saved_log.get(topic, [])[-count:]

            if len(batch) != count:
                raise RuntimeError("Published events are missing from the log")

            for event, expected in zip(batch, accepted):
                if (
                    event.get("event_id") != expected["event_id"]
                    or event.get("payload") != expected["payload"]
                    or event.get("topic") != topic
                ):
                    raise RuntimeError("Saved event differs from its publish")

            return batch

        def replay_and_ack(subscriber_id, expected_events, final_offset,
                           check_end=False):
            #Receive expected events and acknowledge the last processed event
            with socket.create_connection(
                ("127.0.0.1", test_port), timeout=5
            ) as subscriber:
                message = f"SUB|{topic}|{subscriber_id}\n"
                subscriber.sendall(message.encode("utf-8"))

                if read_reply(subscriber) != f"SUB_ACK|{topic}":
                    raise RuntimeError("Subscription was not acknowledged")

                received = []

                for expected_event in expected_events:
                    reply = read_reply(subscriber)
                    command, separator, event_json = reply.partition("|")

                    if command != "EVENT" or not separator:
                        raise RuntimeError(f"Expected EVENT, received {reply}")

                    event = json.loads(event_json)
                    received.append(event)

                    if event != expected_event:
                        raise RuntimeError(
                            f"Unexpected replay event for {subscriber_id}"
                        )

                event_id = received[-1]["event_id"]
                message = f"ACK|{topic}|{subscriber_id}|{event_id}\n"
                subscriber.sendall(message.encode("utf-8"))

                deadline = time.monotonic() + 5

                while read_offset(
                    offsets_file, subscriber_id, topic
                ) != final_offset:
                    if not broker_process.is_alive():
                        raise RuntimeError("Broker exited while saving the ACK")

                    if time.monotonic() >= deadline:
                        raise TimeoutError("Subscriber offset was not saved")

                    time.sleep(0.05)

                if check_end:
                    #Check for unexpected events after the expected replay
                    subscriber.settimeout(0.3)

                    try:
                        extra = subscriber.recv(1)
                    except socket.timeout:
                        pass
                    else:
                        if extra:
                            raise RuntimeError("Unexpected extra replay data")
                        raise RuntimeError("Broker closed the connection")

            return received

        try:
            broker_process.start()
            wait_for_broker(broker_process, test_port)
            print("F-06 test broker is ready")

            initial_events = publish_batch(1, 3)

            if len(json.loads(
                event_log.read_text(encoding="utf-8")
            ).get(topic, [])) != 3:
                raise RuntimeError("Expected exactly three initial events")

            #Give the subscribers different acknowledged positions
            replay_and_ack(slow_id, initial_events[:1], 0)
            replay_and_ack(fast_id, initial_events, 2)

            offsets_before = json.loads(
                offsets_file.read_text(encoding="utf-8")
            )
            observed["offsets_before_offline_publish"] = offsets_before
            observed["initial_events"] = initial_events

            print("Slow subscriber offset: 0")
            print("Fast subscriber offset: 2")
            print("Both subscribers disconnected")

            #Publish more events while both subscriber connections are closed
            offline_events = publish_batch(4, 2)
            retained_events = json.loads(
                event_log.read_text(encoding="utf-8")
            ).get(topic, [])

            observed["offline_events"] = offline_events
            observed["retained_events"] = retained_events

            if retained_events != initial_events + offline_events:
                raise RuntimeError("Previously saved events were lost or changed")

            offsets_after = json.loads(
                offsets_file.read_text(encoding="utf-8")
            )
            observed["offsets_after_offline_publish"] = offsets_after

            if offsets_after != offsets_before:
                raise RuntimeError("Offsets changed while subscribers were offline")

            print("All five events retained")
            print("Offline subscriber offsets unchanged")

            #Verify each subscriber recovers all events after its own offset
            slow_replay = replay_and_ack(
                slow_id, retained_events[1:], 4, check_end=True
            )
            observed["slow_subscriber_replay"] = slow_replay

            fast_replay = replay_and_ack(
                fast_id, retained_events[3:], 4, check_end=True
            )
            observed["fast_subscriber_replay"] = fast_replay

            observed["final_offsets"] = json.loads(
                offsets_file.read_text(encoding="utf-8")
            )
            observed["all_events_retained"] = True

            print("Slow subscriber recovered four unread events")
            print("Fast subscriber recovered two unread events")
            print("Both subscriber offsets advanced to 4")

        finally:
            #Stop the test broker before removing temporary data
            stop_test_process(broker_process)
            observed["broker_stopped"] = True
            print("F-06 test broker stopped")


def run_f07(observed):
    #Check that a duplicate subscriber ID replaces the old connection
    src_dir = Path(__file__).resolve().parents[1]
    subscriber_id = "f07-check"
    topic = "sighting"

    with tempfile.TemporaryDirectory(prefix="vigil-f07-") as test_dir:
        data_dir = Path(test_dir) / "data"
        data_dir.mkdir()

        event_log = data_dir / "event_log.json"
        offsets_file = data_dir / "offsets.json"

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as test_socket:
            test_socket.bind(("127.0.0.1", 0))
            test_port = test_socket.getsockname()[1]

        context = multiprocessing.get_context("spawn")
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

        observed["test_port"] = test_port
        observed["subscriber_id"] = subscriber_id

        def publish_event(kind):
            #Publish and verify one identifiable event
            payload = {
                "kind": kind,
                "sev": 2,
                "source": "fault_inject",
            }

            with socket.create_connection(
                ("127.0.0.1", test_port), timeout=5
            ) as publisher:
                message = f"PUB|{topic}|{json.dumps(payload)}\n"
                publisher.sendall(message.encode("utf-8"))
                reply = read_reply(publisher)

            command, separator, event_id = reply.partition("|")

            if command != "PUB_ACK" or not separator or not event_id:
                raise RuntimeError(f"Expected PUB_ACK, received {reply}")

            saved_events = json.loads(
                event_log.read_text(encoding="utf-8")
            ).get(topic, [])

            if not saved_events:
                raise RuntimeError("Published event is missing from the log")

            event = saved_events[-1]

            if (
                event.get("event_id") != event_id
                or event.get("topic") != topic
                or event.get("payload") != payload
            ):
                raise RuntimeError("Saved event differs from the publish")

            return event

        def subscribe(connection):
            #Register the test subscriber and check its acknowledgment
            message = f"SUB|{topic}|{subscriber_id}\n"
            connection.sendall(message.encode("utf-8"))
            reply = read_reply(connection)

            if reply != f"SUB_ACK|{topic}":
                raise RuntimeError(f"Unexpected subscription reply: {reply}")

        def receive_event(connection):
            #Read one complete event from the test connection
            reply = read_reply(connection)
            command, separator, event_json = reply.partition("|")

            if command != "EVENT" or not separator:
                raise RuntimeError(f"Expected EVENT, received {reply}")

            return json.loads(event_json)

        def acknowledge(connection, event, expected_offset):
            #Wait for the broker to save the acknowledged position
            message = f"ACK|{topic}|{subscriber_id}|{event['event_id']}\n"
            connection.sendall(message.encode("utf-8"))
            deadline = time.monotonic() + 5

            while read_offset(
                offsets_file, subscriber_id, topic
            ) != expected_offset:
                if not broker_process.is_alive():
                    raise RuntimeError("Broker exited while saving the ACK")

                if time.monotonic() >= deadline:
                    raise TimeoutError("Subscriber offset was not saved")

                time.sleep(0.05)

        try:
            broker_process.start()
            wait_for_broker(broker_process, test_port)
            print("F-07 test broker is ready")

            warmup_event = publish_event("F-07 warmup")

            with socket.create_connection(
                ("127.0.0.1", test_port), timeout=5
            ) as old_subscriber:
                subscribe(old_subscriber)

                if receive_event(old_subscriber) != warmup_event:
                    raise RuntimeError("Old subscriber received the wrong warmup")

                acknowledge(old_subscriber, warmup_event, 0)
                observed["offset_before_replacement"] = 0
                print("Old subscriber registered and acknowledged warmup")

                with socket.create_connection(
                    ("127.0.0.1", test_port), timeout=5
                ) as new_subscriber:
                    #Replace the old connection using the same subscriber ID
                    subscribe(new_subscriber)
                    old_subscriber.settimeout(5)

                    try:
                        remaining = old_subscriber.recv(1)
                    except ConnectionResetError:
                        remaining = b""

                    if remaining:
                        raise RuntimeError("Old connection received unexpected data")

                    observed["old_socket_closed"] = True
                    print("Old subscriber connection closed")

                    #Check live delivery on the replacement connection
                    live_event = publish_event("F-07 replacement delivery")
                    received_event = receive_event(new_subscriber)

                    observed["published_event"] = live_event
                    observed["replacement_received_event"] = received_event

                    if received_event != live_event:
                        raise RuntimeError("Replacement received the wrong event")

                    acknowledge(new_subscriber, live_event, 1)
                    observed["offset_after_replacement_ack"] = 1

                    #Check that the replacement receives no extra copy
                    new_subscriber.settimeout(0.3)

                    try:
                        extra = new_subscriber.recv(1)
                    except socket.timeout:
                        pass
                    else:
                        if extra:
                            raise RuntimeError("Replacement received extra event data")
                        raise RuntimeError("Replacement connection closed unexpectedly")

                    observed["extra_data_observation_seconds"] = 0.3
                    observed["replacement_received_one_copy"] = True

            saved_events = json.loads(
                event_log.read_text(encoding="utf-8")
            ).get(topic, [])

            if saved_events != [warmup_event, live_event]:
                raise RuntimeError("Stored events changed during replacement")

            print("New subscriber received the published event once")
            print("Replacement ACK advanced the offset to 1")

        finally:
            #Stop the test broker before removing temporary data
            stop_test_process(broker_process)
            observed["broker_stopped"] = True
            print("F-07 test broker stopped")

def run_f08_subscriber(src_dir, test_port, output_file):
    #Run the normal subscriber and capture its output
    sys.path.insert(0, str(src_dir))

    import common.constants as constants

    constants.PORT = test_port

    import subscriber

    with output_file.open("w", encoding="utf-8") as output:
        with contextlib.redirect_stdout(output):
            subscriber.main(["--as", "ResponseTeam"])

def publish_f08_load(
    test_port,
    publisher_name,
    start_barrier,
    shutdown_started,
    stop_load,
    load_ready,
):
    #Publish traffic until shutdown or the safety limit is reached
    accepted = []
    interrupted = []

    with socket.create_connection(
        ("127.0.0.1", test_port), timeout=5
    ) as publisher:
        start_barrier.wait(timeout=5)

        for number in range(1000):
            if stop_load.is_set():
                break

            payload = {
                "kind": f"F-08 {publisher_name} event {number}",
                "sev": 2,
                "source": publisher_name,
            }

            try:
                message = f"PUB|sighting|{json.dumps(payload)}\n"
                publisher.sendall(message.encode("utf-8"))
                reply = read_reply(publisher)
            except OSError as exc:
                if not shutdown_started.is_set():
                    raise

                interrupted.append({
                    "payload": payload,
                    "error": f"{type(exc).__name__}: {exc}",
                })
                break

            command, separator, event_id = reply.partition("|")

            if command != "PUB_ACK" or not separator or not event_id:
                raise RuntimeError(f"Expected PUB_ACK, received {reply}")

            accepted.append({
                "event_id": event_id,
                "payload": payload,
            })

            #Signal that this publisher has produced active traffic
            if len(accepted) >= 5:
                load_ready.set()

    return {
        "accepted": accepted,
        "interrupted": interrupted,
    }


def run_f08(observed):
    #Check graceful broker shutdown while publishers and a subscriber are active
    src_dir = Path(__file__).resolve().parents[1]
    sys.path.insert(0, str(src_dir))

    from common.constants import DRAIN_S

    subscriber_id = "responseteam"
    topic = "sighting"

    with tempfile.TemporaryDirectory(prefix="vigil-f08-") as test_dir:
        data_dir = Path(test_dir) / "data"
        data_dir.mkdir()

        event_log = data_dir / "event_log.json"
        offsets_file = data_dir / "offsets.json"
        output_file = Path(test_dir) / "subscriber-output.txt"

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as test_socket:
            test_socket.bind(("127.0.0.1", 0))
            test_port = test_socket.getsockname()[1]

        context = multiprocessing.get_context("spawn")
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
        subscriber_process = None

        shutdown_started = threading.Event()
        stop_load = threading.Event()
        first_ready = threading.Event()
        second_ready = threading.Event()
        start_barrier = threading.Barrier(3)

        observed["test_port"] = test_port
        observed["shutdown_budget_seconds"] = DRAIN_S

        try:
            broker_process.start()
            wait_for_broker(broker_process, test_port)
            print("F-08 test broker is ready")

            #Save a warmup event to verify subscriber readiness
            warmup_payload = {
                "kind": "F-08 warmup",
                "sev": 2,
                "source": "fault_inject",
            }

            with socket.create_connection(
                ("127.0.0.1", test_port), timeout=5
            ) as publisher:
                message = f"PUB|{topic}|{json.dumps(warmup_payload)}\n"
                publisher.sendall(message.encode("utf-8"))
                reply = read_reply(publisher)

            command, separator, warmup_id = reply.partition("|")

            if command != "PUB_ACK" or not separator or not warmup_id:
                raise RuntimeError(f"Expected PUB_ACK, received {reply}")

            observed["warmup_event_id"] = warmup_id

            subscriber_process = context.Process(
                target=run_f08_subscriber,
                args=(src_dir, test_port, output_file),
            )
            subscriber_process.start()

            deadline = time.monotonic() + 5

            while read_offset(offsets_file, subscriber_id, topic) != 0:
                if not subscriber_process.is_alive():
                    raise RuntimeError("Subscriber exited during startup")

                if time.monotonic() >= deadline:
                    raise TimeoutError("Subscriber did not acknowledge warmup")

                time.sleep(0.05)

            print("F-08 subscriber is ready")

            #Start two publishers and request shutdown during their traffic
            with ThreadPoolExecutor(max_workers=2) as workers:
                first = workers.submit(
                    publish_f08_load,
                    test_port,
                    "publisher-a",
                    start_barrier,
                    shutdown_started,
                    stop_load,
                    first_ready,
                )
                second = workers.submit(
                    publish_f08_load,
                    test_port,
                    "publisher-b",
                    start_barrier,
                    shutdown_started,
                    stop_load,
                    second_ready,
                )

                try:
                    start_barrier.wait(timeout=5)

                    if not first_ready.wait(timeout=5):
                        raise TimeoutError("First publisher did not establish load")

                    if not second_ready.wait(timeout=5):
                        raise TimeoutError("Second publisher did not establish load")

                    if first.done() or second.done():
                        raise RuntimeError("Publisher finished before shutdown")

                    if not subscriber_process.is_alive():
                        raise RuntimeError("Subscriber exited before shutdown")

                    observed["active_publishers_at_sigint"] = 2
                    print("Both publishers active; sending SIGINT")

                    shutdown_started.set()
                    shutdown_time = time.monotonic()
                    os.kill(broker_process.pid, signal.SIGINT)
                    broker_process.join(timeout=DRAIN_S)

                    duration = time.monotonic() - shutdown_time
                    observed["shutdown_duration_seconds"] = duration
                    observed["broker_exit_code"] = broker_process.exitcode

                    if broker_process.is_alive():
                        raise TimeoutError("Broker exceeded its shutdown budget")

                    if duration > DRAIN_S:
                        raise RuntimeError("Broker shutdown took too long")

                    if broker_process.exitcode != 0:
                        raise RuntimeError("Broker did not exit with status 0")

                finally:
                    stop_load.set()

                first_result = first.result(timeout=6)
                second_result = second.result(timeout=6)

            #Collect acknowledged and interrupted publishes from both workers
            accepted = first_result["accepted"] + second_result["accepted"]
            interrupted = (
                first_result["interrupted"] + second_result["interrupted"]
            )

            observed["publisher_a_acknowledged"] = len(first_result["accepted"])
            observed["publisher_b_acknowledged"] = len(second_result["accepted"])
            observed["acknowledged_publishes"] = accepted
            observed["interrupted_publishes"] = interrupted

            if (
                len(first_result["accepted"]) < 5
                or len(second_result["accepted"]) < 5
            ):
                raise RuntimeError("Insufficient acknowledged traffic")

            #Stop the subscriber after the broker shutdown and flush its output
            stop_test_process(subscriber_process)
            observed["subscriber_exit_code"] = subscriber_process.exitcode

            if subscriber_process.exitcode != 0:
                raise RuntimeError("Subscriber did not shut down cleanly")

            observed["subscriber_output"] = output_file.read_text(
                encoding="utf-8"
            )

            #Verify valid files and preservation of every acknowledged publish
            saved_log = json.loads(event_log.read_text(encoding="utf-8"))
            saved_events = saved_log.get(topic, [])
            stored_by_id = {
                event["event_id"]: event for event in saved_events
            }

            if len(stored_by_id) != len(saved_events):
                raise RuntimeError("Saved log contains duplicate event IDs")

            expected_payloads = {warmup_id: warmup_payload}

            for event in accepted:
                if event["event_id"] in expected_payloads:
                    raise RuntimeError("Acknowledged event IDs are not unique")

                expected_payloads[event["event_id"]] = event["payload"]

            for event_id, payload in expected_payloads.items():
                saved_event = stored_by_id.get(event_id)

                if saved_event is None:
                    raise RuntimeError(
                        f"Acknowledged event missing after shutdown: {event_id}"
                    )

                if (
                    saved_event.get("topic") != topic
                    or saved_event.get("payload") != payload
                ):
                    raise RuntimeError("Acknowledged event changed on disk")

            saved_offsets = json.loads(
                offsets_file.read_text(encoding="utf-8")
            )
            last_offset = saved_offsets.get(subscriber_id, {}).get(topic, -1)

            if (
                type(last_offset) is not int
                or not 0 <= last_offset < len(saved_events)
            ):
                raise RuntimeError("Stored subscriber offset is invalid")

            observed["saved_offsets"] = saved_offsets
            observed["stored_event_count"] = len(saved_events)
            observed["acknowledged_events_preserved"] = True
            observed["saved_files_valid_json"] = True
            observed["stored_without_pub_ack"] = [
                event_id
                for event_id in stored_by_id
                if event_id not in expected_payloads
            ]

            print("Broker exited with status 0")
            print("Shutdown duration:", duration)
            print("All acknowledged events preserved")
            print("Saved log and offsets are valid JSON")
            print("Interrupted publishes:", len(interrupted))

        finally:
            #Clean up both processes even when a shutdown check fails
            stop_load.set()

            try:
                stop_test_process(subscriber_process)
                observed["subscriber_stopped"] = True
            finally:
                stop_test_process(broker_process)
                observed["broker_stopped"] = True

            print("F-08 test processes stopped")


def main():
    #reads the selected fault case and displays it
    cases = {
        "F-01": run_f01,
        "F-02": run_f02,
        "F-03": run_f03,
        "F-04": run_f04,
        "F-05": run_f05,
        "F-06": run_f06,
        "F-07": run_f07,
        "F-08": run_f08,
    }


    #selctes behaviour of report
    expected_results = {
        "F-01": (
            "The acknowledged baseline survives SIGKILL after temporary-file "
            "fsync and before replacement, and replays unchanged after restart."
        ),
        "F-02": (
            "An event received by a subscriber killed before ACK replays "
            "when the same subscriber ID reconnects. Its offset advances "
            "only after the replay is acknowledged."
        ),
        "F-03": (
            "A suppressed ACK leaves the offset unchanged. The same subscriber "
            "process reconnects, receives the event again, prints it once, "
            "and acknowledges the replay so the offset advances."
        ),
        "F-04": (
            "Two concurrent publishers each send 10 events to the same topic. "
            "All 20 events arrive intact, with unique acknowledged IDs, "
            "and subscriber delivery order matches the committed log."
        ),
        "F-05": (
            "Saved events and subscriber offsets survive a broker crash. "
            "After restart, the same subscriber ID receives only its two "
            "unacknowledged events in log order."
        ),
        "F-06": (
            "Under the keep-all policy, events remain available while both "
            "subscribers are offline. Each subscriber recovers its unread "
            "events in order using its own offset. Pruning is not implemented."
        ),
        "F-07": (
            "A second connection using the same subscriber ID closes the old "
            "connection. The replacement receives the published event once "
            "and advances the shared subscriber offset through ACK."
        ),
        "F-08": (
            "During active publisher traffic, SIGINT makes the broker exit "
            "with status 0 within its configured shutdown budget. Saved JSON "
            "files remain valid and all acknowledged publishes remain intact. "
            "Interrupted publishes are recorded."
        ),
    }

    parser = argparse.ArgumentParser(description="VIGIL QA Tests")
    parser.add_argument("--case", required=True, choices=cases)

    args = parser.parse_args()
    print("Selected case:", args.case)

    #records the test result after the checks and cleanup finish
  
  
    observed = {}
    verdict = "FAIL"
    error = None

    try:
        cases[args.case](observed)
        verdict = "PASS"
    except Exception as exc:
        error = f"{type(exc).__name__}: {exc}"
        print("Test error:", error)
    except KeyboardInterrupt:
        error = "Test interrupted by the user"
        print(error)

    result = {
        "case": args.case,
        "date": datetime.now().astimezone().isoformat(),
        "command": f"python3 src/tools/fault_inject.py --case {args.case}",
        "verdict": verdict,
        "observed": observed,
        "error": error,
    }

    #aaaappends the result to the project's fault test report
    report_path = (
        Path(__file__).resolve().parents[2]
        / "docs"
        / "fault-test-report.md"
    )
    report_path.parent.mkdir(parents=True, exist_ok=True)

    with report_path.open("a", encoding="utf-8") as report:
        report.write(f"\n### {args.case} Automated Run\n\n")
        report.write(f"**Date:** {result['date']}\n\n")
        report.write(f"**Verdict:** {verdict}\n\n")
        report.write(f"**Expected:** {expected_results[args.case]}\n\n")
        report.write("**Observed results and evidence:**\n\n")
        report.write("```json\n")
        report.write(json.dumps(result, indent=2))
        report.write("\n```\n")

    print(args.case, "result:", verdict)
    print("Report updated:", report_path)

    return 0 if verdict == "PASS" else 1


if __name__ == "__main__":
    sys.exit(main())