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

class TestBroker:
    def __init__(self, root, observed):
        #Prepare shared settings for one isolated test run
        self.root = root
        self.observed = observed
        self.src_dir = Path(__file__).resolve().parents[1]
        # Ask the operating system for an available local porrrt
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as test_socket:
            test_socket.bind(("127.0.0.1", 0))
            self.port = test_socket.getsockname()[1]
        # Set up the files used by the test broker.
        data_dir = root / "data"
        data_dir.mkdir()
        self.event_log = data_dir / "event_log.json"
        self.offsets_file = data_dir / "offsets.json"
        self.context = multiprocessing.get_context("spawn")
        #These signals control when the broker pauses for the crash test
        self.armed = self.context.Event()
        self.checkpoint = self.context.Event()
        self.processes = []
        self.broker_process = None
        observed["test_port"] = self.port

    def start_process(self, target, args, role="subscriber"):
        #Track each owned process before starting it
        process = self.context.Process(target=target, args=args)
        self.processes.append((role, process))
        process.start()
        return process

    def start_broker(self):
        #Start or restart the broker using this run's data files
        self.broker_process = self.start_process(
            run_test_broker,
            (self.src_dir, self.port, self.event_log, self.offsets_file,
             self.armed, self.checkpoint),
            role="broker",
        )
        wait_for_broker(self.broker_process, self.port)

@contextlib.contextmanager
def broker_test(case_id, observed):
    #Create temp folder for fault testss
    with tempfile.TemporaryDirectory(prefix=f"vigil-{case_id.lower()}-") as folder:
        test = TestBroker(Path(folder), observed)
        test_error = None
        try:
            test.start_broker()
            yield test
        except BaseException as exc:
            test_error = exc
            raise
        finally:
            #This attempts cleanup of both processes even if one cleanup fails
            #Clean up both processes even if one cleanup fails
            #Stop the owned broker on success or failure
            cleanup_errors = []
            for role, process in reversed(test.processes):
                try:
                    stop_test_process(process)
                except Exception as exc:
                    cleanup_errors.append(f"{role}: {exc}")
            #confirmation before succesful cleanup
            for role in {role for role, _ in test.processes}:
                observed[f"{role}_stopped"] = all(
                    not process.is_alive()
                    for name, process in test.processes if name == role
                )
            if cleanup_errors:
                observed["cleanup_errors"] = cleanup_errors
                if test_error is None:
                    raise RuntimeError("Cleanup failed: " + "; ".join(cleanup_errors))
            print(case_id, "test processes stopped")
    observed["temporary_directory_removed"] = not Path(folder).exists()

def send_line(connection, message):
    #Send one complete protocol line with a timeout
    connection.settimeout(5)
    connection.sendall((message + "\n").encode("utf-8"))

def send_publish(connection, topic, payload):
    #Check the acknowledgment for one publish
    send_line(connection, f"PUB|{topic}|{json.dumps(payload)}")
    reply = read_reply(connection)
    command, separator, event_id = reply.partition("|")
    if command != "PUB_ACK" or not separator or not event_id:
        raise RuntimeError(f"Expected PUB_ACK, received {reply}")
    return event_id

def publish_saved_event(test, payload, topic="sighting"):
    #Publish the first event and verify its stored envelope
    with socket.create_connection(("127.0.0.1", test.port), timeout=5) as publisher:
        event_id = send_publish(publisher, topic, payload)
    saved_events = json.loads(test.event_log.read_text(encoding="utf-8")).get(topic, [])
    if len(saved_events) != 1:
        raise RuntimeError("Expected exactly one saved event")
    event = saved_events[0]
    if (event.get("event_id") != event_id or event.get("topic") != topic
            or event.get("payload") != payload):
        raise RuntimeError("Saved event does not match the publish")
    return event

@contextlib.contextmanager
def subscribe_client(test, subscriber_id, topic="sighting"):
    #Connect a harness subscriber and check its SUB_ACK
    #Register the test subscriber and check its acknowledgment
    with socket.create_connection(("127.0.0.1", test.port), timeout=5) as connection:
        send_line(connection, f"SUB|{topic}|{subscriber_id}")
        reply = read_reply(connection)
        if reply != f"SUB_ACK|{topic}":
            raise RuntimeError(f"Unexpected subscription reply: {reply}")
        yield connection

def read_event(connection):
    #Read and validate an EVENT envelope
    #Read one replayed event from the broker
    #Read one complete event from the test connection
    reply = read_reply(connection)
    command, separator, event_json = reply.partition("|")
    if command != "EVENT" or not separator:
        raise RuntimeError(f"Expected EVENT, received {reply}")
    event = json.loads(event_json)
    if not isinstance(event, dict):
        raise RuntimeError("Received event is not an object")
    return event

def wait_for_offset(test, subscriber_id, expected_offset, subscriber_process=None):
    #Wait for the expected offset while checking process health
    deadline = time.monotonic() + 5
    while read_offset(test.offsets_file, subscriber_id, "sighting") != expected_offset:
        if not test.broker_process.is_alive():
            raise RuntimeError("Broker exited while waiting for an ACK")
        if subscriber_process is not None and not subscriber_process.is_alive():
            raise RuntimeError("Subscriber exited while waiting for an ACK")
        if time.monotonic() >= deadline:
            raise TimeoutError(f"ACK did not save offset {expected_offset}")
        time.sleep(0.05)

def load_events(test):
    #Read the saved sighting events for the current test
    return json.loads(test.event_log.read_text(encoding="utf-8")).get("sighting", [])

def load_offsets(test):
    #Read the stored subscriber offsets
    return json.loads(test.offsets_file.read_text(encoding="utf-8"))

def publish_batch(test, payloads):
    #Publish a batch and verify each acknowledged event on disk
    #Publish and verify one identifiable event
    with socket.create_connection(("127.0.0.1", test.port), timeout=5) as publisher:
        accepted = [send_publish(publisher, "sighting", payload) for payload in payloads]
    batch = load_events(test)[-len(payloads):]
    if len(batch) != len(payloads):
        raise RuntimeError("Published events are missing from the log")
    for event, event_id, payload in zip(batch, accepted, payloads):
        if (event.get("event_id") != event_id or event.get("topic") != "sighting"
                or event.get("payload") != payload):
            raise RuntimeError("Saved event differs from its publish")
    return batch

def acknowledge_event(test, connection, subscriber_id, event, expected_offset):
    #Send an ACK and wait for its stored offset
    #Wait for the broker to save the acknowledged position
    send_line(connection, f"ACK|sighting|{subscriber_id}|{event['event_id']}")
    wait_for_offset(test, subscriber_id, expected_offset)

def expect_no_data(connection):
    #Reject extra data or an unexpected connection closure
    connection.settimeout(0.3)
    try:
        extra = connection.recv(1)
    except socket.timeout:
        return
    if extra:
        raise RuntimeError("Unexpected extra event data")
    raise RuntimeError("Broker closed the subscriber connection")

def replay_and_ack(test, subscriber_id, expected_events, final_offset, check_end=False):
    #Receive expected events and acknowledge the last processed event
    with subscribe_client(test, subscriber_id) as connection:
        received = [read_event(connection) for _ in expected_events]
        if received != expected_events:
            raise RuntimeError(f"Unexpected replay events for {subscriber_id}")
        acknowledge_event(test, connection, subscriber_id, received[-1], final_offset)
        if check_end:
            #Check for unexpected events after the expected replay
            expect_no_data(connection)
    return received

def run_f01(observed):
    with broker_test("F-01", observed) as test:
        print("Test broker is ready")
        baseline_event = publish_saved_event(test, {
            "kind": "F-01 baseline", "sev": 2, "source": "fault_inject",
        })
        baseline_bytes = test.event_log.read_bytes()
        print("Baseline acknowledged:", baseline_event["event_id"])
        print("Baseline event verified on disk")
        observed["baseline_event"] = baseline_event
        observed["baseline_saved"] = True

        #This enables the pause during the next event save
        test.armed.set()
        payload = {"kind": "F-01 interrupted event", "sev": 2, "source": "fault_inject"}
        with socket.create_connection(("127.0.0.1", test.port), timeout=5) as publisher:
            send_line(publisher, f"PUB|sighting|{json.dumps(payload)}")
            #This waits until the temporary event log has been synced
            if not test.checkpoint.wait(timeout=5):
                raise TimeoutError("Broker did not reach the crash checkpoint")
            print("Crash checkpoint reached")
            observed["crash_point"] = "After temporary-file fsync, before replacement"
            #This kills the test broker while the save is paused
            test.broker_process.kill()
            test.broker_process.join(timeout=5)
            if test.broker_process.exitcode != -signal.SIGKILL:
                raise RuntimeError("Test broker did not exit through SIGKILL")
            observed["crashed_broker_exit_code"] = test.broker_process.exitcode
            print("Test broker killed with SIGKILL")

        #This checks that the committed log survived the interrupted save
        after_crash_bytes = test.event_log.read_bytes()
        json.loads(after_crash_bytes)
        if after_crash_bytes != baseline_bytes:
            raise RuntimeError("Committed log changed before replacement")
        observed["committed_log_valid"] = True
        observed["committed_log_unchanged"] = True
        print("Committed log is valid JSON")
        print("Acknowledged baseline event preserved")
        #This disables the crash checkpoint before restarting the broker
        test.armed.clear()
        test.checkpoint.clear()
        #This restarts sssthe broker using the same saved data
        test.start_broker()
        print("Test broker restarted")
        # subscribe test client to receive the saved baseline event
        with subscribe_client(test, "f01-check") as subscriber:
            replayed_event = read_event(subscriber)
            #This compares the replayed event the original saved eventtt
            if replayed_event != baseline_event:
                raise RuntimeError("Replayed event differs from the baseline")
        observed["replayed_event"] = replayed_event
        observed["replay_matches_baseline"] = True
        print("Replayed event:", replayed_event["event_id"])
        print("Baseline replay verified after broker restart")

def run_f02(observed):
    #This checks that an event replays after its subscriber dies before ACK
    subscriber_id = "responseteam"
    observed["subscriber_id"] = subscriber_id
    #This starts the broker with its crash checkpoint disabled
    with broker_test("F-02", observed) as test:
        print("F-02 test broker is ready")
        saved_event = publish_saved_event(test, {
            "kind": "F-02 unacknowledged event", "sev": 2, "source": "fault_inject",
        })
        event_id = saved_event["event_id"]
        observed["saved_event"] = saved_event
        print("F-02 event saved:", event_id)
        #This records the offset before the subscriber receives the event
        before_offset = read_offset(test.offsets_file, subscriber_id, "sighting")
        observed["offset_before_delivery"] = before_offset
        if before_offset != -1:
            raise RuntimeError("Fresh subscriber already has an ACK offset")
        ack_checkpoint = test.context.Event()
        subscriber_process = test.start_process(
            run_f02_subscriber, (test.src_dir, test.port, ack_checkpoint)
        )
        #This waits until the real subscriber pauses before sending its ACK
        if not ack_checkpoint.wait(timeout=5):
            raise TimeoutError("Subscriber did not reach the ACK checkpoint")
        paused_offset = read_offset(test.offsets_file, subscriber_id, "sighting")
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
        after_kill_offset = read_offset(test.offsets_file, subscriber_id, "sighting")
        observed["offset_after_subscriber_kill"] = after_kill_offset
        if after_kill_offset != before_offset:
            raise RuntimeError("Offset changed despite the missing ACK")
        #This reconnects a harness client using the same subscriber ID
        with subscribe_client(test, subscriber_id) as subscriber:
            replayed_event = read_event(subscriber)
            observed["replayed_event"] = replayed_event
            if replayed_event != saved_event:
                raise RuntimeError("Replayed event differs from the saved event")
            print("Unacknowledged event replayed:", event_id)
            #This acknowledges the replay and waits for the saved offset
            send_line(subscriber, f"ACK|sighting|{subscriber_id}|{event_id}")
            wait_for_offset(test, subscriber_id, 0)
        observed["offset_after_replay_ack"] = 0
        observed["replay_matches_saved_event"] = True
        print("Replay ACK advanced the offset to 0")

def run_f03(observed):
    #Check replay and duplicate suppression after a lost ACK
    subscriber_id = "responseteam"
    observed["subscriber_id"] = subscriber_id
    #Start the isolated broker
    with broker_test("F-03", observed) as test:
        print("F-03 test broker is ready")
        payload = {"kind": "F-03 lost ACK", "sev": 2, "source": "fault_inject"}
        saved_event = publish_saved_event(test, payload)
        event_id = saved_event["event_id"]
        observed["saved_event"] = saved_event
        print("F-03 event saved:", event_id)
        before_offset = read_offset(test.offsets_file, subscriber_id, "sighting")
        observed["offset_before_delivery"] = before_offset
        if before_offset != -1:
            raise RuntimeError("Fresh subscriber already has an ACK offset")
        output_file = test.root / "subscriber-output.txt"
        deliveries_file = test.root / "deliveries.jsonl"
        ack_dropped = test.context.Event()
        allow_reconnect = test.context.Event()
        #Start the subscriber that suppresses its first ACK
        subscriber_process = test.start_process(
            run_f03_subscriber,
            (test.src_dir, test.port, output_file, deliveries_file,
             ack_dropped, allow_reconnect),
        )
        if not ack_dropped.wait(timeout=5):
            raise TimeoutError("Subscriber did not reach the lost-ACK checkpoint")
        dropped_offset = read_offset(test.offsets_file, subscriber_id, "sighting")
        observed["offset_after_dropped_ack"] = dropped_offset
        if dropped_offset != before_offset:
            raise RuntimeError("Offset advanced despite the suppressed ACK")
        print("First ACK suppressed; offset remains -1")
        #Allow the same subscriber process to reconnect
        allow_reconnect.set()
        wait_for_offset(test, subscriber_id, 0, subscriber_process)
        observed["offset_after_replay_ack"] = 0
        print("Replay acknowledged; offset advanced to 0")
        #Stop the subscriber and flush its captured output
        stop_test_process(subscriber_process)
        observed["subscriber_exit_code"] = subscriber_process.exitcode
        if subscriber_process.exitcode != 0:
            raise RuntimeError("Subscriber did not shut down cleanly")
        delivered_events = [
            json.loads(line)
            for line in deliveries_file.read_text(encoding="utf-8").splitlines()
            if line.strip()
        ]
        observed["received_events"] = delivered_events
        if delivered_events != [saved_event, saved_event]:
            raise RuntimeError("Expected the same saved event to arrive twice")
        #Compare received deliveries with displayed events
        output = output_file.read_text(encoding="utf-8")
        printed_events = [line for line in output.splitlines() if line.startswith("Event ")]
        observed["subscriber_output"] = output
        observed["printed_events"] = printed_events
        observed["delivery_count"] = len(delivered_events)
        observed["print_count"] = len(printed_events)
        if (len(printed_events) != 1 or event_id[:8] not in printed_events[0]
                or payload["kind"] not in printed_events[0]):
            raise RuntimeError("Expected the target event to be printed once")
        print("Same event received twice")
        print("Same event printed once")
        print("Duplicate suppression verified")

def publish_f04_events(test_port, publisher_name, start_barrier):
    #Publish identifiable events after both publishers are connected
    accepted_events = []
    with socket.create_connection(("127.0.0.1", test_port), timeout=5) as publisher:
        start_barrier.wait(timeout=5)
        for number in range(10):
            payload = {
                "kind": f"F-04 {publisher_name} event {number}",
                "sev": 2, "source": publisher_name, "note": "a|b " * 100,
            }
            event_id = send_publish(publisher, "sighting", payload)
            accepted_events.append({"event_id": event_id, "payload": payload})
    return accepted_events

def run_f04(observed):
    #Check concurrent publishes against the saved event order
    subscriber_id = "f04-check"
    observed["events_per_publisher"] = 10
    with broker_test("F-04", observed) as test:
        print("F-04 test broker is ready")
        #Save a warmup event to confirm the subscriber becomes ready
        warmup_event = publish_saved_event(test, {
            "kind": "F-04 warmup", "sev": 2, "source": "fault_inject",
        })
        warmup_id = warmup_event["event_id"]
        observed["warmup_event_id"] = warmup_id
        with subscribe_client(test, subscriber_id) as subscriber:
            if read_event(subscriber) != warmup_event:
                raise RuntimeError("Warmup event does not match the publish")
            #Wait for the warmup ACK to confirm subscription processing finished
            send_line(subscriber, f"ACK|sighting|{subscriber_id}|{warmup_id}")
            wait_for_offset(test, subscriber_id, 0)
            print("F-04 subscriber is ready")
            #Start both publishers and receive their events concurrently
            start_barrier = threading.Barrier(3)
            with ThreadPoolExecutor(max_workers=2) as workers:
                first = workers.submit(publish_f04_events, test.port, "publisher-a", start_barrier)
                second = workers.submit(publish_f04_events, test.port, "publisher-b", start_barrier)
                start_barrier.wait(timeout=5)
                received_events = [read_event(subscriber) for _ in range(20)]
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
        observed["publisher_a_event_ids"] = [event["event_id"] for event in first_events]
        observed["publisher_b_event_ids"] = [event["event_id"] for event in second_events]
        observed["received_event_ids"] = [event.get("event_id") for event in received_events]
        #Compare the complete received events with their committed log entries
        stored_events = json.loads(test.event_log.read_text(encoding="utf-8")).get("sighting", [])
        if len(stored_events) != 21:
            raise RuntimeError("Expected one warmup and 20 concurrent events")
        if stored_events[0] != warmup_event:
            raise RuntimeError("Warmup event changed in the log")
        concurrent_events = stored_events[1:]
        observed["stored_event_ids"] = [event.get("event_id") for event in concurrent_events]
        if received_events != concurrent_events:
            raise RuntimeError("Subscriber events differ from the saved log order")
        accepted_by_id = {
            event["event_id"]: event["payload"] for event in first_events + second_events
        }
        if len(accepted_by_id) != 20:
            raise RuntimeError("Expected 20 unique acknowledged event IDs")
        for event in concurrent_events:
            event_id = event.get("event_id")
            if event_id not in accepted_by_id:
                raise RuntimeError("Log contains an unrecognized event ID")
            if event.get("topic") != "sighting" or event.get("payload") != accepted_by_id[event_id]:
                raise RuntimeError("Stored event differs from its published payload")
        observed["received_count"] = len(received_events)
        observed["stored_concurrent_count"] = len(concurrent_events)
        observed["delivery_matches_log_order"] = True
        observed["all_payloads_preserved"] = True
        print("Both publishers completed 10 events")
        print("All 20 events received and stored")
        print("Delivery order matches the saved log")
        print("All payloads preserved")

def run_f05(observed):
    #Check saved events and ACK offsets after a broker restart
    subscriber_id = "f05-check"
    observed["subscriber_id"] = subscriber_id
    with broker_test("F-05", observed) as test:
        print("F-05 test broker is ready")
        #Save three acknowledged publishes before crashing the broker
        payloads = [
            {"kind": f"F-05 restart event {number}", "sev": 2, "source": "fault_inject"}
            for number in range(1, 4)
        ]
        saved_events = publish_batch(test, payloads)
        event_ids = [event["event_id"] for event in saved_events]
        if len(load_events(test)) != 3 or len(set(event_ids)) != 3:
            raise RuntimeError("Expected three unique saved events")
        observed["saved_events"] = saved_events
        print("Three events saved:", event_ids)
        #Acknowledge only the first event before the broker crash
        replay_and_ack(test, subscriber_id, saved_events[:1], 0)
        observed["offset_before_crash"] = 0
        log_before = test.event_log.read_bytes()
        offsets_before = load_offsets(test)
        observed["offsets_before_crash"] = offsets_before
        print("First event acknowledged; offset is 0")
        #Kill the broker after events and offsets have been saved
        test.broker_process.kill()
        test.broker_process.join(timeout=5)
        if test.broker_process.exitcode != -signal.SIGKILL:
            raise RuntimeError("Broker did not exit through SIGKILL")
        observed["crashed_broker_exit_code"] = test.broker_process.exitcode
        print("Broker killed with SIGKILL")
        #Restart using the same event log and offsets
        test.start_broker()
        if test.event_log.read_bytes() != log_before:
            raise RuntimeError("Saved event log changed across restart")
        offsets_after = load_offsets(test)
        observed["offsets_after_restart"] = offsets_after
        if offsets_after != offsets_before:
            raise RuntimeError("Saved offsets changed across restart")
        print("Broker restarted with saved events and offsets")
        #Reconnect with the same ID and read only the two pending events
        with subscribe_client(test, subscriber_id) as subscriber:
            replayed_events = [read_event(subscriber) for _ in range(2)]
            observed["replayed_events"] = replayed_events
            if replayed_events != saved_events[1:]:
                raise RuntimeError("Replay did not match the pending events")
            for index, event in enumerate(replayed_events, start=1):
                acknowledge_event(test, subscriber, subscriber_id, event, index)
            #Check that the acknowledged first event was not replayed again
            expect_no_data(subscriber)
        observed["offset_after_replay_ack"] = 2
        observed["event_log_preserved"] = True
        observed["saved_offsets_preserved"] = True
        observed["replay_matches_pending_events"] = True
        print("Only the two pending events replayed in order")
        print("Replay ACKs advanced the offset to 2")
        #Stop the test broker before removing temporary data

def run_f06(observed):
    #Check unread events remain available while all subscribers are offline
    slow_id, fast_id = "f06-slow", "f06-fast"
    observed["retention_policy"] = "Keep all events; no pruning"
    with broker_test("F-06", observed) as test:
        print("F-06 test broker is ready")
        payloads = [
            {"kind": f"F-06 retained event {number}", "sev": 2, "source": "fault_inject"}
            for number in range(1, 6)
        ]
        initial_events = publish_batch(test, payloads[:3])
        if len(load_events(test)) != 3:
            raise RuntimeError("Expected exactly three initial events")
        #Give the subscribers different acknowledged positions
        replay_and_ack(test, slow_id, initial_events[:1], 0)
        replay_and_ack(test, fast_id, initial_events, 2)
        offsets_before = load_offsets(test)
        observed["offsets_before_offline_publish"] = offsets_before
        observed["initial_events"] = initial_events
        print("Slow subscriber offset: 0")
        print("Fast subscriber offset: 2")
        print("Both subscribers disconnected")
        #Publish more events while both subscriber connections are closed
        offline_events = publish_batch(test, payloads[3:])
        retained_events = load_events(test)
        observed["offline_events"] = offline_events
        observed["retained_events"] = retained_events
        if retained_events != initial_events + offline_events:
            raise RuntimeError("Previously saved events were lost or changed")
        offsets_after = load_offsets(test)
        observed["offsets_after_offline_publish"] = offsets_after
        if offsets_after != offsets_before:
            raise RuntimeError("Offsets changed while subscribers were offline")
        print("All five events retained")
        print("Offline subscriber offsets unchanged")
        #Verify each subscriber recovers all events after its own offset
        observed["slow_subscriber_replay"] = replay_and_ack(
            test, slow_id, retained_events[1:], 4, check_end=True
        )
        observed["fast_subscriber_replay"] = replay_and_ack(
            test, fast_id, retained_events[3:], 4, check_end=True
        )
        observed["final_offsets"] = load_offsets(test)
        observed["all_events_retained"] = True
        print("Slow subscriber recovered four unread events")
        print("Fast subscriber recovered two unread events")
        print("Both subscriber offsets advanced to 4")
        #Stop the test broker before removing temporary data

def run_f07(observed):
    #Check that a duplicate subscriber ID replaces the old connection
    subscriber_id = "f07-check"
    observed["subscriber_id"] = subscriber_id
    with broker_test("F-07", observed) as test:
        print("F-07 test broker is ready")
        warmup_event = publish_saved_event(test, {
            "kind": "F-07 warmup", "sev": 2, "source": "fault_inject",
        })
        with subscribe_client(test, subscriber_id) as old_subscriber:
            if read_event(old_subscriber) != warmup_event:
                raise RuntimeError("Old subscriber received the wrong warmup")
            acknowledge_event(test, old_subscriber, subscriber_id, warmup_event, 0)
            observed["offset_before_replacement"] = 0
            print("Old subscriber registered and acknowledged warmup")
            #Replace the old connection using the same subscriber ID
            with subscribe_client(test, subscriber_id) as new_subscriber:
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
                live_event = publish_batch(test, [{
                    "kind": "F-07 replacement delivery", "sev": 2, "source": "fault_inject",
                }])[0]
                received_event = read_event(new_subscriber)
                observed["published_event"] = live_event
                observed["replacement_received_event"] = received_event
                if received_event != live_event:
                    raise RuntimeError("Replacement received the wrong event")
                acknowledge_event(test, new_subscriber, subscriber_id, live_event, 1)
                observed["offset_after_replacement_ack"] = 1
                #Check that the replacement receives no extra copy
                expect_no_data(new_subscriber)
                observed["extra_data_observation_seconds"] = 0.3
                observed["replacement_received_one_copy"] = True
        if load_events(test) != [warmup_event, live_event]:
            raise RuntimeError("Stored events changed during replacement")
        print("New subscriber received the published event once")
        print("Replacement ACK advanced the offset to 1")
        #Stop the test broker before removing temporary data

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
    observed["shutdown_budget_seconds"] = DRAIN_S
    with broker_test("F-08", observed) as test:
        print("F-08 test broker is ready")
        output_file = test.root / "subscriber-output.txt"
        shutdown_started = threading.Event()
        stop_load = threading.Event()
        first_ready = threading.Event()
        second_ready = threading.Event()
        start_barrier = threading.Barrier(3)
        try:
            #Save a warmup event to verify subscriber readiness
            warmup_payload = {"kind": "F-08 warmup", "sev": 2, "source": "fault_inject"}
            warmup_event = publish_saved_event(test, warmup_payload)
            warmup_id = warmup_event["event_id"]
            observed["warmup_event_id"] = warmup_id
            subscriber_process = test.start_process(
                run_f08_subscriber, (test.src_dir, test.port, output_file)
            )
            wait_for_offset(test, subscriber_id, 0, subscriber_process)
            print("F-08 subscriber is ready")
            #Start two publishers and request shutdown during their traffic
            with ThreadPoolExecutor(max_workers=2) as workers:
                first = workers.submit(
                    publish_f08_load, test.port, "publisher-a", start_barrier,
                    shutdown_started, stop_load, first_ready,
                )
                second = workers.submit(
                    publish_f08_load, test.port, "publisher-b", start_barrier,
                    shutdown_started, stop_load, second_ready,
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
                    os.kill(test.broker_process.pid, signal.SIGINT)
                    test.broker_process.join(timeout=DRAIN_S)
                    duration = time.monotonic() - shutdown_time
                    observed["shutdown_duration_seconds"] = duration
                    observed["broker_exit_code"] = test.broker_process.exitcode
                    if test.broker_process.is_alive() or duration > DRAIN_S:
                        raise TimeoutError("Broker exceeded its shutdown budget")
                    if test.broker_process.exitcode != 0:
                        raise RuntimeError("Broker did not exit with status 0")
                finally:
                    stop_load.set()
                first_result = first.result(timeout=6)
                second_result = second.result(timeout=6)
            #Collect acknowledged and interrupted publishes from both workers
            accepted = first_result["accepted"] + second_result["accepted"]
            interrupted = first_result["interrupted"] + second_result["interrupted"]
            observed["publisher_a_acknowledged"] = len(first_result["accepted"])
            observed["publisher_b_acknowledged"] = len(second_result["accepted"])
            observed["acknowledged_publishes"] = accepted
            observed["interrupted_publishes"] = interrupted
            if len(first_result["accepted"]) < 5 or len(second_result["accepted"]) < 5:
                raise RuntimeError("Insufficient acknowledged traffic")
            #Stop the subscriber after the broker shutdown and flush its output
            stop_test_process(subscriber_process)
            observed["subscriber_exit_code"] = subscriber_process.exitcode
            if subscriber_process.exitcode != 0:
                raise RuntimeError("Subscriber did not shut down cleanly")
            observed["subscriber_output"] = output_file.read_text(encoding="utf-8")
            #Verify valid files and preservation of every acknowledged publish
            saved_events = load_events(test)
            stored_by_id = {event["event_id"]: event for event in saved_events}
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
                    raise RuntimeError(f"Acknowledged event missing after shutdown: {event_id}")
                if saved_event.get("topic") != "sighting" or saved_event.get("payload") != payload:
                    raise RuntimeError("Acknowledged event changed on disk")
            saved_offsets = load_offsets(test)
            last_offset = saved_offsets.get(subscriber_id, {}).get("sighting", -1)
            if type(last_offset) is not int or not 0 <= last_offset < len(saved_events):
                raise RuntimeError("Stored subscriber offset is invalid")
            observed["saved_offsets"] = saved_offsets
            observed["stored_event_count"] = len(saved_events)
            observed["acknowledged_events_preserved"] = True
            observed["saved_files_valid_json"] = True
            observed["stored_without_pub_ack"] = [
                event_id for event_id in stored_by_id if event_id not in expected_payloads
            ]
            print("Broker exited with status 0")
            print("Shutdown duration:", duration)
            print("All acknowledged events preserved")
            print("Saved log and offsets are valid JSON")
            print("Interrupted publishes:", len(interrupted))
        finally:
            #Clean up both processes even when a shutdown check fails
            stop_load.set()

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


    print(args.case, "result:", verdict)
    print(json.dumps(result, indent=2))

    

    return 0 if verdict == "PASS" else 1

if __name__ == "__main__":
    sys.exit(main())
