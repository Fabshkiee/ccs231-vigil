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


def main():
    #reads the selected fault case and displays it
    cases = {
        "F-01": run_f01,
        "F-02": run_f02,
        "F-03": run_f03,
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