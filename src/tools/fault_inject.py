# Test harness for the failure cases. Starts its own broker on a
# spare port, breaks exactly one thing, checks what actually happened, and
# writes the row in the report.
# A case that still needs you to kill a window by hand isn't a case yet.






from pathlib import Path
import argparse
import tempfile
import socket
import sys

def run_test_broker(src_dir, test_port, event_log, offsets_file):
    sys.path.insert(0, str(src_dir))

    import common.constants as constants

    constants.PORT = test_port
    constants.EVENT_LOG_FILE = str(event_log)
    constants.OFFSETS_FILE = str(offsets_file)

    import broker

    broker.BROKER_HOST = "127.0.0.1"
    broker.start_broker()



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


if __name__ == "__main__":
    main()