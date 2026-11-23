# Test harness for the failure cases. Starts its own broker on a
# spare port, breaks exactly one thing, checks what actually happened, and
# writes the row in the report.
# A case that still needs you to kill a window by hand isn't a case yet.

import argparse
import tempfile 
import socket



def main():
    #This reads the selected fault case and displays it
    parser = argparse.ArgumentParser(description="VIGIL QA Tests")
    parser.add_argument("--case", required=True, choices=["F-01"])



    args = parser.parse_args()
    print("Selected case:", args.case)


     #Create temp folder for fault testss
    with tempfile.TemporaryDirectory(prefix="vigil-f01-") as test_dir:
                print("Test folder:", test_dir)


    # Ask the operating system for an available local porrrt
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as test_socket:
            test_socket.bind(("127.0.0.1", 0))
            test_port = test_socket.getsockname()[1]

            print("Test port:", test_port)

if __name__ == "__main__":
    main(

    )

