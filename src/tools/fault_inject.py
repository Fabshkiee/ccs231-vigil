# Test harness for the failure cases. Starts its own broker on a
# spare port, breaks exactly one thing, checks what actually happened, and
# writes the row in the report.
# A case that still needs you to kill a window by hand isn't a case yet.

import argparse



def main():
    #This reads the selected fault case and displays it
    parser = argparse.ArgumentParser(description="VIGIL QA Tests")
    parser.add_argument("--case", required=True, choices=["F-01"])

    args = parser.parse_args()
    print("Selected case:", args.case)

if __name__ == "__main__":
    main()


