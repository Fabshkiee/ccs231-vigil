#!/usr/bin/env python3
"""Find the demo broker on the LAN. Plain UDP, no authentication."""
import argparse
import socket
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "src"))

from common.constants import PORT  # noqa: E402

BEACON_PORT = 5002
MAGIC = "VIGIL-BROKER"
INTERVAL = 2.0


def lan_ip():
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        sock.connect(("8.8.8.8", 80))
        return sock.getsockname()[0]
    except OSError:
        return "127.0.0.1"
    finally:
        sock.close()


def beacon():
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
    payload = f"{MAGIC}|{lan_ip()}|{PORT}".encode()
    print(f"beacon: {payload.decode()}", flush=True)
    while True:
        sock.sendto(payload, ("255.255.255.255", BEACON_PORT))
        sock.sendto(payload, ("127.0.0.1", BEACON_PORT))
        time.sleep(INTERVAL)


def discover(wait):
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind(("", BEACON_PORT))
    deadline = time.time() + wait
    while time.time() < deadline:
        sock.settimeout(max(0.1, deadline - time.time()))
        try:
            data, _ = sock.recvfrom(1024)
        except socket.timeout:
            break
        parts = data.decode("utf-8", "replace").split("|")
        if (len(parts) == 3 and parts[0] == MAGIC
                and parts[2].isdigit() and int(parts[2]) == PORT):
            print(parts[1])
            return 0
    return 1


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("beacon", help="broadcast this machine's broker address")
    find = sub.add_parser("discover", help="print the first broker address found")
    find.add_argument("--wait", type=float, default=5.0, help="seconds to listen")
    args = parser.parse_args(argv)
    if args.command == "beacon":
        beacon()
        return 0
    return discover(args.wait)


if __name__ == "__main__":
    sys.exit(main())
