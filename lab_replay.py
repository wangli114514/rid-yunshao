#!/usr/bin/env python3
"""Control the firmware's RF-disabled frame loopback over USB serial.

This helper intentionally has no network or Wi-Fi functionality.  It only
writes the documented ``frame loopback`` commands to a connected board and
prints the board's serial diagnostics.  The firmware performs the actual
protocol build and feeds the bytes to its in-memory decoder.
"""

from __future__ import annotations

import argparse
import sys
import time


def _command(args: argparse.Namespace) -> str:
    if args.action == "status":
        return "frame loopback status"
    if args.action == "stop":
        return "frame loopback off"
    if args.action == "once":
        return "frame loopback once " + args.kind
    return "frame loopback on %s %d" % (args.kind, args.interval)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="T-Display-S3 RF-disabled RID frame loopback controller"
    )
    parser.add_argument("--port", required=True, help="serial port, for example COM6")
    parser.add_argument("--baud", type=int, default=115200)
    parser.add_argument(
        "action", choices=("status", "once", "start", "enable", "stop"),
        help="query, inject one batch, run continuously, arm continuous mode, or stop",
    )
    parser.add_argument("--kind", choices=("pack", "beacon", "nan", "all"), default="beacon")
    parser.add_argument("--interval", type=int, default=1000, help="loop interval in milliseconds")
    parser.add_argument("--quiet", action="store_true", help="do not print non-JSON diagnostics")
    args = parser.parse_args()
    if not 200 <= args.baud <= 2_000_000:
        parser.error("--baud out of range")
    if args.action in {"start", "enable"} and not 100 <= args.interval <= 60_000:
        parser.error("--interval must be between 100 and 60000 ms")

    try:
        import serial
    except ImportError:
        print("pyserial is required; run .venv\\Scripts\\python.exe -m pip install -r requirements.txt", file=sys.stderr)
        return 2

    try:
        port = serial.Serial(args.port, args.baud, timeout=0.25)
    except serial.SerialException as exc:
        print("cannot open %s: %s" % (args.port, exc), file=sys.stderr)
        return 1

    # Opening the native USB CDC port can reset the board.  Give the firmware
    # a short window to finish its boot-selection page before issuing a command.
    time.sleep(1.0)
    port.reset_input_buffer()
    command = _command(args).encode("ascii") + b"\n"
    port.write(command)
    port.flush()
    deadline = time.monotonic() + (1.5 if args.action not in {"start", "enable"} else 2.0)
    try:
        while True:
            line = port.readline()
            if line:
                text = line.decode("utf-8", errors="replace").rstrip()
                if not args.quiet or text.startswith("{") or text.startswith("[frame] loopback"):
                    print(text)
            if args.action != "start" and time.monotonic() >= deadline:
                break
            if args.action == "start" and getattr(port, "is_open", True):
                # Keep streaming until Ctrl-C.  The firmware emits snapshots
                # once per second while the loopback remains enabled.
                continue
    except KeyboardInterrupt:
        if args.action == "start":
            try:
                port.write(b"frame loopback off\n")
                port.flush()
            except serial.SerialException:
                pass
            print("[lab] loopback stopped")
    finally:
        port.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
