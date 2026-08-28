#!/usr/bin/env python3
"""Minimal, allow-listed HTTP CONNECT relay for the UOM WMS endpoint.

The relay is intended to run on the operator's workstation and be reached
through an SSH reverse port-forward.  It deliberately accepts only one
destination (``uom.caac.gov.cn:443``), never logs request headers or tunnel
bytes, and binds to loopback by default.  It is not a general-purpose proxy.
"""

from __future__ import annotations

import argparse
import ipaddress
import select
import signal
import socket
import socketserver
import sys
import threading
import time
from typing import Optional, Tuple


TARGET_HOST = "uom.caac.gov.cn"
TARGET_PORT = 443
DEFAULT_BIND = "127.0.0.1"
DEFAULT_PORT = 19090
MAX_HEADER_BYTES = 8 * 1024
BUFFER_BYTES = 64 * 1024
MAX_PENDING_BYTES = 4 * BUFFER_BYTES
DEFAULT_IDLE_TIMEOUT = 45.0
DEFAULT_MAX_CONNECTIONS = 8


def allowed_target(target: str) -> bool:
    """Return whether *target* is exactly the single permitted authority."""
    if not isinstance(target, str):
        return False
    host, separator, port = target.rpartition(":")
    return bool(separator and host.lower() == TARGET_HOST and port == str(TARGET_PORT))


def parse_connect_request(raw: bytes) -> Tuple[str, str]:
    """Parse a bounded HTTP CONNECT header and return (target, version).

    No header value is returned to callers.  This prevents accidental logging
    of a credential if a client sends one in a non-standard header.
    """
    if not isinstance(raw, bytes) or len(raw) > MAX_HEADER_BYTES:
        raise ValueError("header_too_large")
    marker = raw.find(b"\r\n\r\n")
    if marker < 0:
        raise ValueError("incomplete_header")
    header = raw[:marker].decode("ascii", errors="strict")
    lines = header.split("\r\n")
    if not lines or len(lines[0].split()) != 3:
        raise ValueError("malformed_request_line")
    method, target, version = lines[0].split()
    if method.upper() != "CONNECT":
        raise PermissionError("connect_required")
    if version not in {"HTTP/1.0", "HTTP/1.1"}:
        raise ValueError("unsupported_http_version")
    if not allowed_target(target):
        raise PermissionError("target_not_allowed")
    return target, version


def _response(status: str) -> bytes:
    return (
        f"HTTP/1.1 {status}\r\n"
        "Connection: close\r\n"
        "Content-Length: 0\r\n"
        "\r\n"
    ).encode("ascii")


class RelayServer(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True

    def __init__(self, address, handler, *, idle_timeout: float, max_connections: int):
        super().__init__(address, handler)
        self.idle_timeout = idle_timeout
        self.connection_slots = threading.BoundedSemaphore(max_connections)


class RelayHandler(socketserver.BaseRequestHandler):
    """Handle one CONNECT request and copy bytes until either side closes."""

    server: RelayServer

    def _reject(self, status: str) -> None:
        try:
            self.request.sendall(_response(status))
        except OSError:
            pass

    def handle(self) -> None:
        if not self.server.connection_slots.acquire(blocking=False):
            self._reject("503 Service Unavailable")
            return
        upstream: Optional[socket.socket] = None
        try:
            self.request.settimeout(5.0)
            raw = bytearray()
            while b"\r\n\r\n" not in raw:
                chunk = self.request.recv(min(2048, MAX_HEADER_BYTES + 1 - len(raw)))
                if not chunk:
                    self._reject("400 Bad Request")
                    return
                raw.extend(chunk)
                if len(raw) > MAX_HEADER_BYTES:
                    self._reject("431 Request Header Fields Too Large")
                    return
            try:
                target, _version = parse_connect_request(bytes(raw))
            except PermissionError as exc:
                self._reject(
                    "405 Method Not Allowed" if str(exc) == "connect_required"
                    else "403 Forbidden"
                )
                return
            except (UnicodeError, ValueError):
                self._reject("400 Bad Request")
                return

            # Resolve the fixed hostname here, never a client-supplied host.
            # The target string is retained only for the local connection call;
            # no request URL, headers, or tunnel data is written to logs.
            del target
            try:
                upstream = socket.create_connection(
                    (TARGET_HOST, TARGET_PORT), timeout=10.0
                )
            except OSError:
                self._reject("502 Bad Gateway")
                return
            self.request.sendall(
                b"HTTP/1.1 200 Connection Established\r\n"
                b"Connection: keep-alive\r\n"
                b"\r\n"
            )
            self._tunnel(upstream)
        finally:
            for sock in (upstream, self.request):
                if sock is not None:
                    try:
                        sock.shutdown(socket.SHUT_RDWR)
                    except OSError:
                        pass
                    try:
                        sock.close()
                    except OSError:
                        pass
            self.server.connection_slots.release()

    def _tunnel(self, upstream: socket.socket) -> None:
        self.request.setblocking(False)
        upstream.setblocking(False)
        sockets = (self.request, upstream)
        peer = {self.request: upstream, upstream: self.request}
        read_open = {sock: True for sock in sockets}
        write_shutdown = {sock: False for sock in sockets}
        pending = {sock: bytearray() for sock in sockets}
        last_activity = time.monotonic()

        while True:
            if time.monotonic() - last_activity > self.server.idle_timeout:
                return

            for source, destination in peer.items():
                if (not read_open[source] and not pending[destination]
                        and not write_shutdown[destination]):
                    try:
                        destination.shutdown(socket.SHUT_WR)
                    except OSError:
                        pass
                    write_shutdown[destination] = True

            if not any(read_open.values()) and not any(pending.values()):
                return

            readable_sockets = [
                source for source in sockets
                if read_open[source] and len(pending[peer[source]]) < MAX_PENDING_BYTES
            ]
            writable_sockets = [
                destination for destination in sockets
                if pending[destination] and not write_shutdown[destination]
            ]
            if not readable_sockets and not writable_sockets:
                return

            try:
                readable, writable, _ = select.select(
                    readable_sockets, writable_sockets, (), 1.0,
                )
            except (OSError, ValueError):
                return

            for destination in writable:
                try:
                    sent = destination.send(pending[destination])
                except (BlockingIOError, InterruptedError):
                    continue
                except OSError:
                    return
                if sent <= 0:
                    return
                del pending[destination][:sent]
                last_activity = time.monotonic()

            for source in readable:
                destination = peer[source]
                available = MAX_PENDING_BYTES - len(pending[destination])
                if available <= 0:
                    continue
                try:
                    data = source.recv(min(BUFFER_BYTES, available))
                except (BlockingIOError, InterruptedError):
                    continue
                except OSError:
                    return
                if not data:
                    read_open[source] = False
                    continue
                pending[destination].extend(data)
                last_activity = time.monotonic()


def _validate_bind(value: str) -> str:
    try:
        address = ipaddress.ip_address(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("bind must be 127.0.0.1 or ::1") from exc
    if not address.is_loopback:
        raise argparse.ArgumentTypeError("relay must bind to loopback")
    return value


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="UOM-only HTTPS CONNECT relay")
    parser.add_argument("--bind", default=DEFAULT_BIND, type=_validate_bind)
    parser.add_argument("--port", default=DEFAULT_PORT, type=int)
    parser.add_argument("--idle-timeout", default=DEFAULT_IDLE_TIMEOUT, type=float)
    parser.add_argument("--max-connections", default=DEFAULT_MAX_CONNECTIONS, type=int)
    return parser


def run(argv=None) -> int:
    args = build_parser().parse_args(argv)
    if not 1024 <= args.port <= 65535:
        raise SystemExit("--port must be between 1024 and 65535")
    if args.idle_timeout <= 0 or args.idle_timeout > 600:
        raise SystemExit("--idle-timeout must be between 0 and 600 seconds")
    if not 1 <= args.max_connections <= 64:
        raise SystemExit("--max-connections must be between 1 and 64")

    server = RelayServer(
        (args.bind, args.port), RelayHandler,
        idle_timeout=args.idle_timeout,
        max_connections=args.max_connections,
    )

    def stop(_signum, _frame):
        # shutdown() must run outside the request thread on some Windows
        # Python builds; a tiny daemon keeps signal handling non-blocking.
        threading.Thread(target=server.shutdown, daemon=True).start()

    for signum in (getattr(signal, "SIGINT", None), getattr(signal, "SIGTERM", None)):
        if signum is not None:
            try:
                signal.signal(signum, stop)
            except (ValueError, OSError):
                pass
    print(
        f"UOM CONNECT relay listening on {args.bind}:{args.port}; "
        f"allow-list={TARGET_HOST}:{TARGET_PORT}",
        flush=True,
    )
    try:
        server.serve_forever(poll_interval=0.5)
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
    return 0


if __name__ == "__main__":
    sys.exit(run())
