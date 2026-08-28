#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Restricted local CONNECT relay for the authorized UOM HTTPS upstream.

The production server cannot reach the UOM network directly.  This process is
intended to run on the operator's China-connected workstation and be reached
only through an SSH reverse tunnel.  It deliberately understands CONNECT
only, permits one fixed host/port, and never logs request headers or URLs.
"""

from __future__ import annotations

import argparse
import select
import socket
import socketserver
import threading
import time
from urllib.parse import urlsplit


DEFAULT_TARGET_HOST = "uom.caac.gov.cn"
DEFAULT_TARGET_PORT = 443
MAX_HEADER_BYTES = 16 * 1024
CONNECT_TIMEOUT_SECONDS = 12.0
RELAY_IDLE_SECONDS = 120.0
BUFFER_SIZE = 64 * 1024


class RelayError(Exception):
    pass


def _read_headers(client: socket.socket) -> bytes:
    client.settimeout(10.0)
    data = bytearray()
    while len(data) < MAX_HEADER_BYTES:
        chunk = client.recv(min(4096, MAX_HEADER_BYTES - len(data)))
        if not chunk:
            break
        data.extend(chunk)
        if b"\r\n\r\n" in data:
            return bytes(data)
    raise RelayError("request headers are incomplete or too large")


def _parse_connect(request: bytes, allowed_host: str, allowed_port: int) -> None:
    first_line, separator, headers = request.partition(b"\r\n")
    if not separator:
        raise RelayError("malformed proxy request")
    try:
        method, authority, version = first_line.decode("ascii").split(" ", 2)
    except (UnicodeDecodeError, ValueError) as exc:
        raise RelayError("malformed proxy request") from exc
    if method.upper() != "CONNECT" or version.upper() not in {"HTTP/1.0", "HTTP/1.1"}:
        raise RelayError("only CONNECT is supported")
    if "@" in authority or "/" in authority or "?" in authority or "#" in authority:
        raise RelayError("invalid CONNECT authority")
    parsed = urlsplit("//" + authority)
    if parsed.username or parsed.password or parsed.hostname is None:
        raise RelayError("invalid CONNECT authority")
    try:
        port = parsed.port
    except ValueError as exc:
        raise RelayError("invalid CONNECT port") from exc
    if parsed.hostname.casefold() != allowed_host.casefold() or port != allowed_port:
        raise RelayError("destination is not allowed")


def _send_error(client: socket.socket, status: str) -> None:
    try:
        client.sendall(
            f"HTTP/1.1 {status}\r\nConnection: close\r\nContent-Length: 0\r\n\r\n".encode("ascii")
        )
    except OSError:
        pass


def _relay_bidirectional(left: socket.socket, right: socket.socket) -> None:
    sockets = [left, right]
    deadline = time.monotonic() + RELAY_IDLE_SECONDS
    while sockets and time.monotonic() < deadline:
        try:
            readable, _, exceptional = select.select(sockets, [], sockets, 5.0)
        except (OSError, ValueError):
            return
        if exceptional:
            return
        if not readable:
            continue
        for source in readable:
            try:
                payload = source.recv(BUFFER_SIZE)
            except (OSError, socket.timeout):
                return
            if not payload:
                return
            target = right if source is left else left
            try:
                target.sendall(payload)
            except OSError:
                return
            deadline = time.monotonic() + RELAY_IDLE_SECONDS


class _RelayHandler(socketserver.BaseRequestHandler):
    def handle(self) -> None:
        client = self.request
        server = self.server
        try:
            request = _read_headers(client)
            _parse_connect(request, server.allowed_host, server.allowed_port)
            upstream = socket.create_connection(
                (server.allowed_host, server.allowed_port), timeout=CONNECT_TIMEOUT_SECONDS
            )
        except (OSError, RelayError):
            _send_error(client, "403 Forbidden")
            return

        try:
            client.sendall(
                b"HTTP/1.1 200 Connection Established\r\n"
                b"Connection: keep-alive\r\n"
                b"Proxy-Agent: RID-UOM-Relay\r\n\r\n"
            )
            _relay_bidirectional(client, upstream)
        finally:
            try:
                upstream.close()
            except OSError:
                pass


class RelayServer(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True

    def __init__(self, address, allowed_host: str, allowed_port: int):
        self.allowed_host = allowed_host
        self.allowed_port = allowed_port
        super().__init__(address, _RelayHandler)


def main() -> None:
    parser = argparse.ArgumentParser(description="Restricted UOM HTTPS CONNECT relay")
    parser.add_argument("--listen-host", default="127.0.0.1")
    parser.add_argument("--listen-port", type=int, default=19181)
    parser.add_argument("--allowed-host", default=DEFAULT_TARGET_HOST)
    parser.add_argument("--allowed-port", type=int, default=DEFAULT_TARGET_PORT)
    args = parser.parse_args()
    if args.listen_port < 1 or args.listen_port > 65535:
        parser.error("--listen-port is out of range")
    if args.allowed_port < 1 or args.allowed_port > 65535:
        parser.error("--allowed-port is out of range")
    if args.allowed_host.casefold() != DEFAULT_TARGET_HOST:
        parser.error("only the UOM hostname is supported")

    with RelayServer((args.listen_host, args.listen_port), args.allowed_host, args.allowed_port) as server:
        print(
            f"[uom-relay] listening on {args.listen_host}:{args.listen_port}; "
            f"destination restricted to {args.allowed_host}:{args.allowed_port}",
            flush=True,
        )
        try:
            server.serve_forever(poll_interval=0.5)
        except KeyboardInterrupt:
            pass


if __name__ == "__main__":
    main()
