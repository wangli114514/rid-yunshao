#!/usr/bin/env python3
"""Bridge one Docker-host address to the loopback UOM CONNECT relay.

This process is meant to run on the 216 host.  It binds only to the named
Docker bridge address, accepts only clients from that bridge subnet, and
forwards bytes to the loopback-only ``uom_connect_relay.py`` listener.  The
actual destination allow-list and TLS boundary remain in that relay.
"""

from __future__ import annotations

import argparse
import ipaddress
import select
import socket
import socketserver
import threading
import time


DEFAULT_BIND = "172.22.0.1"
DEFAULT_SOURCE_NETWORK = "172.22.0.0/16"
DEFAULT_TARGET = ("127.0.0.1", 19090)
DEFAULT_LISTEN_PORT = 19091
MAX_CONNECTIONS = 16
IDLE_TIMEOUT = 180.0
BUFFER_SIZE = 64 * 1024
MAX_PENDING_BYTES = 4 * BUFFER_SIZE


def _validate_ip(value: str) -> str:
    try:
        address = ipaddress.ip_address(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("bind must be an IPv4 address") from exc
    if address.version != 4 or address.is_loopback or address.is_unspecified:
        raise argparse.ArgumentTypeError("bind must be a specific non-loopback IPv4 address")
    return value


def _validate_network(value: str) -> ipaddress.IPv4Network:
    try:
        network = ipaddress.ip_network(value, strict=False)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("source-network must be an IPv4 network") from exc
    if network.version != 4:
        raise argparse.ArgumentTypeError("source-network must be IPv4")
    return network


class BridgeServer(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True

    def __init__(self, address, handler, *, source_network, target_host, target_port):
        super().__init__(address, handler)
        self.source_network = source_network
        self.target = (target_host, target_port)
        self.slots = threading.BoundedSemaphore(MAX_CONNECTIONS)


class BridgeHandler(socketserver.BaseRequestHandler):
    server: BridgeServer

    def handle(self) -> None:
        peer_ip = self.client_address[0]
        try:
            peer = ipaddress.ip_address(peer_ip)
        except ValueError:
            print("[uom-bridge] rejected invalid peer address", flush=True)
            return
        if peer not in self.server.source_network:
            print(f"[uom-bridge] rejected source={peer}", flush=True)
            return
        if not self.server.slots.acquire(blocking=False):
            print("[uom-bridge] rejected connection limit reached", flush=True)
            return
        upstream = None
        try:
            upstream = socket.create_connection(self.server.target, timeout=10.0)
            self._tunnel(upstream)
        except (OSError, TimeoutError) as exc:
            # Never log tunneled bytes or request headers; the exception class
            # is sufficient to distinguish a broken local forward from a
            # rejected client without exposing credentials.
            print(f"[uom-bridge] transport error={type(exc).__name__}", flush=True)
            return
        finally:
            for sock in (upstream, self.request):
                if sock is None:
                    continue
                try:
                    sock.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass
                try:
                    sock.close()
                except OSError:
                    pass
            self.server.slots.release()

    def _tunnel(self, upstream: socket.socket) -> None:
        self.request.setblocking(False)
        upstream.setblocking(False)
        sockets = (self.request, upstream)
        peer = {self.request: upstream, upstream: self.request}
        read_open = {sock: True for sock in sockets}
        write_shutdown = {sock: False for sock in sockets}
        pending = {sock: bytearray() for sock in sockets}
        last_activity = time.monotonic()

        while time.monotonic() - last_activity <= IDLE_TIMEOUT:
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
                    payload = source.recv(min(BUFFER_SIZE, available))
                except (BlockingIOError, InterruptedError):
                    continue
                except OSError:
                    return
                if not payload:
                    read_open[source] = False
                    continue
                pending[destination].extend(payload)
                last_activity = time.monotonic()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Docker-only UOM relay bridge")
    parser.add_argument("--bind", default=DEFAULT_BIND, type=_validate_ip)
    parser.add_argument("--source-network", default=DEFAULT_SOURCE_NETWORK, type=_validate_network)
    parser.add_argument("--listen-port", default=DEFAULT_LISTEN_PORT, type=int)
    parser.add_argument("--target-host", default=DEFAULT_TARGET[0])
    parser.add_argument("--target-port", default=DEFAULT_TARGET[1], type=int)
    return parser


def main(argv=None) -> int:
    args = build_parser().parse_args(argv)
    if args.target_host != "127.0.0.1" or not 1 <= args.target_port <= 65535:
        raise SystemExit("target must remain the loopback relay")
    if not 1024 <= args.listen_port <= 65535:
        raise SystemExit("listen-port must be between 1024 and 65535")
    server = BridgeServer(
        (args.bind, args.listen_port), BridgeHandler,
        source_network=args.source_network,
        target_host=args.target_host,
        target_port=args.target_port,
    )
    print(
        f"UOM host bridge listening on {args.bind}:{args.listen_port}; "
        f"source={args.source_network}; target=127.0.0.1:{args.target_port}",
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
    raise SystemExit(main())
