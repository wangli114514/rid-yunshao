#!/usr/bin/env python3

import ipaddress
import socket
import sys
import threading
import time
import unittest
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import uom_host_bridge as bridge  # noqa: E402


class UomHostBridgeTests(unittest.TestCase):
    def test_bind_and_source_network_are_bounded(self):
        self.assertEqual(bridge._validate_ip("172.22.0.1"), "172.22.0.1")
        self.assertEqual(
            bridge._validate_network("172.22.0.0/16"),
            ipaddress.ip_network("172.22.0.0/16"),
        )
        for value in ("127.0.0.1", "0.0.0.0", "::1", "bad"):
            with self.assertRaises(Exception):
                bridge._validate_ip(value)
        with self.assertRaises(Exception):
            bridge._validate_network("::1/128")

    def test_parser_keeps_loopback_target(self):
        args = bridge.build_parser().parse_args([])
        self.assertEqual(args.target_host, "127.0.0.1")
        self.assertEqual(args.target_port, 19090)
        self.assertEqual(args.bind, "172.22.0.1")
        self.assertEqual(args.listen_port, 19091)

    def test_tunnel_preserves_large_payload_for_slow_receiver(self):
        client, bridge_client = socket.socketpair()
        bridge_upstream, remote = socket.socketpair()
        payload = (bytes(range(251)) * 17000)[:4 * 1024 * 1024]
        tunnel_errors = []
        writer_errors = []

        bridge_client.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 4096)
        client.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4096)
        client.settimeout(10.0)
        remote.settimeout(10.0)

        handler = object.__new__(bridge.BridgeHandler)
        handler.request = bridge_client

        def run_tunnel():
            try:
                handler._tunnel(bridge_upstream)
            except Exception as exc:  # pragma: no cover - asserted below
                tunnel_errors.append(exc)
            finally:
                bridge_client.close()
                bridge_upstream.close()

        def write_payload():
            try:
                remote.sendall(payload)
                remote.shutdown(socket.SHUT_WR)
            except Exception as exc:  # pragma: no cover - asserted below
                writer_errors.append(exc)
            finally:
                remote.close()

        tunnel_thread = threading.Thread(target=run_tunnel, daemon=True)
        writer_thread = threading.Thread(target=write_payload, daemon=True)
        received = bytearray()
        try:
            tunnel_thread.start()
            writer_thread.start()
            time.sleep(0.15)
            while True:
                chunk = client.recv(4096)
                if not chunk:
                    break
                received.extend(chunk)
                time.sleep(0.0002)
        finally:
            client.close()

        writer_thread.join(10.0)
        tunnel_thread.join(10.0)
        self.assertFalse(writer_thread.is_alive(), "payload writer did not finish")
        self.assertFalse(tunnel_thread.is_alive(), "tunnel did not finish after both peers closed")
        self.assertEqual(tunnel_errors, [])
        self.assertEqual(writer_errors, [])
        self.assertEqual(received, payload)


if __name__ == "__main__":
    unittest.main()
