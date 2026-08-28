#!/usr/bin/env python3
"""Unit and loopback integration tests for the UOM-only CONNECT relay."""

import os
import socket
import threading
import time
import unittest
from pathlib import Path
from unittest import mock

HERE = Path(__file__).resolve().parent
import sys

sys.path.insert(0, str(HERE))

import server  # noqa: E402
import uom_connect_relay as relay  # noqa: E402


class UomConnectRelayTests(unittest.TestCase):
    def test_allow_list_is_exact(self):
        self.assertTrue(relay.allowed_target("uom.caac.gov.cn:443"))
        self.assertTrue(relay.allowed_target("UOM.CAAC.GOV.CN:443"))
        for target in (
            "uom.caac.gov.cn:444",
            "uom.caac.gov.cn:0443",
            "example.com:443",
            "uom.caac.gov.cn:443:extra",
            "uom.caac.gov.cn",
            "uom.caac.gov.cn:443/path",
        ):
            self.assertFalse(relay.allowed_target(target), target)

    def test_parser_accepts_only_connect(self):
        raw = (
            b"CONNECT uom.caac.gov.cn:443 HTTP/1.1\r\n"
            b"Host: uom.caac.gov.cn:443\r\n\r\n"
        )
        self.assertEqual(
            relay.parse_connect_request(raw), ("uom.caac.gov.cn:443", "HTTP/1.1")
        )
        with self.assertRaises(PermissionError):
            relay.parse_connect_request(
                b"GET https://uom.caac.gov.cn/ HTTP/1.1\r\n\r\n"
            )
        with self.assertRaises(PermissionError):
            relay.parse_connect_request(
                b"CONNECT example.com:443 HTTP/1.1\r\n\r\n"
            )

    def test_proxy_configuration_is_secret_free(self):
        with mock.patch.dict(
            os.environ,
            {server.UOM_WMS_PROXY_ENV: "http://127.0.0.1:19090"},
            clear=False,
        ):
            status = server.uom_wms_status()
            self.assertEqual(status["proxy"]["mode"], "http-connect")
            self.assertEqual(status["proxy"]["host"], "127.0.0.1")
            self.assertEqual(status["proxy"]["port"], 19090)

        secret_url = "http://relay-user:relay-secret@127.0.0.1:19090/path"
        with mock.patch.dict(
            os.environ, {server.UOM_WMS_PROXY_ENV: secret_url}, clear=False
        ):
            status = server.uom_wms_status()
            self.assertFalse(status["proxy"]["valid"])
            self.assertNotIn("relay-secret", repr(status))

    def test_tunnel_preserves_large_payload_for_slow_receiver(self):
        client, relay_client = socket.socketpair()
        relay_upstream, remote = socket.socketpair()
        payload = (bytes(range(251)) * 17000)[:4 * 1024 * 1024]
        tunnel_errors = []
        writer_errors = []

        relay_client.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 4096)
        client.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4096)
        client.settimeout(10.0)
        remote.settimeout(10.0)

        handler = object.__new__(relay.RelayHandler)
        handler.request = relay_client
        handler.server = mock.Mock(idle_timeout=10.0)

        def run_tunnel():
            try:
                handler._tunnel(relay_upstream)
            except Exception as exc:  # pragma: no cover - asserted below
                tunnel_errors.append(exc)
            finally:
                relay_client.close()
                relay_upstream.close()

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

    def test_loopback_tunnel_round_trip_and_rejects_other_target(self):
        upstream_ready = threading.Event()
        stop_upstream = threading.Event()
        upstream_port = {}

        def upstream_worker():
            with socket.socket() as listener:
                listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                listener.bind(("127.0.0.1", 0))
                listener.listen(4)
                upstream_port["value"] = listener.getsockname()[1]
                upstream_ready.set()
                listener.settimeout(0.2)
                while not stop_upstream.is_set():
                    try:
                        client, _ = listener.accept()
                    except socket.timeout:
                        continue
                    with client:
                        client.settimeout(2)
                        try:
                            payload = client.recv(1024)
                            if payload:
                                client.sendall(b"echo:" + payload)
                        except OSError:
                            pass

        thread = threading.Thread(target=upstream_worker, daemon=True)
        thread.start()
        self.assertTrue(upstream_ready.wait(2))

        real_connect = relay.socket.create_connection

        def redirect_connect(address, timeout=None, source_address=None):
            self.assertEqual(address, (relay.TARGET_HOST, relay.TARGET_PORT))
            return real_connect(
                ("127.0.0.1", upstream_port["value"]), timeout, source_address
            )

        relay_server = relay.RelayServer(
            ("127.0.0.1", 0), relay.RelayHandler, idle_timeout=5, max_connections=2
        )
        relay_thread = threading.Thread(
            target=relay_server.serve_forever, kwargs={"poll_interval": 0.05}, daemon=True
        )
        relay_thread.start()
        relay_port = relay_server.server_address[1]
        try:
            with mock.patch.object(relay.socket, "create_connection", redirect_connect):
                with socket.socket() as client:
                    client.settimeout(2)
                    client.connect(("127.0.0.1", relay_port))
                    client.sendall(
                        b"CONNECT uom.caac.gov.cn:443 HTTP/1.1\r\n"
                        b"Host: uom.caac.gov.cn:443\r\n\r\n"
                    )
                    self.assertIn(b"200 Connection Established", client.recv(256))
                    client.sendall(b"probe")
                    self.assertEqual(client.recv(256), b"echo:probe")

                with socket.socket() as client:
                    client.settimeout(2)
                    client.connect(("127.0.0.1", relay_port))
                    client.sendall(
                        b"CONNECT example.com:443 HTTP/1.1\r\n"
                        b"Host: example.com:443\r\n\r\n"
                    )
                    self.assertIn(b"403 Forbidden", client.recv(256))
        finally:
            relay_server.shutdown()
            relay_server.server_close()
            relay_thread.join(2)
            stop_upstream.set()
            thread.join(2)


if __name__ == "__main__":
    unittest.main()
