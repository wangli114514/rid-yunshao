#!/usr/bin/env python3
"""Regression checks for the RF-disabled lab loopback contract."""

from __future__ import annotations

import json
import os
import pathlib
import subprocess
import sys
import tempfile
import unittest
from types import SimpleNamespace

HERE = pathlib.Path(__file__).resolve().parent
ROOT = HERE
sys.path.insert(0, str(HERE))

import gateway  # noqa: E402
import server  # noqa: E402


class LabLoopbackContractTests(unittest.TestCase):
    @unittest.skipUnless((ROOT / "src").is_dir(), "firmware sources are not in this web-only repository")
    def test_firmware_tx_is_gated_behind_explicit_arming(self):
        source = "\n".join(
            path.read_text(encoding="utf-8", errors="ignore")
            for path in (ROOT / "src").rglob("*")
            if path.suffix.lower() in {".c", ".cc", ".cpp", ".h"}
        )
        # Raw TX is allowed only inside the gated sniffer helper.
        self.assertIn("esp_wifi_80211_tx", source)
        self.assertNotIn("esp_wifi_internal_tx", source)
        self.assertIn("snifferStartTransmit", source)
        self.assertIn("snifferInjectLabFrame", source)
        # Loopback path keeps advertising RF_TX=disabled.
        self.assertIn("RF_TX=disabled", source)
        # Detect-mode init must not arm TX: snifferInit resets the flag.
        self.assertIn("g_txEnabled = false", source)

    @unittest.skipUnless((ROOT / "src" / "frame_lab.cpp").is_file(), "firmware sources are not in this web-only repository")
    def test_loopback_never_transmits(self):
        source = (ROOT / "src" / "frame_lab.cpp").read_text(encoding="utf-8")
        loopback_zone = source.split("runLoopbackOne")[1].split("transmitOne")[0]
        self.assertNotIn("snifferTransmitFrame", loopback_zone)
        self.assertNotIn("esp_wifi_80211_tx", loopback_zone)

    @unittest.skipUnless((ROOT / "src" / "frame_lab.cpp").is_file(), "firmware sources are not in this web-only repository")
    def test_loopback_excludes_high_level_simulator_records(self):
        source = (ROOT / "src" / "frame_lab.cpp").read_text(encoding="utf-8")
        self.assertIn("enterLabMode", source)
        self.assertIn("simulatorSetEnabled(false)", source)
        self.assertIn("DRONE_PROVENANCE_SIMULATOR", source)

    def test_normalization_derives_simulated_from_lab_loopback(self):
        normalized = server.normalize_snapshot({
            "t": "snap",
            "stationId": "lab-01",
            "hardwareConnected": True,
            "labLoopback": True,
            "drones": [{"mac": "02:00:5E:20:00:01", "id": "LAB-1"}],
        })
        self.assertTrue(normalized["labLoopback"])
        self.assertTrue(normalized["simulated"])
        self.assertTrue(normalized["drones"][0]["labLoopback"])
        self.assertTrue(normalized["drones"][0]["simulated"])

    def test_gateway_stamps_loopback_transport(self):
        with tempfile.TemporaryDirectory() as temp:
            spool = gateway.Spool(os.path.join(temp, "spool.db"))
            sender = SimpleNamespace(notify=lambda: None)
            args = SimpleNamespace(station_id="lab-01", station_name="RF-disabled lab")
            raw = json.dumps({
                "t": "snap", "labLoopback": True,
                "drones": [{"mac": "02:00:5E:20:00:01", "labLoopback": True}],
            })
            self.assertTrue(gateway.enqueue_line(raw, args, spool, sender))
            payload = json.loads(spool.first()["payload"])
            self.assertEqual(payload["sourceTransport"], "usb-serial-loopback")
            self.assertTrue(payload["hardwareConnected"])
            self.assertTrue(payload["simulated"])

    def test_controller_syntax(self):
        result = subprocess.run(
            [sys.executable, "-m", "py_compile", str(HERE / "lab_replay.py")],
            capture_output=True,
            text=True,
        )
        self.assertEqual(result.returncode, 0, result.stderr)


if __name__ == "__main__":
    unittest.main()
