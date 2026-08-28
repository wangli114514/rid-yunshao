#!/usr/bin/env python3
"""Tests for the deterministic demo Remote ID simulator."""

import json
import os
import subprocess
import sys
import unittest
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

import simulator  # noqa: E402


class DemoSimulatorTests(unittest.TestCase):
    def test_default_snapshot_fields_and_demo_bounds(self):
        sim = simulator.DemoSimulator(seed=77)
        snapshot = sim.snapshot(elapsed_seconds=25, captured_at_ms=1_700_000_000_000)

        self.assertEqual(snapshot["t"], "snap")
        self.assertEqual(snapshot["n"], simulator.DEFAULT_COUNT)
        self.assertEqual(len(snapshot["drones"]), simulator.DEFAULT_COUNT)
        self.assertTrue(snapshot["simulated"])
        self.assertEqual(snapshot["sourceType"], "server-simulator")
        self.assertFalse(snapshot["hardwareConnected"])
        self.assertEqual(snapshot["station"]["district"], "演示区域")
        self.assertEqual(snapshot["stationId"], "demo-sim-01")

        required = {
            "mac", "model", "id", "lat", "lon", "alt", "spd", "heading",
            "rssi", "olat", "olon", "proto", "protocol", "motion", "simulated",
        }
        for drone in snapshot["drones"]:
            self.assertTrue(required.issubset(drone))
            self.assertGreaterEqual(drone["lat"], 39.82)
            self.assertLessEqual(drone["lat"], 40.00)
            self.assertGreaterEqual(drone["lon"], 116.28)
            self.assertLessEqual(drone["lon"], 116.54)
            self.assertGreaterEqual(drone["olat"], 39.82)
            self.assertLessEqual(drone["olat"], 40.00)
            self.assertGreaterEqual(drone["olon"], 116.28)
            self.assertLessEqual(drone["olon"], 116.54)
            self.assertGreaterEqual(drone["heading"], 0)
            self.assertLess(drone["heading"], 360)
            self.assertGreater(drone["alt"], 0)
            self.assertLessEqual(drone["rssi"], -38)
            self.assertGreaterEqual(drone["rssi"], -96)

        self.assertEqual(
            {item["motion"] for item in snapshot["drones"]},
            set(simulator.MOTION_TYPES),
        )
        self.assertGreaterEqual(len({item["proto"] for item in snapshot["drones"]}), 3)

    def test_every_trajectory_moves_continuously(self):
        sim = simulator.DemoSimulator(count=16, seed=99)
        first = sim.snapshot(elapsed_seconds=10, captured_at_ms=1000)
        second = sim.snapshot(elapsed_seconds=17, captured_at_ms=8000)
        by_mac = {drone["mac"]: drone for drone in second["drones"]}
        for before in first["drones"]:
            after = by_mac[before["mac"]]
            displacement = abs(after["lat"] - before["lat"]) + abs(after["lon"] - before["lon"])
            self.assertGreater(displacement, 0.0000001, before["mac"])

    def test_seed_is_reproducible(self):
        first = simulator.DemoSimulator(count=12, seed=1234).snapshot(
            elapsed_seconds=42.5, captured_at_ms=123456789
        )
        second = simulator.DemoSimulator(count=12, seed=1234).snapshot(
            elapsed_seconds=42.5, captured_at_ms=123456789
        )
        different = simulator.DemoSimulator(count=12, seed=1235).snapshot(
            elapsed_seconds=42.5, captured_at_ms=123456789
        )
        self.assertEqual(first, second)
        self.assertNotEqual(first["drones"], different["drones"])

    def test_count_parameter(self):
        for count in (1, 12, 14, 16, 40):
            snapshot = simulator.DemoSimulator(count=count, seed=1).snapshot(0, 1)
            self.assertEqual(snapshot["n"], count)
            self.assertEqual(len(snapshot["drones"]), count)
        with self.assertRaises(ValueError):
            simulator.DemoSimulator(count=0)
        with self.assertRaises(ValueError):
            simulator.DemoSimulator(count=257)

    def test_once_cli_outputs_valid_frame_without_secrets(self):
        env = os.environ.copy()
        env.pop("RID_SIMULATOR_INGEST_URL", None)
        env.pop("RID_CLOUD_INGEST_URL", None)
        env.pop("RID_SIMULATOR_TOKEN", None)
        env.pop("RID_INGEST_TOKEN", None)
        result = subprocess.run(
            [sys.executable, str(HERE / "simulator.py"), "--once", "--count", "12", "--seed", "8"],
            cwd=HERE,
            env=env,
            check=True,
            capture_output=True,
            text=True,
            encoding="utf-8",
        )
        payload = json.loads(result.stdout)
        self.assertEqual(payload["n"], 12)
        self.assertEqual(payload["seed"], 8)

    def test_ingest_url_requires_https_and_adds_default_path(self):
        self.assertEqual(
            simulator.normalize_ingest_url("https://rid.example.test"),
            "https://rid.example.test/api/ingest",
        )
        self.assertEqual(
            simulator.normalize_ingest_url("https://rid.example.test/api/ingest/"),
            "https://rid.example.test/api/ingest",
        )
        self.assertEqual(
            simulator.normalize_ingest_url("https://rid.example.test/rid-cloud/"),
            "https://rid.example.test/rid-cloud/api/ingest",
        )
        with self.assertRaises(ValueError):
            simulator.normalize_ingest_url("http://rid.example.test/api/ingest")


if __name__ == "__main__":
    unittest.main(verbosity=2)
