#!/usr/bin/env python3
"""Deterministic 演示城市 Remote ID traffic simulator.

The simulator emits the same ``snap`` payload accepted by ``POST /api/ingest``.
It uses only the Python standard library and deliberately marks every payload
and aircraft as simulated so test traffic is distinguishable from live RF data.
"""

import argparse
import json
import math
import os
import random
import signal
import ssl
import sys
import threading
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from urllib.parse import urlsplit, urlunsplit


DEMO_CENTER_LAT = 39.9042
DEMO_CENTER_LON = 116.4074
DEFAULT_COUNT = 16
DEFAULT_SEED = 20260819

# proto follows the receiver contract: 0=ASTM Wi-Fi, 1=CN Wi-Fi, 2=ODID BLE.
AIRCRAFT_CATALOG = (
    ("DJI Mavic 3 Pro", 1, "CN 46750 Wi-Fi"),
    ("DJI Air 3S", 1, "CN 46750 Wi-Fi"),
    ("DJI Mini 4 Pro", 2, "OpenDroneID BLE"),
    ("DJI Matrice 350 RTK", 1, "CN 46750 Wi-Fi"),
    ("DJI Avata 2", 2, "OpenDroneID BLE"),
    ("Autel EVO II Pro V3", 0, "ASTM F3411 Wi-Fi"),
    ("Autel EVO Max 4T", 1, "CN 46750 Wi-Fi"),
    ("FIMI X8 Pro", 0, "ASTM F3411 Wi-Fi"),
    ("Parrot ANAFI Ai", 0, "ASTM F3411 Wi-Fi"),
    ("DJI Inspire 3", 1, "CN 46750 Wi-Fi"),
    ("DJI Mavic 3 Enterprise", 1, "CN 46750 Wi-Fi"),
    ("DJI Matrice 30T", 1, "CN 46750 Wi-Fi"),
    ("DJI Neo", 2, "OpenDroneID BLE"),
    ("Autel EVO Nano+", 0, "ASTM F3411 Wi-Fi"),
    ("DJI FlyCart 30", 1, "CN 46750 Wi-Fi"),
    ("Parrot ANAFI USA", 0, "ASTM F3411 Wi-Fi"),
)

MOTION_TYPES = ("orbit", "patrol", "hover", "figure8")


@dataclass(frozen=True)
class _Aircraft:
    mac: str
    model: str
    uas_id: str
    proto: int
    protocol_name: str
    motion: str
    base_north: float
    base_east: float
    route_heading: float
    radius: float
    period: float
    phase: float
    altitude: float
    altitude_wave: float
    operator_north: float
    operator_east: float


def _offset_to_coordinate(north_m, east_m):
    lat = DEMO_CENTER_LAT + north_m / 111_320.0
    lon_scale = 111_320.0 * math.cos(math.radians(DEMO_CENTER_LAT))
    lon = DEMO_CENTER_LON + east_m / lon_scale
    return lat, lon


def _heading(east_speed, north_speed, fallback=0.0):
    if abs(east_speed) + abs(north_speed) < 1e-9:
        return fallback % 360.0
    return math.degrees(math.atan2(east_speed, north_speed)) % 360.0


class DemoSimulator:
    """Generate pure, reproducible Remote ID snapshots for the public demo location."""

    def __init__(
        self,
        count=DEFAULT_COUNT,
        seed=DEFAULT_SEED,
        station_id="demo-sim-01",
        station_name="演示哨站",
    ):
        count = int(count)
        if count < 1 or count > 256:
            raise ValueError("count must be between 1 and 256")
        self.count = count
        self.seed = int(seed)
        self.station_id = str(station_id).strip() or "demo-sim-01"
        self.station_name = str(station_name).strip() or self.station_id
        self.aircraft = self._create_aircraft()

    def _create_aircraft(self):
        rng = random.Random(self.seed)
        aircraft = []
        for index in range(self.count):
            model, proto, protocol_name = AIRCRAFT_CATALOG[index % len(AIRCRAFT_CATALOG)]
            motion = MOTION_TYPES[index % len(MOTION_TYPES)]

            bearing = (2.0 * math.pi * index / self.count) + rng.uniform(-0.18, 0.18)
            distance = rng.uniform(900.0, 4_000.0)
            base_north = math.cos(bearing) * distance
            base_east = math.sin(bearing) * distance
            route_heading = rng.uniform(0.0, 2.0 * math.pi)

            if motion == "orbit":
                radius = rng.uniform(280.0, 760.0)
                period = rng.uniform(120.0, 260.0)
            elif motion == "patrol":
                radius = rng.uniform(500.0, 1_100.0)
                period = rng.uniform(100.0, 230.0)
            elif motion == "hover":
                radius = rng.uniform(10.0, 28.0)
                period = rng.uniform(65.0, 140.0)
            else:
                radius = rng.uniform(400.0, 850.0)
                period = rng.uniform(130.0, 280.0)

            mac_bytes = [rng.randrange(256) for _ in range(5)]
            mac = "02:" + ":".join(f"{part:02X}" for part in mac_bytes)
            uas_id = f"SIM-SJ-{self.seed & 0xFFFF:04X}-{index + 1:03d}"

            operator_bearing = rng.uniform(0.0, 2.0 * math.pi)
            operator_distance = rng.uniform(80.0, 420.0)
            aircraft.append(_Aircraft(
                mac=mac,
                model=model,
                uas_id=uas_id,
                proto=proto,
                protocol_name=protocol_name,
                motion=motion,
                base_north=base_north,
                base_east=base_east,
                route_heading=route_heading,
                radius=radius,
                period=period,
                phase=rng.uniform(0.0, 2.0 * math.pi),
                altitude=rng.uniform(32.0, 145.0),
                altitude_wave=rng.uniform(2.0, 16.0),
                operator_north=base_north + math.cos(operator_bearing) * operator_distance,
                operator_east=base_east + math.sin(operator_bearing) * operator_distance,
            ))
        return tuple(aircraft)

    @staticmethod
    def _position(spec, elapsed_seconds):
        omega = 2.0 * math.pi / spec.period
        theta = spec.phase + omega * elapsed_seconds

        if spec.motion == "orbit":
            east = spec.base_east + spec.radius * math.sin(theta)
            north = spec.base_north + spec.radius * math.cos(theta)
            east_speed = spec.radius * omega * math.cos(theta)
            north_speed = -spec.radius * omega * math.sin(theta)
        elif spec.motion == "patrol":
            along = spec.radius * math.sin(theta)
            along_speed = spec.radius * omega * math.cos(theta)
            east_unit = math.sin(spec.route_heading)
            north_unit = math.cos(spec.route_heading)
            east = spec.base_east + along * east_unit
            north = spec.base_north + along * north_unit
            east_speed = along_speed * east_unit
            north_speed = along_speed * north_unit
        elif spec.motion == "hover":
            east = spec.base_east + spec.radius * math.sin(theta)
            north = spec.base_north + 0.65 * spec.radius * math.cos(theta)
            east_speed = spec.radius * omega * math.cos(theta)
            north_speed = -0.65 * spec.radius * omega * math.sin(theta)
        else:
            east = spec.base_east + spec.radius * math.sin(theta)
            north = spec.base_north + 0.52 * spec.radius * math.sin(2.0 * theta)
            east_speed = spec.radius * omega * math.cos(theta)
            north_speed = 1.04 * spec.radius * omega * math.cos(2.0 * theta)

        return north, east, north_speed, east_speed

    def snapshot(self, elapsed_seconds=0.0, captured_at_ms=None):
        """Build one snapshot without mutating simulator state."""
        elapsed_seconds = max(0.0, float(elapsed_seconds))
        if captured_at_ms is None:
            captured_at_ms = int(time.time() * 1000)

        drones = []
        for index, spec in enumerate(self.aircraft):
            north, east, north_speed, east_speed = self._position(spec, elapsed_seconds)
            lat, lon = _offset_to_coordinate(north, east)
            operator_lat, operator_lon = _offset_to_coordinate(
                spec.operator_north, spec.operator_east
            )
            speed = math.hypot(north_speed, east_speed)
            altitude_phase = spec.phase * 0.5 + elapsed_seconds * 2.0 * math.pi / (spec.period * 1.7)
            altitude = spec.altitude + spec.altitude_wave * math.sin(altitude_phase)

            receiver_distance = max(20.0, math.hypot(north, east))
            rssi = -40.0 - 18.0 * math.log10(receiver_distance / 20.0)
            rssi += 1.8 * math.sin(elapsed_seconds / 7.0 + index * 1.13)
            rssi = int(round(max(-96.0, min(-38.0, rssi))))

            drones.append({
                "mac": spec.mac,
                "model": spec.model,
                "id": spec.uas_id,
                "lat": round(lat, 7),
                "lon": round(lon, 7),
                "alt": round(max(5.0, altitude), 1),
                "spd": round(speed, 2),
                "heading": round(_heading(east_speed, north_speed, spec.route_heading), 1),
                "rssi": rssi,
                "olat": round(operator_lat, 7),
                "olon": round(operator_lon, 7),
                "proto": spec.proto,
                "protocol": spec.protocol_name,
                "motion": spec.motion,
                "simulated": True,
            })

        channel = (1, 6, 11)[int(elapsed_seconds // 20) % 3]
        return {
            "t": "snap",
            "n": len(drones),
            "ch": channel,
            "bat": int(round(88.0 + 4.0 * math.sin(elapsed_seconds / 180.0))),
            "capturedAt": int(captured_at_ms),
            "stationId": self.station_id,
            "stationName": self.station_name,
            "sourceType": "server-simulator",
            "sourceTransport": "process",
            "hardwareConnected": False,
            "station": {
                "id": self.station_id,
                "name": self.station_name,
                "lat": DEMO_CENTER_LAT,
                "lon": DEMO_CENTER_LON,
                "district": "演示区域",
                "source": "simulator",
            },
            "simulated": True,
            "seed": self.seed,
            "drones": drones,
        }


def normalize_ingest_url(url):
    """Normalize a public HTTPS base URL to the ingest endpoint."""
    value = (url or "").strip()
    parsed = urlsplit(value)
    if parsed.scheme.lower() != "https" or not parsed.netloc:
        raise ValueError("ingest URL must be an absolute https:// URL")
    path = parsed.path.rstrip("/")
    if not path.endswith("/api/ingest"):
        path += "/api/ingest"
    return urlunsplit(("https", parsed.netloc, path, parsed.query, ""))


def post_snapshot(url, token, snapshot, timeout=10.0, ca_file=None):
    """POST one snapshot and return the decoded server response."""
    endpoint = normalize_ingest_url(url)
    if not token:
        raise ValueError("ingest token is required when an URL is configured")
    body = json.dumps(snapshot, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    request = urllib.request.Request(
        endpoint,
        data=body,
        method="POST",
        headers={
            "Authorization": "Bearer " + token,
            "Content-Type": "application/json",
            "User-Agent": "RID-Demo-Simulator/1.0",
        },
    )
    context = ssl.create_default_context(cafile=ca_file)
    with urllib.request.urlopen(request, timeout=float(timeout), context=context) as response:
        status = response.status
        response_body = response.read(65_536)
        if status < 200 or status >= 300:
            raise RuntimeError(f"HTTP {status}")
    if not response_body:
        return {"status": status}
    try:
        result = json.loads(response_body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        result = {"status": status, "body": response_body.decode("utf-8", "replace")}
    return result


def _positive_interval(value):
    parsed = float(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("interval must be greater than zero")
    return parsed


def _aircraft_count(value):
    parsed = int(value)
    if parsed < 1 or parsed > 256:
        raise argparse.ArgumentTypeError("count must be between 1 and 256")
    return parsed


def build_parser():
    parser = argparse.ArgumentParser(
        description="Generate deterministic Remote ID traffic around 演示城市"
    )
    parser.add_argument(
        "--url",
        default=os.environ.get("RID_SIMULATOR_INGEST_URL")
        or os.environ.get("RID_CLOUD_INGEST_URL"),
        help="HTTPS cloud base URL or full /api/ingest URL",
    )
    parser.add_argument(
        "--token",
        default=os.environ.get("RID_SIMULATOR_TOKEN") or os.environ.get("RID_INGEST_TOKEN"),
        help="Bearer token (prefer RID_SIMULATOR_TOKEN or RID_INGEST_TOKEN)",
    )
    parser.add_argument("--station-id", default="demo-sim-01")
    parser.add_argument("--station-name", default="演示哨站")
    parser.add_argument("--interval", type=_positive_interval, default=3.0)
    parser.add_argument("--count", type=_aircraft_count, default=DEFAULT_COUNT)
    parser.add_argument("--seed", type=int, default=DEFAULT_SEED)
    parser.add_argument("--timeout", type=_positive_interval, default=10.0)
    parser.add_argument("--ca-file", help="Optional custom CA certificate bundle")
    parser.add_argument(
        "--once",
        action="store_true",
        help="Emit one JSON snapshot, or upload one snapshot when --url is set",
    )
    return parser


def _json_line(value):
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def run(args):
    simulator = DemoSimulator(
        count=args.count,
        seed=args.seed,
        station_id=args.station_id,
        station_name=args.station_name,
    )
    if args.url:
        endpoint = normalize_ingest_url(args.url)
        if not args.token:
            raise ValueError("--token or RID_SIMULATOR_TOKEN is required with --url")
    else:
        endpoint = None

    if args.once:
        snapshot = simulator.snapshot(0.0)
        if endpoint:
            result = post_snapshot(endpoint, args.token, snapshot, args.timeout, args.ca_file)
            print(_json_line(result))
        else:
            print(_json_line(snapshot))
        return 0

    stop_event = threading.Event()

    def request_stop(_signum, _frame):
        stop_event.set()

    for signal_name in ("SIGINT", "SIGTERM"):
        signal_value = getattr(signal, signal_name, None)
        if signal_value is not None:
            signal.signal(signal_value, request_stop)

    started = time.monotonic()
    deadline = started
    sent = 0
    while not stop_event.is_set():
        now = time.monotonic()
        snapshot = simulator.snapshot(now - started)
        if endpoint:
            try:
                post_snapshot(endpoint, args.token, snapshot, args.timeout, args.ca_file)
                sent += 1
                if sent == 1 or sent % 60 == 0:
                    print(
                        f"[simulator] uploaded {sent} snapshots, {args.count} aircraft/frame",
                        file=sys.stderr,
                    )
            except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError, OSError, RuntimeError) as exc:
                print(f"[simulator] upload failed: {exc}", file=sys.stderr)
        else:
            print(_json_line(snapshot), flush=True)

        deadline += args.interval
        remaining = deadline - time.monotonic()
        if remaining < 0:
            deadline = time.monotonic()
            remaining = 0
        stop_event.wait(remaining)
    print("[simulator] stopped", file=sys.stderr)
    return 0


def main():
    parser = build_parser()
    args = parser.parse_args()
    try:
        return run(args)
    except (ValueError, OSError) as exc:
        parser.error(str(exc))


if __name__ == "__main__":
    raise SystemExit(main())
