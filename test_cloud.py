#!/usr/bin/env python3
"""Regression tests for RID history, cloud auth, live merge, and gateway spool."""

import asyncio
import hashlib
import http.client
import inspect
import json
import os
import socket
import sqlite3
import stat
import struct
import subprocess
import sys
import tempfile
import threading
import time
import unittest
import urllib.error
import urllib.request
import zlib
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from types import SimpleNamespace
from urllib.parse import parse_qs, urlencode, urlparse
from unittest import mock

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

import gateway  # noqa: E402
import server  # noqa: E402


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def png_fixture(width=256, height=256, include_idat=True, idat_payload=None):
    def chunk(kind, payload):
        crc = zlib.crc32(kind)
        crc = zlib.crc32(payload, crc) & 0xFFFFFFFF
        return struct.pack(">I", len(payload)) + kind + payload + struct.pack(">I", crc)

    ihdr = struct.pack(">IIBBBBB", width, height, 8, 6, 0, 0, 0)
    body = server.PNG_SIGNATURE + chunk(b"IHDR", ihdr)
    if include_idat:
        row = b"\x00" + b"\x20\xd0\x80\xff" * width
        pixels = zlib.compress(row * height) if idat_payload is None else idat_payload
        body += chunk(b"IDAT", pixels)
    return body + chunk(b"IEND", b"")


def drone(mac, lat=39.9, lon=116.4, rssi=-60):
    return {
        "mac": mac,
        "model": "Test RID",
        "id": "UAS-" + mac[-2:],
        "lat": lat,
        "lon": lon,
        "alt": 50,
        "spd": 5,
        "rssi": rssi,
        "proto": 0,
    }


def snapshot(station, captured_at, drones):
    return {
        "t": "snap",
        "stationId": station,
        "stationName": "Station " + station,
        "capturedAt": captured_at,
        "ch": 6,
        "bat": 80,
        "drones": drones,
    }


def circle_geofence(name="Test circle", severity="high", enabled=True,
                     lon=116.4, lat=39.9, radius=1000, min_altitude=None, max_altitude=None):
    return {
        "name": name,
        "shapeType": "circle",
        "severity": severity,
        "enabled": enabled,
        "minAltitude": min_altitude,
        "maxAltitude": max_altitude,
        "geometry": {
            "type": "Circle",
            "center": [lon, lat],
            "radiusMeters": radius,
        },
    }


def airspace_feature(feature_id, name, zone_class, coordinates, geometry_type="Polygon", **properties):
    values = {"name": name, "zoneClass": zone_class}
    values.update(properties)
    return {
        "type": "Feature",
        "id": feature_id,
        "properties": values,
        "geometry": {"type": geometry_type, "coordinates": coordinates},
    }


def uom_wms_params(layer=None):
    layers = layer or server.UOM_WMS_LAYER_GROUPS[2]
    return {
        "SERVICE": ["WMS"],
        "REQUEST": ["GetMap"],
        "VERSION": ["1.1.0"],
        "LAYERS": [layers],
        "STYLES": [server.UOM_WMS_STYLES_BY_LAYER_GROUP[layers]],
        "FORMAT": ["image/png8"],
        "TRANSPARENT": ["true"],
        "SRS": ["EPSG:3857"],
        "BBOX": ["13500000,3600000,13501000,3601000"],
        "WIDTH": ["256"],
        "HEIGHT": ["256"],
    }


class HistoryStoreTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.store = server.HistoryStore(
            os.path.join(self.temp.name, "history.db"),
            flight_gap_seconds=15,
            min_point_meters=1000,
            max_point_interval_seconds=5,
            retention_days=30,
        )

    def tearDown(self):
        self.store.finish_all()
        self.temp.cleanup()

    def test_station_isolation_sampling_and_exports(self):
        now = int(time.time() * 1000) - 10_000
        mac = "AA:BB:CC:00:00:01"
        self.assertTrue(self.store.ingest_snapshot(snapshot("A", now, [drone(mac)])))
        self.assertTrue(self.store.ingest_snapshot(snapshot("A", now + 1000, [drone(mac)])))
        self.assertTrue(self.store.ingest_snapshot(snapshot("A", now + 6000, [drone(mac)])))
        self.assertTrue(self.store.ingest_snapshot(snapshot("B", now + 1000, [drone(mac, rssi=-70)])))

        result = self.store.list_flights({})
        self.assertEqual(result["total"], 2)
        self.assertEqual({item["stationId"] for item in result["items"]}, {"A", "B"})
        by_station = {item["stationId"]: item for item in result["items"]}
        self.assertEqual(by_station["A"]["n"], 2)
        self.assertEqual(by_station["B"]["n"], 1)

        detail = self.store.get_flight(by_station["A"]["id"])
        self.assertEqual(len(detail["points"]), 2)
        feature = self.store.track_geojson(by_station["A"]["id"])
        self.assertEqual(feature["geometry"]["type"], "LineString")
        csv_text = self.store.export_csv({})
        self.assertIn("station_id", csv_text)
        self.assertIn("AA:BB:CC:00:00:01", csv_text)

    def test_geofence_crud_validation_and_polygon_boundary(self):
        circle = self.store.create_geofence(circle_geofence())
        self.assertEqual(circle["shapeType"], "circle")
        self.assertEqual(circle["geometry"]["type"], "Circle")
        self.assertEqual(self.store.list_geofences()["total"], 1)

        polygon_payload = {
            "name": "Altitude polygon",
            "shapeType": "polygon",
            "severity": "medium",
            "enabled": True,
            "minAltitude": 30,
            "maxAltitude": 100,
            "geometry": {
                "type": "Polygon",
                "coordinates": [[
                    [116.39, 39.89], [116.41, 39.89], [116.41, 39.91], [116.39, 39.91]
                ]],
            },
        }
        polygon = self.store.create_geofence(polygon_payload)
        self.assertEqual(polygon["geometry"]["coordinates"][0][0],
                         polygon["geometry"]["coordinates"][0][-1])
        self.assertTrue(self.store._geofence_contains(polygon, 39.89, 116.4, 50))
        self.assertTrue(self.store._geofence_contains(polygon, 39.9, 116.4, 50))
        self.assertFalse(self.store._geofence_contains(polygon, 39.9, 116.4, 20))
        self.assertFalse(self.store._geofence_contains(polygon, 39.92, 116.4, 50))

        updated_payload = circle_geofence(name="Updated", severity="critical", enabled=False)
        updated = self.store.update_geofence(circle["id"], updated_payload)
        self.assertEqual(updated["name"], "Updated")
        self.assertFalse(updated["enabled"])
        self.assertTrue(self.store.delete_geofence(polygon["id"]))
        self.assertFalse(self.store.delete_geofence(999999))

        invalid = circle_geofence()
        invalid["geometry"]["radiusMeters"] = 0
        with self.assertRaises(ValueError):
            self.store.create_geofence(invalid)
        invalid_polygon = dict(polygon_payload)
        invalid_polygon["geometry"] = {
            "type": "Polygon",
            "coordinates": [[[116.4, 39.9], [116.5, 40.0], [116.6, 40.1]]],
        }
        with self.assertRaises(ValueError):
            self.store.create_geofence(invalid_polygon)

    def test_airspace_geojson_atomic_activation_query_and_sha_idempotency(self):
        now = int(time.time() * 1000)
        ring = [
            [121.220, 31.020], [121.240, 31.020],
            [121.240, 31.040], [121.220, 31.040], [121.220, 31.020],
        ]
        multi_ring = [
            [121.250, 31.025], [121.260, 31.025],
            [121.260, 31.035], [121.250, 31.035], [121.250, 31.025],
        ]
        payload = {
            "source": {
                "slug": "uom-test", "name": "UOM test source", "provider": "fixture",
                "sourceVersion": "v1", "publishedAt": now - 1000,
                "validFrom": now - 10_000, "validTo": now + 60_000,
            },
            "data": {
                "type": "FeatureCollection",
                "features": [
                    airspace_feature("suitable-1", "Suitable zone", "suitable", [ring]),
                    airspace_feature(
                        "prohibited-1", "Prohibited zone", "prohibited", [[multi_ring]],
                        geometry_type="MultiPolygon", minAltitude=0, maxAltitude=120,
                        altitudeReference="AGL",
                    ),
                ],
            },
        }
        first = self.store.import_airspace(payload)
        self.assertFalse(first["idempotent"])
        self.assertEqual(first["status"], "succeeded")
        self.assertEqual(first["zonesImported"], 2)
        first_dataset_id = first["dataset"]["id"]
        self.assertEqual(first["dataset"]["status"], "active")

        queried = self.store.list_airspace_zones({
            "bbox": ["121.21,31.01,121.27,31.05"], "at": [str(now)],
        })
        self.assertEqual(queried["total"], 2)
        self.assertEqual({item["zoneClass"] for item in queried["items"]},
                         {"suitable", "prohibited"})
        prohibited = self.store.list_airspace_zones({
            "bbox": ["121.21,31.01,121.27,31.05"],
            "classes": ["prohibited"], "at": [str(now)],
        })
        self.assertEqual(prohibited["total"], 1)
        self.assertEqual(prohibited["items"][0]["altitudeReference"], "agl")
        self.assertEqual(prohibited["items"][0]["dataset"]["id"], first_dataset_id)
        self.assertEqual(self.store.list_airspace_zones({
            "bbox": ["120,30,120.5,30.5"], "at": [str(now)],
        })["total"], 0)

        duplicate = self.store.import_airspace(payload)
        self.assertTrue(duplicate["idempotent"])
        self.assertEqual(duplicate["status"], "unchanged")
        self.assertEqual(duplicate["dataset"]["id"], first_dataset_id)
        with self.store._db() as conn:
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM airspace_datasets").fetchone()[0], 1)
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM airspace_zones").fetchone()[0], 2)

        replacement = json.loads(json.dumps(payload))
        replacement["source"]["sourceVersion"] = "v2"
        replacement["data"]["features"][0]["properties"]["name"] = "Suitable zone v2"
        second = self.store.import_airspace(replacement)
        self.assertFalse(second["idempotent"])
        self.assertNotEqual(second["dataset"]["id"], first_dataset_id)
        after_swap = self.store.list_airspace_zones({
            "bbox": ["121.21,31.01,121.27,31.05"], "at": [str(now)],
        })
        self.assertIn("Suitable zone v2", {item["name"] for item in after_swap["items"]})
        self.assertNotIn("Suitable zone", {item["name"] for item in after_swap["items"]})
        with self.store._db() as conn:
            statuses = {
                row["id"]: row["status"]
                for row in conn.execute("SELECT id,status FROM airspace_datasets")
            }
        self.assertEqual(statuses[first_dataset_id], "superseded")
        self.assertEqual(statuses[second["dataset"]["id"]], "active")

        invalid = json.loads(json.dumps(replacement))
        invalid["data"]["features"].append(invalid["data"]["features"][0])
        with self.assertRaisesRegex(ValueError, "重复的空域"):
            self.store.import_airspace(invalid)
        current = self.store.list_airspace_zones({
            "bbox": ["121.21,31.01,121.27,31.05"], "at": [str(now)],
        })
        self.assertTrue(all(item["dataset"]["id"] == second["dataset"]["id"]
                            for item in current["items"]))

    def test_airspace_source_forward_migration_adds_provenance_mode(self):
        with tempfile.TemporaryDirectory() as temp:
            path = os.path.join(temp, "legacy.db")
            conn = sqlite3.connect(path)
            conn.execute(
                """
                CREATE TABLE airspace_sources (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    slug TEXT NOT NULL UNIQUE,name TEXT NOT NULL,provider TEXT,authority TEXT,
                    source_url TEXT,enabled INTEGER NOT NULL DEFAULT 1,
                    last_attempt_at INTEGER,last_success_at INTEGER,last_error TEXT,
                    created_at INTEGER NOT NULL,updated_at INTEGER NOT NULL
                )
                """
            )
            conn.execute(
                "INSERT INTO airspace_sources(slug,name,created_at,updated_at) VALUES ('legacy','Legacy',1,1)"
            )
            conn.commit()
            conn.close()

            migrated = server.HistoryStore(path)
            try:
                with migrated._db() as conn:
                    columns = {row[1] for row in conn.execute("PRAGMA table_info(airspace_sources)")}
                    row = conn.execute(
                        "SELECT sync_mode,dist_code,coverage_scope FROM airspace_sources "
                        "WHERE slug='legacy'"
                    ).fetchone()
                self.assertIn("sync_mode", columns)
                self.assertIn("dist_code", columns)
                self.assertIn("coverage_scope", columns)
                self.assertEqual(row["sync_mode"], "manual_import")
                self.assertIsNone(row["dist_code"])
                self.assertEqual(row["coverage_scope"], "unknown")
            finally:
                migrated.finish_all()

    def test_airspace_rtree_backfill_triggers_replacement_and_query_path(self):
        ring = [[121.20, 31.00], [121.22, 31.00], [121.22, 31.02], [121.20, 31.00]]
        payload = {
            "source": {
                "slug": "rtree-test", "name": "RTree test", "distCode": "120000",
                "coverageScope": "city", "sourceVersion": "v1",
            },
            "data": {"type": "FeatureCollection", "features": [
                airspace_feature("rtree-v1", "RTree v1", "warning", [ring]),
            ]},
        }
        first = self.store.import_airspace(payload)
        first_dataset_id = first["dataset"]["id"]
        with self.store._db() as conn:
            zone = conn.execute(
                "SELECT id FROM airspace_zones WHERE dataset_id=?", (first_dataset_id,)
            ).fetchone()
            self.assertIsNotNone(conn.execute(
                "SELECT id FROM airspace_zone_rtree WHERE id=?", (zone["id"],)
            ).fetchone())
            conn.execute("DELETE FROM airspace_zone_rtree WHERE id=?", (zone["id"],))

        # The bbox path must use RTree: a missing index row makes an otherwise active zone invisible.
        self.assertEqual(self.store.list_airspace_zones({
            "bbox": ["121.19,30.99,121.23,31.03"],
        })["total"], 0)

        reopened = server.HistoryStore(self.store.path)
        try:
            restored = reopened.list_airspace_zones({
                "bbox": ["121.19,30.99,121.23,31.03"],
            })
            self.assertEqual(restored["total"], 1)
            with reopened._db() as conn:
                rtree_row = conn.execute(
                    "SELECT * FROM airspace_zone_rtree WHERE id=?", (zone["id"],)
                ).fetchone()
                self.assertIsNotNone(rtree_row)
                conn.execute(
                    "UPDATE airspace_zones SET min_lon=122.0,max_lon=122.1," 
                    "min_lat=32.0,max_lat=32.1 WHERE id=?",
                    (zone["id"],),
                )
                moved = conn.execute(
                    "SELECT * FROM airspace_zone_rtree WHERE id=?", (zone["id"],)
                ).fetchone()
                self.assertLessEqual(moved["min_lon"], 122.0)
                self.assertGreaterEqual(moved["max_lon"], 122.1)
            self.assertEqual(reopened.list_airspace_zones({
                "bbox": ["121.19,30.99,121.23,31.03"],
            })["total"], 0)
            self.assertEqual(reopened.list_airspace_zones({
                "bbox": ["121.99,31.99,122.11,32.11"],
            })["total"], 1)

            replacement = json.loads(json.dumps(payload))
            replacement["source"]["sourceVersion"] = "v2"
            replacement["data"]["features"][0]["id"] = "rtree-v2"
            replacement["data"]["features"][0]["properties"]["name"] = "RTree v2"
            second = reopened.import_airspace(replacement)
            self.assertNotEqual(second["dataset"]["id"], first_dataset_id)
            current = reopened.list_airspace_zones({
                "bbox": ["121.19,30.99,121.23,31.03"],
            })
            self.assertEqual([item["name"] for item in current["items"]], ["RTree v2"])

            with reopened._db() as conn:
                new_zone_id = conn.execute(
                    "SELECT id FROM airspace_zones WHERE dataset_id=?",
                    (second["dataset"]["id"],),
                ).fetchone()["id"]
                conn.execute("DELETE FROM airspace_zones WHERE id=?", (new_zone_id,))
                self.assertIsNone(conn.execute(
                    "SELECT id FROM airspace_zone_rtree WHERE id=?", (new_zone_id,)
                ).fetchone())
        finally:
            reopened.finish_all()

    def test_airspace_city_partition_isolation_validation_and_coverage_summary(self):
        def city_payload(slug, dist_code, version, name, lon, lat, zone_class):
            ring = [
                [lon, lat], [lon + 0.02, lat], [lon + 0.02, lat + 0.02],
                [lon, lat + 0.02], [lon, lat],
            ]
            return {
                "source": {
                    "slug": slug, "name": name, "distCode": dist_code,
                    "coverageScope": "city", "sourceVersion": version,
                },
                "data": {"type": "FeatureCollection", "features": [
                    airspace_feature(slug + "-zone", name + " zone", zone_class, [ring]),
                ]},
            }

        demo_city_v1 = self.store.import_airspace(city_payload(
            "city-120000", "120000", "demo-v1", "演示城市", 117.20, 39.10,
            "warning",
        ))
        haidian = self.store.import_airspace(city_payload(
            "city-110108", "110108", "hd-v1", "演示城区", 116.20, 39.90,
            "controlled",
        ))
        national = self.store.import_airspace({
            "source": {
                "slug": "national-manual", "name": "National manual package",
                "coverageScope": "national", "sourceVersion": "cn-v1",
            },
            "data": {"type": "FeatureCollection", "features": [
                airspace_feature("cn-zone", "National package sample", "suitable", [[
                    [110.0, 30.0], [110.1, 30.0], [110.1, 30.1], [110.0, 30.0],
                ]]),
            ]},
        })
        self.assertEqual(demo_city_v1["source"]["distCode"], "120000")
        self.assertEqual(demo_city_v1["source"]["coverageScope"], "city")
        self.assertIsNone(national["source"]["distCode"])
        self.assertEqual(national["source"]["coverageScope"], "national")

        status = self.store.airspace_status({
            "configured": False, "supported": False,
            "status": "unconfigured", "message": "not configured",
        })
        self.assertEqual(status["activeRegionCount"], 2)
        self.assertEqual(status["nationalPackageCount"], 1)
        self.assertEqual(set(status["coverageByDistCode"]), {"110108", "120000"})
        self.assertEqual(status["coverageByDistCode"]["120000"]["activeZoneCount"], 1)
        self.assertEqual(
            status["coverageByDistCode"]["110108"]["zoneClasses"]["controlled"], 1,
        )

        beijing_before = self.store.list_airspace_zones({
            "bbox": ["116.19,39.89,116.23,39.93"],
        })
        self.assertEqual(beijing_before["total"], 1)
        self.assertEqual(beijing_before["items"][0]["dataset"]["id"], haidian["dataset"]["id"])
        self.assertEqual(beijing_before["items"][0]["source"]["distCode"], "110108")

        demo_city_v2_payload = city_payload(
            "city-120000", "120000", "demo-v2", "演示城市", 117.23, 39.13,
            "prohibited",
        )
        demo_city_v2 = self.store.import_airspace(demo_city_v2_payload)
        self.assertNotEqual(demo_city_v2["dataset"]["id"], demo_city_v1["dataset"]["id"])
        beijing_after = self.store.list_airspace_zones({
            "bbox": ["116.19,39.89,116.23,39.93"],
        })
        self.assertEqual(beijing_after["total"], 1)
        self.assertEqual(beijing_after["items"][0]["dataset"]["id"], haidian["dataset"]["id"])
        old_demo_city = self.store.list_airspace_zones({
            "bbox": ["117.19,39.09,117.225,39.125"],
        })
        self.assertEqual(old_demo_city["total"], 0)
        new_demo_city = self.store.list_airspace_zones({
            "bbox": ["117.225,39.125,117.26,39.16"],
        })
        self.assertEqual(new_demo_city["total"], 1)
        self.assertEqual(new_demo_city["items"][0]["zoneClass"], "prohibited")

        base = city_payload("invalid-region", "120000", "v1", "Invalid", 117.0, 39.0, "warning")
        for bad_code in ("31011", "31011x", True):
            invalid = json.loads(json.dumps(base))
            invalid["source"]["distCode"] = bad_code
            with self.assertRaisesRegex(ValueError, "6 位行政区划"):
                server.normalize_airspace_import_payload(invalid)
        missing_city_code = json.loads(json.dumps(base))
        missing_city_code["source"].pop("distCode")
        with self.assertRaisesRegex(ValueError, "city 覆盖"):
            server.normalize_airspace_import_payload(missing_city_code)
        invalid_scope = json.loads(json.dumps(base))
        invalid_scope["source"]["coverageScope"] = "province"
        with self.assertRaisesRegex(ValueError, "coverageScope"):
            server.normalize_airspace_import_payload(invalid_scope)

    def test_airspace_catalog_covers_mainland_and_beijing_rule(self):
        catalog = server.airspace_catalog_summary()
        self.assertEqual(catalog["regionCount"], 31)
        self.assertEqual(catalog["uomLayerGroupCount"], 6)
        self.assertEqual(catalog["uomObservedRegionCount"], 30)
        self.assertEqual(catalog["uomNotObservedDistCodes"], ["110000"])
        self.assertEqual(catalog["beijingRule"]["distCode"], "110000")
        self.assertEqual(catalog["beijingRule"]["zoneClass"], "prohibited")
        self.assertFalse(catalog["beijingRule"]["authoritative"])
        self.assertEqual(
            {item["distCode"] for item in catalog["regions"]},
            set(catalog["uomObservedDistCodes"]) | {"110000"},
        )

    def test_builtin_beijing_reference_seed_is_idempotent_and_queryable(self):
        seeded = server.HistoryStore(
            os.path.join(self.temp.name, "seeded.db"), seed_builtin_airspace=True,
        )
        try:
            first_status = seeded.airspace_status(server.airspace_sync_configuration())
            self.assertEqual(first_status["activeZones"], 1)
            source = next(
                item for item in first_status["sources"]
                if item["slug"] == "uom-beijing-110000"
            )
            self.assertEqual(source["distCode"], "110000")
            self.assertEqual(source["coverageScope"], "city")
            self.assertFalse(source["authoritative"])
            self.assertEqual(source["catalog"]["zoneClass"], "prohibited")
            result = seeded.list_airspace_zones({
                "bbox": ["115.3,39.3,117.7,41.2"], "classes": ["prohibited"],
            })
            self.assertEqual(result["total"], 1)
            self.assertEqual(
                result["items"][0]["externalId"],
                "beijing-110000-uom-prohibited-reference",
            )
            with seeded._db() as conn:
                metadata = conn.execute(
                    "SELECT metadata_json FROM airspace_datasets WHERE status='active'"
                ).fetchone()[0]
            self.assertIn("ruleBasis", metadata)
            again = seeded.ensure_builtin_airspace()
            self.assertFalse(again["seeded"])
            self.assertEqual(
                seeded.airspace_status(server.airspace_sync_configuration())["activeZones"], 1,
            )
        finally:
            seeded.finish_all()

    def test_packaged_uom_derived_snapshot_is_verified_seeded_and_drawable(self):
        payload = server.packaged_uom_derived_airspace_payload()
        self.assertIsNotNone(payload)
        self.assertEqual(payload["source"]["slug"], "uom-derived-national")
        self.assertEqual(payload["source"]["coverageScope"], "national")
        self.assertFalse(payload["source"]["referenceOnly"])
        self.assertGreater(len(payload["data"]["features"]), 1000)
        self.assertTrue(all(
            feature["properties"]["zoneClass"] == "suitable"
            and feature["properties"]["referenceOnly"] is True
            and feature["properties"]["authoritative"] is False
            for feature in payload["data"]["features"]
        ))

        seeded = server.HistoryStore(
            os.path.join(self.temp.name, "packaged.db"),
            seed_packaged_airspace=True,
        )
        try:
            status = seeded.airspace_status(server.airspace_sync_configuration())
            source = next(
                item for item in status["sources"]
                if item["slug"] == "uom-derived-national"
            )
            self.assertEqual(source["coverageScope"], "national")
            self.assertTrue(source["displayOnMap"])
            self.assertFalse(source["referenceOnly"])
            self.assertFalse(source["authoritative"])
            self.assertEqual(status["nationalPackageCount"], 1)
            self.assertEqual(
                source["activeDataset"]["featureCount"],
                len(payload["data"]["features"]),
            )
            queried = seeded.list_airspace_zones({
                "bbox": ["121,30.8,121.6,31.4"], "classes": ["suitable"],
            })
            self.assertGreater(queried["total"], 0)
            self.assertFalse(queried["items"][0]["source"]["referenceOnly"])
            self.assertTrue(queried["items"][0]["source"]["displayOnMap"])
            again = seeded.ensure_packaged_uom_airspace()
            self.assertFalse(again["seeded"])
            self.assertEqual(again["reason"], "version_present")
        finally:
            seeded.finish_all()

    def test_uom_flyable_airspace_standard_format_and_validation_limits(self):
        value = (
            "104.359131, 30.481567|104.359131,30.487061|"
            "104.364624,30.487061|104.364624,30.481567; "
            "104.304199,30.481567|104.304199,30.487061|"
            "104.309692,30.487061|104.309692,30.481567;"
        )
        result = self.store.import_airspace({
            "source": {"slug": "uom-standard", "name": "UOM standard fixture"},
            "data": {"code": 200, "data": {"flyableAirspace": value}},
        })
        self.assertEqual(result["zonesImported"], 2)
        queried = self.store.list_airspace_zones({
            "bbox": ["104.29,30.47,104.38,30.50"], "classes": ["suitable"],
        })
        self.assertEqual(queried["total"], 2)
        for item in queried["items"]:
            ring = item["geometry"]["coordinates"][0]
            self.assertEqual(ring[0], ring[-1])
            self.assertEqual(item["zoneClass"], "suitable")

        with self.assertRaisesRegex(ValueError, "少于三个坐标"):
            server.normalize_airspace_import_payload({
                "data": {"flyableAirspace": "104.1,30.1|104.2,30.2;"},
            })
        original_limit = server.MAX_AIRSPACE_FEATURES
        server.MAX_AIRSPACE_FEATURES = 1
        try:
            with self.assertRaisesRegex(ValueError, "空域要素不能超过 1 个"):
                server.normalize_airspace_import_payload({
                    "data": {"flyableAirspace": value},
                })
        finally:
            server.MAX_AIRSPACE_FEATURES = original_limit

        original_vertex_limit = server.MAX_AIRSPACE_VERTICES_PER_FEATURE
        server.MAX_AIRSPACE_VERTICES_PER_FEATURE = 3
        try:
            with self.assertRaisesRegex(ValueError, "顶点数必须"):
                server.normalize_airspace_import_payload({
                    "data": {"type": "FeatureCollection", "features": [
                        airspace_feature("too-many", "too many", "warning", [[
                            [104.0, 30.0], [104.1, 30.0], [104.1, 30.1],
                            [104.0, 30.1], [104.0, 30.0],
                        ]]),
                    ]},
                })
        finally:
            server.MAX_AIRSPACE_VERTICES_PER_FEATURE = original_vertex_limit

        with self.assertRaisesRegex(ValueError, "bbox 参数必填"):
            self.store.list_airspace_zones({})
        with self.assertRaisesRegex(ValueError, "无效分类"):
            self.store.list_airspace_zones({
                "bbox": ["104,30,105,31"], "classes": ["illegal"],
            })
        with self.assertRaisesRegex(ValueError, "defaultClass"):
            server.normalize_airspace_import_payload({
                "defaultClass": "anything-goes",
                "data": {"type": "FeatureCollection", "features": [
                    airspace_feature("x", "x", "suitable", [[
                        [104.0, 30.0], [104.1, 30.0], [104.1, 30.1], [104.0, 30.0],
                    ]]),
                ]},
            })
        with self.assertRaisesRegex(ValueError, "坐标系必须"):
            server.normalize_airspace_import_payload({
                "coordinateSystem": "GCJ-02",
                "data": {"flyableAirspace": value},
            })

    def test_risk_fields_incident_aggregation_and_actions(self):
        fence = self.store.create_geofence(circle_geofence(severity="high"))
        now = int(time.time() * 1000) - 10_000
        first = drone("AA:BB:CC:00:20:01")
        first.update({"olat": 39.901, "olon": 116.401})
        first_snapshot = snapshot("risk-unit", now, [first])
        self.assertTrue(self.store.ingest_snapshot(first_snapshot))
        self.assertEqual(first["riskScore"], 65)
        self.assertEqual(first["riskLevel"], "high")
        self.assertEqual(first["geofenceIds"], [fence["id"]])
        self.assertIsInstance(first["incidentId"], int)
        first_detail = self.store.get_incident(first["incidentId"])
        first_hash = first_detail["evidenceSha256"]
        self.assertEqual(first_detail["evidence"]["geofences"][0], fence)

        same_risk = drone("AA:BB:CC:00:20:01")
        same_risk.update({"olat": 39.901, "olon": 116.401})
        self.assertTrue(self.store.ingest_snapshot(
            snapshot("risk-unit", now + 500, [same_risk])
        ))
        same_detail = self.store.get_incident(first["incidentId"])
        self.assertEqual(same_detail["evidenceSha256"], first_hash)
        self.assertEqual(same_detail["evidence"], first_detail["evidence"])

        second = drone("AA:BB:CC:00:20:01")
        second.pop("id")
        second.update({"alt": 9000, "spd": 25})
        second_snapshot = snapshot("risk-unit", now + 1000, [second])
        self.assertTrue(self.store.ingest_snapshot(second_snapshot))
        self.assertEqual(second["riskScore"], 100)
        self.assertEqual(second["riskLevel"], "critical")
        self.assertEqual(second["incidentId"], first["incidentId"])

        incidents = self.store.list_incidents({})
        self.assertEqual(incidents["total"], 1)
        self.assertEqual(incidents["items"][0]["riskScore"], 100)
        self.assertIn("缺少UAS ID", incidents["items"][0]["riskReasons"])
        detail = self.store.set_incident_status(
            first["incidentId"], "acknowledged", "checked by unit test", "tester"
        )
        self.assertEqual(detail["status"], "acknowledged")
        self.assertEqual(detail["actions"][0]["note"], "checked by unit test")
        self.assertEqual(detail["actions"][0]["actor"], "tester")
        action_count_after_ack = len(detail["actions"])
        duplicate_ack = self.store.set_incident_status(
            first["incidentId"], "acknowledged", "", "tester"
        )
        self.assertEqual(len(duplicate_ack["actions"]), action_count_after_ack)
        noted_ack = self.store.set_incident_status(
            first["incidentId"], "acknowledged", "second review", "tester"
        )
        self.assertEqual(noted_ack["actions"][-1]["action"], "note")
        self.assertEqual(noted_ack["actions"][-1]["note"], "second review")
        self.assertEqual(detail["evidence"]["riskScore"], 100)
        self.assertFalse(detail["simulated"])
        self.assertEqual(len(detail["evidenceSha256"]), 64)
        self.assertIn("incident_id", self.store.export_incidents_csv({}))

        self.store.set_incident_status(first["incidentId"], "resolved", "risk cleared", "tester")
        third = drone("AA:BB:CC:00:20:01")
        third.update({"olat": 39.901, "olon": 116.401})
        self.assertTrue(self.store.ingest_snapshot(snapshot("risk-unit", now + 2000, [third])))
        reopened = self.store.get_incident(first["incidentId"])
        self.assertEqual(reopened["status"], "open")
        self.assertEqual(reopened["actions"][-1]["action"], "reopened")
        self.assertEqual(reopened["actions"][-1]["fromStatus"], "resolved")
        self.assertEqual(reopened["actions"][-1]["toStatus"], "open")
        self.assertEqual(reopened["actions"][-1]["note"], "风险条件再次出现")
        self.assertEqual(reopened["actions"][-1]["actor"], "system")

        dismissed = self.store.set_incident_status(
            first["incidentId"], "dismissed", "known test flight", "tester"
        )
        action_count = len(dismissed["actions"])
        fourth = drone("AA:BB:CC:00:20:01")
        fourth.update({"olat": 39.901, "olon": 116.401})
        self.assertTrue(self.store.ingest_snapshot(snapshot("risk-unit", now + 3000, [fourth])))
        still_dismissed = self.store.get_incident(first["incidentId"])
        self.assertEqual(still_dismissed["status"], "dismissed")
        self.assertEqual(len(still_dismissed["actions"]), action_count)

        evidence_before_change = still_dismissed["evidence"]
        changed_payload = circle_geofence(name="Changed later", severity="low", radius=2000)
        self.store.update_geofence(fence["id"], changed_payload)
        self.assertTrue(self.store.delete_geofence(fence["id"]))
        evidence_after_delete = self.store.get_incident(first["incidentId"])["evidence"]
        self.assertEqual(evidence_after_delete, evidence_before_change)
        self.assertEqual(evidence_after_delete["geofences"][0]["name"], "Test circle")
        self.assertEqual(evidence_after_delete["geofences"][0]["severity"], "high")

    def test_incident_summary_uses_filtered_unpaginated_population(self):
        now = int(time.time() * 1000)
        cases = [
            ("low", "open", 116.0, "01"),
            ("medium", "acknowledged", 117.0, "02"),
            ("high", "resolved", 118.0, "03"),
            ("critical", "dismissed", 119.0, "04"),
            ("high", "open", 120.0, "05"),
        ]
        for severity, status, lon, suffix in cases:
            self.store.create_geofence(circle_geofence(
                name=severity, severity=severity, lon=lon, lat=39.0, radius=100,
            ))
            target = drone(f"AA:BB:CC:00:23:{suffix}", lat=39.0, lon=lon)
            target.update({"olat": 39.001, "olon": lon + 0.001})
            self.assertTrue(self.store.ingest_snapshot(
                snapshot("summary-" + severity, now, [target])
            ))
            if status != "open":
                self.store.set_incident_status(target["incidentId"], status, status, "tester")

        result = self.store.list_incidents({"page": ["1"], "page_size": ["2"]})
        self.assertEqual(result["total"], 5)
        self.assertEqual(len(result["items"]), 2)
        self.assertEqual(result["pages"], 3)
        self.assertEqual(result["summary"], {
            "open": 2, "acknowledged": 1, "resolved": 1, "dismissed": 1,
            "critical": 1, "high": 2, "medium": 1, "low": 1,
        })
        filtered = self.store.list_incidents({"status": ["resolved"], "page_size": ["1"]})
        self.assertEqual(filtered["total"], 1)
        self.assertEqual(filtered["summary"], {
            "open": 0, "acknowledged": 0, "resolved": 1, "dismissed": 0,
            "critical": 0, "high": 1, "medium": 0, "low": 0,
        })

    def test_csv_exports_escape_formula_like_external_text(self):
        self.store.create_geofence(circle_geofence(severity="high"))
        now = int(time.time() * 1000)
        target = drone("AA:BB:CC:00:24:01")
        target.update({
            "model": "=cmd", "id": "+evil", "olat": 39.901, "olon": 116.401,
        })
        frame = snapshot("-station", now, [target])
        frame["stationName"] = "  @operations"
        self.assertTrue(self.store.ingest_snapshot(frame))

        flight_csv = self.store.export_csv({})
        self.assertIn("'-station", flight_csv)
        self.assertIn("'@operations", flight_csv)
        self.assertIn("'=cmd", flight_csv)
        self.assertIn("'+evil", flight_csv)
        incident_csv = self.store.export_incidents_csv({})
        self.assertIn("'-station", incident_csv)
        self.assertIn("'=cmd", incident_csv)
        self.assertIn("'+evil", incident_csv)
        self.assertEqual(server._csv_safe(" harmless"), " harmless")
        self.assertEqual(server._csv_safe("  =formula"), "'  =formula")

    def test_geometric_altitude_does_not_create_implicit_risk(self):
        now = int(time.time() * 1000)
        high_altitude = drone("AA:BB:CC:00:20:02")
        high_altitude.update({"alt": 9000, "olat": 39.901, "olon": 116.401})
        self.assertTrue(self.store.ingest_snapshot(snapshot("alt-unit", now, [high_altitude])))
        self.assertEqual(high_altitude["riskScore"], 0)
        self.assertEqual(high_altitude["riskLevel"], "normal")
        self.assertEqual(high_altitude["riskReasons"], [])
        self.assertIsNone(high_altitude["incidentId"])
        self.assertEqual(self.store.list_incidents({})["total"], 0)

    def test_prune_preserves_incident_flight_and_track(self):
        self.store.retention_days = 1
        self.store.create_geofence(circle_geofence(severity="high"))
        now = int(time.time() * 1000)
        old = now - 3 * 24 * 60 * 60 * 1000

        protected = drone("AA:BB:CC:00:21:01")
        protected.update({"olat": 39.901, "olon": 116.401})
        self.assertTrue(self.store.ingest_snapshot(snapshot("protected", old, [protected])))
        incident = self.store.list_incidents({})["items"][0]
        protected_flight_id = incident["flightId"]

        ordinary = drone("AA:BB:CC:00:21:02", lat=41.0, lon=118.0)
        ordinary.update({"olat": 41.001, "olon": 118.001})
        self.assertTrue(self.store.ingest_snapshot(snapshot("ordinary", old, [ordinary])))
        ordinary_flight_id = next(
            item["id"] for item in self.store.list_flights({})["items"]
            if item["stationId"] == "ordinary"
        )

        self.store.expire_sessions(old + 16_000)
        self.assertEqual(self.store.prune(now), 1)
        protected_flight = self.store.get_flight(protected_flight_id)
        self.assertIsNotNone(protected_flight)
        self.assertGreaterEqual(len(protected_flight["points"]), 1)
        self.assertIsNone(self.store.get_flight(ordinary_flight_id))
        self.assertEqual(self.store.get_incident(incident["id"])["flightId"], protected_flight_id)

    def test_future_captured_at_is_clamped_for_history_and_live_state(self):
        before = int(time.time() * 1000)
        future = before + 4 * 60 * 1000
        first = drone("AA:BB:CC:00:22:01")
        first.update({"olat": 39.901, "olon": 116.401})
        self.assertTrue(self.store.ingest_snapshot(snapshot("future-history", future, [first])))
        after = int(time.time() * 1000)
        flight = self.store.list_flights({"station_id": ["future-history"]})["items"][0]
        self.assertGreaterEqual(flight["start"], before)
        self.assertLessEqual(flight["start"], after)

        second = drone("AA:BB:CC:00:22:02")
        second.update({"olat": 39.902, "olon": 116.402})
        self.assertTrue(self.store.ingest_snapshot(snapshot("future-history", after, [second])))
        self.assertEqual(self.store.list_flights({"station_id": ["future-history"]})["total"], 2)

        saved = (
            server.HISTORY, server.STATION_LIVE, server.LATEST_SNAPSHOT,
            server.LATEST_SNAPSHOT_AT, server.SNAPSHOT_SEQUENCE,
        )
        try:
            server.HISTORY = self.store
            server.STATION_LIVE = {}
            live_before = int(time.time() * 1000)
            live = drone("AA:BB:CC:00:22:03")
            live.update({"olat": 39.903, "olon": 116.403})
            self.assertTrue(server.publish_snapshot(
                snapshot("future-live", live_before + 4 * 60 * 1000, [live]),
                raise_on_history_error=True,
            ))
            live_after = int(time.time() * 1000)
            live_state = server.STATION_LIVE["future-live"]
            self.assertGreaterEqual(live_state["capturedAt"], live_before)
            self.assertLessEqual(live_state["capturedAt"], live_after)
        finally:
            (server.HISTORY, server.STATION_LIVE, server.LATEST_SNAPSHOT,
             server.LATEST_SNAPSHOT_AT, server.SNAPSHOT_SEQUENCE) = saved

    def test_fifo_backfill_reopens_same_flight_after_query_expiry(self):
        old = int(time.time() * 1000) - 60 * 60 * 1000
        mac = "AA:BB:CC:00:00:02"
        self.assertTrue(self.store.ingest_snapshot(snapshot("offline", old, [drone(mac)])))
        self.assertEqual(self.store.list_flights({})["total"], 1)
        self.assertTrue(self.store.ingest_snapshot(snapshot("offline", old + 1000, [drone(mac)])))
        rows = self.store.list_flights({})
        self.assertEqual(rows["total"], 1)
        self.assertEqual(rows["items"][0]["n"], 1)

    def test_stale_out_of_order_snapshot_is_rejected(self):
        now = int(time.time() * 1000)
        self.assertTrue(self.store.ingest_snapshot(
            snapshot("ordered", now, [drone("AA:BB:CC:00:00:03")])
        ))
        self.assertFalse(self.store.ingest_snapshot(
            snapshot("ordered", now - 30_000, [drone("AA:BB:CC:00:00:04")])
        ))
        self.assertEqual(self.store.list_flights({})["total"], 1)


class SessionTests(unittest.TestCase):
    def setUp(self):
        self.saved = (
            server.ADMIN_USER,
            server.SESSION_SECRET,
            server.SESSION_TTL_SECONDS,
        )
        server.ADMIN_USER = "admin"
        server.SESSION_SECRET = b"s" * 32
        server.SESSION_TTL_SECONDS = 600

    def tearDown(self):
        server.ADMIN_USER, server.SESSION_SECRET, server.SESSION_TTL_SECONDS = self.saved

    def test_signed_session_expiry_and_tamper_detection(self):
        token = server.create_session("admin", now=1000)
        self.assertEqual(server.verify_session(token, now=1200)["u"], "admin")
        self.assertIsNone(server.verify_session(token, now=1700))
        self.assertIsNone(server.verify_session(token + "x", now=1200))


class TelemetryNormalizationTests(unittest.TestCase):
    def test_monitor_location_uses_demo_defaults_and_rejects_invalid_coordinates(self):
        with mock.patch.dict(os.environ, {}, clear=True):
            self.assertEqual(server._monitor_location(), (39.9042, 116.4074))
        with mock.patch.dict(os.environ, {
            "RID_MONITOR_LAT": "91",
            "RID_MONITOR_LON": "not-a-number",
        }, clear=True):
            self.assertEqual(server._monitor_location(), (39.9042, 116.4074))
        with mock.patch.dict(os.environ, {
            "RID_MONITOR_LAT": "39.9042",
            "RID_MONITOR_LON": "116.4074",
        }, clear=True):
            self.assertEqual(server._monitor_location(), (39.9042, 116.4074))

    def test_untrusted_drone_fields_are_bounded_before_broadcast(self):
        raw = snapshot("<station>", 1_700_000_000_000, [{
            "mac": "aa:bb:cc:00:00:01",
            "model": "<img src=x onerror=alert(1)>",
            "id": "RID\x00-001",
            "rssi": "<img src=x>",
            "lat": "39.9042",
            "lon": "116.4074",
            "alt": 80,
            "spd": 8,
            "heading": 721,
            "proto": 2,
            "motion": "orbit",
            "simulated": True,
        }, {
            "mac": "not-a-mac",
            "model": "must be dropped",
        }])
        normalized = server.normalize_snapshot(raw)
        self.assertEqual(normalized["stationId"], "station")
        self.assertEqual(normalized["n"], 1)
        self.assertEqual(normalized["drones"][0]["mac"], "AA:BB:CC:00:00:01")
        self.assertNotIn("<", normalized["drones"][0]["model"])
        self.assertNotIn("rssi", normalized["drones"][0])
        self.assertEqual(normalized["drones"][0]["heading"], 1)
        self.assertEqual(normalized["drones"][0]["proto"], 2)
        self.assertTrue(normalized["drones"][0]["simulated"])

        top_level_simulated = snapshot("sim", 1_700_000_000_000, [
            drone("AA:BB:CC:00:00:09")
        ])
        top_level_simulated["simulated"] = True
        propagated = server.normalize_snapshot(top_level_simulated)
        self.assertTrue(propagated["simulated"])
        self.assertTrue(propagated["drones"][0]["simulated"])

    def test_builtin_demo_marks_snapshot_and_every_drone_simulated(self):
        captured = []
        original_publish = server.publish_snapshot
        original_sleep = server.time.sleep
        server.publish_snapshot = lambda value: captured.append(value) or True
        server.time.sleep = lambda _seconds: (_ for _ in ()).throw(RuntimeError("stop demo"))
        try:
            with self.assertRaisesRegex(RuntimeError, "stop demo"):
                server.demo_worker(0.01)
        finally:
            server.publish_snapshot = original_publish
            server.time.sleep = original_sleep
        self.assertEqual(len(captured), 1)
        self.assertTrue(captured[0]["simulated"])
        self.assertEqual(captured[0]["sourceType"], "server-simulator")
        self.assertFalse(captured[0]["hardwareConnected"])
        self.assertTrue(all(item["simulated"] is True for item in captured[0]["drones"]))


class GatewaySpoolTests(unittest.TestCase):
    def test_snapshot_is_stamped_before_spooling(self):
        with tempfile.TemporaryDirectory() as temp:
            spool = gateway.Spool(os.path.join(temp, "spool.db"))
            sender = SimpleNamespace(notify=lambda: None)
            args = SimpleNamespace(station_id="win-station", station_name="Windows receiver")
            raw = json.dumps({"t": "snap", "drones": []})
            self.assertTrue(gateway.enqueue_line(raw, args, spool, sender))
            item = spool.first()
            payload = json.loads(item["payload"])
            self.assertEqual(payload["stationId"], "win-station")
            self.assertEqual(payload["stationName"], "Windows receiver")
            self.assertEqual(payload["sourceType"], "hardware-usb")
            self.assertEqual(payload["sourceTransport"], "usb-serial")
            self.assertTrue(payload["hardwareConnected"])
            self.assertGreater(payload["capturedAt"], 0)
            self.assertEqual(spool.count(), 1)


class UomWmsProxyTests(unittest.TestCase):
    def test_managed_token_is_atomic_persistent_and_takes_precedence(self):
        managed_token = "e3ba91f0-e7bc-4dce-a684-f0bf50f7d020"
        with tempfile.TemporaryDirectory() as temp:
            token_path = os.path.join(temp, "uom_wms_token")
            with mock.patch.object(server, "UOM_WMS_TOKEN_PATH", token_path):
                with mock.patch.dict(
                    os.environ, {"RID_UOM_WMS_TOKEN": "stale-environment-token"}, clear=False,
                ):
                    previous_revision = server._uom_wms_runtime_state()["credentialRevision"]
                    result = server.store_uom_wms_token(managed_token)
                    self.assertEqual(result, {
                        "ok": True, "configured": True,
                        "credentialSource": "managed-file",
                    })
                    self.assertEqual(server._uom_wms_token(), managed_token)
                    self.assertEqual(server.uom_wms_status()["credentialSource"], "managed-file")
                    with open(token_path, "r", encoding="utf-8") as handle:
                        self.assertEqual(handle.read(), managed_token + "\n")
                    if os.name == "posix":
                        self.assertEqual(stat.S_IMODE(os.stat(token_path).st_mode), 0o600)
                    self.assertNotEqual(
                        server._uom_wms_runtime_state()["credentialRevision"],
                        previous_revision,
                    )
                    with self.assertRaises(ValueError):
                        server.store_uom_wms_token("not-a-token")
                    self.assertEqual(server._uom_wms_token(), managed_token)

    def test_normalizes_only_the_observed_uom_layer_groups(self):
        self.assertEqual(len(server.UOM_WMS_LAYER_GROUPS), 6)
        self.assertEqual(sum(group.count("sf") for group in server.UOM_WMS_LAYER_GROUPS), 30)
        normalized = server.normalize_uom_wms_request(uom_wms_params())
        self.assertEqual(normalized["LAYERS"], server.UOM_WMS_LAYER_GROUPS[2])
        self.assertEqual(normalized["STYLES"], server.UOM_WMS_STYLE_GROUPS[2])
        self.assertEqual(
            len(normalized["LAYERS"].split(",")), len(normalized["STYLES"].split(",")),
        )
        self.assertEqual(normalized["SRS"], "EPSG:3857")
        self.assertEqual(normalized["BBOX"], "13500000,3600000,13501000,3601000")

    def test_rejects_untrusted_wms_parameters_and_invalid_bounds(self):
        invalid_cases = []
        unexpected = uom_wms_params()
        unexpected["token"] = ["client-supplied-token"]
        invalid_cases.append(unexpected)
        duplicate = uom_wms_params()
        duplicate["layers"] = [duplicate["LAYERS"][0]]
        invalid_cases.append(duplicate)
        wrong_style = uom_wms_params()
        wrong_style["STYLES"] = ["other-style"]
        invalid_cases.append(wrong_style)
        wrong_layer = uom_wms_params()
        wrong_layer["LAYERS"] = ["QGSFKYFW:sf110000"]
        invalid_cases.append(wrong_layer)
        wrong_srs = uom_wms_params()
        wrong_srs["SRS"] = ["EPSG:4326"]
        invalid_cases.append(wrong_srs)
        outside_world = uom_wms_params()
        outside_world["BBOX"] = ["0,0,30000000,1"]
        invalid_cases.append(outside_world)
        too_large = uom_wms_params()
        too_large["WIDTH"] = ["2049"]
        invalid_cases.append(too_large)

        for params in invalid_cases:
            with self.subTest(params=params):
                with self.assertRaises(ValueError):
                    server.normalize_uom_wms_request(params)

    def test_upstream_request_injects_token_without_echoing_it(self):
        image = png_fixture()
        response = mock.MagicMock()
        response.headers = {"Content-Type": "image/png; charset=binary", "Content-Length": str(len(image))}
        response.read.return_value = image
        upstream = mock.MagicMock()
        upstream.__enter__.return_value = response

        normalized = server.normalize_uom_wms_request(uom_wms_params())
        with mock.patch.object(server, "_open_uom_wms", return_value=upstream) as open_wms:
            result = server.fetch_uom_wms(
                normalized, "unit-test-token",
            )

        self.assertEqual(result, image)
        request = open_wms.call_args.args[0]
        upstream_query = parse_qs(urlparse(request.full_url).query)
        expected_query = {name.lower(): [value] for name, value in normalized.items()}
        expected_query["token"] = ["unit-test-token"]
        self.assertEqual(upstream_query, expected_query)
        self.assertNotIn("token", server.normalize_uom_wms_request(uom_wms_params()))

    def test_rejects_empty_truncated_corrupt_and_length_mismatched_png(self):
        valid = png_fixture()
        corrupt = bytearray(valid)
        corrupt[-5] ^= 0x01
        invalid_bodies = [
            b"",
            server.PNG_SIGNATURE + b"not-a-png",
            valid[:-1],
            bytes(corrupt),
            b"not-png" + valid,
            png_fixture(include_idat=False),
            png_fixture(idat_payload=b"not-a-zlib-stream"),
            png_fixture(width=32, height=32),
        ]
        for body in invalid_bodies:
            with self.subTest(length=len(body)):
                response = mock.MagicMock()
                response.headers = {
                    "Content-Type": "image/png",
                    "Content-Length": str(len(body)),
                }
                response.read.return_value = body
                upstream = mock.MagicMock()
                upstream.__enter__.return_value = response
                with mock.patch.object(server, "_open_uom_wms", return_value=upstream):
                    with self.assertRaises(server.UomWmsUpstreamError):
                        server.fetch_uom_wms(
                            server.normalize_uom_wms_request(uom_wms_params()), "secret",
                        )

        response = mock.MagicMock()
        response.headers = {
            "Content-Type": "image/png",
            "Content-Length": str(len(valid) + 1),
        }
        response.read.return_value = valid
        upstream = mock.MagicMock()
        upstream.__enter__.return_value = response
        with mock.patch.object(server, "_open_uom_wms", return_value=upstream):
            with self.assertRaises(server.UomWmsUpstreamError):
                server.fetch_uom_wms(
                    server.normalize_uom_wms_request(uom_wms_params()), "secret",
                )

    def test_fetch_concurrency_is_bounded_before_the_connect_relay(self):
        image = png_fixture()
        state = {"active": 0, "peak": 0}
        state_lock = threading.Lock()

        class SlowResponse:
            headers = {"Content-Type": "image/png", "Content-Length": str(len(image))}

            def __enter__(self):
                with state_lock:
                    state["active"] += 1
                    state["peak"] = max(state["peak"], state["active"])
                return self

            def read(self, _limit):
                time.sleep(0.04)
                return image

            def __exit__(self, _kind, _error, _traceback):
                with state_lock:
                    state["active"] -= 1

        normalized = server.normalize_uom_wms_request(uom_wms_params())
        slots = threading.BoundedSemaphore(3)
        with mock.patch.object(server, "UOM_WMS_FETCH_SLOTS", slots):
            with mock.patch.object(server, "UOM_WMS_FETCH_QUEUE_TIMEOUT_SECONDS", 2):
                with mock.patch.object(
                    server, "_open_uom_wms", side_effect=lambda _request: SlowResponse(),
                ) as open_wms:
                    with ThreadPoolExecutor(max_workers=12) as executor:
                        results = list(executor.map(
                            lambda _index: server.fetch_uom_wms(normalized, "secret"),
                            range(12),
                        ))

        self.assertTrue(all(result == image for result in results))
        self.assertEqual(open_wms.call_count, 12)
        self.assertLessEqual(state["peak"], 3)

    def test_revision_scoped_tile_cache_avoids_duplicate_upstream_fetches(self):
        image = png_fixture()
        response = mock.MagicMock()
        response.headers = {"Content-Type": "image/png", "Content-Length": str(len(image))}
        response.read.return_value = image
        upstream = mock.MagicMock()
        upstream.__enter__.return_value = response
        normalized = server.normalize_uom_wms_request(uom_wms_params())
        server._clear_uom_wms_cache()
        try:
            with mock.patch.object(server, "_open_uom_wms", return_value=upstream) as open_wms:
                first = server.fetch_uom_wms(normalized, "secret", "cache-revision")
                second = server.fetch_uom_wms(normalized, "secret", "cache-revision")
            self.assertEqual(first, image)
            self.assertEqual(second, image)
            self.assertEqual(open_wms.call_count, 1)
        finally:
            server._clear_uom_wms_cache()

    def test_stale_success_and_error_cannot_mutate_rotated_credential_health(self):
        server._reset_uom_wms_runtime()
        old_revision = server._uom_wms_runtime_state()["credentialRevision"]
        server._reset_uom_wms_runtime()
        before = server._uom_wms_runtime_state()

        self.assertFalse(server._record_uom_wms_result(
            True, credential_revision=old_revision,
        ))
        self.assertFalse(server._record_uom_wms_result(
            False, 401, "http_error", old_revision,
        ))
        self.assertEqual(server._uom_wms_runtime_state(), before)

    def test_status_distinguishes_configured_verifying_ready_and_expired(self):
        base_runtime = {
            "credentialRevision": "revision-a",
            "lastResult": None,
            "lastSuccessAt": None,
            "lastErrorAt": None,
            "lastErrorStatus": None,
        }
        proxy = {"configured": True, "valid": True, "mode": "http-connect"}
        with mock.patch.object(server, "_uom_wms_token_state", return_value=("secret", "managed-file")):
            with mock.patch.object(server, "_uom_wms_proxy_status", return_value=proxy):
                with mock.patch.object(server, "_uom_wms_runtime_state", return_value=base_runtime):
                    verifying = server.uom_wms_status()
                expired_runtime = dict(
                    base_runtime,
                    lastResult="error",
                    lastErrorAt=int(time.time() * 1000),
                    lastErrorStatus=401,
                )
                with mock.patch.object(server, "_uom_wms_runtime_state", return_value=expired_runtime):
                    expired = server.uom_wms_status()

        self.assertTrue(verifying["configured"])
        self.assertTrue(verifying["enabled"])
        self.assertFalse(verifying["ready"])
        self.assertEqual(verifying["status"], "verifying")
        self.assertFalse(expired["ready"])
        self.assertEqual(expired["status"], "expired")
        self.assertEqual(expired["upstreamStatus"], 401)

    def test_proxy_rejects_stale_non_secret_credential_revision(self):
        class CapturingHandler:
            def __init__(self):
                self.response = None

            def _send_json(self, status_code, payload, headers=None):
                self.response = (status_code, payload, headers)

        params = uom_wms_params()
        params["_ridv"] = ["old-revision"]
        handler = CapturingHandler()
        with mock.patch.object(
            server, "_uom_wms_request_credential", return_value=("secret", "new-revision"),
        ):
            with mock.patch.object(server, "fetch_uom_wms") as fetch:
                server.Handler._proxy_uom_wms(handler, params)
        self.assertEqual(handler.response[0], 409)
        self.assertEqual(handler.response[1]["error"], "stale_uom_wms_revision")
        fetch.assert_not_called()

    def test_status_and_upstream_error_never_expose_token(self):
        secret = "unit-test-uom-wms-token"
        runtime = {
            "credentialRevision": "public-revision",
            "lastResult": "success",
            "lastSuccessAt": int(time.time() * 1000),
            "lastErrorAt": None,
            "lastErrorStatus": None,
        }
        with mock.patch.dict(os.environ, {"RID_UOM_WMS_TOKEN": secret}, clear=False):
            with mock.patch.object(server, "_uom_wms_runtime_state", return_value=runtime):
                with mock.patch("builtins.print") as output:
                    status = server.uom_wms_status()
        self.assertTrue(status["enabled"])
        self.assertTrue(status["configured"])
        self.assertTrue(status["ready"])
        self.assertEqual(status["status"], "ready")
        self.assertEqual(status["wmsPath"], "/api/airspace/uom-wms")
        self.assertEqual(status["zoomRange"], [9, 18])
        self.assertEqual(status["renderMode"], "suitable-raster")
        self.assertEqual(status["credentialRevision"], "public-revision")
        self.assertEqual(len(status["layerGroups"]), 6)
        self.assertNotIn(secret, json.dumps(status, ensure_ascii=False))
        output.assert_not_called()
        class CapturingHandler:
            def __init__(self):
                self.response = None

            def _send_json(self, status_code, payload, headers=None):
                self.response = (status_code, payload, headers)

        handler = CapturingHandler()
        with mock.patch.dict(os.environ, {"RID_UOM_WMS_TOKEN": secret}, clear=False):
            with mock.patch.object(
                server, "_uom_wms_request_credential", return_value=(secret, "public-revision"),
            ):
                with mock.patch.object(
                    server, "fetch_uom_wms", side_effect=server.UomWmsUpstreamError(401),
                ) as fetch:
                    with mock.patch.object(server, "_record_uom_wms_result") as record:
                        with mock.patch("builtins.print") as output:
                            server.Handler._proxy_uom_wms(handler, uom_wms_params())
        self.assertEqual(handler.response[0], 502)
        self.assertEqual(handler.response[1]["error"], "uom_wms_upstream_error")
        self.assertEqual(handler.response[1]["upstreamStatus"], 401)
        self.assertNotIn(secret, json.dumps(handler.response[1], ensure_ascii=False))
        self.assertEqual(fetch.call_args.args[1], secret)
        self.assertEqual(fetch.call_args.args[2], "public-revision")
        record.assert_called_once_with(False, 401, "upstream_error", "public-revision")
        output.assert_not_called()

    def test_proxy_success_returns_png_without_upstream_metadata(self):
        image = png_fixture()

        class CapturingHandler:
            def __init__(self):
                self.response = None

            def _send_json(self, status_code, payload, headers=None):
                self.response = (status_code, payload, headers)

            def _send_bytes(self, status_code, body, content_type, filename=None, headers=None):
                self.response = (status_code, body, content_type, filename, headers)

        handler = CapturingHandler()
        with mock.patch.object(
            server, "_uom_wms_request_credential",
            return_value=("proxy-secret", "public-revision"),
        ):
            with mock.patch.object(server, "fetch_uom_wms", return_value=image) as fetch:
                with mock.patch.object(
                    server, "_record_uom_wms_result", return_value=True,
                ) as record:
                    server.Handler._proxy_uom_wms(handler, uom_wms_params())

        self.assertEqual(handler.response[0], 200)
        self.assertEqual(handler.response[1], image)
        self.assertEqual(handler.response[2], "image/png")
        self.assertIsNone(handler.response[3])
        self.assertEqual(handler.response[4], {"Cache-Control": "private, no-store"})
        self.assertEqual(fetch.call_args.args[0]["LAYERS"], server.UOM_WMS_LAYER_GROUPS[2])
        self.assertEqual(fetch.call_args.args[1], "proxy-secret")
        self.assertEqual(fetch.call_args.args[2], "public-revision")
        record.assert_called_once_with(True, credential_revision="public-revision")
        self.assertNotIn("proxy-secret", repr(handler.response))

    def test_proxy_discards_a_success_that_completed_after_credential_rotation(self):
        image = png_fixture()

        class CapturingHandler:
            def __init__(self):
                self.response = None

            def _send_json(self, status_code, payload, headers=None):
                self.response = (status_code, payload, headers)

            def _send_bytes(self, *_args, **_kwargs):
                raise AssertionError("stale tile must not be returned")

        handler = CapturingHandler()
        with mock.patch.object(
            server, "_uom_wms_request_credential", return_value=("old-token", "old-revision"),
        ):
            with mock.patch.object(server, "fetch_uom_wms", return_value=image):
                with mock.patch.object(
                    server, "_record_uom_wms_result", return_value=False,
                ) as record:
                    server.Handler._proxy_uom_wms(handler, uom_wms_params())

        self.assertEqual(handler.response[0], 409)
        self.assertEqual(handler.response[1]["error"], "stale_uom_wms_revision")
        record.assert_called_once_with(True, credential_revision="old-revision")

    def test_upstream_errors_are_sanitized_but_keep_status_metadata(self):
        error = urllib.error.HTTPError(
            server.UOM_WMS_ENDPOINT, 401, "Unauthorized", {}, None,
        )
        with mock.patch.object(server, "_open_uom_wms", side_effect=error):
            with self.assertRaises(server.UomWmsUpstreamError) as raised:
                server.fetch_uom_wms(
                    server.normalize_uom_wms_request(uom_wms_params()), "unit-test-token",
                )
        self.assertEqual(raised.exception.upstream_status, 401)
        self.assertNotIn("unit-test-token", str(raised.exception))


class CloudIntegrationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temp = tempfile.TemporaryDirectory()
        cls.http_port = free_port()
        cls.ws_port = free_port()
        cls.base = f"http://127.0.0.1:{cls.http_port}"
        cls.ws_url = f"ws://127.0.0.1:{cls.ws_port}/"
        cls.ingest_token = "ingest-token-0123456789abcdef"
        cls.admin_password = "abc123"
        env = os.environ.copy()
        env.update({
            "PYTHONUNBUFFERED": "1",
            "PYTHONUTF8": "1",
            "RID_INGEST_TOKEN": cls.ingest_token,
            "RID_ADMIN_USER": "admin",
            "RID_ADMIN_PASSWORD": cls.admin_password,
            "RID_SESSION_SECRET": "session-secret-0123456789abcdef0123456789abcdef",
            "RID_COOKIE_SECURE": "1",
            "AMAP_KEY": "test-amap-key",
            "AMAP_SECURITY_CODE": "test-amap-security-code",
            "RID_MONITOR_LAT": "39.9042",
            "RID_MONITOR_LON": "116.4074",
        })
        for key in (
            "RID_UOM_WMS_TOKEN", "RID_UOM_WMS_TOKEN_FILE",
            "RID_UOM_AIRSPACE_ENDPOINT", "RID_UOM_AIRSPACE_CLIENT_ID",
            "RID_UOM_AIRSPACE_CREDENTIAL",
            "RID_AIRSPACE_IMPORT_MAX_BYTES", "RID_AIRSPACE_MAX_FEATURES",
            "RID_AIRSPACE_MAX_VERTICES_PER_FEATURE", "RID_AIRSPACE_MAX_TOTAL_VERTICES",
        ):
            env.pop(key, None)
        cls.proc = subprocess.Popen(
            [
                sys.executable,
                str(HERE / "server.py"),
                "--cloud",
                "--bind", "127.0.0.1",
                "--http", str(cls.http_port),
                "--ws", str(cls.ws_port),
                "--db", os.path.join(cls.temp.name, "cloud.db"),
            ],
            cwd=HERE,
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",
        )
        deadline = time.time() + 15
        while time.time() < deadline:
            if cls.proc.poll() is not None:
                output = cls.proc.stdout.read()
                raise RuntimeError("cloud server exited during startup:\n" + output)
            try:
                with urllib.request.urlopen(cls.base + "/healthz", timeout=0.5):
                    break
            except (OSError, urllib.error.URLError):
                time.sleep(0.1)
        else:
            raise RuntimeError("cloud server did not become healthy")

        status, headers, _ = cls.request(
            "/api/auth/login",
            method="POST",
            payload={"username": "admin", "password": cls.admin_password},
        )
        if status != 200:
            raise RuntimeError(f"login failed during setup: HTTP {status}")
        set_cookie = headers.get("Set-Cookie", "")
        cls.cookie = set_cookie.split(";", 1)[0]
        cls.set_cookie = set_cookie

    @classmethod
    def tearDownClass(cls):
        cls.proc.terminate()
        try:
            cls.proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            cls.proc.kill()
            cls.proc.wait(timeout=5)
        if cls.proc.stdout:
            cls.proc.stdout.close()
        cls.temp.cleanup()

    @classmethod
    def request(cls, path, method="GET", payload=None, headers=None):
        body = None
        request_headers = dict(headers or {})
        if payload is not None:
            body = json.dumps(payload).encode("utf-8")
            request_headers.setdefault("Content-Type", "application/json")
        request = urllib.request.Request(
            cls.base + path,
            data=body,
            method=method,
            headers=request_headers,
        )
        try:
            with urllib.request.urlopen(request, timeout=5) as response:
                return response.status, response.headers, response.read()
        except urllib.error.HTTPError as exc:
            return exc.code, exc.headers, exc.read()

    def test_auth_static_allowlist_and_runtime_config(self):
        status, _, _ = self.request("/api/status")
        self.assertEqual(status, 401)
        status, _, _ = self.request("/server.py")
        self.assertEqual(status, 404)
        status, _, _ = self.request("/.env")
        self.assertEqual(status, 404)
        status, dashboard_headers, body = self.request("/dashboard.html")
        self.assertEqual(status, 200)
        self.assertIn(b"loginOverlay", body)
        self.assertIn(
            "'unsafe-eval'",
            dashboard_headers.get("Content-Security-Policy", ""),
        )
        status, _, body = self.request("/runtime-config.js")
        self.assertEqual(status, 200)
        self.assertIn(b"test-amap-key", body)
        self.assertIn(b'"wsPort":null', body)
        self.assertIn(b'"monitorLat":39.9042', body)
        self.assertIn(b'"monitorLon":116.4074', body)
        self.assertIn("Secure", self.set_cookie)

        empty = asyncio.run(self._receive_authenticated())
        self.assertEqual(empty["n"], 0)
        self.assertIn("serverId", empty)
        self.assertIn("seq", empty)

        status, _, _ = self.request(
            "/api/auth/login",
            method="POST",
            payload={"username": "admin", "password": "wrong-password"},
        )
        self.assertEqual(status, 401)
        status, _, _ = self.request(
            "/api/auth/login",
            method="POST",
            payload={"username": "管理员", "password": "错误密码"},
        )
        self.assertEqual(status, 401)
        status, _, _ = self.request(
            "/api/auth/login",
            method="POST",
            payload={"username": "\ud800", "password": "wrong-password"},
        )
        self.assertEqual(status, 401)
        status, _, body = self.request("/api/auth/me", headers={"Cookie": self.cookie})
        self.assertEqual(status, 200)
        self.assertEqual(json.loads(body)["user"], "admin")
        status, _, body = self.request(
            "/api/flights?from=Infinity", headers={"Cookie": self.cookie}
        )
        self.assertEqual(status, 400)
        self.assertEqual(json.loads(body)["error"], "invalid_query")

    def test_uom_wms_route_requires_auth_and_rejects_invalid_requests(self):
        query = urlencode({key: values[0] for key, values in uom_wms_params().items()})
        status, _, body = self.request("/api/airspace/uom-wms?" + query)
        self.assertEqual(status, 401)
        self.assertNotIn(b"unit-test", body)

        auth_headers = {"Cookie": self.cookie}
        status, _, body = self.request(
            "/api/airspace/uom-wms?" + query, headers=auth_headers,
        )
        self.assertEqual(status, 503)
        unconfigured = json.loads(body)
        self.assertEqual(unconfigured["error"], "uom_wms_not_configured")
        self.assertNotIn("unit-test-uom-wms-token", json.dumps(unconfigured, ensure_ascii=False))

        invalid_query = query.replace(
            urlencode({"LAYERS": uom_wms_params()["LAYERS"][0]}),
            urlencode({"LAYERS": "QGSFKYFW:sf110000"}),
        )
        status, _, body = self.request(
            "/api/airspace/uom-wms?" + invalid_query, headers=auth_headers,
        )
        self.assertEqual(status, 400)
        self.assertEqual(json.loads(body)["error"], "invalid_uom_wms_request")

    def test_uom_token_update_requires_admin_and_never_echoes_secret(self):
        token = "04c79a84-b89e-42a2-bf34-83b720f4b685"
        token_path = os.path.join(self.temp.name, "uom_wms_token")
        try:
            status, _, body = self.request(
                "/api/airspace/uom-token", method="POST", payload={"token": token},
            )
            self.assertEqual(status, 401)
            self.assertNotIn(token.encode("utf-8"), body)

            status, _, body = self.request(
                "/api/airspace/uom-token", method="POST", payload={"token": token},
                headers={"Cookie": self.cookie, "Sec-Fetch-Site": "cross-site"},
            )
            self.assertEqual(status, 403)
            self.assertNotIn(token.encode("utf-8"), body)

            status, _, body = self.request(
                "/api/airspace/uom-token", method="POST", payload={"token": "invalid"},
                headers={"Cookie": self.cookie},
            )
            self.assertEqual(status, 400)
            self.assertFalse(os.path.exists(token_path))

            status, _, body = self.request(
                "/api/airspace/uom-token", method="POST", payload={"token": token},
                headers={"Cookie": self.cookie},
            )
            self.assertEqual(status, 200)
            response = json.loads(body)
            self.assertEqual(response["credentialSource"], "managed-file")
            self.assertNotIn(token, json.dumps(response, ensure_ascii=False))
            with open(token_path, "r", encoding="utf-8") as handle:
                self.assertEqual(handle.read(), token + "\n")

            status, _, body = self.request(
                "/api/airspace/uom-status", headers={"Cookie": self.cookie},
            )
            self.assertEqual(status, 200)
            uom_status = json.loads(body)
            self.assertTrue(uom_status["enabled"])
            self.assertEqual(uom_status["credentialSource"], "managed-file")
            self.assertNotIn(token, json.dumps(uom_status, ensure_ascii=False))
        finally:
            try:
                os.unlink(token_path)
            except FileNotFoundError:
                pass

    def test_uom_wms_http_auth_configuration_and_parameter_guards(self):
        query = urlencode(uom_wms_params(), doseq=True)

        status, _, body = self.request("/api/airspace/uom-wms?" + query)
        self.assertEqual(status, 401)
        self.assertEqual(json.loads(body)["error"], "authentication_required")

        auth_headers = {"Cookie": self.cookie}
        status, _, body = self.request(
            "/api/airspace/uom-wms?" + query, headers=auth_headers,
        )
        self.assertEqual(status, 503)
        unconfigured = json.loads(body)
        self.assertEqual(unconfigured["error"], "uom_wms_not_configured")
        self.assertEqual(unconfigured["missing"], ["RID_UOM_WMS_TOKEN"])
        self.assertNotIn("uom-wms-secret", json.dumps(unconfigured, ensure_ascii=False))

        invalid = dict(uom_wms_params())
        invalid["LAYERS"] = ["QGSFKYFW:sf110000"]
        status, _, body = self.request(
            "/api/airspace/uom-wms?" + urlencode(invalid, doseq=True),
            headers=auth_headers,
        )
        self.assertEqual(status, 400)
        self.assertEqual(json.loads(body)["error"], "invalid_uom_wms_request")

        status, _, body = self.request(
            "/api/airspace/uom-wms?" + query.replace("WIDTH=256", "WIDTH=2049"),
            headers=auth_headers,
        )
        self.assertEqual(status, 400)
        self.assertEqual(json.loads(body)["error"], "invalid_uom_wms_request")

    def test_ingest_history_and_multistation_live_merge(self):
        now = int(time.time() * 1000)
        status, _, _ = self.request(
            "/api/ingest",
            method="POST",
            payload=snapshot("A", now, [drone("AA:BB:CC:00:10:01")]),
        )
        self.assertEqual(status, 401)
        status, _, _ = self.request(
            "/api/ingest",
            method="POST",
            payload=snapshot("A", now, [drone("AA:BB:CC:00:10:01")]),
            headers={"Authorization": "Bearer é"},
        )
        self.assertEqual(status, 401)
        ingest_headers = {"Authorization": "Bearer " + self.ingest_token}
        disabled_demo = snapshot("demo", now, [drone("AA:BB:CC:00:10:09")])
        disabled_demo["sourceType"] = "server-simulator"
        status, _, body = self.request(
            "/api/ingest", method="POST", payload=disabled_demo, headers=ingest_headers,
        )
        self.assertEqual(status, 403)
        self.assertEqual(json.loads(body)["error"], "server_simulator_disabled")

        untrusted_sim = snapshot("untrusted-sim", now, [drone("AA:BB:CC:00:10:08")])
        untrusted_sim["drones"][0]["simulated"] = True
        status, _, body = self.request(
            "/api/ingest", method="POST", payload=untrusted_sim, headers=ingest_headers,
        )
        self.assertEqual(status, 403)
        self.assertEqual(json.loads(body)["error"], "hardware_source_required")

        for station_id, mac in (("A", "AA:BB:CC:00:10:01"), ("B", "AA:BB:CC:00:10:02")):
            status, _, body = self.request(
                "/api/ingest",
                method="POST",
                payload=snapshot(station_id, now, [drone(mac)]),
                headers=ingest_headers,
            )
            self.assertEqual(status, 202)
            self.assertTrue(json.loads(body)["accepted"])

        status, _, body = self.request("/api/flights?page_size=10", headers={"Cookie": self.cookie})
        self.assertEqual(status, 200)
        flights = json.loads(body)
        self.assertGreaterEqual(flights["total"], 2)
        self.assertTrue({"A", "B"}.issubset({item["stationId"] for item in flights["items"]}))

        old = now - 60 * 60 * 1000
        status, _, body = self.request(
            "/api/ingest",
            method="POST",
            payload=snapshot("backfill", old, [drone("AA:BB:CC:00:10:03")]),
            headers=ingest_headers,
        )
        self.assertEqual(status, 202)
        self.assertTrue(json.loads(body)["accepted"])

        merged = asyncio.run(self._receive_authenticated())
        self.assertIn("serverId", merged)
        self.assertGreater(merged["seq"], 0)
        self.assertEqual(merged["stationCount"], 2)
        self.assertFalse(merged["hardwareConnected"])
        self.assertEqual(
            {item["mac"] for item in merged["drones"]},
            {"AA:BB:CC:00:10:01", "AA:BB:CC:00:10:02"},
        )

        status, _, body = self.request("/api/flights?page_size=20", headers={"Cookie": self.cookie})
        self.assertEqual(status, 200)
        flights = json.loads(body)
        self.assertIn("backfill", {item["stationId"] for item in flights["items"]})

    def test_risk_geofence_incident_management(self):
        status, _, _ = self.request("/api/geofences")
        self.assertEqual(status, 401)
        status, _, _ = self.request(
            "/api/geofences", method="POST",
            payload=circle_geofence(lon=116.4074, lat=39.9042),
        )
        self.assertEqual(status, 401)

        auth_headers = {"Cookie": self.cookie}
        status, _, body = self.request(
            "/api/geofences", method="POST", payload={"name": "invalid"},
            headers=auth_headers,
        )
        self.assertEqual(status, 400)
        status, _, body = self.request(
            "/api/geofences", method="POST",
            payload=circle_geofence(
                name="演示保护区", severity="medium",
                lon=116.4074, lat=39.9042, radius=800,
            ),
            headers=auth_headers,
        )
        self.assertEqual(status, 201)
        geofence = json.loads(body)
        self.assertEqual(geofence["shapeType"], "circle")
        self.assertEqual(geofence["geometry"]["center"], [116.4074, 39.9042])

        replacement = circle_geofence(
            name="演示保护区", severity="high",
            lon=116.4074, lat=39.9042, radius=900,
        )
        status, _, body = self.request(
            f"/api/geofences/{geofence['id']}", method="PUT", payload=replacement,
            headers=auth_headers,
        )
        self.assertEqual(status, 200)
        self.assertEqual(json.loads(body)["severity"], "high")
        status, _, body = self.request("/api/geofences", headers=auth_headers)
        self.assertEqual(status, 200)
        self.assertIn(geofence["id"], {item["id"] for item in json.loads(body)["items"]})

        now = int(time.time() * 1000)
        target = drone("AA:BB:CC:00:30:01", lat=39.9042, lon=116.4074)
        target.update({
            "alt": 130, "spd": 5, "olat": 39.905, "olon": 116.408, "simulated": True,
        })
        simulated_frame = snapshot("risk-api", now, [target])
        simulated_frame["hardwareConnected"] = True
        simulated_frame["sourceType"] = "hardware-usb"
        status, _, body = self.request(
            "/api/ingest", method="POST",
            payload=simulated_frame,
            headers={"Authorization": "Bearer " + self.ingest_token},
        )
        self.assertEqual(status, 202)
        self.assertTrue(json.loads(body)["accepted"])

        merged = asyncio.run(self._receive_authenticated())
        live_target = next(item for item in merged["drones"] if item["mac"] == target["mac"])
        self.assertEqual(live_target["riskScore"], 65)
        self.assertEqual(live_target["riskLevel"], "high")
        self.assertEqual(live_target["geofenceIds"], [geofence["id"]])
        self.assertIsInstance(live_target["incidentId"], int)
        self.assertEqual(live_target["riskReasons"], ["进入high围栏：演示保护区"])

        status, _, body = self.request(
            "/api/incidents?station_id=risk-api&page_size=10", headers=auth_headers,
        )
        self.assertEqual(status, 200)
        incident_list = json.loads(body)
        self.assertEqual(incident_list["total"], 1)
        incident = incident_list["items"][0]
        self.assertEqual(incident["id"], live_target["incidentId"])
        self.assertEqual(incident["status"], "open")

        status, _, body = self.request(
            f"/api/incidents/{incident['id']}/status", method="POST",
            payload={"status": "acknowledged", "note": "operator checked"},
            headers=auth_headers,
        )
        self.assertEqual(status, 200)
        incident = json.loads(body)
        self.assertEqual(incident["status"], "acknowledged")
        self.assertTrue(incident["simulated"])
        self.assertEqual(len(incident["evidenceSha256"]), 64)
        self.assertEqual(incident["actions"][-1]["note"], "operator checked")
        self.assertEqual(incident["actions"][-1]["actor"], "admin")

        status, evidence_headers, body = self.request(
            f"/api/incidents/{incident['id']}/evidence.json", headers=auth_headers,
        )
        self.assertEqual(status, 200)
        body_hash = hashlib.sha256(body).hexdigest()
        self.assertEqual(body_hash, evidence_headers.get("X-Evidence-SHA256"))
        self.assertEqual(body_hash, incident["evidenceSha256"])
        self.assertIn(
            f"rid_incident_{incident['id']}_evidence.json",
            evidence_headers.get("Content-Disposition", ""),
        )
        evidence = json.loads(body)
        self.assertEqual(evidence, incident["evidence"])
        self.assertEqual(evidence["flightId"], incident["flightId"])
        self.assertEqual(evidence["drone"]["mac"], target["mac"])
        status, _, body = self.request("/api/incidents/export.csv", headers=auth_headers)
        self.assertEqual(status, 200)
        self.assertIn(target["mac"].encode(), body)

        status, _, body = self.request("/api/status", headers=auth_headers)
        self.assertEqual(status, 200)
        service_status = json.loads(body)
        self.assertGreaterEqual(service_status["geofences"], 1)
        self.assertGreaterEqual(service_status["incidents"], 1)
        station = next(item for item in service_status["liveStationDetails"]
                       if item["id"] == "risk-api")
        self.assertEqual(station["drones"], 1)
        self.assertEqual(station["status"], "online")
        self.assertGreaterEqual(station["latencyMs"], 0)

        status, _, body = self.request(
            f"/api/geofences/{geofence['id']}", method="DELETE", headers=auth_headers,
        )
        self.assertEqual(status, 200)
        self.assertTrue(json.loads(body)["ok"])

    def test_airspace_import_query_status_and_explicit_unconfigured_sync(self):
        now = int(time.time() * 1000)
        payload = {
            "source": {
                "slug": "http-airspace", "name": "HTTP airspace fixture",
                "provider": "unit-test", "sourceVersion": "http-v1",
                "publishedAt": now, "distCode": "120000", "coverageScope": "city",
            },
            "data": {
                "type": "FeatureCollection",
                "features": [airspace_feature(
                    "http-warning-1", "HTTP warning zone", "warning", [[
                        [117.215, 39.125], [117.235, 39.125],
                        [117.235, 39.140], [117.215, 39.140], [117.215, 39.125],
                    ]],
                )],
            },
        }
        status, _, _ = self.request("/api/airspace/status")
        self.assertEqual(status, 401)
        status, _, _ = self.request("/api/airspace/import", method="POST", payload=payload)
        self.assertEqual(status, 401)

        auth_headers = {"Cookie": self.cookie}
        status, _, body = self.request(
            "/api/airspace/import", method="POST", payload=payload, headers=auth_headers,
        )
        self.assertEqual(status, 201)
        imported = json.loads(body)
        self.assertTrue(imported["ok"])
        self.assertFalse(imported["idempotent"])
        self.assertEqual(imported["source"]["status"], "active")
        self.assertEqual(imported["source"]["syncMode"], "manual_import")
        self.assertEqual(imported["source"]["distCode"], "120000")
        self.assertEqual(imported["source"]["coverageScope"], "city")
        self.assertFalse(imported["source"]["authoritative"])
        self.assertIn("WGS-84", imported["message"])

        status, _, body = self.request(
            "/api/airspace/import", method="POST", payload=payload, headers=auth_headers,
        )
        self.assertEqual(status, 200)
        self.assertTrue(json.loads(body)["idempotent"])

        status, _, body = self.request("/api/airspace/zones", headers=auth_headers)
        self.assertEqual(status, 400)
        self.assertEqual(json.loads(body)["error"], "invalid_query")
        status, _, body = self.request(
            "/api/airspace/zones?bbox=117.20,39.11,117.25,39.15&classes=warning&at="
            + str(now),
            headers=auth_headers,
        )
        self.assertEqual(status, 200)
        zones = json.loads(body)
        self.assertGreaterEqual(zones["total"], 1)
        target = next(item for item in zones["items"] if item["externalId"] == "http-warning-1")
        self.assertEqual(target["zoneClass"], "warning")
        self.assertEqual(target["source"]["slug"], "http-airspace")
        self.assertEqual(target["source"]["distCode"], "120000")
        self.assertEqual(target["dataset"]["sourceVersion"], "http-v1")

        status, _, body = self.request("/api/airspace/status", headers=auth_headers)
        self.assertEqual(status, 200)
        service_status = json.loads(body)
        self.assertFalse(service_status["configured"])
        self.assertFalse(service_status["syncSupported"])
        self.assertGreaterEqual(service_status["activeZones"], 1)
        self.assertGreaterEqual(service_status["activeRegionCount"], 1)
        self.assertIn("120000", service_status["coverageByDistCode"])
        source = next(item for item in service_status["sources"]
                      if item["slug"] == "http-airspace")
        self.assertEqual(source["status"], "active")
        self.assertEqual(source["syncMode"], "manual_import")
        self.assertEqual(source["latestTriggerType"], "manual_import")
        self.assertEqual(source["distCode"], "120000")
        self.assertFalse(source["authoritative"])
        self.assertEqual(source["activeDataset"]["sourceVersion"], "http-v1")

        status, _, body = self.request(
            "/api/airspace/sync", method="POST", payload={}, headers=auth_headers,
        )
        self.assertEqual(status, 503)
        unavailable = json.loads(body)
        self.assertFalse(unavailable["ok"])
        self.assertFalse(unavailable["configured"])
        self.assertEqual(unavailable["status"], "unconfigured")
        self.assertEqual(unavailable["error"], "airspace_sync_not_configured")
        self.assertIn("RID_UOM_AIRSPACE_ENDPOINT", unavailable["missing"])
        self.assertEqual(unavailable["source"]["slug"], "uom")
        self.assertEqual(unavailable["source"]["status"], "unconfigured")
        self.assertEqual(unavailable["source"]["syncMode"], "unconfigured")

    def test_airspace_import_has_dedicated_http_payload_limit(self):
        connection = http.client.HTTPConnection("127.0.0.1", self.http_port, timeout=5)
        try:
            connection.putrequest("POST", "/api/airspace/import")
            connection.putheader("Cookie", self.cookie)
            connection.putheader("Content-Type", "application/json")
            connection.putheader("Content-Length", str(20 * 1024 * 1024 + 1))
            connection.endheaders()
            response = connection.getresponse()
            body = response.read()
        finally:
            connection.close()
        self.assertEqual(response.status, 413)
        self.assertEqual(json.loads(body)["error"], "payload_too_large")

    def test_websocket_rejects_missing_cookie_and_bad_origin(self):
        missing_cookie = asyncio.run(self._closed_code(cookie=None, origin=self._valid_origin()))
        self.assertEqual(missing_cookie, 1008)
        bad_origin = asyncio.run(self._closed_code(cookie=self.cookie, origin="https://evil.example"))
        self.assertEqual(bad_origin, 1008)

    def _valid_origin(self):
        return f"http://127.0.0.1:{self.ws_port}"

    def _connect_kwargs(self, cookie, origin):
        import websockets

        kwargs = {"origin": origin, "open_timeout": 5, "close_timeout": 1}
        if cookie:
            parameter = "additional_headers" if "additional_headers" in inspect.signature(websockets.connect).parameters else "extra_headers"
            kwargs[parameter] = {"Cookie": cookie}
        return kwargs

    async def _receive_authenticated(self):
        import websockets

        async with websockets.connect(
            self.ws_url,
            **self._connect_kwargs(self.cookie, self._valid_origin()),
        ) as websocket:
            return json.loads(await asyncio.wait_for(websocket.recv(), timeout=5))

    async def _closed_code(self, cookie, origin):
        import websockets

        try:
            async with websockets.connect(
                self.ws_url,
                **self._connect_kwargs(cookie, origin),
            ) as websocket:
                await asyncio.wait_for(websocket.recv(), timeout=5)
        except websockets.exceptions.ConnectionClosed as exc:
            return exc.rcvd.code if exc.rcvd is not None else None
        return None


if __name__ == "__main__":
    unittest.main(verbosity=2)
