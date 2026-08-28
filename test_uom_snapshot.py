#!/usr/bin/env python3

import hashlib
import json
import sys
import tempfile
import unittest
from pathlib import Path
from urllib.error import URLError
from unittest import mock

from PIL import Image, ImageDraw

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

import uom_snapshot as snapshot  # noqa: E402


class UomSnapshotTests(unittest.TestCase):
    def test_tile_plan_exactly_covers_snapped_group_extent(self):
        group = snapshot.LAYER_GROUPS[2]
        plan = snapshot._tile_plan(group, 1000.0, 256)
        self.assertGreater(plan["rows"], 1)
        self.assertGreater(plan["columns"], 1)
        self.assertEqual(len(plan["tiles"]), plan["rows"] * plan["columns"])
        self.assertEqual(
            sum(tile["width"] * tile["height"] for tile in plan["tiles"]),
            plan["width"] * plan["height"],
        )
        self.assertEqual(plan["tiles"][0]["bbox3857"][0], plan["bbox3857"][0])
        self.assertEqual(plan["tiles"][0]["bbox3857"][3], plan["bbox3857"][3])

    def test_proxy_rejects_credentials_and_paths(self):
        self.assertEqual(
            snapshot._validate_proxy_url("http://127.0.0.1:19090"),
            "http://127.0.0.1:19090",
        )
        for value in (
            "https://127.0.0.1:19090",
            "http://user:secret@127.0.0.1:19090",
            "http://127.0.0.1:19090/proxy",
            "http://127.0.0.1:19090/?token=secret",
        ):
            with self.assertRaises(snapshot.SnapshotError):
                snapshot._validate_proxy_url(value)

    def test_download_error_never_contains_credential_url(self):
        class FailingOpener:
            def open(self, request, timeout=None):
                raise URLError("https://uom.caac.gov.cn/?token=never-print-this")

        tile = {
            "bbox3857": [0.0, 0.0, 1000.0, 1000.0],
            "width": 1,
            "height": 1,
        }
        with tempfile.TemporaryDirectory() as temp_dir:
            with self.assertRaises(snapshot.SnapshotError) as caught:
                snapshot._download_one(
                    FailingOpener(), "never-print-this", snapshot.LAYER_GROUPS[0],
                    tile, Path(temp_dir) / "tile.png", retries=0,
                )
        self.assertNotIn("never-print-this", str(caught.exception))
        self.assertNotIn("token=", str(caught.exception))

    def test_capture_manifest_is_secret_free(self):
        secret = "unit-test-temporary-credential"

        def fake_download(_opener, token, _group, tile, output_path, _retries):
            self.assertEqual(token, secret)
            image = Image.new("RGBA", (tile["width"], tile["height"]), (0, 0, 0, 0))
            image.save(output_path, format="PNG")
            body = Path(output_path).read_bytes()
            return {"bytes": len(body), "sha256": hashlib.sha256(body).hexdigest()}

        with tempfile.TemporaryDirectory() as temp_dir:
            with mock.patch.object(snapshot, "_download_one", side_effect=fake_download):
                manifest = snapshot.capture_rasters(
                    temp_dir, resolution=5000, tile_pixels=4096, workers=1, retries=0,
                    groups=["east"], token=secret,
                )
            body = (Path(temp_dir) / "capture-manifest.json").read_bytes()
        self.assertFalse(manifest["credentialStored"])
        self.assertNotIn(secret.encode(), body)
        self.assertNotIn(b"token=", body.lower())

    def test_vectorize_and_verify_reference_package(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            tiles = root / "tiles"
            tiles.mkdir()
            tile_path = tiles / "east-00-00.png"
            image = Image.new("RGBA", (16, 16), (255, 255, 255, 0))
            draw = ImageDraw.Draw(image)
            draw.rectangle((2, 2, 13, 13), fill=(0, 245, 245, 102))
            draw.rectangle((6, 6, 9, 9), fill=(255, 255, 255, 0))
            image.save(tile_path, format="PNG")
            body = tile_path.read_bytes()
            capture = {
                "schemaVersion": "1.0",
                "captureType": "uom-wms-suitable-raster",
                "capturedAt": "2026-08-19T12:00:00Z",
                "sourceEndpoint": snapshot.WMS_ENDPOINT,
                "credentialStored": False,
                "coordinateSystem": snapshot.WMS_SRS,
                "resolutionMeters": 1000.0,
                "tilePixels": 16,
                "wms": {
                    "service": "WMS", "request": "GetMap",
                    "version": snapshot.WMS_VERSION, "format": snapshot.WMS_FORMAT,
                    "transparent": True, "srs": snapshot.WMS_SRS,
                    "style": snapshot.WMS_STYLE,
                },
                "groups": [{
                    "id": "east",
                    "distCodes": ["310000"],
                    "layers": "QGSFKYFW:sf310000",
                    "styles": snapshot.WMS_STYLE,
                    "bboxWgs84": [0, 0, 1, 1],
                    "bbox3857": [0, 0, 16000, 16000],
                    "width": 16, "height": 16, "rows": 1, "columns": 1,
                    "tiles": [{
                        "row": 0, "column": 0, "width": 16, "height": 16,
                        "bbox3857": [0, 0, 16000, 16000],
                        "file": tile_path.name,
                        "bytes": len(body),
                        "sha256": hashlib.sha256(body).hexdigest(),
                    }],
                }],
                "tileCount": 1,
            }
            (root / "capture-manifest.json").write_text(
                json.dumps(capture), encoding="utf-8",
            )
            geojson_path = root / "snapshot.geojson"
            manifest_path = root / "snapshot.manifest.json"
            result = snapshot.vectorize_rasters(
                root, geojson_path, manifest_path,
                alpha_threshold=48, block_pixels=64, simplify_pixels=0,
                snapshot_version="unit-test-snapshot",
            )
            verified = snapshot.verify_package(manifest_path, geojson_path)
            geojson = json.loads(geojson_path.read_text(encoding="utf-8"))

        self.assertEqual(result["snapshotVersion"], "unit-test-snapshot")
        self.assertTrue(verified["ok"])
        self.assertGreater(result["featureCount"], 0)
        self.assertGreater(result["totalVertices"], 3)
        self.assertEqual(geojson["type"], "FeatureCollection")
        self.assertTrue(geojson["metadata"]["referenceOnly"])
        self.assertFalse(geojson["metadata"]["authoritativeVector"])
        self.assertTrue(all(
            feature["properties"]["zoneClass"] == "suitable"
            and feature["properties"]["referenceOnly"] is True
            for feature in geojson["features"]
        ))


if __name__ == "__main__":
    unittest.main()
