#!/usr/bin/env python3
"""Capture UOM WMS suitable-airspace rasters and derive a local vector snapshot.

The UOM public map exposes the suitable-airspace layer as authenticated WMS
PNG images, not as WFS/GeoJSON.  This tool keeps that distinction explicit:
it downloads the observed WMS layer groups, polygonizes non-transparent
pixels, and emits a reference-only WGS-84 GeoJSON package.  The credential is
read only from the process environment and is never written to logs, files,
URLs in manifests, or exceptions returned by this module.
"""

from __future__ import annotations

import argparse
import hashlib
import io
import json
import math
import os
import sys
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode, urlsplit
from urllib.request import HTTPRedirectHandler, ProxyHandler, Request, build_opener

WEB_MERCATOR_LIMIT = 20037508.342789244
WMS_ENDPOINT = "https://uom.caac.gov.cn/map/airspace/wms"
WMS_STYLE = "QGSFKYFW:shifeikongyu"
WMS_VERSION = "1.1.0"
WMS_SRS = "EPSG:3857"
WMS_FORMAT = "image/png8"
TOKEN_ENV = "RID_UOM_WMS_TOKEN"
PROXY_ENV = "RID_UOM_WMS_PROXY"
MAX_RESPONSE_BYTES = 16 * 1024 * 1024
DEFAULT_RESOLUTION_METERS = 500.0
DEFAULT_TILE_PIXELS = 2048
DEFAULT_VECTOR_BLOCK_PIXELS = 512
DEFAULT_ALPHA_THRESHOLD = 48
DEFAULT_SIMPLIFY_PIXELS = 0.75
DEFAULT_MAX_FEATURES = 45_000
DEFAULT_MAX_VERTICES = 900_000

# Bounding boxes are deliberately padded around each observed provincial layer
# group.  The extra area renders transparent and prevents province-edge loss.
LAYER_GROUPS = (
    {
        "id": "north-1",
        "distCodes": ("120000", "130000", "140000", "150000"),
        "bboxWgs84": (104.0, 33.0, 127.0, 54.2),
    },
    {
        "id": "north-east",
        "distCodes": ("210000", "220000", "230000"),
        "bboxWgs84": (117.5, 38.0, 136.0, 54.2),
    },
    {
        "id": "east",
        "distCodes": ("310000", "320000", "330000", "340000", "350000", "360000", "370000"),
        "bboxWgs84": (112.8, 22.0, 124.0, 38.0),
    },
    {
        "id": "central-south",
        "distCodes": ("410000", "420000", "430000", "440000", "450000", "460000"),
        "bboxWgs84": (102.5, 3.0, 122.5, 37.0),
    },
    {
        "id": "south-west",
        "distCodes": ("500000", "510000", "520000", "530000", "540000"),
        "bboxWgs84": (77.0, 20.0, 111.5, 37.5),
    },
    {
        "id": "north-west",
        "distCodes": ("610000", "620000", "630000", "640000", "650000"),
        "bboxWgs84": (72.0, 30.0, 112.5, 50.5),
    },
)


class SnapshotError(RuntimeError):
    """A sanitized failure that never includes a credential-bearing URL."""

    def __init__(self, message, *, status=None):
        super().__init__(message)
        self.status = status if isinstance(status, int) else None


class _NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def _utc_now():
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _sha256_bytes(value):
    return hashlib.sha256(value).hexdigest()


def _sha256_path(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _atomic_write(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile("wb", dir=path.parent, delete=False) as handle:
        handle.write(value)
        temp_path = Path(handle.name)
    os.replace(temp_path, path)


def _json_bytes(value):
    return json.dumps(
        value, ensure_ascii=False, separators=(",", ":"), sort_keys=True,
    ).encode("utf-8") + b"\n"


def _lon_to_mercator(lon):
    return float(lon) * WEB_MERCATOR_LIMIT / 180.0


def _lat_to_mercator(lat):
    latitude = max(-85.05112878, min(85.05112878, float(lat)))
    value = math.log(math.tan((90.0 + latitude) * math.pi / 360.0)) / (math.pi / 180.0)
    return value * WEB_MERCATOR_LIMIT / 180.0


def _snap_bbox(bbox_wgs84, resolution):
    min_lon, min_lat, max_lon, max_lat = bbox_wgs84
    min_x = math.floor(_lon_to_mercator(min_lon) / resolution) * resolution
    min_y = math.floor(_lat_to_mercator(min_lat) / resolution) * resolution
    max_x = math.ceil(_lon_to_mercator(max_lon) / resolution) * resolution
    max_y = math.ceil(_lat_to_mercator(max_lat) / resolution) * resolution
    return (min_x, min_y, max_x, max_y)


def _layer_names(dist_codes):
    return ",".join(f"QGSFKYFW:sf{code}" for code in dist_codes)


def _style_names(dist_codes):
    return ",".join(WMS_STYLE for _ in dist_codes)


def _validate_proxy_url(raw):
    raw = str(raw or "").strip()
    if not raw:
        return None
    parsed = urlsplit(raw)
    if parsed.scheme.lower() != "http" or not parsed.hostname:
        raise SnapshotError("RID_UOM_WMS_PROXY must be a plain http proxy URL")
    if parsed.username is not None or parsed.password is not None:
        raise SnapshotError("RID_UOM_WMS_PROXY must not include credentials")
    if parsed.path not in ("", "/") or parsed.query or parsed.fragment:
        raise SnapshotError("RID_UOM_WMS_PROXY must not include a path or query")
    try:
        port = parsed.port
    except ValueError as exc:
        raise SnapshotError("RID_UOM_WMS_PROXY has an invalid port") from exc
    if port is None or not 1 <= port <= 65535:
        raise SnapshotError("RID_UOM_WMS_PROXY must include a valid port")
    return raw


def _build_opener(proxy_url):
    if proxy_url:
        return build_opener(ProxyHandler({"https": proxy_url}), _NoRedirect())
    return build_opener(_NoRedirect())


def _wms_query(token, group, bbox, width, height):
    return urlencode({
        "token": token,
        "service": "WMS",
        "request": "GetMap",
        "layers": _layer_names(group["distCodes"]),
        "styles": _style_names(group["distCodes"]),
        "format": WMS_FORMAT,
        "transparent": "true",
        "version": WMS_VERSION,
        "srs": WMS_SRS,
        "width": str(width),
        "height": str(height),
        "bbox": ",".join(format(value, ".15g") for value in bbox),
    }, safe=":,")


def _validate_png(body, width, height):
    if len(body) < 8 or body[:8] != b"\x89PNG\r\n\x1a\n":
        raise SnapshotError("UOM WMS returned a non-PNG response")
    try:
        from PIL import Image

        with Image.open(io.BytesIO(body)) as image:
            if image.size != (width, height):
                raise SnapshotError("UOM WMS returned an unexpected image size")
            image.verify()
    except SnapshotError:
        raise
    except Exception as exc:
        raise SnapshotError("UOM WMS returned an invalid PNG") from exc


def _request_png(opener, token, group, tile, retries):
    query = _wms_query(token, group, tile["bbox3857"], tile["width"], tile["height"])
    request = Request(
        WMS_ENDPOINT + "?" + query,
        headers={"Accept": "image/png", "User-Agent": "RID-Airspace-Snapshot/1.0"},
        method="GET",
    )
    for attempt in range(retries + 1):
        try:
            with opener.open(request, timeout=30) as response:
                content_type = (response.headers.get("Content-Type") or "").split(";", 1)[0].lower()
                if content_type not in {"image/png", "image/png8"}:
                    raise SnapshotError("UOM WMS returned an unexpected content type")
                body = response.read(MAX_RESPONSE_BYTES + 1)
                if len(body) > MAX_RESPONSE_BYTES:
                    raise SnapshotError("UOM WMS image exceeds the size limit")
            _validate_png(body, tile["width"], tile["height"])
            return body
        except HTTPError as exc:
            status = exc.code if isinstance(exc.code, int) else 0
            # The UOM map edge has occasionally returned a transient 404 for a
            # valid GetMap tile. Authentication and request-shape failures are
            # permanent; other statuses get the same bounded retry treatment
            # as transport errors.
            if attempt >= retries or status in {400, 401, 403}:
                raise SnapshotError(
                    f"UOM WMS request failed with HTTP {status or 'error'}",
                    status=status,
                ) from None
        except SnapshotError:
            if attempt >= retries:
                raise
        except (URLError, OSError, TimeoutError):
            if attempt >= retries:
                raise SnapshotError("UOM WMS request failed") from None
        time.sleep(min(4.0, 0.75 * (2 ** attempt)))
    raise SnapshotError("UOM WMS request failed")


def _split_tile(tile):
    width = int(tile["width"])
    height = int(tile["height"])
    left_width = width // 2
    right_width = width - left_width
    top_height = height // 2
    bottom_height = height - top_height
    min_x, min_y, max_x, max_y = map(float, tile["bbox3857"])
    pixel_x = (max_x - min_x) / width
    pixel_y = (max_y - min_y) / height
    split_x = min_x + left_width * pixel_x
    split_y = max_y - top_height * pixel_y
    return (
        ({"width": left_width, "height": top_height,
          "bbox3857": [min_x, split_y, split_x, max_y]}, (0, 0)),
        ({"width": right_width, "height": top_height,
          "bbox3857": [split_x, split_y, max_x, max_y]}, (left_width, 0)),
        ({"width": left_width, "height": bottom_height,
          "bbox3857": [min_x, min_y, split_x, split_y]}, (0, top_height)),
        ({"width": right_width, "height": bottom_height,
          "bbox3857": [split_x, min_y, max_x, split_y]}, (left_width, top_height)),
    )


def _request_png_adaptive(opener, token, group, tile, retries, minimum_pixels=256):
    try:
        return _request_png(opener, token, group, tile, retries)
    except SnapshotError as exc:
        if exc.status in {400, 401, 403}:
            raise
        if int(tile["width"]) <= minimum_pixels and int(tile["height"]) <= minimum_pixels:
            raise

    from PIL import Image

    canvas = Image.new("RGBA", (int(tile["width"]), int(tile["height"])), (255, 255, 255, 0))
    for child, offset in _split_tile(tile):
        body = _request_png_adaptive(
            opener, token, group, child, retries, minimum_pixels=minimum_pixels,
        )
        with Image.open(io.BytesIO(body)) as image:
            canvas.paste(image.convert("RGBA"), offset)
    encoded = io.BytesIO()
    canvas.save(encoded, format="PNG", optimize=True)
    body = encoded.getvalue()
    _validate_png(body, int(tile["width"]), int(tile["height"]))
    return body


def _download_one(opener, token, group, tile, output_path, retries):
    body = _request_png_adaptive(opener, token, group, tile, retries)
    _atomic_write(output_path, body)
    return {
        "bytes": len(body),
        "sha256": _sha256_bytes(body),
    }


def _tile_plan(group, resolution, tile_pixels):
    min_x, min_y, max_x, max_y = _snap_bbox(group["bboxWgs84"], resolution)
    width = int(round((max_x - min_x) / resolution))
    height = int(round((max_y - min_y) / resolution))
    columns = math.ceil(width / tile_pixels)
    rows = math.ceil(height / tile_pixels)
    tiles = []
    for row in range(rows):
        pixel_y = row * tile_pixels
        tile_height = min(tile_pixels, height - pixel_y)
        tile_max_y = max_y - pixel_y * resolution
        tile_min_y = tile_max_y - tile_height * resolution
        for column in range(columns):
            pixel_x = column * tile_pixels
            tile_width = min(tile_pixels, width - pixel_x)
            tile_min_x = min_x + pixel_x * resolution
            tile_max_x = tile_min_x + tile_width * resolution
            tiles.append({
                "row": row,
                "column": column,
                "width": tile_width,
                "height": tile_height,
                "bbox3857": [tile_min_x, tile_min_y, tile_max_x, tile_max_y],
                "file": f"{group['id']}-{row:02d}-{column:02d}.png",
            })
    return {
        "bbox3857": [min_x, min_y, max_x, max_y],
        "width": width,
        "height": height,
        "rows": rows,
        "columns": columns,
        "tiles": tiles,
    }


def capture_rasters(output_dir, *, resolution=DEFAULT_RESOLUTION_METERS,
                    tile_pixels=DEFAULT_TILE_PIXELS, workers=4, retries=2,
                    groups=None, token=None, proxy_url=None, captured_at=None):
    token = str(token if token is not None else os.environ.get(TOKEN_ENV, "")).strip()
    if not token:
        raise SnapshotError(f"{TOKEN_ENV} is not configured")
    proxy_url = _validate_proxy_url(
        proxy_url if proxy_url is not None else os.environ.get(PROXY_ENV, "")
    )
    if not 100 <= resolution <= 5000:
        raise SnapshotError("resolution must be between 100 and 5000 meters")
    if not 128 <= tile_pixels <= 4096:
        raise SnapshotError("tile-pixels must be between 128 and 4096")
    if not 1 <= workers <= 8:
        raise SnapshotError("workers must be between 1 and 8")
    if not 0 <= retries <= 5:
        raise SnapshotError("retries must be between 0 and 5")

    selected_ids = set(groups or [group["id"] for group in LAYER_GROUPS])
    selected = [group for group in LAYER_GROUPS if group["id"] in selected_ids]
    if len(selected) != len(selected_ids):
        raise SnapshotError("one or more layer group identifiers are invalid")
    output_dir = Path(output_dir).resolve()
    tile_dir = output_dir / "tiles"
    tile_dir.mkdir(parents=True, exist_ok=True)
    captured_at = captured_at or _utc_now()
    opener = _build_opener(proxy_url)

    group_records = []
    jobs = []
    for group in selected:
        plan = _tile_plan(group, float(resolution), int(tile_pixels))
        record = {
            "id": group["id"],
            "distCodes": list(group["distCodes"]),
            "layers": _layer_names(group["distCodes"]),
            "styles": _style_names(group["distCodes"]),
            "bboxWgs84": list(group["bboxWgs84"]),
            "bbox3857": plan["bbox3857"],
            "width": plan["width"],
            "height": plan["height"],
            "rows": plan["rows"],
            "columns": plan["columns"],
            "tiles": plan["tiles"],
        }
        group_records.append(record)
        for tile in record["tiles"]:
            jobs.append((group, tile, tile_dir / tile["file"]))

    completed = 0
    with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="uom-wms") as pool:
        futures = {}
        for group, tile, path in jobs:
            if path.exists():
                try:
                    body = path.read_bytes()
                    _validate_png(body, tile["width"], tile["height"])
                    tile.update({"bytes": len(body), "sha256": _sha256_bytes(body)})
                    completed += 1
                    continue
                except (OSError, SnapshotError):
                    pass
            future = pool.submit(
                _download_one, opener, token, group, tile, path, retries,
            )
            futures[future] = tile
        for future in as_completed(futures):
            tile = futures[future]
            tile.update(future.result())
            completed += 1
            print(f"[uom-snapshot] raster {completed}/{len(jobs)}", flush=True)

    manifest = {
        "schemaVersion": "1.0",
        "captureType": "uom-wms-suitable-raster",
        "capturedAt": captured_at,
        "sourceEndpoint": WMS_ENDPOINT,
        "credentialStored": False,
        "coordinateSystem": WMS_SRS,
        "resolutionMeters": float(resolution),
        "tilePixels": int(tile_pixels),
        "wms": {
            "service": "WMS",
            "request": "GetMap",
            "version": WMS_VERSION,
            "format": WMS_FORMAT,
            "transparent": True,
            "srs": WMS_SRS,
            "style": WMS_STYLE,
        },
        "groups": group_records,
        "tileCount": len(jobs),
    }
    encoded = _json_bytes(manifest)
    if token.encode("utf-8") in encoded:
        raise SnapshotError("credential safety check failed")
    _atomic_write(output_dir / "capture-manifest.json", encoded)
    return manifest


def _iter_polygons(geometry):
    geometry_type = geometry.geom_type
    if geometry_type == "Polygon":
        yield geometry
    elif geometry_type in {"MultiPolygon", "GeometryCollection"}:
        for part in geometry.geoms:
            yield from _iter_polygons(part)


def _geometry_vertices(geometry):
    geometry_type = geometry.get("type")
    coordinates = geometry.get("coordinates") or []
    if geometry_type == "Polygon":
        return sum(max(0, len(ring) - 1) for ring in coordinates)
    if geometry_type == "MultiPolygon":
        return sum(max(0, len(ring) - 1) for polygon in coordinates for ring in polygon)
    return 0


def _load_vector_dependencies():
    try:
        import numpy as np
        from affine import Affine
        from PIL import Image
        from rasterio.features import shapes
        from rasterio.warp import transform_geom
        from shapely import make_valid
        from shapely.geometry import mapping, shape
    except ImportError as exc:
        raise SnapshotError(
            "vectorization dependencies are missing; install snapshot-requirements.txt"
        ) from exc
    return np, Affine, Image, shapes, transform_geom, make_valid, mapping, shape


def _vectorize_with_tolerance(capture_dir, capture_manifest, *, alpha_threshold,
                              block_pixels, simplify_pixels, min_area_pixels):
    (np, Affine, Image, shapes, transform_geom,
     make_valid, mapping, shape) = _load_vector_dependencies()
    resolution = float(capture_manifest["resolutionMeters"])
    simplify_meters = max(0.0, float(simplify_pixels) * resolution)
    minimum_area = max(0.0, float(min_area_pixels) * resolution * resolution)
    tile_dir = Path(capture_dir) / "tiles"
    features = []
    feature_sequence = 0
    total_vertices = 0

    for group in capture_manifest.get("groups") or []:
        group_id = str(group.get("id") or "unknown")
        for tile in group.get("tiles") or []:
            tile_path = tile_dir / str(tile["file"])
            if _sha256_path(tile_path) != tile.get("sha256"):
                raise SnapshotError(f"raster integrity check failed for {tile_path.name}")
            with Image.open(tile_path) as image:
                rgba = np.asarray(image.convert("RGBA"))
            if rgba.shape[:2] != (int(tile["height"]), int(tile["width"])):
                raise SnapshotError(f"raster dimensions changed for {tile_path.name}")
            mask = rgba[:, :, 3] >= int(alpha_threshold)
            if not bool(mask.any()):
                continue
            tile_min_x, _tile_min_y, _tile_max_x, tile_max_y = tile["bbox3857"]
            for block_y in range(0, mask.shape[0], block_pixels):
                block_height = min(block_pixels, mask.shape[0] - block_y)
                for block_x in range(0, mask.shape[1], block_pixels):
                    block_width = min(block_pixels, mask.shape[1] - block_x)
                    block = mask[
                        block_y:block_y + block_height,
                        block_x:block_x + block_width,
                    ]
                    if not bool(block.any()):
                        continue
                    transform = Affine(
                        resolution, 0.0, tile_min_x + block_x * resolution,
                        0.0, -resolution, tile_max_y - block_y * resolution,
                    )
                    for raw_geometry, value in shapes(
                            block.astype("uint8"), mask=block,
                            transform=transform, connectivity=4):
                        if int(value) != 1:
                            continue
                        candidate = shape(raw_geometry)
                        if candidate.area < minimum_area:
                            continue
                        if simplify_meters:
                            candidate = candidate.simplify(
                                simplify_meters, preserve_topology=True,
                            )
                        candidate = make_valid(candidate)
                        for polygon in _iter_polygons(candidate):
                            if polygon.is_empty or polygon.area < minimum_area:
                                continue
                            wgs84 = transform_geom(
                                "EPSG:3857", "EPSG:4326", mapping(polygon),
                                precision=6,
                            )
                            vertices = _geometry_vertices(wgs84)
                            if vertices < 3:
                                continue
                            feature_sequence += 1
                            total_vertices += vertices
                            features.append({
                                "type": "Feature",
                                "id": f"uom-derived-{group_id}-{feature_sequence:06d}",
                                "properties": {
                                    "externalId": f"uom-derived-{group_id}-{feature_sequence:06d}",
                                    "name": "UOM 适飞参考（栅格派生）",
                                    "zoneClass": "suitable",
                                    "sourceClass": "uom_wms_derived_suitable",
                                    "wmsLayerGroup": group_id,
                                    "resolutionMeters": resolution,
                                    "derivation": "alpha-mask-polygonize",
                                    "referenceOnly": True,
                                    "authoritative": False,
                                },
                                "geometry": wgs84,
                            })
    return features, total_vertices


def _feature_collection_bbox(features):
    values = [180.0, 90.0, -180.0, -90.0]

    def visit(value):
        if (isinstance(value, list) and len(value) >= 2
                and all(isinstance(item, (int, float)) for item in value[:2])):
            values[0] = min(values[0], value[0])
            values[1] = min(values[1], value[1])
            values[2] = max(values[2], value[0])
            values[3] = max(values[3], value[1])
        elif isinstance(value, list):
            for item in value:
                visit(item)

    for feature in features:
        visit((feature.get("geometry") or {}).get("coordinates"))
    return values


def vectorize_rasters(capture_dir, geojson_output, manifest_output, *,
                      alpha_threshold=DEFAULT_ALPHA_THRESHOLD,
                      block_pixels=DEFAULT_VECTOR_BLOCK_PIXELS,
                      simplify_pixels=DEFAULT_SIMPLIFY_PIXELS,
                      min_area_pixels=1.0,
                      max_features=DEFAULT_MAX_FEATURES,
                      max_vertices=DEFAULT_MAX_VERTICES,
                      snapshot_version=None):
    capture_dir = Path(capture_dir).resolve()
    capture_manifest_path = capture_dir / "capture-manifest.json"
    try:
        capture_manifest = json.loads(capture_manifest_path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise SnapshotError("capture-manifest.json is missing or invalid") from exc
    if capture_manifest.get("credentialStored") is not False:
        raise SnapshotError("capture manifest did not pass the credential-safety check")
    if not 1 <= alpha_threshold <= 255:
        raise SnapshotError("alpha-threshold must be between 1 and 255")
    if not 64 <= block_pixels <= 1024:
        raise SnapshotError("block-pixels must be between 64 and 1024")
    if not 0 <= simplify_pixels <= 8:
        raise SnapshotError("simplify-pixels must be between 0 and 8")

    tolerance = float(simplify_pixels)
    for _attempt in range(5):
        features, total_vertices = _vectorize_with_tolerance(
            capture_dir, capture_manifest,
            alpha_threshold=alpha_threshold,
            block_pixels=block_pixels,
            simplify_pixels=tolerance,
            min_area_pixels=min_area_pixels,
        )
        if len(features) <= max_features and total_vertices <= max_vertices:
            break
        tolerance = max(1.0, tolerance * 1.75)
    else:
        raise SnapshotError("derived snapshot exceeds the configured feature or vertex limit")
    if not features:
        raise SnapshotError("the captured rasters contain no suitable-airspace pixels")

    captured_at = str(capture_manifest.get("capturedAt") or _utc_now())
    snapshot_version = snapshot_version or (
        "uom-wms-derived-" + captured_at.replace("-", "").replace(":", "").replace("Z", "Z")
    )
    feature_collection = {
        "type": "FeatureCollection",
        "name": "UOM suitable-airspace derived reference snapshot",
        "bbox": _feature_collection_bbox(features),
        "metadata": {
            "snapshotVersion": snapshot_version,
            "capturedAt": captured_at,
            "coordinateSystem": "WGS84",
            "sourceType": "authenticated-uom-wms-raster",
            "derivation": "alpha-mask-polygonize",
            "referenceOnly": True,
            "authoritativeVector": False,
            "resolutionMeters": capture_manifest["resolutionMeters"],
            "alphaThreshold": int(alpha_threshold),
            "simplifyPixels": tolerance,
        },
        "features": features,
    }
    geojson_bytes = _json_bytes(feature_collection)
    _atomic_write(geojson_output, geojson_bytes)
    source_manifest_sha = _sha256_path(capture_manifest_path)
    packaged_manifest = {
        "schemaVersion": "1.0",
        "snapshotVersion": snapshot_version,
        "generatedAt": _utc_now(),
        "capturedAt": captured_at,
        "sourceType": "authenticated-uom-wms-raster",
        "sourceEndpoint": WMS_ENDPOINT,
        "coordinateSystem": "WGS84",
        "coverageScope": "national",
        "referenceOnly": True,
        "authoritativeVector": False,
        "derivation": "alpha-mask-polygonize",
        "resolutionMeters": capture_manifest["resolutionMeters"],
        "alphaThreshold": int(alpha_threshold),
        "simplifyPixels": tolerance,
        "featureCount": len(features),
        "totalVertices": total_vertices,
        "bbox": feature_collection["bbox"],
        "geojsonFile": Path(geojson_output).name,
        "geojsonSha256": _sha256_bytes(geojson_bytes),
        "captureManifestSha256": source_manifest_sha,
        "wms": capture_manifest.get("wms"),
        "layerGroups": [{
            "id": group.get("id"),
            "distCodes": group.get("distCodes"),
            "layers": group.get("layers"),
            "styles": group.get("styles"),
        } for group in capture_manifest.get("groups") or []],
        "notice": "由经授权 UOM WMS 适飞栅格派生，不是 UOM 官方矢量或飞行审批结论",
        "credentialStored": False,
    }
    manifest_bytes = _json_bytes(packaged_manifest)
    _atomic_write(manifest_output, manifest_bytes)
    return packaged_manifest


def verify_package(manifest_path, geojson_path=None):
    manifest_path = Path(manifest_path).resolve()
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise SnapshotError("packaged snapshot manifest is missing or invalid") from exc
    if manifest.get("credentialStored") is not False:
        raise SnapshotError("snapshot manifest credential-safety flag is invalid")
    geojson_path = Path(geojson_path or manifest_path.with_name(manifest.get("geojsonFile", "")))
    body = geojson_path.read_bytes()
    if _sha256_bytes(body) != manifest.get("geojsonSha256"):
        raise SnapshotError("snapshot GeoJSON SHA-256 does not match the manifest")
    try:
        data = json.loads(body)
    except ValueError as exc:
        raise SnapshotError("snapshot GeoJSON is invalid") from exc
    features = data.get("features") if isinstance(data, dict) else None
    if data.get("type") != "FeatureCollection" or not isinstance(features, list):
        raise SnapshotError("snapshot GeoJSON must be a FeatureCollection")
    if len(features) != manifest.get("featureCount"):
        raise SnapshotError("snapshot feature count does not match the manifest")
    if any((feature.get("properties") or {}).get("zoneClass") != "suitable"
           for feature in features):
        raise SnapshotError("snapshot contains a non-suitable feature")
    return {
        "ok": True,
        "snapshotVersion": manifest.get("snapshotVersion"),
        "featureCount": len(features),
        "totalVertices": manifest.get("totalVertices"),
        "geojsonSha256": manifest.get("geojsonSha256"),
    }


def _build_parser():
    parser = argparse.ArgumentParser(description="Capture and vectorize UOM WMS reference data")
    subparsers = parser.add_subparsers(dest="command", required=True)

    capture = subparsers.add_parser("capture", help="download WMS rasters and derive GeoJSON")
    capture.add_argument("--work-dir", required=True)
    capture.add_argument("--geojson-output", required=True)
    capture.add_argument("--manifest-output", required=True)
    capture.add_argument("--resolution", type=float, default=DEFAULT_RESOLUTION_METERS)
    capture.add_argument("--tile-pixels", type=int, default=DEFAULT_TILE_PIXELS)
    capture.add_argument("--block-pixels", type=int, default=DEFAULT_VECTOR_BLOCK_PIXELS)
    capture.add_argument("--alpha-threshold", type=int, default=DEFAULT_ALPHA_THRESHOLD)
    capture.add_argument("--simplify-pixels", type=float, default=DEFAULT_SIMPLIFY_PIXELS)
    capture.add_argument("--min-area-pixels", type=float, default=1.0)
    capture.add_argument("--workers", type=int, default=4)
    capture.add_argument("--retries", type=int, default=2)
    capture.add_argument("--group", action="append", dest="groups")
    capture.add_argument("--snapshot-version")

    vectorize = subparsers.add_parser("vectorize", help="derive GeoJSON from captured rasters")
    vectorize.add_argument("--work-dir", required=True)
    vectorize.add_argument("--geojson-output", required=True)
    vectorize.add_argument("--manifest-output", required=True)
    vectorize.add_argument("--block-pixels", type=int, default=DEFAULT_VECTOR_BLOCK_PIXELS)
    vectorize.add_argument("--alpha-threshold", type=int, default=DEFAULT_ALPHA_THRESHOLD)
    vectorize.add_argument("--simplify-pixels", type=float, default=DEFAULT_SIMPLIFY_PIXELS)
    vectorize.add_argument("--min-area-pixels", type=float, default=1.0)
    vectorize.add_argument("--snapshot-version")

    verify = subparsers.add_parser("verify", help="verify a packaged snapshot")
    verify.add_argument("--manifest", required=True)
    verify.add_argument("--geojson")
    return parser


def main(argv=None):
    args = _build_parser().parse_args(argv)
    try:
        if args.command == "capture":
            capture_rasters(
                args.work_dir,
                resolution=args.resolution,
                tile_pixels=args.tile_pixels,
                workers=args.workers,
                retries=args.retries,
                groups=args.groups,
            )
            result = vectorize_rasters(
                args.work_dir, args.geojson_output, args.manifest_output,
                alpha_threshold=args.alpha_threshold,
                block_pixels=args.block_pixels,
                simplify_pixels=args.simplify_pixels,
                min_area_pixels=args.min_area_pixels,
                snapshot_version=args.snapshot_version,
            )
        elif args.command == "vectorize":
            result = vectorize_rasters(
                args.work_dir, args.geojson_output, args.manifest_output,
                alpha_threshold=args.alpha_threshold,
                block_pixels=args.block_pixels,
                simplify_pixels=args.simplify_pixels,
                min_area_pixels=args.min_area_pixels,
                snapshot_version=args.snapshot_version,
            )
        else:
            result = verify_package(args.manifest, args.geojson)
        print(json.dumps(result, ensure_ascii=False, sort_keys=True))
        return 0
    except SnapshotError as exc:
        print(f"uom snapshot failed: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
