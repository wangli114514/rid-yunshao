#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
RID 监测智慧大屏 - PC 端数据服务
=================================
从 T-Display-S3 串口读取 JSON 全量快照，持久化到 SQLite，并通过
WebSocket 实时推送给 dashboard.html。HTTP 服务同时提供历史查询/导出 API。

用法:
  python server.py --port COM5
  python server.py --demo
  python server.py --demo --db data/rid_history.db
"""
import argparse
import asyncio
import base64
import csv
import hashlib
import hmac
import io
import json
import math
import os
import random
import re
import secrets
import sqlite3
import threading
import time
import zlib
from collections import OrderedDict
from contextlib import contextmanager
from datetime import datetime
from http.cookies import SimpleCookie
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qs, urlencode, urlparse, urlsplit
from urllib.request import HTTPRedirectHandler, ProxyHandler, Request, build_opener

HERE = os.path.dirname(os.path.abspath(__file__))
DEFAULT_DB = os.path.join(HERE, "data", "rid_history.db")
CLIENTS = set()
LOCK = threading.Lock()
MAIN_LOOP = None
LATEST_SNAPSHOT = None
LATEST_SNAPSHOT_AT = None
HISTORY = None
STATION_LIVE = {}
STATION_TIMEOUT_MS = 15_000
LIVE_MAX_AGE_MS = 30_000
# Ingest is intentionally source-aware: simulated targets produced by the
# hardware are valid test telemetry, while the old server-side demo producer is
# never allowed to become a live station again.
DEFAULT_INGEST_SOURCE_TYPE = "hardware-usb"
SERVER_SIMULATOR_SOURCE_TYPE = "server-simulator"
SERVER_SIMULATOR_UA = "RID-Demo-Simulator/"
INGEST_TOKEN = None
CLOUD_MODE = False
WS_PORT = 8765
MAX_INGEST_BYTES = 1024 * 1024
ADMIN_USER = None
ADMIN_PASSWORD = None
SESSION_SECRET = None
SESSION_TTL_SECONDS = 12 * 60 * 60
COOKIE_SECURE = False
LOGIN_FAILURES = {}
AUTH_LOCK = threading.Lock()
SERVER_INSTANCE_ID = secrets.token_hex(8)
SNAPSHOT_SEQUENCE = 0
MAC_PATTERN = re.compile(r"^(?:[0-9A-F]{2}:){5}[0-9A-F]{2}$")
CONTROL_CHAR_PATTERN = re.compile(r"[\x00-\x1f\x7f]")
GEOFENCE_SEVERITY_SCORES = {"low": 25, "medium": 45, "high": 65, "critical": 80}
INCIDENT_STATUSES = {"open", "acknowledged", "resolved", "dismissed"}
MAX_GEOFENCE_VERTICES = 512
AIRSPACE_CLASSES = {"suitable", "warning", "controlled", "prohibited", "unknown"}
AIRSPACE_ALTITUDE_REFERENCES = {"unknown", "wgs84", "amsl", "agl"}
AIRSPACE_COVERAGE_SCOPES = {"city", "national", "unknown"}
DEFAULT_MONITOR_LAT = 39.9042
DEFAULT_MONITOR_LON = 116.4074


def _env_int(name, default, minimum, maximum):
    raw = os.environ.get(name)
    if raw in (None, ""):
        return default
    try:
        return max(minimum, min(maximum, int(raw)))
    except ValueError:
        return default


MAX_AIRSPACE_IMPORT_BYTES = _env_int(
    "RID_AIRSPACE_IMPORT_MAX_BYTES", 20 * 1024 * 1024, 64 * 1024, 100 * 1024 * 1024,
)
MAX_AIRSPACE_FEATURES = _env_int("RID_AIRSPACE_MAX_FEATURES", 50_000, 1, 250_000)
MAX_AIRSPACE_VERTICES_PER_FEATURE = _env_int(
    "RID_AIRSPACE_MAX_VERTICES_PER_FEATURE", 20_000, 3, 200_000,
)
MAX_AIRSPACE_TOTAL_VERTICES = _env_int(
    "RID_AIRSPACE_MAX_TOTAL_VERTICES", 1_000_000, 3, 5_000_000,
)
MAX_AIRSPACE_QUERY_ZONES = 2_000
AIRSPACE_SOURCE_SLUG_PATTERN = re.compile(r"^[a-z0-9][a-z0-9._-]{0,63}$")
AIRSPACE_CATALOG_PATH = os.path.join(HERE, "airspace_catalog.json")
BUILTIN_BEIJING_AIRSPACE_PATH = os.path.join(
    HERE, "airspace-data", "beijing-uom-prohibited-reference.geojson",
)
PACKAGED_UOM_DERIVED_AIRSPACE_PATH = os.path.join(
    HERE, "airspace-data", "uom-derived-suitable.geojson",
)
PACKAGED_UOM_DERIVED_MANIFEST_PATH = os.path.join(
    HERE, "airspace-data", "uom-derived-suitable.manifest.json",
)

# The UOM map uses six observed GetMap layer groups covering 30 provincial
# administrative areas. Keep this separate from the local vector snapshot
# catalog: this proxy only returns the authenticated UOM raster reference.
UOM_WMS_ENDPOINT = "https://uom.caac.gov.cn/map/airspace/wms"
UOM_WMS_VERSION = "1.1.0"
UOM_WMS_SRS = "EPSG:3857"
UOM_WMS_FORMAT = "image/png8"
UOM_WMS_STYLE = "QGSFKYFW:shifeikongyu"
UOM_WMS_GROUP_DEFINITIONS = (
    ("north-1", ("120000", "130000", "140000", "150000")),
    ("north-east", ("210000", "220000", "230000")),
    ("east", ("310000", "320000", "330000", "340000", "350000", "360000", "370000")),
    ("central-south", ("410000", "420000", "430000", "440000", "450000", "460000")),
    ("south-west", ("500000", "510000", "520000", "530000", "540000")),
    ("north-west", ("610000", "620000", "630000", "640000", "650000")),
)
UOM_WMS_LAYER_GROUPS = tuple(
    ",".join(f"QGSFKYFW:sf{code}" for code in dist_codes)
    for _, dist_codes in UOM_WMS_GROUP_DEFINITIONS
)
UOM_WMS_STYLE_GROUPS = tuple(
    ",".join(UOM_WMS_STYLE for _ in dist_codes)
    for _, dist_codes in UOM_WMS_GROUP_DEFINITIONS
)
UOM_WMS_ALLOWED_LAYER_GROUPS = frozenset(UOM_WMS_LAYER_GROUPS)
UOM_WMS_STYLES_BY_LAYER_GROUP = dict(zip(UOM_WMS_LAYER_GROUPS, UOM_WMS_STYLE_GROUPS))
UOM_WMS_ALLOWED_PARAMETERS = frozenset({
    "SERVICE", "REQUEST", "VERSION", "LAYERS", "STYLES", "FORMAT", "TRANSPARENT",
    "SRS", "BBOX", "WIDTH", "HEIGHT",
})
UOM_WMS_MAX_DIMENSION = 2048
UOM_WMS_MAX_PIXELS = UOM_WMS_MAX_DIMENSION * UOM_WMS_MAX_DIMENSION
UOM_WMS_MAX_RESPONSE_BYTES = 16 * 1024 * 1024
UOM_WMS_TIMEOUT_SECONDS = 10
UOM_WMS_READY_TTL_MS = 120_000
UOM_WMS_MAX_CONCURRENT_FETCHES = 6
UOM_WMS_FETCH_QUEUE_TIMEOUT_SECONDS = 20
UOM_WMS_CACHE_TTL_SECONDS = 20
UOM_WMS_CACHE_MAX_ENTRIES = 256
UOM_WMS_CACHE_MAX_BYTES = 24 * 1024 * 1024
UOM_WMS_CACHE_MAX_ENTRY_BYTES = 1024 * 1024
HTTP_MAX_WORKERS = 48
WEB_MERCATOR_LIMIT = 20037508.342789244
UOM_WMS_PROXY_ENV = "RID_UOM_WMS_PROXY"
UOM_WMS_TOKEN_FILE_ENV = "RID_UOM_WMS_TOKEN_FILE"
UOM_WMS_TOKEN_PATTERN = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
    re.IGNORECASE,
)
UOM_WMS_TOKEN_PATH = os.path.join(HERE, "data", "uom_wms_token")
UOM_WMS_TOKEN_LOCK = threading.RLock()
UOM_WMS_RUNTIME_LOCK = threading.Lock()
UOM_WMS_CREDENTIAL_REVISION = secrets.token_hex(8)
UOM_WMS_LAST_RESULT = None
UOM_WMS_LAST_SUCCESS_AT = None
UOM_WMS_LAST_ERROR_AT = None
UOM_WMS_LAST_ERROR_STATUS = None
UOM_WMS_LAST_ERROR_KIND = None
UOM_WMS_FETCH_SLOTS = threading.BoundedSemaphore(UOM_WMS_MAX_CONCURRENT_FETCHES)
UOM_WMS_CACHE_LOCK = threading.Lock()
UOM_WMS_CACHE = OrderedDict()
UOM_WMS_CACHE_BYTES = 0
PNG_SIGNATURE = b"\x89PNG\r\n\x1a\n"


class _UomWmsNoRedirect(HTTPRedirectHandler):
    """Do not allow a UOM redirect to receive the server-side token."""

    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


_UOM_WMS_OPENER = build_opener(_UomWmsNoRedirect())


class UomWmsUpstreamError(Exception):
    """A sanitized UOM upstream failure suitable for returning to a client."""

    def __init__(self, upstream_status=None, failure_kind="upstream_error"):
        super().__init__("uom_wms_upstream_error")
        self.upstream_status = (
            upstream_status if isinstance(upstream_status, int) and 400 <= upstream_status <= 599
            else None
        )
        self.failure_kind = (
            failure_kind
            if failure_kind in {
                "http_error", "transport_error", "invalid_content_length",
                "invalid_content_type", "invalid_png", "invalid_png_dimensions",
                "response_too_large", "busy", "unexpected_error", "upstream_error",
            }
            else "upstream_error"
        )


def _uom_wms_token_state():
    """Return the active token and its non-secret storage source."""
    with UOM_WMS_TOKEN_LOCK:
        try:
            with open(UOM_WMS_TOKEN_PATH, "r", encoding="utf-8") as handle:
                managed = handle.read(129).strip()
        except (FileNotFoundError, PermissionError, OSError):
            managed = ""
        if UOM_WMS_TOKEN_PATTERN.fullmatch(managed):
            return managed, "managed-file"
        configured = os.environ.get("RID_UOM_WMS_TOKEN", "").strip()
        return configured, ("environment" if configured else "unconfigured")


def _uom_wms_token():
    """Read the token only when needed; never include it in application state."""
    return _uom_wms_token_state()[0]


def _uom_wms_request_credential():
    """Capture a token and its non-secret revision as one rotation-safe pair."""
    with UOM_WMS_TOKEN_LOCK:
        token, _source = _uom_wms_token_state()
        with UOM_WMS_RUNTIME_LOCK:
            revision = UOM_WMS_CREDENTIAL_REVISION
    return token, revision


def _clear_uom_wms_cache():
    global UOM_WMS_CACHE_BYTES
    with UOM_WMS_CACHE_LOCK:
        UOM_WMS_CACHE.clear()
        UOM_WMS_CACHE_BYTES = 0


def _uom_wms_cache_key(normalized_request, credential_revision):
    if not credential_revision:
        return None
    return (
        credential_revision,
        tuple(sorted((str(name), str(value)) for name, value in normalized_request.items())),
    )


def _uom_wms_cache_get(key):
    global UOM_WMS_CACHE_BYTES
    if key is None:
        return None
    now = time.monotonic()
    with UOM_WMS_CACHE_LOCK:
        for candidate, (expires_at, body) in list(UOM_WMS_CACHE.items()):
            if expires_at <= now:
                UOM_WMS_CACHE.pop(candidate, None)
                UOM_WMS_CACHE_BYTES -= len(body)
        entry = UOM_WMS_CACHE.get(key)
        if entry is None:
            return None
        UOM_WMS_CACHE.move_to_end(key)
        return entry[1]


def _uom_wms_cache_put(key, body):
    global UOM_WMS_CACHE_BYTES
    if (key is None or not isinstance(body, bytes)
            or len(body) > UOM_WMS_CACHE_MAX_ENTRY_BYTES):
        return
    with UOM_WMS_CACHE_LOCK:
        previous = UOM_WMS_CACHE.pop(key, None)
        if previous is not None:
            UOM_WMS_CACHE_BYTES -= len(previous[1])
        UOM_WMS_CACHE[key] = (time.monotonic() + UOM_WMS_CACHE_TTL_SECONDS, body)
        UOM_WMS_CACHE_BYTES += len(body)
        while (len(UOM_WMS_CACHE) > UOM_WMS_CACHE_MAX_ENTRIES
               or UOM_WMS_CACHE_BYTES > UOM_WMS_CACHE_MAX_BYTES):
            _old_key, (_expires_at, old_body) = UOM_WMS_CACHE.popitem(last=False)
            UOM_WMS_CACHE_BYTES -= len(old_body)


def _reset_uom_wms_runtime():
    """Invalidate prior health when the short-lived credential changes."""
    global UOM_WMS_CREDENTIAL_REVISION, UOM_WMS_LAST_ERROR_AT
    global UOM_WMS_LAST_ERROR_KIND, UOM_WMS_LAST_ERROR_STATUS
    global UOM_WMS_LAST_RESULT, UOM_WMS_LAST_SUCCESS_AT
    with UOM_WMS_RUNTIME_LOCK:
        UOM_WMS_CREDENTIAL_REVISION = secrets.token_hex(8)
        UOM_WMS_LAST_RESULT = None
        UOM_WMS_LAST_SUCCESS_AT = None
        UOM_WMS_LAST_ERROR_AT = None
        UOM_WMS_LAST_ERROR_STATUS = None
        UOM_WMS_LAST_ERROR_KIND = None
    _clear_uom_wms_cache()


def _record_uom_wms_result(
        success, upstream_status=None, failure_kind=None, credential_revision=None):
    """Record only browser-safe health metadata, never request URLs or credentials."""
    global UOM_WMS_LAST_ERROR_AT, UOM_WMS_LAST_ERROR_KIND, UOM_WMS_LAST_ERROR_STATUS
    global UOM_WMS_LAST_RESULT, UOM_WMS_LAST_SUCCESS_AT
    now_ms = int(time.time() * 1000)
    with UOM_WMS_RUNTIME_LOCK:
        if (credential_revision is not None
                and (not isinstance(credential_revision, str)
                     or not hmac.compare_digest(
                         credential_revision, UOM_WMS_CREDENTIAL_REVISION,
                     ))):
            return False
        if success:
            UOM_WMS_LAST_RESULT = "success"
            UOM_WMS_LAST_SUCCESS_AT = now_ms
        else:
            UOM_WMS_LAST_RESULT = "error"
            UOM_WMS_LAST_ERROR_AT = now_ms
            UOM_WMS_LAST_ERROR_STATUS = (
                upstream_status
                if isinstance(upstream_status, int) and 400 <= upstream_status <= 599
                else None
            )
            UOM_WMS_LAST_ERROR_KIND = failure_kind or "upstream_error"
        return True


def _uom_wms_runtime_state():
    with UOM_WMS_RUNTIME_LOCK:
        return {
            "credentialRevision": UOM_WMS_CREDENTIAL_REVISION,
            "lastResult": UOM_WMS_LAST_RESULT,
            "lastSuccessAt": UOM_WMS_LAST_SUCCESS_AT,
            "lastErrorAt": UOM_WMS_LAST_ERROR_AT,
            "lastErrorStatus": UOM_WMS_LAST_ERROR_STATUS,
            "lastErrorKind": UOM_WMS_LAST_ERROR_KIND,
        }


def store_uom_wms_token(value):
    """Atomically persist one short-lived WMS token outside the image layer."""
    if not isinstance(value, str) or not UOM_WMS_TOKEN_PATTERN.fullmatch(value.strip()):
        raise ValueError("UOM WMS token must be a UUID")
    token = value.strip()
    target = os.path.abspath(UOM_WMS_TOKEN_PATH)
    directory = os.path.dirname(target)
    os.makedirs(directory, mode=0o700, exist_ok=True)
    temporary = target + ".tmp-" + secrets.token_hex(8)
    with UOM_WMS_TOKEN_LOCK:
        descriptor = None
        try:
            descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
            with os.fdopen(descriptor, "w", encoding="utf-8", newline="\n") as handle:
                descriptor = None
                handle.write(token + "\n")
                handle.flush()
                os.fsync(handle.fileno())
            os.chmod(temporary, 0o600)
            os.replace(temporary, target)
            if os.name == "posix":
                directory_fd = os.open(
                    directory, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0),
                )
                try:
                    os.fsync(directory_fd)
                finally:
                    os.close(directory_fd)
        finally:
            if descriptor is not None:
                os.close(descriptor)
            try:
                os.unlink(temporary)
            except FileNotFoundError:
                pass
        _reset_uom_wms_runtime()
    return {"ok": True, "configured": True, "credentialSource": "managed-file"}


def _uom_wms_proxy_url():
    """Return a validated plain HTTP proxy URL, or ``None`` when unset.

    The proxy is intended for the loopback SSH/CONNECT relay.  Credentials,
    paths, query strings, and fragments are rejected so they cannot be
    accidentally mixed into a request or exposed in an error message.
    """
    raw = os.environ.get(UOM_WMS_PROXY_ENV, "").strip()
    if not raw:
        return None
    parsed = urlsplit(raw)
    if parsed.scheme.lower() != "http":
        raise ValueError("UOM WMS proxy must use http")
    if not parsed.hostname or parsed.username is not None or parsed.password is not None:
        raise ValueError("UOM WMS proxy authority is invalid")
    if parsed.path not in ("", "/") or parsed.query or parsed.fragment:
        raise ValueError("UOM WMS proxy must not include a path or query")
    try:
        port = parsed.port
    except ValueError as exc:
        raise ValueError("UOM WMS proxy port is invalid") from exc
    if port is None or not 1 <= port <= 65535:
        raise ValueError("UOM WMS proxy port is invalid")
    return raw


def _uom_wms_proxy_status():
    """Return non-secret proxy metadata for the status API."""
    raw = os.environ.get(UOM_WMS_PROXY_ENV, "").strip()
    if not raw:
        return {"configured": False, "valid": True, "mode": "direct"}
    try:
        proxy = _uom_wms_proxy_url()
        parsed = urlsplit(proxy)
    except ValueError:
        return {
            "configured": True,
            "valid": False,
            "mode": "invalid",
            "message": "UOM CONNECT 中继地址配置无效",
        }
    return {
        "configured": True,
        "valid": True,
        "mode": "http-connect",
        "host": parsed.hostname,
        "port": parsed.port,
    }


def uom_wms_status():
    """Return browser-safe UOM raster configuration without the credential."""
    token, credential_source = _uom_wms_token_state()
    proxy = _uom_wms_proxy_status()
    runtime = _uom_wms_runtime_state()
    configured = bool(token)
    enabled = configured and proxy.get("valid") is not False
    now_ms = int(time.time() * 1000)
    success_at = runtime["lastSuccessAt"]
    success_fresh = bool(
        success_at and 0 <= now_ms - success_at <= UOM_WMS_READY_TTL_MS
    )
    ready = enabled and runtime["lastResult"] == "success" and success_fresh
    upstream_status = runtime["lastErrorStatus"]
    failure_kind = runtime.get("lastErrorKind")
    if not configured:
        status = "unconfigured"
        message = "UOM WMS 服务端令牌尚未配置"
    elif not enabled:
        status = "invalid"
        message = proxy.get("message") or "UOM CONNECT 中继配置无效"
    elif ready:
        status = "ready"
        message = "UOM 实时适飞栅格已通过上游响应验证；不代表飞行审批结果"
    elif runtime["lastResult"] == "error":
        status = "expired" if upstream_status in {401, 403} else "error"
        message = (
            "UOM WMS 临时令牌已失效，请更新令牌"
            if status == "expired"
            else "UOM WMS 上游或 CONNECT 中继当前不可用"
        )
    else:
        status = "verifying"
        message = "UOM WMS 已配置，正在验证实时栅格链路"
    return {
        "enabled": enabled,
        "configured": configured,
        "ready": ready,
        "status": status,
        "message": message,
        "layerGroups": [
            {"id": group_id, "layers": layers, "styles": styles}
            for (group_id, _), layers, styles in zip(
                UOM_WMS_GROUP_DEFINITIONS, UOM_WMS_LAYER_GROUPS, UOM_WMS_STYLE_GROUPS,
            )
        ],
        "wmsPath": "/api/airspace/uom-wms",
        "zoomRange": [9, 18],
        "renderMode": "suitable-raster",
        "credentialSource": credential_source,
        "credentialRevision": runtime["credentialRevision"] if configured else "",
        "lastSuccessAt": success_at,
        "lastErrorAt": runtime["lastErrorAt"],
        "upstreamStatus": upstream_status if runtime["lastResult"] == "error" else None,
        "failureKind": failure_kind if runtime["lastResult"] == "error" else None,
        "proxy": proxy,
    }


def _parse_uom_wms_dimension(value, label):
    if not isinstance(value, str) or not re.fullmatch(r"[1-9][0-9]{0,3}", value):
        raise ValueError(f"{label} 必须是正整数")
    dimension = int(value)
    if dimension > UOM_WMS_MAX_DIMENSION:
        raise ValueError(f"{label} 不能超过 {UOM_WMS_MAX_DIMENSION}")
    return dimension


def _format_uom_wms_number(value):
    return format(value, ".15g")


def normalize_uom_wms_request(params):
    """Validate and canonicalize the fixed UOM WMS GetMap request shape."""
    canonical = {}
    for raw_name, values in (params or {}).items():
        name = str(raw_name).upper()
        if name not in UOM_WMS_ALLOWED_PARAMETERS:
            raise ValueError(f"不支持的 UOM WMS 参数: {raw_name}")
        if name in canonical or not isinstance(values, list) or len(values) != 1:
            raise ValueError(f"{name} 必须且只能出现一次")
        value = values[0]
        if not isinstance(value, str):
            raise ValueError(f"{name} 参数无效")
        canonical[name] = value

    required = UOM_WMS_ALLOWED_PARAMETERS
    missing = sorted(required - set(canonical))
    if missing:
        raise ValueError("缺少 UOM WMS 参数: " + ",".join(missing))
    if canonical["SERVICE"].upper() != "WMS":
        raise ValueError("SERVICE 必须是 WMS")
    if canonical["REQUEST"].lower() != "getmap":
        raise ValueError("REQUEST 必须是 GetMap")
    if canonical["VERSION"] != UOM_WMS_VERSION:
        raise ValueError(f"VERSION 必须是 {UOM_WMS_VERSION}")
    if canonical["SRS"].upper() != UOM_WMS_SRS:
        raise ValueError(f"SRS 必须是 {UOM_WMS_SRS}")
    if canonical["FORMAT"].lower() != UOM_WMS_FORMAT:
        raise ValueError(f"FORMAT 必须是 {UOM_WMS_FORMAT}")
    if canonical["TRANSPARENT"].lower() != "true":
        raise ValueError("TRANSPARENT 必须是 true")
    if canonical["LAYERS"] not in UOM_WMS_ALLOWED_LAYER_GROUPS:
        raise ValueError("LAYERS 不在允许的 UOM 图层组内")
    expected_styles = UOM_WMS_STYLES_BY_LAYER_GROUP[canonical["LAYERS"]]
    if canonical["STYLES"] != expected_styles:
        raise ValueError("STYLES 不匹配允许的 UOM 样式")

    bbox_parts = canonical["BBOX"].split(",")
    if len(bbox_parts) != 4:
        raise ValueError("BBOX 必须包含四个坐标")
    try:
        bbox = [float(part.strip()) for part in bbox_parts]
    except ValueError as exc:
        raise ValueError("BBOX 坐标无效") from exc
    if (not all(math.isfinite(value) for value in bbox)
            or not (-WEB_MERCATOR_LIMIT <= bbox[0] < bbox[2] <= WEB_MERCATOR_LIMIT
                    and -WEB_MERCATOR_LIMIT <= bbox[1] < bbox[3] <= WEB_MERCATOR_LIMIT)):
        raise ValueError("BBOX 超出 EPSG:3857 世界范围或顺序无效")

    width = _parse_uom_wms_dimension(canonical["WIDTH"], "WIDTH")
    height = _parse_uom_wms_dimension(canonical["HEIGHT"], "HEIGHT")
    if width * height > UOM_WMS_MAX_PIXELS:
        raise ValueError("WIDTH 和 HEIGHT 的像素总数过大")

    return {
        "SERVICE": "WMS",
        "REQUEST": "GetMap",
        "VERSION": UOM_WMS_VERSION,
        "LAYERS": canonical["LAYERS"],
        "STYLES": expected_styles,
        "FORMAT": UOM_WMS_FORMAT,
        "TRANSPARENT": "TRUE",
        "SRS": UOM_WMS_SRS,
        "BBOX": ",".join(_format_uom_wms_number(value) for value in bbox),
        "WIDTH": str(width),
        "HEIGHT": str(height),
    }


def _open_uom_wms(request):
    proxy = _uom_wms_proxy_url()
    if proxy:
        # The relay only understands HTTPS CONNECT.  Keep the existing opener
        # for direct mode so tests and deployments without a relay are unchanged.
        opener = build_opener(
            ProxyHandler({"https": proxy}),
            _UomWmsNoRedirect(),
        )
    else:
        opener = _UOM_WMS_OPENER
    return opener.open(request, timeout=UOM_WMS_TIMEOUT_SECONDS)


def _validate_uom_png_pixels(compressed, width, height, bit_depth, color_type, interlace):
    channels = {0: 1, 2: 3, 3: 1, 4: 2, 6: 4}
    valid_depths = {
        0: {1, 2, 4, 8, 16},
        2: {8, 16},
        3: {1, 2, 4, 8},
        4: {8, 16},
        6: {8, 16},
    }
    if color_type not in channels or bit_depth not in valid_depths[color_type]:
        raise UomWmsUpstreamError(failure_kind="invalid_png")
    bits_per_pixel = channels[color_type] * bit_depth
    if interlace == 0:
        row_lengths = [((width * bits_per_pixel) + 7) // 8] * height
    else:
        row_lengths = []
        for x_start, y_start, x_step, y_step in (
                (0, 0, 8, 8), (4, 0, 8, 8), (0, 4, 4, 8), (2, 0, 4, 4),
                (0, 2, 2, 4), (1, 0, 2, 2), (0, 1, 1, 2)):
            pass_width = max(0, (width - x_start + x_step - 1) // x_step)
            pass_height = max(0, (height - y_start + y_step - 1) // y_step)
            if pass_width and pass_height:
                row_length = ((pass_width * bits_per_pixel) + 7) // 8
                row_lengths.extend([row_length] * pass_height)

    decoder = zlib.decompressobj()
    remaining = compressed
    try:
        for row_length in row_lengths:
            required = row_length + 1
            row = bytearray()
            while len(row) < required:
                piece = decoder.decompress(remaining, required - len(row))
                remaining = decoder.unconsumed_tail
                row.extend(piece)
                if len(row) < required and not remaining:
                    piece = decoder.decompress(b"", required - len(row))
                    row.extend(piece)
                    if not piece:
                        raise UomWmsUpstreamError(failure_kind="invalid_png")
            if row[0] > 4:
                raise UomWmsUpstreamError(failure_kind="invalid_png")

        extra = decoder.decompress(remaining, 1)
        remaining = decoder.unconsumed_tail
        if extra or decoder.flush(1):
            raise UomWmsUpstreamError(failure_kind="invalid_png")
    except zlib.error:
        raise UomWmsUpstreamError(failure_kind="invalid_png") from None
    if not decoder.eof or remaining or decoder.unused_data:
        raise UomWmsUpstreamError(failure_kind="invalid_png")


def _validate_uom_png(body, expected_width=None, expected_height=None):
    """Reject empty, truncated, mislabeled, or corrupt upstream PNG bodies."""
    if not isinstance(body, bytes) or len(body) < 33 or not body.startswith(PNG_SIGNATURE):
        raise UomWmsUpstreamError(failure_kind="invalid_png")
    offset = len(PNG_SIGNATURE)
    chunk_index = 0
    saw_ihdr = False
    saw_idat = False
    saw_plte = False
    idat_parts = []
    width = height = bit_depth = color_type = interlace = None
    while offset < len(body):
        if len(body) - offset < 12:
            raise UomWmsUpstreamError(failure_kind="invalid_png")
        length = int.from_bytes(body[offset:offset + 4], "big")
        chunk_type = body[offset + 4:offset + 8]
        chunk_end = offset + 12 + length
        if chunk_end > len(body):
            raise UomWmsUpstreamError(failure_kind="invalid_png")
        chunk_data = body[offset + 8:offset + 8 + length]
        expected_crc = int.from_bytes(body[offset + 8 + length:chunk_end], "big")
        actual_crc = zlib.crc32(chunk_type)
        actual_crc = zlib.crc32(chunk_data, actual_crc) & 0xFFFFFFFF
        if actual_crc != expected_crc:
            raise UomWmsUpstreamError(failure_kind="invalid_png")
        if chunk_index == 0:
            if chunk_type != b"IHDR" or length != 13:
                raise UomWmsUpstreamError(failure_kind="invalid_png")
            width = int.from_bytes(chunk_data[0:4], "big")
            height = int.from_bytes(chunk_data[4:8], "big")
            bit_depth = chunk_data[8]
            color_type = chunk_data[9]
            interlace = chunk_data[12]
            if not (1 <= width <= UOM_WMS_MAX_DIMENSION and 1 <= height <= UOM_WMS_MAX_DIMENSION):
                raise UomWmsUpstreamError(failure_kind="invalid_png")
            if ((expected_width is not None and width != expected_width)
                    or (expected_height is not None and height != expected_height)):
                raise UomWmsUpstreamError(failure_kind="invalid_png_dimensions")
            if chunk_data[10] != 0 or chunk_data[11] != 0 or chunk_data[12] not in {0, 1}:
                raise UomWmsUpstreamError(failure_kind="invalid_png")
            saw_ihdr = True
        elif chunk_type == b"IHDR":
            raise UomWmsUpstreamError(failure_kind="invalid_png")
        elif chunk_type == b"PLTE":
            if saw_idat or length == 0 or length > 768 or length % 3:
                raise UomWmsUpstreamError(failure_kind="invalid_png")
            saw_plte = True
        elif chunk_type == b"IDAT":
            if not saw_ihdr:
                raise UomWmsUpstreamError(failure_kind="invalid_png")
            saw_idat = True
            idat_parts.append(chunk_data)
        if chunk_type == b"IEND":
            if length != 0 or chunk_end != len(body) or not saw_ihdr or not saw_idat:
                raise UomWmsUpstreamError(failure_kind="invalid_png")
            if color_type == 3 and not saw_plte:
                raise UomWmsUpstreamError(failure_kind="invalid_png")
            _validate_uom_png_pixels(
                b"".join(idat_parts), width, height, bit_depth, color_type, interlace,
            )
            return body
        offset = chunk_end
        chunk_index += 1
    raise UomWmsUpstreamError(failure_kind="invalid_png")


def fetch_uom_wms(normalized_request, token, credential_revision=None):
    """Fetch one bounded PNG tile without exposing the credential to callers."""
    cache_key = _uom_wms_cache_key(normalized_request, credential_revision)
    cached = _uom_wms_cache_get(cache_key)
    if cached is not None:
        return cached
    if not UOM_WMS_FETCH_SLOTS.acquire(timeout=UOM_WMS_FETCH_QUEUE_TIMEOUT_SECONDS):
        raise UomWmsUpstreamError(failure_kind="busy")
    try:
        cached = _uom_wms_cache_get(cache_key)
        if cached is not None:
            return cached
        # The UOM gateway treats its otherwise case-insensitive WMS parameter names
        # as case-sensitive. Keep uppercase locally and mirror the authorized client.
        query = [(name.lower(), value) for name, value in normalized_request.items()]
        query.append(("token", token))
        url = UOM_WMS_ENDPOINT + "?" + urlencode(query, safe=":,")
        request = Request(url, headers={"Accept": "image/png"}, method="GET")
        with _open_uom_wms(request) as response:
            content_length = response.headers.get("Content-Length", "")
            declared_length = None
            if content_length:
                try:
                    declared_length = int(content_length)
                except ValueError:
                    raise UomWmsUpstreamError(failure_kind="invalid_content_length")
                if declared_length < 0 or declared_length > UOM_WMS_MAX_RESPONSE_BYTES:
                    raise UomWmsUpstreamError(failure_kind="response_too_large")
            content_type = response.headers.get("Content-Type", "")
            mime_type = content_type.split(";", 1)[0].strip().lower()
            if mime_type not in {"image/png", "image/png8"}:
                raise UomWmsUpstreamError(failure_kind="invalid_content_type")
            body = response.read(UOM_WMS_MAX_RESPONSE_BYTES + 1)
            if (len(body) > UOM_WMS_MAX_RESPONSE_BYTES
                    or declared_length is not None and declared_length != len(body)):
                raise UomWmsUpstreamError(
                    failure_kind=(
                        "response_too_large"
                        if len(body) > UOM_WMS_MAX_RESPONSE_BYTES
                        else "invalid_content_length"
                    )
                )
            image = _validate_uom_png(
                body,
                expected_width=int(normalized_request["WIDTH"]),
                expected_height=int(normalized_request["HEIGHT"]),
            )
            _uom_wms_cache_put(cache_key, image)
            return image
    except UomWmsUpstreamError:
        raise
    except HTTPError as exc:
        raise UomWmsUpstreamError(exc.code, "http_error") from None
    except (URLError, OSError, TimeoutError):
        raise UomWmsUpstreamError(failure_kind="transport_error") from None
    except Exception:
        # Do not pass upstream exception text through because it may contain its URL.
        raise UomWmsUpstreamError(failure_kind="unexpected_error") from None
    finally:
        UOM_WMS_FETCH_SLOTS.release()


def _load_airspace_catalog():
    """Load the checked-in coverage ledger without making network requests."""
    try:
        with open(AIRSPACE_CATALOG_PATH, "r", encoding="utf-8") as handle:
            value = json.load(handle)
    except (OSError, ValueError) as exc:
        print(f"[airspace] coverage catalog unavailable: {exc}")
        return {
            "schemaVersion": "0",
            "catalogVersion": "unavailable",
            "generatedAt": None,
            "coordinateSystem": "WGS84",
            "scope": "unknown",
            "uom": {"layerGroups": [], "observedDistCodes": [], "notObservedDistCodes": []},
            "regions": [],
            "beijingRule": None,
        }
    if not isinstance(value, dict):
        raise RuntimeError("airspace catalog root must be an object")
    return value


AIRSPACE_CATALOG = _load_airspace_catalog()


def airspace_catalog_summary():
    """Return public, non-secret coverage metadata for API/UI consumption."""
    uom = AIRSPACE_CATALOG.get("uom") or {}
    regions = AIRSPACE_CATALOG.get("regions") or []
    groups = uom.get("layerGroups") or []
    observed = [str(code) for code in (uom.get("observedDistCodes") or [])]
    missing = [str(code) for code in (uom.get("notObservedDistCodes") or [])]
    return {
        "schemaVersion": AIRSPACE_CATALOG.get("schemaVersion"),
        "catalogVersion": AIRSPACE_CATALOG.get("catalogVersion"),
        "generatedAt": AIRSPACE_CATALOG.get("generatedAt"),
        "coordinateSystem": AIRSPACE_CATALOG.get("coordinateSystem", "WGS84"),
        "scope": AIRSPACE_CATALOG.get("scope"),
        "regionCount": len(regions),
        "uomLayerGroupCount": len(groups),
        "uomObservedRegionCount": len(observed),
        "uomNotObservedRegionCount": len(missing),
        "uomObservedDistCodes": observed,
        "uomNotObservedDistCodes": missing,
        "uom": {
            "portalUrl": uom.get("portalUrl"),
            "standardReference": uom.get("standardReference"),
            "wmsEndpoint": uom.get("wmsEndpoint"),
            "queryType": uom.get("queryType"),
            "observedAt": uom.get("observedAt"),
            "accessStatus": uom.get("accessStatus"),
            "authorizationNote": uom.get("authorizationNote"),
            "layerGroups": groups,
        },
        "regions": regions,
        "beijingRule": AIRSPACE_CATALOG.get("beijingRule"),
    }


def builtin_beijing_airspace_payload():
    """Build the offline Beijing policy-reference record used on fresh installs.

    This is deliberately retained as provenance, not an airspace vector that can
    be rendered as if it were a UOM response.
    """
    with open(BUILTIN_BEIJING_AIRSPACE_PATH, "r", encoding="utf-8") as handle:
        data = json.load(handle)
    uom = AIRSPACE_CATALOG.get("uom") or {}
    rule = AIRSPACE_CATALOG.get("beijingRule") or {}
    return {
        "source": {
            "slug": "uom-beijing-110000",
            "name": "北京禁飞规则参考（非 UOM 矢量）",
            "provider": "本地规则参考",
            "authority": "运营方确认，未取得 UOM 矢量授权",
            "url": uom.get("portalUrl"),
            "sourceVersion": AIRSPACE_CATALOG.get("catalogVersion") or "uom-rule-reference",
            "publishedAt": AIRSPACE_CATALOG.get("generatedAt"),
            "coordinateSystem": "WGS84",
            "distCode": "110000",
            "coverageScope": "city",
            "referenceOnly": True,
            "provenance": {
                "ruleBasis": rule.get("label"),
                "authoritative": False,
                "evidence": rule.get("evidence") or [],
                "boundaryNote": rule.get("boundaryNote"),
            },
        },
        "defaultClass": "prohibited",
        "data": data,
    }


def packaged_uom_derived_airspace_payload():
    """Load the packaged WMS-derived snapshot after checking its provenance and hash."""
    if not (os.path.isfile(PACKAGED_UOM_DERIVED_AIRSPACE_PATH)
            and os.path.isfile(PACKAGED_UOM_DERIVED_MANIFEST_PATH)):
        return None
    with open(PACKAGED_UOM_DERIVED_MANIFEST_PATH, "rb") as handle:
        manifest_body = handle.read(1024 * 1024 + 1)
    if len(manifest_body) > 1024 * 1024:
        raise RuntimeError("packaged UOM snapshot manifest is too large")
    try:
        manifest = json.loads(manifest_body.decode("utf-8"))
    except (UnicodeError, ValueError) as exc:
        raise RuntimeError("packaged UOM snapshot manifest is invalid") from exc
    expected = {
        "sourceType": "authenticated-uom-wms-raster",
        "sourceEndpoint": UOM_WMS_ENDPOINT,
        "coordinateSystem": "WGS84",
        "coverageScope": "national",
        "derivation": "alpha-mask-polygonize",
    }
    if (not isinstance(manifest, dict)
            or any(manifest.get(key) != value for key, value in expected.items())
            or manifest.get("credentialStored") is not False
            or manifest.get("referenceOnly") is not True
            or manifest.get("authoritativeVector") is not False):
        raise RuntimeError("packaged UOM snapshot provenance is invalid")
    if manifest.get("geojsonFile") != os.path.basename(PACKAGED_UOM_DERIVED_AIRSPACE_PATH):
        raise RuntimeError("packaged UOM snapshot filename is invalid")
    with open(PACKAGED_UOM_DERIVED_AIRSPACE_PATH, "rb") as handle:
        geojson_body = handle.read(MAX_AIRSPACE_IMPORT_BYTES + 1)
    if len(geojson_body) > MAX_AIRSPACE_IMPORT_BYTES:
        raise RuntimeError("packaged UOM snapshot exceeds the import size limit")
    if hashlib.sha256(geojson_body).hexdigest() != manifest.get("geojsonSha256"):
        raise RuntimeError("packaged UOM snapshot SHA-256 does not match its manifest")
    try:
        data = json.loads(geojson_body.decode("utf-8"))
    except (UnicodeError, ValueError) as exc:
        raise RuntimeError("packaged UOM snapshot GeoJSON is invalid") from exc
    features = data.get("features") if isinstance(data, dict) else None
    if (not isinstance(data, dict) or data.get("type") != "FeatureCollection"
            or not isinstance(features, list)
            or len(features) != manifest.get("featureCount")):
        raise RuntimeError("packaged UOM snapshot feature count is invalid")
    snapshot_version = _clean_text(manifest.get("snapshotVersion"), 128)
    if not snapshot_version:
        raise RuntimeError("packaged UOM snapshot version is missing")
    return {
        "source": {
            "slug": "uom-derived-national",
            "name": "UOM 全国适飞参考派生矢量快照",
            "provider": "UOM WMS（栅格派生）",
            "authority": "经授权 UOM WMS 采样；非 UOM 官方矢量",
            "url": "https://uom.caac.gov.cn/#/main",
            "sourceVersion": snapshot_version,
            "publishedAt": manifest.get("capturedAt"),
            "coordinateSystem": "WGS84",
            "coverageScope": "national",
            # The source remains drawable. referenceOnly in the database means
            # hidden policy placeholder, which is reserved for the Beijing rule.
            "referenceOnly": False,
            "provenance": {
                "sourceType": manifest.get("sourceType"),
                "derivation": manifest.get("derivation"),
                "resolutionMeters": manifest.get("resolutionMeters"),
                "alphaThreshold": manifest.get("alphaThreshold"),
                "authoritativeVector": False,
                "referenceOnly": True,
                "notice": manifest.get("notice"),
                "geojsonSha256": manifest.get("geojsonSha256"),
            },
        },
        "defaultClass": "suitable",
        "data": data,
    }


def _number(value):
    """Return a finite float, or None for missing/invalid telemetry."""
    if value is None or isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _monitor_location():
    """Return a valid WGS-84 monitor point. Defaults are a public demo location."""
    lat = _bounded_number(os.environ.get("RID_MONITOR_LAT"), -90, 90)
    lon = _bounded_number(os.environ.get("RID_MONITOR_LON"), -180, 180)
    return (
        DEFAULT_MONITOR_LAT if lat is None else lat,
        DEFAULT_MONITOR_LON if lon is None else lon,
    )


def _integer(value):
    number = _number(value)
    return int(number) if number is not None else None


def _clean_text(value, max_length):
    if not isinstance(value, str):
        return None
    cleaned = CONTROL_CHAR_PATTERN.sub("", value).replace("<", "").replace(">", "").strip()
    return cleaned[:max_length] or None


def _csv_safe(value):
    """Prevent spreadsheet formula execution while preserving non-string cells."""
    if not isinstance(value, str):
        return value
    first = value.lstrip()[:1]
    return "'" + value if first in {"=", "+", "-", "@"} else value


def _bounded_number(value, minimum, maximum):
    number = _number(value)
    return number if number is not None and minimum <= number <= maximum else None


def _risk_level(score):
    if score >= 80:
        return "critical"
    if score >= 65:
        return "high"
    if score >= 45:
        return "medium"
    if score >= 25:
        return "low"
    return "normal"


def _coordinate(value, label):
    if not isinstance(value, (list, tuple)) or len(value) != 2:
        raise ValueError(f"{label} 必须为 [lon,lat]")
    lon = _bounded_number(value[0], -180, 180)
    lat = _bounded_number(value[1], -90, 90)
    if lon is None or lat is None:
        raise ValueError(f"{label} 坐标无效")
    return [lon, lat]


def normalize_geofence_payload(payload):
    """Validate and canonicalize one geofence management payload."""
    if not isinstance(payload, dict):
        raise ValueError("围栏必须是 JSON 对象")
    name = _clean_text(payload.get("name"), 128)
    if not name:
        raise ValueError("name 不能为空")
    severity = str(payload.get("severity") or "medium").strip().lower()
    if severity not in GEOFENCE_SEVERITY_SCORES:
        raise ValueError("severity 必须是 low、medium、high 或 critical")
    enabled = payload.get("enabled", True)
    if not isinstance(enabled, bool):
        raise ValueError("enabled 必须是布尔值")

    altitude = {}
    for api_name, key in (("minAltitude", "min_altitude"), ("maxAltitude", "max_altitude")):
        raw = payload.get(api_name)
        if raw is None:
            altitude[key] = None
            continue
        value = _bounded_number(raw, -1000, 10000)
        if value is None:
            raise ValueError(f"{api_name} 必须在 -1000 到 10000 米之间")
        altitude[key] = value
    if (altitude["min_altitude"] is not None and altitude["max_altitude"] is not None
            and altitude["min_altitude"] > altitude["max_altitude"]):
        raise ValueError("minAltitude 不能大于 maxAltitude")

    geometry = payload.get("geometry")
    if not isinstance(geometry, dict):
        raise ValueError("geometry 必须是 JSON 对象")
    geometry_type = str(geometry.get("type") or "").strip().lower()
    requested_type = str(payload.get("shapeType") or geometry_type).strip().lower()
    if requested_type not in {"circle", "polygon"} or geometry_type != requested_type:
        raise ValueError("shapeType 必须与 geometry.type 的 circle/polygon 一致")

    if geometry_type == "circle":
        center = _coordinate(geometry.get("center"), "geometry.center")
        radius = _bounded_number(geometry.get("radiusMeters"), 1, 200000)
        if radius is None:
            raise ValueError("geometry.radiusMeters 必须在 1 到 200000 米之间")
        canonical_geometry = {"type": "Circle", "center": center, "radiusMeters": radius}
    else:
        coordinates = geometry.get("coordinates")
        if (not isinstance(coordinates, list) or len(coordinates) != 1
                or not isinstance(coordinates[0], list)):
            raise ValueError("Polygon coordinates 必须只包含一个外环")
        raw_ring = coordinates[0]
        if len(raw_ring) < 3 or len(raw_ring) > MAX_GEOFENCE_VERTICES + 1:
            raise ValueError(f"Polygon 顶点数必须在 3 到 {MAX_GEOFENCE_VERTICES} 之间")
        ring = [_coordinate(point, f"geometry.coordinates[0][{index}]")
                for index, point in enumerate(raw_ring)]
        if ring[0] == ring[-1]:
            ring = ring[:-1]
        if len(ring) > MAX_GEOFENCE_VERTICES:
            raise ValueError(f"Polygon 顶点数不能超过 {MAX_GEOFENCE_VERTICES}")
        if len(ring) < 3 or len({(point[0], point[1]) for point in ring}) < 3:
            raise ValueError("Polygon 至少需要三个不同顶点")
        area2 = sum(
            ring[index][0] * ring[(index + 1) % len(ring)][1]
            - ring[(index + 1) % len(ring)][0] * ring[index][1]
            for index in range(len(ring))
        )
        if abs(area2) < 1e-12:
            raise ValueError("Polygon 面积不能为零")
        ring.append(list(ring[0]))
        canonical_geometry = {"type": "Polygon", "coordinates": [ring]}

    return {
        "name": name,
        "shape_type": geometry_type,
        "severity": severity,
        "enabled": 1 if enabled else 0,
        "min_altitude": altitude["min_altitude"],
        "max_altitude": altitude["max_altitude"],
        "geometry_json": json.dumps(canonical_geometry, ensure_ascii=False, separators=(",", ":")),
    }


def _point_in_polygon(lon, lat, ring):
    inside = False
    for index in range(len(ring) - 1):
        x1, y1 = ring[index]
        x2, y2 = ring[index + 1]
        cross = (lon - x1) * (y2 - y1) - (lat - y1) * (x2 - x1)
        if (abs(cross) <= 1e-10 and min(x1, x2) - 1e-10 <= lon <= max(x1, x2) + 1e-10
                and min(y1, y2) - 1e-10 <= lat <= max(y1, y2) + 1e-10):
            return True
        if (y1 > lat) != (y2 > lat):
            intersection = (x2 - x1) * (lat - y1) / (y2 - y1) + x1
            if lon < intersection:
                inside = not inside
    return inside


def normalize_snapshot(snapshot):
    """Return the bounded telemetry contract used by history and WebSocket clients."""
    if (not isinstance(snapshot, dict) or snapshot.get("t") != "snap"
            or not isinstance(snapshot.get("drones"), list)):
        raise ValueError("invalid snapshot")
    if len(snapshot["drones"]) > 1024:
        raise ValueError("too many drones")

    normalized = {
        "t": "snap",
        "stationId": _clean_text(snapshot.get("stationId"), 128) or "local",
        "stationName": _clean_text(snapshot.get("stationName"), 256),
        "sourceType": _clean_text(snapshot.get("sourceType"), 48) or DEFAULT_INGEST_SOURCE_TYPE,
        "sourceTransport": _clean_text(snapshot.get("sourceTransport"), 48),
        "hardwareConnected": snapshot.get("hardwareConnected") is True,
        "ch": _integer(snapshot.get("ch")),
        "bat": _integer(snapshot.get("bat")),
        # A software frame loopback is always test telemetry.  Keep this bit
        # derived server-side so a client cannot hide its provenance by omitting
        # the ordinary simulated flag.
        "labLoopback": snapshot.get("labLoopback") is True,
        "simulated": snapshot.get("simulated") is True or snapshot.get("labLoopback") is True,
        "drones": [],
    }
    if normalized["ch"] is None or not 0 <= normalized["ch"] <= 196:
        normalized["ch"] = None
    if normalized["bat"] is None or not -1 <= normalized["bat"] <= 100:
        normalized["bat"] = -1
    try:
        captured_at = _parse_time(snapshot.get("capturedAt"))
    except ValueError:
        captured_at = None
    if captured_at is not None:
        normalized["capturedAt"] = captured_at
    seed = _integer(snapshot.get("seed"))
    if seed is not None:
        normalized["seed"] = seed

    for raw in snapshot["drones"]:
        if not isinstance(raw, dict):
            continue
        mac = _clean_text(raw.get("mac"), 17)
        mac = mac.upper() if mac else ""
        if not MAC_PATTERN.fullmatch(mac):
            continue
        drone = {"mac": mac}
        for source, target, limit in (
            ("model", "model", 96),
            ("id", "id", 128),
            ("protocol", "protocol", 64),
            ("motion", "motion", 32),
            ("operatorId", "operatorId", 128),
            ("desc", "desc", 128),
        ):
            value = _clean_text(raw.get(source), limit)
            if value is not None:
                drone[target] = value
        for key, minimum, maximum in (
            ("lat", -90, 90), ("lon", -180, 180),
            ("olat", -90, 90), ("olon", -180, 180),
            ("alt", -1000, 10000), ("relAlt", -1000, 10000),
            ("spd", 0, 500), ("vspd", -100, 100),
        ):
            value = _bounded_number(raw.get(key), minimum, maximum)
            if value is not None:
                drone[key] = value
        rssi = _integer(raw.get("rssi"))
        if rssi is not None and -127 <= rssi <= 20:
            drone["rssi"] = rssi
        proto = _integer(raw.get("proto"))
        drone["proto"] = proto if proto in {0, 1, 2} else 0
        heading = _number(raw.get("heading"))
        if heading is not None:
            drone["heading"] = heading % 360
        status = _integer(raw.get("status"))
        if status is not None and 0 <= status <= 4:
            drone["status"] = status
        battery = _integer(raw.get("battery"))
        if battery is not None and 0 <= battery <= 100:
            drone["battery"] = battery
        best_rssi = _integer(raw.get("bestRssi"))
        if best_rssi is not None and -127 <= best_rssi <= 20:
            drone["bestRssi"] = best_rssi
        packets = _integer(raw.get("packets"))
        if packets is not None and 0 <= packets <= 4294967295:
            drone["packets"] = packets
        if normalized["labLoopback"] or raw.get("labLoopback") is True:
            drone["labLoopback"] = True
            drone["simulated"] = True
        elif normalized["simulated"] or raw.get("simulated") is True:
            drone["simulated"] = True
        normalized["drones"].append(drone)
    normalized["n"] = len(normalized["drones"])
    return normalized


def _parse_time(value):
    """Accept Unix seconds/ms or an ISO-8601 date and return Unix milliseconds."""
    if value is None or value == "":
        return None
    number = _number(value)
    if number is not None:
        return int(number * 1000 if abs(number) < 100_000_000_000 else number)
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        return int(parsed.timestamp() * 1000)
    except ValueError as exc:
        raise ValueError(f"无效时间: {value}") from exc


def _parse_boolean(value, label):
    """Parse an explicit boolean without accepting truthy/falsy strings by accident."""
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        token = value.strip().lower()
        if token in {"1", "true", "yes"}:
            return True
        if token in {"0", "false", "no"}:
            return False
    raise ValueError(f"{label} 必须是布尔值")


def _airspace_property(properties, *names):
    if not isinstance(properties, dict):
        return None
    for name in names:
        if name in properties:
            return properties[name]
    lower = {str(key).lower(): value for key, value in properties.items()}
    for name in names:
        if name.lower() in lower:
            return lower[name.lower()]
    return None


def _normalize_airspace_class(value, default="unknown"):
    default = str(default or "unknown").strip().lower()
    if default not in AIRSPACE_CLASSES:
        raise ValueError("defaultClass 必须是 suitable、warning、controlled、prohibited 或 unknown")
    if value in (None, ""):
        return default
    token = str(value).strip().lower()
    compact = re.sub(r"[\s_-]+", "", token)
    aliases = {
        "suitable": "suitable", "flyable": "suitable", "allowed": "suitable",
        "allow": "suitable", "green": "suitable", "适飞": "suitable",
        "适飞区": "suitable", "适飞空域": "suitable",
        "warning": "warning", "warn": "warning", "caution": "warning",
        "alert": "warning", "警告": "warning", "警告区": "warning",
        "预警": "warning", "预警区": "warning",
        "controlled": "controlled", "control": "controlled", "restricted": "controlled",
        "restriction": "controlled", "管制": "controlled", "管制区": "controlled",
        "限制": "controlled", "限制区": "controlled",
        "prohibited": "prohibited", "prohibit": "prohibited", "forbidden": "prohibited",
        "nofly": "prohibited", "禁飞": "prohibited", "禁飞区": "prohibited",
        "禁止": "prohibited", "禁止区": "prohibited",
        "unknown": "unknown", "未知": "unknown",
    }
    return aliases.get(compact, "unknown")


def _normalize_altitude_reference(value):
    if value in (None, ""):
        return "unknown"
    token = re.sub(r"[\s_-]+", "", str(value).strip().lower())
    aliases = {
        "unknown": "unknown", "未知": "unknown",
        "wgs84": "wgs84", "ellipsoid": "wgs84", "ellipsoidal": "wgs84", "椭球": "wgs84",
        "amsl": "amsl", "msl": "amsl", "海拔": "amsl", "平均海平面": "amsl",
        "agl": "agl", "真高": "agl", "距地": "agl", "相对地面": "agl",
    }
    normalized = aliases.get(token)
    if normalized not in AIRSPACE_ALTITUDE_REFERENCES:
        raise ValueError(f"未知高度基准: {value}")
    return normalized


def _normalize_airspace_ring(raw_ring, label):
    if not isinstance(raw_ring, list):
        raise ValueError(f"{label} 必须是坐标数组")
    if len(raw_ring) < 3 or len(raw_ring) > MAX_AIRSPACE_VERTICES_PER_FEATURE + 1:
        raise ValueError(
            f"{label} 顶点数必须在 3 到 {MAX_AIRSPACE_VERTICES_PER_FEATURE} 之间"
        )
    ring = [_coordinate(point, f"{label}[{index}]") for index, point in enumerate(raw_ring)]
    if ring[0] == ring[-1]:
        ring = ring[:-1]
    if len(ring) < 3 or len({(point[0], point[1]) for point in ring}) < 3:
        raise ValueError(f"{label} 至少需要三个不同顶点")
    area2 = sum(
        ring[index][0] * ring[(index + 1) % len(ring)][1]
        - ring[(index + 1) % len(ring)][0] * ring[index][1]
        for index in range(len(ring))
    )
    if abs(area2) < 1e-12:
        raise ValueError(f"{label} 面积不能为零")
    count = len(ring)
    ring.append(list(ring[0]))
    return ring, count


def _normalize_airspace_geometry(geometry, label):
    if not isinstance(geometry, dict):
        raise ValueError(f"{label}.geometry 必须是 GeoJSON 对象")
    geometry_type = str(geometry.get("type") or "").strip().lower()
    coordinates = geometry.get("coordinates")
    total_vertices = 0
    all_points = []

    def normalize_polygon(raw_polygon, polygon_label):
        nonlocal total_vertices
        if not isinstance(raw_polygon, list) or not raw_polygon:
            raise ValueError(f"{polygon_label} 至少需要一个外环")
        normalized_rings = []
        for ring_index, raw_ring in enumerate(raw_polygon):
            ring, count = _normalize_airspace_ring(
                raw_ring, f"{polygon_label}[{ring_index}]",
            )
            total_vertices += count
            if total_vertices > MAX_AIRSPACE_VERTICES_PER_FEATURE:
                raise ValueError(
                    f"{label} 顶点总数不能超过 {MAX_AIRSPACE_VERTICES_PER_FEATURE}"
                )
            normalized_rings.append(ring)
            all_points.extend(ring[:-1])
        return normalized_rings

    if geometry_type == "polygon":
        canonical_type = "Polygon"
        canonical_coordinates = normalize_polygon(coordinates, f"{label}.coordinates")
    elif geometry_type == "multipolygon":
        if not isinstance(coordinates, list) or not coordinates:
            raise ValueError(f"{label}.coordinates 至少需要一个多边形")
        canonical_type = "MultiPolygon"
        canonical_coordinates = [
            normalize_polygon(raw_polygon, f"{label}.coordinates[{index}]")
            for index, raw_polygon in enumerate(coordinates)
        ]
    else:
        raise ValueError(f"{label}.geometry 只支持 Polygon 或 MultiPolygon")

    if not all_points:
        raise ValueError(f"{label}.geometry 不包含有效坐标")
    min_lon = min(point[0] for point in all_points)
    max_lon = max(point[0] for point in all_points)
    min_lat = min(point[1] for point in all_points)
    max_lat = max(point[1] for point in all_points)
    canonical = {"type": canonical_type, "coordinates": canonical_coordinates}
    canonical_json = json.dumps(
        canonical, ensure_ascii=False, separators=(",", ":"), sort_keys=True,
    )
    return canonical, hashlib.sha256(canonical_json.encode("utf-8")).hexdigest(), (
        min_lon, min_lat, max_lon, max_lat,
    ), total_vertices


def _extract_flyable_airspace(value, depth=0):
    if depth > 5:
        return False, None
    if isinstance(value, dict):
        for key, nested in value.items():
            if str(key).lower() == "flyableairspace":
                return True, nested
        for key in ("data", "result", "body", "payload"):
            if key in value:
                found, nested = _extract_flyable_airspace(value[key], depth + 1)
                if found:
                    return found, nested
    elif isinstance(value, list):
        for nested in value[:MAX_AIRSPACE_FEATURES + 1]:
            found, parsed = _extract_flyable_airspace(nested, depth + 1)
            if found:
                return found, parsed
    elif isinstance(value, str) and value.lstrip().startswith(("{", "[")):
        try:
            return _extract_flyable_airspace(json.loads(value), depth + 1)
        except json.JSONDecodeError:
            pass
    return False, None


def _uom_flyable_features(raw_value):
    if not isinstance(raw_value, str) or not raw_value.strip():
        raise ValueError("flyableAirspace 必须是非空字符串")
    grids = [item.strip() for item in re.split(r"[;；]+", raw_value) if item.strip()]
    if not grids:
        raise ValueError("flyableAirspace 不包含空域网格")
    if len(grids) > MAX_AIRSPACE_FEATURES:
        raise ValueError(f"空域要素不能超过 {MAX_AIRSPACE_FEATURES} 个")
    coordinate_pattern = re.compile(
        r"([-+]?(?:\d+(?:\.\d*)?|\.\d+))\s*[,，]\s*"
        r"([-+]?(?:\d+(?:\.\d*)?|\.\d+))"
    )
    features = []
    for index, grid in enumerate(grids):
        matches = coordinate_pattern.findall(grid)
        if len(matches) < 3:
            raise ValueError(f"flyableAirspace 第 {index + 1} 个网格少于三个坐标")
        residual = coordinate_pattern.sub("", grid)
        if re.sub(r"[\s|¦]+", "", residual):
            raise ValueError(f"flyableAirspace 第 {index + 1} 个网格格式无效")
        ring = [_coordinate([lon, lat], f"flyableAirspace[{index}]") for lon, lat in matches]
        features.append({
            "type": "Feature",
            "id": f"uom-flyable-{index + 1}",
            "properties": {
                "name": f"UOM 适飞空域 {index + 1}",
                "zoneClass": "suitable",
                "sourceClass": "flyableAirspace",
            },
            "geometry": {"type": "Polygon", "coordinates": [ring]},
        })
    return features


def _bounded_airspace_metadata(properties):
    if not isinstance(properties, dict):
        return {}
    metadata = {}
    for key, value in list(properties.items())[:64]:
        clean_key = _clean_text(str(key), 64)
        if not clean_key:
            continue
        if isinstance(value, str):
            metadata[clean_key] = _clean_text(value, 512)
        elif value is None or isinstance(value, (bool, int, float)):
            metadata[clean_key] = value if not isinstance(value, float) or math.isfinite(value) else None
        elif isinstance(value, list) and len(value) <= 32:
            bounded_items = []
            for item in value:
                if isinstance(item, str):
                    bounded_items.append(_clean_text(item, 512))
                elif item is None or isinstance(item, (bool, int, float)):
                    bounded_items.append(item if not isinstance(item, float) or math.isfinite(item) else None)
            if len(bounded_items) == len(value):
                metadata[clean_key] = bounded_items
    return metadata


def _normalize_airspace_feature(feature, index, default_class):
    if not isinstance(feature, dict) or str(feature.get("type") or "").lower() != "feature":
        raise ValueError(f"features[{index}] 必须是 GeoJSON Feature")
    properties = feature.get("properties") or {}
    if not isinstance(properties, dict):
        raise ValueError(f"features[{index}].properties 必须是对象")
    geometry, geometry_sha, bbox, vertex_count = _normalize_airspace_geometry(
        feature.get("geometry"), f"features[{index}]",
    )
    raw_class = _airspace_property(
        properties, "zoneClass", "zone_class", "airspaceClass", "airspace_class",
        "airspaceType", "airspace_type", "category", "class",
    )
    zone_class = _normalize_airspace_class(raw_class, default_class)
    source_class = _clean_text(
        str(_airspace_property(properties, "sourceClass", "source_class") or raw_class or ""), 128,
    )
    name = _clean_text(
        _airspace_property(properties, "name", "zoneName", "zone_name", "airspaceName",
                           "airspace_name", "areaName", "area_name", "title"),
        256,
    ) or f"空域 {index + 1}"
    raw_external_id = feature.get("id")
    if raw_external_id in (None, ""):
        raw_external_id = _airspace_property(
            properties, "externalId", "external_id", "zoneId", "zone_id", "areaId", "area_id", "id",
        )
    external_id = _clean_text(str(raw_external_id), 256) if raw_external_id not in (None, "") else None

    def optional_altitude(names, label):
        raw = _airspace_property(properties, *names)
        if raw in (None, ""):
            return None
        value = _bounded_number(raw, -1000, 100000)
        if value is None:
            raise ValueError(f"features[{index}].{label} 必须在 -1000 到 100000 米之间")
        return value

    min_altitude = optional_altitude(
        ("minAltitude", "min_altitude", "lowerAltitude", "lower_altitude"), "minAltitude",
    )
    max_altitude = optional_altitude(
        ("maxAltitude", "max_altitude", "upperAltitude", "upper_altitude", "height"), "maxAltitude",
    )
    if min_altitude is not None and max_altitude is not None and min_altitude > max_altitude:
        raise ValueError(f"features[{index}] 最低高度不能大于最高高度")
    altitude_reference = _normalize_altitude_reference(_airspace_property(
        properties, "altitudeReference", "altitude_reference", "heightReference", "height_reference",
    ))
    valid_from = _parse_time(_airspace_property(
        properties, "validFrom", "valid_from", "startTime", "start_time", "effectiveFrom",
    ))
    valid_to = _parse_time(_airspace_property(
        properties, "validTo", "valid_to", "endTime", "end_time", "effectiveTo",
    ))
    if valid_from is not None and valid_to is not None and valid_from > valid_to:
        raise ValueError(f"features[{index}] 生效时间不能晚于失效时间")
    return {
        "external_id": external_id,
        "name": name,
        "zone_class": zone_class,
        "source_class": source_class,
        "geometry_type": geometry["type"].lower(),
        "geometry_json": json.dumps(
            geometry, ensure_ascii=False, separators=(",", ":"), sort_keys=True,
        ),
        "geometry_sha256": geometry_sha,
        "min_lon": bbox[0], "min_lat": bbox[1], "max_lon": bbox[2], "max_lat": bbox[3],
        "min_altitude": min_altitude, "max_altitude": max_altitude,
        "altitude_reference": altitude_reference,
        "valid_from": valid_from, "valid_to": valid_to,
        "metadata_json": json.dumps(
            _bounded_airspace_metadata(properties), ensure_ascii=False,
            separators=(",", ":"), sort_keys=True,
        ),
        "vertex_count": vertex_count,
    }


def normalize_airspace_import_payload(payload):
    """Normalize GeoJSON or the UOM flyableAirspace exchange field to WGS-84 zones."""
    if not isinstance(payload, dict):
        raise ValueError("空域导入必须是 JSON 对象")
    data = payload if str(payload.get("type") or "").lower() == "featurecollection" else (
        payload.get("data", payload.get("payload", payload))
    )
    found_uom, flyable_airspace = _extract_flyable_airspace(data)
    source_raw = payload.get("source") or {}
    if isinstance(source_raw, str):
        source_raw = {"slug": source_raw}
    if not isinstance(source_raw, dict):
        raise ValueError("source 必须是对象或来源标识字符串")

    declared_crs = source_raw.get("coordinateSystem", payload.get("coordinateSystem"))
    if declared_crs in (None, "") and isinstance(data, dict) and data.get("crs") is not None:
        declared_crs = data.get("crs")
        if isinstance(declared_crs, dict):
            declared_crs = (declared_crs.get("properties") or {}).get("name")
    if declared_crs not in (None, ""):
        compact_crs = re.sub(r"[^A-Z0-9]", "", str(declared_crs).upper())
        if compact_crs not in {
                "WGS84", "EPSG4326", "CRS84", "OGCCRS84",
                "URNOGCDEFCRSOGC13CRS84", "URNOGCDEFCRSEPSG4326"}:
            raise ValueError("空域坐标系必须是 WGS-84/EPSG:4326，不能直接导入 GCJ-02 或投影坐标")

    raw_default_class = payload.get("defaultClass")
    if raw_default_class in (None, ""):
        default_class = "suitable" if found_uom else "unknown"
    else:
        default_class = str(raw_default_class).strip().lower()
        if default_class not in AIRSPACE_CLASSES:
            raise ValueError(
                "defaultClass 必须是 suitable、warning、controlled、prohibited 或 unknown"
            )
    if isinstance(data, dict) and str(data.get("type") or "").lower() == "featurecollection":
        features = data.get("features")
        import_format = "geojson"
    elif found_uom:
        features = _uom_flyable_features(flyable_airspace)
        import_format = "uom-flyableAirspace"
    else:
        raise ValueError("data 必须是 GeoJSON FeatureCollection 或包含 flyableAirspace")
    if not isinstance(features, list):
        raise ValueError("FeatureCollection.features 必须是数组")
    if not features:
        raise ValueError("空域导入至少需要一个要素")
    if len(features) > MAX_AIRSPACE_FEATURES:
        raise ValueError(f"空域要素不能超过 {MAX_AIRSPACE_FEATURES} 个")

    zones = []
    total_vertices = 0
    external_ids = set()
    for index, feature in enumerate(features):
        zone = _normalize_airspace_feature(feature, index, default_class)
        total_vertices += zone.pop("vertex_count")
        if total_vertices > MAX_AIRSPACE_TOTAL_VERTICES:
            raise ValueError(f"导入顶点总数不能超过 {MAX_AIRSPACE_TOTAL_VERTICES}")
        if not zone["external_id"]:
            zone["external_id"] = f"feature-{index + 1}-{zone['geometry_sha256'][:12]}"
        if zone["external_id"] in external_ids:
            raise ValueError(f"重复的空域 externalId: {zone['external_id']}")
        external_ids.add(zone["external_id"])
        zones.append(zone)

    slug = _clean_text(
        source_raw.get("slug") or source_raw.get("id") or ("uom" if found_uom else "manual"), 64,
    )
    slug = slug.lower() if slug else ""
    if not AIRSPACE_SOURCE_SLUG_PATTERN.fullmatch(slug):
        raise ValueError("source.slug 只能包含小写字母、数字、点、下划线和连字符")
    source_name = _clean_text(
        source_raw.get("name"), 128,
    ) or ("UOM 空域导入" if found_uom else "手工空域导入")
    provider = _clean_text(source_raw.get("provider"), 128) or ("UOM" if found_uom else "manual")
    authority = _clean_text(source_raw.get("authority"), 128)
    source_url = _clean_text(source_raw.get("url") or source_raw.get("sourceUrl"), 2048)
    raw_dist_code = source_raw.get("distCode", source_raw.get("dist_code"))
    if raw_dist_code in (None, ""):
        dist_code = None
    elif isinstance(raw_dist_code, bool):
        raise ValueError("source.distCode 必须是 6 位行政区划代码")
    else:
        dist_code = str(raw_dist_code).strip()
        if not re.fullmatch(r"\d{6}", dist_code):
            raise ValueError("source.distCode 必须是 6 位行政区划代码")
    raw_coverage_scope = source_raw.get(
        "coverageScope", source_raw.get("coverage_scope")
    )
    coverage_scope = (
        "city" if raw_coverage_scope in (None, "") and dist_code
        else "unknown" if raw_coverage_scope in (None, "")
        else str(raw_coverage_scope).strip().lower()
    )
    if coverage_scope not in AIRSPACE_COVERAGE_SCOPES:
        raise ValueError("source.coverageScope 必须是 city、national 或 unknown")
    if coverage_scope == "city" and dist_code is None:
        raise ValueError("city 覆盖来源必须提供 source.distCode")
    enabled = source_raw.get("enabled", True)
    if not isinstance(enabled, bool):
        raise ValueError("source.enabled 必须是布尔值")
    reference_only = source_raw.get(
        "referenceOnly", source_raw.get("reference_only", False)
    )
    if not isinstance(reference_only, bool):
        raise ValueError("source.referenceOnly 必须是布尔值")

    def dataset_value(name):
        return source_raw.get(name) if name in source_raw else payload.get(name)

    published_at = _parse_time(dataset_value("publishedAt"))
    valid_from = _parse_time(dataset_value("validFrom"))
    valid_to = _parse_time(dataset_value("validTo"))
    if valid_from is not None and valid_to is not None and valid_from > valid_to:
        raise ValueError("数据集生效时间不能晚于失效时间")
    canonical = json.dumps({
        "format": import_format,
        "validFrom": valid_from,
        "validTo": valid_to,
        "zones": zones,
    }, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
    sha256 = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
    source_version = _clean_text(dataset_value("sourceVersion"), 128) or sha256[:12]
    provenance = source_raw.get("provenance")
    if not isinstance(provenance, dict):
        provenance = {}
    dataset_metadata = {
        "format": import_format,
        "coordinateSystem": "WGS84",
        "totalVertices": total_vertices,
    }
    bounded_provenance = _bounded_airspace_metadata(provenance)
    if bounded_provenance:
        dataset_metadata["provenance"] = bounded_provenance
    return {
        "source": {
            "slug": slug, "name": source_name, "provider": provider, "authority": authority,
            "source_url": source_url, "sync_mode": "manual_import",
            "dist_code": dist_code, "coverage_scope": coverage_scope,
            "enabled": 1 if enabled else 0,
            "reference_only": 1 if reference_only else 0,
        },
        "dataset": {
            "source_version": source_version, "published_at": published_at,
            "valid_from": valid_from, "valid_to": valid_to, "sha256": sha256,
            "feature_count": len(zones),
            "metadata_json": json.dumps(
                dataset_metadata,
                ensure_ascii=False, separators=(",", ":"), sort_keys=True,
            ),
        },
        "zones": zones,
    }


def _parse_airspace_bbox(value):
    if value in (None, ""):
        raise ValueError("bbox 参数必填，格式为 minLon,minLat,maxLon,maxLat")
    parts = str(value).split(",")
    if len(parts) != 4:
        raise ValueError("bbox 必须包含四个逗号分隔坐标")
    numbers = [_number(part.strip()) for part in parts]
    if any(number is None for number in numbers):
        raise ValueError("bbox 坐标无效")
    min_lon, min_lat, max_lon, max_lat = numbers
    if not (-180 <= min_lon < max_lon <= 180 and -90 <= min_lat < max_lat <= 90):
        raise ValueError("bbox 坐标范围或顺序无效")
    return [min_lon, min_lat, max_lon, max_lat]


def _parse_airspace_classes(values):
    requested = []
    for value in values or []:
        requested.extend(item.strip().lower() for item in str(value).split(",") if item.strip())
    if not requested:
        return sorted(AIRSPACE_CLASSES)
    invalid = sorted(set(requested) - AIRSPACE_CLASSES)
    if invalid:
        raise ValueError("classes 包含无效分类: " + ",".join(invalid))
    return sorted(set(requested))


def airspace_sync_configuration():
    required = {
        "RID_UOM_AIRSPACE_ENDPOINT": os.environ.get("RID_UOM_AIRSPACE_ENDPOINT", "").strip(),
        "RID_UOM_AIRSPACE_CLIENT_ID": os.environ.get("RID_UOM_AIRSPACE_CLIENT_ID", "").strip(),
        "RID_UOM_AIRSPACE_CREDENTIAL": os.environ.get("RID_UOM_AIRSPACE_CREDENTIAL", "").strip(),
    }
    missing = [name for name, value in required.items() if not value]
    if missing:
        return {
            "configured": False,
            "supported": False,
            "status": "unconfigured",
            "message": "未配置正式 UOM 空域接口和凭据",
            "missing": missing,
        }
    return {
        "configured": True,
        "supported": False,
        "status": "unsupported",
        "message": "已检测到 UOM 接口配置，但正式签名/国密适配器尚未实现，未发起网络请求",
        "missing": [],
    }


def _b64encode(value):
    return base64.urlsafe_b64encode(value).rstrip(b"=").decode("ascii")


def _b64decode(value):
    raw = value.encode("ascii")
    return base64.urlsafe_b64decode(raw + b"=" * (-len(raw) % 4))


def create_session(username, now=None):
    now = int(time.time()) if now is None else int(now)
    payload = json.dumps(
        {"u": username, "iat": now, "exp": now + SESSION_TTL_SECONDS,
         "n": secrets.token_urlsafe(8)},
        separators=(",", ":"),
    ).encode("utf-8")
    encoded = _b64encode(payload)
    signature = hmac.new(SESSION_SECRET, encoded.encode("ascii"), hashlib.sha256).digest()
    return encoded + "." + _b64encode(signature)


def verify_session(token, now=None):
    if not SESSION_SECRET or not token:
        return None
    try:
        encoded, supplied = token.split(".", 1)
        expected = hmac.new(SESSION_SECRET, encoded.encode("ascii"), hashlib.sha256).digest()
        if not hmac.compare_digest(_b64decode(supplied), expected):
            return None
        payload = json.loads(_b64decode(encoded).decode("utf-8"))
        now = int(time.time()) if now is None else int(now)
        if payload.get("u") != ADMIN_USER or int(payload.get("exp", 0)) < now:
            return None
        return payload
    except Exception:
        return None


def session_from_cookie(cookie_header):
    if not cookie_header:
        return None
    try:
        cookie = SimpleCookie()
        cookie.load(cookie_header)
        morsel = cookie.get("rid_session")
        return verify_session(morsel.value if morsel else "")
    except (KeyError, ValueError):
        return None


def _login_limited(client_ip, now=None):
    now = time.time() if now is None else float(now)
    with AUTH_LOCK:
        recent = [stamp for stamp in LOGIN_FAILURES.get(client_ip, []) if now - stamp < 300]
        LOGIN_FAILURES[client_ip] = recent
        return len(recent) >= 5


def _record_login_failure(client_ip, now=None):
    now = time.time() if now is None else float(now)
    with AUTH_LOCK:
        recent = [stamp for stamp in LOGIN_FAILURES.get(client_ip, []) if now - stamp < 300]
        recent.append(now)
        LOGIN_FAILURES[client_ip] = recent


def _clear_login_failures(client_ip):
    with AUTH_LOCK:
        LOGIN_FAILURES.pop(client_ip, None)


class HistoryStore:
    """Thread-safe flight sessionizer backed by SQLite."""

    def __init__(self, path, flight_gap_seconds=15.0, min_point_meters=2.0,
                 max_point_interval_seconds=5.0, retention_days=30,
                 seed_builtin_airspace=False, seed_packaged_airspace=False):
        self.path = os.path.abspath(path)
        self.flight_gap_ms = max(1000, int(flight_gap_seconds * 1000))
        self.min_point_meters = max(0.0, float(min_point_meters))
        self.max_point_interval_ms = max(1000, int(max_point_interval_seconds * 1000))
        self.retention_days = max(0, int(retention_days))
        self._lock = threading.RLock()
        self._active = {}  # (station_id, mac) -> {id, last_seen, last_point}
        self._next_prune_at = 0
        os.makedirs(os.path.dirname(self.path), exist_ok=True)
        self._init_db()
        self.prune()
        if seed_builtin_airspace:
            self.ensure_builtin_airspace()
        if seed_packaged_airspace:
            self.ensure_packaged_uom_airspace()

    def ensure_builtin_airspace(self):
        """Seed the checked-in Beijing policy reference only when no snapshot exists."""
        payload = builtin_beijing_airspace_payload()
        slug = payload["source"]["slug"]
        with self._lock, self._db() as conn:
            row = conn.execute(
                """SELECT s.id, EXISTS(
                       SELECT 1 FROM airspace_datasets d
                       WHERE d.source_id=s.id AND d.status='active'
                   ) AS has_active
                   FROM airspace_sources s WHERE s.slug=?""",
                (slug,),
            ).fetchone()
        if row is not None and row["has_active"]:
            return {"seeded": False, "reason": "already_present", "slug": slug}
        result = self.import_airspace(payload)
        return {"seeded": True, "slug": slug, "result": result}

    def ensure_packaged_uom_airspace(self):
        """Import a verified packaged UOM-derived snapshot when its version changes."""
        payload = packaged_uom_derived_airspace_payload()
        slug = "uom-derived-national"
        if payload is None:
            return {"seeded": False, "reason": "package_missing", "slug": slug}
        source_version = payload["source"]["sourceVersion"]
        with self._lock, self._db() as conn:
            row = conn.execute(
                """
                SELECT d.source_version
                FROM airspace_sources s
                JOIN airspace_datasets d ON d.source_id=s.id AND d.status='active'
                WHERE s.slug=?
                ORDER BY d.id DESC LIMIT 1
                """,
                (slug,),
            ).fetchone()
        if row is not None and row["source_version"] == source_version:
            return {
                "seeded": False, "reason": "version_present", "slug": slug,
                "sourceVersion": source_version,
            }
        result = self.import_airspace(payload)
        return {
            "seeded": True, "slug": slug, "sourceVersion": source_version,
            "result": result,
        }

    def _connect(self):
        conn = sqlite3.connect(self.path, timeout=10)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA busy_timeout=5000")
        return conn

    @contextmanager
    def _db(self):
        conn = self._connect()
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def _init_db(self):
        with self._db() as conn:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS flights (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    station_id TEXT NOT NULL DEFAULT 'local',
                    station_name TEXT,
                    mac TEXT NOT NULL,
                    model TEXT,
                    uas_id TEXT,
                    proto INTEGER,
                    started_at INTEGER NOT NULL,
                    ended_at INTEGER,
                    last_seen_at INTEGER NOT NULL,
                    max_altitude REAL,
                    max_speed REAL,
                    min_rssi INTEGER,
                    max_rssi INTEGER,
                    point_count INTEGER NOT NULL DEFAULT 0,
                    active INTEGER NOT NULL DEFAULT 1 CHECK(active IN (0, 1))
                );
                CREATE TABLE IF NOT EXISTS track_points (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    flight_id INTEGER NOT NULL REFERENCES flights(id) ON DELETE CASCADE,
                    station_id TEXT NOT NULL DEFAULT 'local',
                    recorded_at INTEGER NOT NULL,
                    lat REAL NOT NULL,
                    lon REAL NOT NULL,
                    altitude REAL,
                    speed REAL,
                    rssi INTEGER,
                    operator_lat REAL,
                    operator_lon REAL
                );
                CREATE TABLE IF NOT EXISTS geofences (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL,
                    shape_type TEXT NOT NULL CHECK(shape_type IN ('circle', 'polygon')),
                    severity TEXT NOT NULL CHECK(severity IN ('low', 'medium', 'high', 'critical')),
                    enabled INTEGER NOT NULL DEFAULT 1 CHECK(enabled IN (0, 1)),
                    min_altitude REAL,
                    max_altitude REAL,
                    geometry_json TEXT NOT NULL,
                    created_at INTEGER NOT NULL,
                    updated_at INTEGER NOT NULL,
                    CHECK(min_altitude IS NULL OR max_altitude IS NULL OR min_altitude <= max_altitude)
                );
                CREATE TABLE IF NOT EXISTS incidents (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    flight_id INTEGER UNIQUE REFERENCES flights(id) ON DELETE SET NULL,
                    station_id TEXT NOT NULL,
                    mac TEXT NOT NULL,
                    model TEXT,
                    uas_id TEXT,
                    status TEXT NOT NULL DEFAULT 'open'
                        CHECK(status IN ('open', 'acknowledged', 'resolved', 'dismissed')),
                    risk_score INTEGER NOT NULL,
                    risk_level TEXT NOT NULL,
                    risk_reasons_json TEXT NOT NULL,
                    geofence_ids_json TEXT NOT NULL,
                    evidence_json TEXT NOT NULL,
                    first_seen_at INTEGER NOT NULL,
                    last_seen_at INTEGER NOT NULL,
                    created_at INTEGER NOT NULL,
                    updated_at INTEGER NOT NULL
                );
                CREATE TABLE IF NOT EXISTS incident_actions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    incident_id INTEGER NOT NULL REFERENCES incidents(id) ON DELETE CASCADE,
                    action TEXT NOT NULL,
                    from_status TEXT NOT NULL,
                    to_status TEXT NOT NULL,
                    note TEXT,
                    actor TEXT,
                    created_at INTEGER NOT NULL
                );
                CREATE TABLE IF NOT EXISTS airspace_sources (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    slug TEXT NOT NULL UNIQUE,
                    name TEXT NOT NULL,
                    provider TEXT,
                    authority TEXT,
                    source_url TEXT,
                    sync_mode TEXT NOT NULL DEFAULT 'manual_import'
                        CHECK(sync_mode IN ('manual_import', 'authorized_sync', 'unconfigured')),
                    dist_code TEXT CHECK(
                        dist_code IS NULL OR (
                            length(dist_code)=6 AND
                            dist_code GLOB '[0-9][0-9][0-9][0-9][0-9][0-9]'
                        )
                    ),
                    coverage_scope TEXT NOT NULL DEFAULT 'unknown'
                        CHECK(coverage_scope IN ('city', 'national', 'unknown')),
                    enabled INTEGER NOT NULL DEFAULT 1 CHECK(enabled IN (0, 1)),
                    reference_only INTEGER NOT NULL DEFAULT 0 CHECK(reference_only IN (0, 1)),
                    last_attempt_at INTEGER,
                    last_success_at INTEGER,
                    last_error TEXT,
                    created_at INTEGER NOT NULL,
                    updated_at INTEGER NOT NULL
                );
                CREATE TABLE IF NOT EXISTS airspace_datasets (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    source_id INTEGER NOT NULL REFERENCES airspace_sources(id) ON DELETE CASCADE,
                    source_version TEXT NOT NULL,
                    published_at INTEGER,
                    fetched_at INTEGER NOT NULL,
                    valid_from INTEGER,
                    valid_to INTEGER,
                    sha256 TEXT NOT NULL,
                    status TEXT NOT NULL
                        CHECK(status IN ('staging', 'active', 'superseded', 'failed')),
                    feature_count INTEGER NOT NULL DEFAULT 0,
                    metadata_json TEXT NOT NULL DEFAULT '{}',
                    created_at INTEGER NOT NULL,
                    UNIQUE(source_id, sha256),
                    CHECK(valid_from IS NULL OR valid_to IS NULL OR valid_from <= valid_to)
                );
                CREATE TABLE IF NOT EXISTS airspace_zones (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    dataset_id INTEGER NOT NULL REFERENCES airspace_datasets(id) ON DELETE CASCADE,
                    external_id TEXT NOT NULL,
                    name TEXT NOT NULL,
                    zone_class TEXT NOT NULL
                        CHECK(zone_class IN ('suitable', 'warning', 'controlled', 'prohibited', 'unknown')),
                    source_class TEXT,
                    geometry_type TEXT NOT NULL CHECK(geometry_type IN ('polygon', 'multipolygon')),
                    geometry_json TEXT NOT NULL,
                    geometry_sha256 TEXT NOT NULL,
                    min_lon REAL NOT NULL,
                    min_lat REAL NOT NULL,
                    max_lon REAL NOT NULL,
                    max_lat REAL NOT NULL,
                    min_altitude REAL,
                    max_altitude REAL,
                    altitude_reference TEXT NOT NULL DEFAULT 'unknown'
                        CHECK(altitude_reference IN ('unknown', 'wgs84', 'amsl', 'agl')),
                    valid_from INTEGER,
                    valid_to INTEGER,
                    metadata_json TEXT NOT NULL DEFAULT '{}',
                    created_at INTEGER NOT NULL,
                    UNIQUE(dataset_id, external_id),
                    CHECK(min_lon <= max_lon AND min_lat <= max_lat),
                    CHECK(min_altitude IS NULL OR max_altitude IS NULL OR min_altitude <= max_altitude),
                    CHECK(valid_from IS NULL OR valid_to IS NULL OR valid_from <= valid_to)
                );
                CREATE TABLE IF NOT EXISTS airspace_sync_runs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    source_id INTEGER NOT NULL REFERENCES airspace_sources(id) ON DELETE CASCADE,
                    dataset_id INTEGER REFERENCES airspace_datasets(id) ON DELETE SET NULL,
                    trigger_type TEXT NOT NULL CHECK(trigger_type IN ('manual_import', 'sync')),
                    status TEXT NOT NULL
                        CHECK(status IN ('running', 'succeeded', 'unchanged', 'failed',
                                         'unconfigured', 'unsupported')),
                    started_at INTEGER NOT NULL,
                    finished_at INTEGER,
                    feature_count INTEGER NOT NULL DEFAULT 0,
                    sha256 TEXT,
                    message TEXT,
                    created_at INTEGER NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_flights_started ON flights(started_at DESC);
                CREATE INDEX IF NOT EXISTS idx_flights_mac_started ON flights(mac, started_at DESC);
                CREATE INDEX IF NOT EXISTS idx_flights_active ON flights(active, last_seen_at DESC);
                CREATE INDEX IF NOT EXISTS idx_points_flight_time ON track_points(flight_id, recorded_at);
                CREATE INDEX IF NOT EXISTS idx_geofences_enabled ON geofences(enabled, updated_at DESC);
                CREATE INDEX IF NOT EXISTS idx_incidents_status_seen
                    ON incidents(status, last_seen_at DESC);
                CREATE INDEX IF NOT EXISTS idx_incidents_station_seen
                    ON incidents(station_id, last_seen_at DESC);
                CREATE INDEX IF NOT EXISTS idx_incident_actions_incident_time
                    ON incident_actions(incident_id, created_at);
                CREATE UNIQUE INDEX IF NOT EXISTS idx_airspace_one_active_dataset
                    ON airspace_datasets(source_id) WHERE status='active';
                CREATE INDEX IF NOT EXISTS idx_airspace_datasets_source_status
                    ON airspace_datasets(source_id, status, fetched_at DESC);
                CREATE INDEX IF NOT EXISTS idx_airspace_zones_dataset_class
                    ON airspace_zones(dataset_id, zone_class);
                CREATE INDEX IF NOT EXISTS idx_airspace_zones_bbox
                    ON airspace_zones(dataset_id, min_lon, max_lon, min_lat, max_lat);
                CREATE INDEX IF NOT EXISTS idx_airspace_sync_runs_source_time
                    ON airspace_sync_runs(source_id, started_at DESC);
                """
            )
            # Small forward migration for databases made by the pre-cloud build.
            flight_columns = {row[1] for row in conn.execute("PRAGMA table_info(flights)")}
            if "station_id" not in flight_columns:
                conn.execute("ALTER TABLE flights ADD COLUMN station_id TEXT NOT NULL DEFAULT 'local'")
            if "station_name" not in flight_columns:
                conn.execute("ALTER TABLE flights ADD COLUMN station_name TEXT")
            point_columns = {row[1] for row in conn.execute("PRAGMA table_info(track_points)")}
            if "station_id" not in point_columns:
                conn.execute("ALTER TABLE track_points ADD COLUMN station_id TEXT NOT NULL DEFAULT 'local'")
            airspace_source_columns = {
                row[1] for row in conn.execute("PRAGMA table_info(airspace_sources)")
            }
            if "sync_mode" not in airspace_source_columns:
                conn.execute(
                    "ALTER TABLE airspace_sources ADD COLUMN sync_mode TEXT NOT NULL "
                    "DEFAULT 'manual_import' CHECK(sync_mode IN "
                    "('manual_import','authorized_sync','unconfigured'))"
                )
            if "dist_code" not in airspace_source_columns:
                conn.execute(
                    "ALTER TABLE airspace_sources ADD COLUMN dist_code TEXT CHECK("
                    "dist_code IS NULL OR (length(dist_code)=6 AND "
                    "dist_code GLOB '[0-9][0-9][0-9][0-9][0-9][0-9]'))"
                )
            if "coverage_scope" not in airspace_source_columns:
                conn.execute(
                    "ALTER TABLE airspace_sources ADD COLUMN coverage_scope TEXT NOT NULL "
                    "DEFAULT 'unknown' CHECK(coverage_scope IN ('city','national','unknown'))"
                )
            if "reference_only" not in airspace_source_columns:
                conn.execute(
                    "ALTER TABLE airspace_sources ADD COLUMN reference_only INTEGER NOT NULL "
                    "DEFAULT 0 CHECK(reference_only IN (0,1))"
                )
            # The previous build persisted this synthetic administrative outline as a
            # normal manual import. Keep its audit record but prevent it from being
            # mistaken for an actual UOM vector after the migration.
            conn.execute(
                "UPDATE airspace_sources SET reference_only=1 "
                "WHERE slug='uom-beijing-110000'"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_airspace_sources_coverage "
                "ON airspace_sources(enabled, reference_only, coverage_scope, dist_code)"
            )
            try:
                conn.execute(
                    "CREATE VIRTUAL TABLE IF NOT EXISTS airspace_zone_rtree "
                    "USING rtree(id,min_lon,max_lon,min_lat,max_lat)"
                )
                conn.executescript(
                    """
                    CREATE TRIGGER IF NOT EXISTS airspace_zones_rtree_insert
                    AFTER INSERT ON airspace_zones BEGIN
                        INSERT OR REPLACE INTO airspace_zone_rtree(
                            id,min_lon,max_lon,min_lat,max_lat
                        ) VALUES (NEW.id,NEW.min_lon,NEW.max_lon,NEW.min_lat,NEW.max_lat);
                    END;
                    CREATE TRIGGER IF NOT EXISTS airspace_zones_rtree_update
                    AFTER UPDATE OF id,min_lon,max_lon,min_lat,max_lat ON airspace_zones BEGIN
                        DELETE FROM airspace_zone_rtree WHERE id=OLD.id;
                        INSERT OR REPLACE INTO airspace_zone_rtree(
                            id,min_lon,max_lon,min_lat,max_lat
                        ) VALUES (NEW.id,NEW.min_lon,NEW.max_lon,NEW.min_lat,NEW.max_lat);
                    END;
                    CREATE TRIGGER IF NOT EXISTS airspace_zones_rtree_delete
                    AFTER DELETE ON airspace_zones BEGIN
                        DELETE FROM airspace_zone_rtree WHERE id=OLD.id;
                    END;
                    """
                )
                conn.execute(
                    "DELETE FROM airspace_zone_rtree "
                    "WHERE id NOT IN (SELECT id FROM airspace_zones)"
                )
                conn.execute(
                    """
                    INSERT OR REPLACE INTO airspace_zone_rtree(
                        id,min_lon,max_lon,min_lat,max_lat
                    )
                    SELECT id,min_lon,max_lon,min_lat,max_lat FROM airspace_zones
                    """
                )
            except sqlite3.Error as exc:
                raise RuntimeError(
                    "SQLite RTree 初始化失败，无法启用全国空域空间索引: " + str(exc)
                ) from exc
            conn.execute("DROP INDEX IF EXISTS idx_one_active_flight_per_mac")
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_flights_station_started "
                "ON flights(station_id, started_at DESC)"
            )
            # Clear process-owned live state before enforcing the partial unique index.
            # A new captured sample within flight_gap may reopen the same row.
            conn.execute(
                "UPDATE flights SET active=0, ended_at=last_seen_at "
                "WHERE active=1"
            )
            conn.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS idx_one_active_flight_per_station_mac "
                "ON flights(station_id, mac) WHERE active=1"
            )

    @staticmethod
    def _finish(conn, flight_id, last_seen):
        conn.execute(
            "UPDATE flights SET active=0, ended_at=? WHERE id=? AND active=1",
            (last_seen, flight_id),
        )

    def _expire_locked(self, conn, now_ms):
        for key, state in list(self._active.items()):
            if now_ms - state["last_seen"] >= self.flight_gap_ms:
                self._finish(conn, state["id"], state["last_seen"])
                del self._active[key]

    @staticmethod
    def _distance_m(lat1, lon1, lat2, lon2):
        rad = math.pi / 180
        dlat = (lat2 - lat1) * rad
        dlon = (lon2 - lon1) * rad
        a = (math.sin(dlat / 2) ** 2
             + math.cos(lat1 * rad) * math.cos(lat2 * rad) * math.sin(dlon / 2) ** 2)
        return 12_742_000 * math.asin(min(1.0, math.sqrt(a)))

    @staticmethod
    def _row_to_geofence(row):
        geometry = json.loads(row["geometry_json"])
        return {
            "id": row["id"],
            "name": row["name"],
            "shapeType": row["shape_type"],
            "severity": row["severity"],
            "enabled": bool(row["enabled"]),
            "minAltitude": row["min_altitude"],
            "maxAltitude": row["max_altitude"],
            "geometry": geometry,
            "createdAt": row["created_at"],
            "updatedAt": row["updated_at"],
        }

    def list_geofences(self):
        with self._db() as conn:
            rows = conn.execute(
                "SELECT * FROM geofences ORDER BY enabled DESC, updated_at DESC, id DESC"
            ).fetchall()
        items = [self._row_to_geofence(row) for row in rows]
        return {"items": items, "total": len(items)}

    def create_geofence(self, payload):
        value = normalize_geofence_payload(payload)
        now_ms = int(time.time() * 1000)
        with self._lock, self._db() as conn:
            cursor = conn.execute(
                """
                INSERT INTO geofences(
                    name,shape_type,severity,enabled,min_altitude,max_altitude,
                    geometry_json,created_at,updated_at
                ) VALUES (?,?,?,?,?,?,?,?,?)
                """,
                (value["name"], value["shape_type"], value["severity"], value["enabled"],
                 value["min_altitude"], value["max_altitude"], value["geometry_json"],
                 now_ms, now_ms),
            )
            row = conn.execute("SELECT * FROM geofences WHERE id=?", (cursor.lastrowid,)).fetchone()
        return self._row_to_geofence(row)

    def update_geofence(self, geofence_id, payload):
        value = normalize_geofence_payload(payload)
        now_ms = int(time.time() * 1000)
        with self._lock, self._db() as conn:
            cursor = conn.execute(
                """
                UPDATE geofences SET name=?,shape_type=?,severity=?,enabled=?,min_altitude=?,
                    max_altitude=?,geometry_json=?,updated_at=? WHERE id=?
                """,
                (value["name"], value["shape_type"], value["severity"], value["enabled"],
                 value["min_altitude"], value["max_altitude"], value["geometry_json"],
                 now_ms, int(geofence_id)),
            )
            if cursor.rowcount == 0:
                return None
            row = conn.execute("SELECT * FROM geofences WHERE id=?", (geofence_id,)).fetchone()
        return self._row_to_geofence(row)

    def delete_geofence(self, geofence_id):
        with self._lock, self._db() as conn:
            cursor = conn.execute("DELETE FROM geofences WHERE id=?", (int(geofence_id),))
            return cursor.rowcount > 0

    @staticmethod
    def _row_to_airspace_source(row):
        return {
            "id": row["id"],
            "slug": row["slug"],
            "name": row["name"],
            "provider": row["provider"],
            "authority": row["authority"],
            "sourceUrl": row["source_url"],
            "syncMode": row["sync_mode"],
            "distCode": row["dist_code"],
            "coverageScope": row["coverage_scope"],
            "enabled": bool(row["enabled"]),
            "referenceOnly": bool(row["reference_only"]),
            "displayOnMap": bool(row["enabled"]) and not bool(row["reference_only"]),
            "lastAttemptAt": row["last_attempt_at"],
            "lastSuccessAt": row["last_success_at"],
            "lastError": row["last_error"],
            "createdAt": row["created_at"],
            "updatedAt": row["updated_at"],
        }

    @staticmethod
    def _row_to_airspace_dataset(row, prefix=""):
        def value(name):
            return row[prefix + name]
        return {
            "id": value("id"),
            "sourceVersion": value("source_version"),
            "publishedAt": value("published_at"),
            "fetchedAt": value("fetched_at"),
            "validFrom": value("valid_from"),
            "validTo": value("valid_to"),
            "sha256": value("sha256"),
            "status": value("status"),
            "featureCount": value("feature_count"),
        }

    @staticmethod
    def _upsert_airspace_source_locked(conn, source, now_ms):
        row = conn.execute(
            "SELECT * FROM airspace_sources WHERE slug=?", (source["slug"],)
        ).fetchone()
        if row is None:
            cursor = conn.execute(
                """
                INSERT INTO airspace_sources(
                    slug,name,provider,authority,source_url,sync_mode,dist_code,coverage_scope,
                    enabled,reference_only,created_at,updated_at
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (source["slug"], source["name"], source["provider"], source["authority"],
                 source["source_url"], source["sync_mode"], source["dist_code"],
                 source["coverage_scope"], source["enabled"], source["reference_only"],
                 now_ms, now_ms),
            )
            row = conn.execute(
                "SELECT * FROM airspace_sources WHERE id=?", (cursor.lastrowid,)
            ).fetchone()
        else:
            conn.execute(
                """
                UPDATE airspace_sources SET name=?,provider=?,authority=?,source_url=?,sync_mode=?,
                    dist_code=?,coverage_scope=?,enabled=?,reference_only=?,updated_at=? WHERE id=?
                """,
                (source["name"], source["provider"], source["authority"], source["source_url"],
                 source["sync_mode"], source["dist_code"], source["coverage_scope"],
                 source["enabled"], source["reference_only"], now_ms, row["id"]),
            )
            row = conn.execute("SELECT * FROM airspace_sources WHERE id=?", (row["id"],)).fetchone()
        return row

    def import_airspace(self, payload):
        normalized = normalize_airspace_import_payload(payload)
        now_ms = int(time.time() * 1000)
        source_value = normalized["source"]
        dataset_value = normalized["dataset"]
        zones = normalized["zones"]
        with self._lock, self._db() as conn:
            source_row = self._upsert_airspace_source_locked(conn, source_value, now_ms)
            source_id = source_row["id"]
            run_cursor = conn.execute(
                """
                INSERT INTO airspace_sync_runs(
                    source_id,trigger_type,status,started_at,feature_count,sha256,created_at
                ) VALUES (?,'manual_import','running',?,?,?,?)
                """,
                (source_id, now_ms, len(zones), dataset_value["sha256"], now_ms),
            )
            run_id = run_cursor.lastrowid
            existing = conn.execute(
                "SELECT * FROM airspace_datasets WHERE source_id=? AND sha256=?",
                (source_id, dataset_value["sha256"]),
            ).fetchone()
            idempotent = existing is not None
            if existing is not None:
                conn.execute(
                    "UPDATE airspace_datasets SET status='superseded' "
                    "WHERE source_id=? AND status='active' AND id<>?",
                    (source_id, existing["id"]),
                )
                conn.execute(
                    "UPDATE airspace_datasets SET status='active',fetched_at=? WHERE id=?",
                    (now_ms, existing["id"]),
                )
                dataset_id = existing["id"]
                run_status = "unchanged"
                message = "空域内容 SHA-256 未变化，已保持该版本为活动数据集"
            else:
                dataset_cursor = conn.execute(
                    """
                    INSERT INTO airspace_datasets(
                        source_id,source_version,published_at,fetched_at,valid_from,valid_to,
                        sha256,status,feature_count,metadata_json,created_at
                    ) VALUES (?,?,?,?,?,?,?,'staging',?,?,?)
                    """,
                    (source_id, dataset_value["source_version"], dataset_value["published_at"],
                     now_ms, dataset_value["valid_from"], dataset_value["valid_to"],
                     dataset_value["sha256"], dataset_value["feature_count"],
                     dataset_value["metadata_json"], now_ms),
                )
                dataset_id = dataset_cursor.lastrowid
                conn.executemany(
                    """
                    INSERT INTO airspace_zones(
                        dataset_id,external_id,name,zone_class,source_class,geometry_type,
                        geometry_json,geometry_sha256,min_lon,min_lat,max_lon,max_lat,
                        min_altitude,max_altitude,altitude_reference,valid_from,valid_to,
                        metadata_json,created_at
                    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    [(
                        dataset_id, zone["external_id"], zone["name"], zone["zone_class"],
                        zone["source_class"], zone["geometry_type"], zone["geometry_json"],
                        zone["geometry_sha256"], zone["min_lon"], zone["min_lat"],
                        zone["max_lon"], zone["max_lat"], zone["min_altitude"],
                        zone["max_altitude"], zone["altitude_reference"], zone["valid_from"],
                        zone["valid_to"], zone["metadata_json"], now_ms,
                    ) for zone in zones],
                )
                inserted_count = conn.execute(
                    "SELECT COUNT(*) FROM airspace_zones WHERE dataset_id=?", (dataset_id,)
                ).fetchone()[0]
                if inserted_count != len(zones):
                    raise RuntimeError("空域数据集写入数量校验失败")
                conn.execute(
                    "UPDATE airspace_datasets SET status='superseded' "
                    "WHERE source_id=? AND status='active' AND id<>?",
                    (source_id, dataset_id),
                )
                conn.execute(
                    "UPDATE airspace_datasets SET status='active' WHERE id=?", (dataset_id,)
                )
                run_status = "succeeded"
                message = f"已原子激活 {len(zones)} 个 WGS-84 空域要素"

            conn.execute(
                """
                UPDATE airspace_sources SET last_attempt_at=?,last_success_at=?,last_error=NULL,
                    updated_at=? WHERE id=?
                """,
                (now_ms, now_ms, now_ms, source_id),
            )
            conn.execute(
                """
                UPDATE airspace_sync_runs SET dataset_id=?,status=?,finished_at=?,message=?
                WHERE id=?
                """,
                (dataset_id, run_status, now_ms, message, run_id),
            )
            source_row = conn.execute(
                "SELECT * FROM airspace_sources WHERE id=?", (source_id,)
            ).fetchone()
            dataset_row = conn.execute(
                "SELECT * FROM airspace_datasets WHERE id=?", (dataset_id,)
            ).fetchone()
        source = self._row_to_airspace_source(source_row)
        source.update({
            "status": "active", "message": message,
            "latestTriggerType": "manual_import", "latestRunStatus": run_status,
            "authoritative": False,
        })
        return {
            "ok": True,
            "idempotent": idempotent,
            "runId": run_id,
            "zonesImported": len(zones),
            "status": run_status,
            "message": message,
            "source": source,
            "dataset": self._row_to_airspace_dataset(dataset_row),
        }

    def record_airspace_sync_unavailable(self, status, message, slug="uom"):
        if status not in {"unconfigured", "unsupported"}:
            raise ValueError("无效空域同步状态")
        slug = str(slug or "uom").strip().lower()
        if not AIRSPACE_SOURCE_SLUG_PATTERN.fullmatch(slug):
            raise ValueError("source 标识无效")
        now_ms = int(time.time() * 1000)
        source_value = {
            "slug": slug,
            "name": "UOM 空域",
            "provider": "UOM",
            "authority": None,
            "source_url": None,
            "sync_mode": "unconfigured" if status == "unconfigured" else "authorized_sync",
            "dist_code": None,
            "coverage_scope": "unknown",
            "enabled": 1,
            "reference_only": 0,
        }
        with self._lock, self._db() as conn:
            source_row = conn.execute(
                "SELECT * FROM airspace_sources WHERE slug=?", (slug,)
            ).fetchone()
            if source_row is None:
                source_row = self._upsert_airspace_source_locked(conn, source_value, now_ms)
            conn.execute(
                "UPDATE airspace_sources SET sync_mode=?,last_attempt_at=?,last_error=?,updated_at=? "
                "WHERE id=?",
                (source_value["sync_mode"], now_ms, message, now_ms, source_row["id"]),
            )
            cursor = conn.execute(
                """
                INSERT INTO airspace_sync_runs(
                    source_id,trigger_type,status,started_at,finished_at,message,created_at
                ) VALUES (?,'sync',?,?,?,?,?)
                """,
                (source_row["id"], status, now_ms, now_ms, message, now_ms),
            )
            source_row = conn.execute(
                "SELECT * FROM airspace_sources WHERE id=?", (source_row["id"],)
            ).fetchone()
        source = self._row_to_airspace_source(source_row)
        source.update({
            "status": status, "message": message,
            "latestTriggerType": "sync", "latestRunStatus": status,
            "authoritative": False,
        })
        return {"runId": cursor.lastrowid, "source": source}

    def airspace_status(self, sync_config):
        now_ms = int(time.time() * 1000)
        catalog_regions = {
            str(item.get("distCode")): item
            for item in (AIRSPACE_CATALOG.get("regions") or [])
            if isinstance(item, dict) and item.get("distCode")
        }
        with self._lock, self._db() as conn:
            rows = conn.execute(
                """
                SELECT s.*,
                       d.id AS dataset_id,d.source_version AS dataset_source_version,
                       d.published_at AS dataset_published_at,d.fetched_at AS dataset_fetched_at,
                       d.valid_from AS dataset_valid_from,d.valid_to AS dataset_valid_to,
                       d.sha256 AS dataset_sha256,d.status AS dataset_status,
                       d.feature_count AS dataset_feature_count,
                       (SELECT r.status FROM airspace_sync_runs r
                        WHERE r.source_id=s.id ORDER BY r.started_at DESC,r.id DESC LIMIT 1)
                           AS last_run_status,
                       (SELECT r.trigger_type FROM airspace_sync_runs r
                        WHERE r.source_id=s.id ORDER BY r.started_at DESC,r.id DESC LIMIT 1)
                           AS last_trigger_type
                FROM airspace_sources s
                LEFT JOIN airspace_datasets d ON d.source_id=s.id AND d.status='active'
                ORDER BY s.name,s.id
                """
            ).fetchall()
            active_datasets = conn.execute(
                "SELECT COUNT(*) FROM airspace_datasets WHERE status='active'"
            ).fetchone()[0]
            active_zones = conn.execute(
                """
                SELECT COUNT(*) FROM airspace_zones z
                JOIN airspace_datasets d ON d.id=z.dataset_id AND d.status='active'
                JOIN airspace_sources s ON s.id=d.source_id AND s.enabled=1
                """
            ).fetchone()[0]
            coverage_rows = conn.execute(
                """
                SELECT s.dist_code,
                       COUNT(DISTINCT s.id) AS source_count,
                       COUNT(DISTINCT d.id) AS active_dataset_count,
                       COUNT(z.id) AS active_zone_count,
                       COALESCE(SUM(z.zone_class='suitable'),0) AS suitable_count,
                       COALESCE(SUM(z.zone_class='warning'),0) AS warning_count,
                       COALESCE(SUM(z.zone_class='controlled'),0) AS controlled_count,
                       COALESCE(SUM(z.zone_class='prohibited'),0) AS prohibited_count,
                       COALESCE(SUM(z.zone_class='unknown'),0) AS unknown_count,
                       MAX(d.fetched_at) AS latest_fetched_at
                FROM airspace_sources s
                JOIN airspace_datasets d ON d.source_id=s.id AND d.status='active'
                LEFT JOIN airspace_zones z ON z.dataset_id=d.id
                WHERE s.enabled=1 AND s.dist_code IS NOT NULL
                GROUP BY s.dist_code
                ORDER BY s.dist_code
                """
            ).fetchall()
            national_package_count = conn.execute(
                """
                SELECT COUNT(DISTINCT s.id)
                FROM airspace_sources s
                JOIN airspace_datasets d ON d.source_id=s.id AND d.status='active'
                WHERE s.enabled=1 AND s.coverage_scope='national'
                """
            ).fetchone()[0]
            total_zone_rows = conn.execute("SELECT COUNT(*) FROM airspace_zones").fetchone()[0]
            indexed_zone_rows = conn.execute(
                "SELECT COUNT(*) FROM airspace_zone_rtree"
            ).fetchone()[0]
        sources = []
        for row in rows:
            source = self._row_to_airspace_source(row)
            active_dataset = None
            if row["dataset_id"] is not None:
                active_dataset = self._row_to_airspace_dataset(row, "dataset_")
            expired = bool(active_dataset and active_dataset["validTo"] is not None
                           and active_dataset["validTo"] < now_ms)
            if expired:
                source_status = "stale"
                source_message = "活动空域数据集已超过有效期"
            elif active_dataset:
                source_status = "active"
                source_message = f"已加载 {active_dataset['featureCount']} 个空域要素"
            elif source["lastError"]:
                source_status = row["last_run_status"] or "error"
                source_message = source["lastError"]
            else:
                source_status = "empty"
                source_message = "尚未导入空域数据"
            source.update({
                "status": source_status,
                "message": source_message,
                "latestTriggerType": row["last_trigger_type"],
                "latestRunStatus": row["last_run_status"],
                "authoritative": bool(
                    source["syncMode"] == "authorized_sync"
                    and row["last_trigger_type"] == "sync"
                    and row["last_run_status"] == "succeeded"
                ),
                "activeDataset": active_dataset,
            })
            catalog_entry = catalog_regions.get(str(source.get("distCode")))
            if catalog_entry:
                source["catalog"] = {
                    "coverageState": catalog_entry.get("coverageState"),
                    "zoneClass": catalog_entry.get("zoneClass"),
                    "ruleBasis": catalog_entry.get("ruleBasis"),
                    "boundaryAsset": catalog_entry.get("boundaryAsset"),
                    "authoritative": catalog_entry.get("authoritative") is True,
                }
            sources.append(source)
        if active_datasets:
            overall_status = "active"
            overall_message = f"{active_datasets} 个活动数据集，共 {active_zones} 个空域要素"
        else:
            overall_status = sync_config["status"]
            overall_message = sync_config["message"]
        coverage = [{
            "distCode": row["dist_code"],
            "sourceCount": row["source_count"],
            "activeDatasetCount": row["active_dataset_count"],
            "activeZoneCount": row["active_zone_count"],
            "zoneClasses": {
                "suitable": row["suitable_count"],
                "warning": row["warning_count"],
                "controlled": row["controlled_count"],
                "prohibited": row["prohibited_count"],
                "unknown": row["unknown_count"],
            },
            "latestFetchedAt": row["latest_fetched_at"],
        } for row in coverage_rows]
        return {
            "configured": sync_config["configured"],
            "syncSupported": sync_config["supported"],
            "status": overall_status,
            "message": overall_message,
            "totalSources": len(sources),
            "activeDatasets": active_datasets,
            "activeZones": active_zones,
            "activeRegionCount": len(coverage),
            "nationalPackageCount": national_package_count,
            "coverage": coverage,
            "coverageByDistCode": {item["distCode"]: item for item in coverage},
            "spatialIndex": {
                "type": "SQLite RTree",
                "indexedZones": indexed_zone_rows,
                "zoneRows": total_zone_rows,
                "consistent": indexed_zone_rows == total_zone_rows,
            },
            "coverageCatalog": airspace_catalog_summary(),
            "serverTime": now_ms,
            "sources": sources,
        }

    @staticmethod
    def _row_to_airspace_zone(row):
        return {
            "id": row["id"],
            "externalId": row["external_id"],
            "name": row["name"],
            "zoneClass": row["zone_class"],
            "sourceClass": row["source_class"],
            "geometry": json.loads(row["geometry_json"]),
            "minAltitude": row["min_altitude"],
            "maxAltitude": row["max_altitude"],
            "altitudeReference": row["altitude_reference"],
            "validFrom": row["valid_from"],
            "validTo": row["valid_to"],
            "geometrySha256": row["geometry_sha256"],
            "source": {
                "id": row["source_id"],
                "slug": row["source_slug"],
                "name": row["source_name"],
                "provider": row["source_provider"],
                "authority": row["source_authority"],
                "syncMode": row["source_sync_mode"],
                "distCode": row["source_dist_code"],
                "coverageScope": row["source_coverage_scope"],
                "referenceOnly": bool(row["source_reference_only"]),
                "displayOnMap": not bool(row["source_reference_only"]),
                "authoritative": bool(row["dataset_authoritative"]),
            },
            "dataset": {
                "id": row["dataset_id"],
                "sourceVersion": row["dataset_source_version"],
                "publishedAt": row["dataset_published_at"],
                "fetchedAt": row["dataset_fetched_at"],
                "validFrom": row["dataset_valid_from"],
                "validTo": row["dataset_valid_to"],
                "sha256": row["dataset_sha256"],
            },
        }

    def list_airspace_zones(self, params):
        bbox = _parse_airspace_bbox(params.get("bbox", [None])[0])
        classes = _parse_airspace_classes(params.get("classes", []))
        at_ms = _parse_time(params.get("at", [None])[0])
        if at_ms is None:
            at_ms = int(time.time() * 1000)
        min_lon, min_lat, max_lon, max_lat = bbox
        class_placeholders = ",".join("?" for _ in classes)
        where = f"""
            WHERE d.status='active' AND s.enabled=1
              AND z.zone_class IN ({class_placeholders})
              AND rz.min_lon<=? AND rz.max_lon>=? AND rz.min_lat<=? AND rz.max_lat>=?
              AND (d.valid_from IS NULL OR d.valid_from<=?)
              AND (d.valid_to IS NULL OR d.valid_to>=?)
              AND (z.valid_from IS NULL OR z.valid_from<=?)
              AND (z.valid_to IS NULL OR z.valid_to>=?)
        """
        values = list(classes) + [max_lon, min_lon, max_lat, min_lat] + [at_ms] * 4
        joined = """
            FROM airspace_zones z
            JOIN airspace_zone_rtree rz ON rz.id=z.id
            JOIN airspace_datasets d ON d.id=z.dataset_id
            JOIN airspace_sources s ON s.id=d.source_id
        """
        with self._lock, self._db() as conn:
            total = conn.execute("SELECT COUNT(*) " + joined + where, values).fetchone()[0]
            rows = conn.execute(
                """
                SELECT z.*,
                       s.id AS source_id,s.slug AS source_slug,s.name AS source_name,
                       s.provider AS source_provider,s.authority AS source_authority,
                       s.sync_mode AS source_sync_mode,
                       s.dist_code AS source_dist_code,
                       s.coverage_scope AS source_coverage_scope,
                       s.reference_only AS source_reference_only,
                       d.source_version AS dataset_source_version,
                       d.published_at AS dataset_published_at,
                       d.fetched_at AS dataset_fetched_at,
                       d.valid_from AS dataset_valid_from,d.valid_to AS dataset_valid_to,
                       d.sha256 AS dataset_sha256,
                       EXISTS(
                           SELECT 1 FROM airspace_sync_runs sr
                           WHERE sr.dataset_id=d.id AND sr.trigger_type='sync'
                             AND sr.status='succeeded'
                       ) AS dataset_authoritative
                """ + joined + where + """
                ORDER BY CASE z.zone_class
                    WHEN 'prohibited' THEN 0 WHEN 'controlled' THEN 1
                    WHEN 'warning' THEN 2 WHEN 'suitable' THEN 3 ELSE 4 END,
                    z.name,z.id
                LIMIT ?
                """,
                values + [MAX_AIRSPACE_QUERY_ZONES],
            ).fetchall()
        return {
            "items": [self._row_to_airspace_zone(row) for row in rows],
            "total": total,
            "truncated": total > len(rows),
            "bbox": bbox,
            "classes": classes,
            "at": at_ms,
        }

    @staticmethod
    def _geofence_contains(geofence, lat, lon, altitude):
        if lat is None or lon is None:
            return False
        if geofence["minAltitude"] is not None:
            if altitude is None or altitude < geofence["minAltitude"]:
                return False
        if geofence["maxAltitude"] is not None:
            if altitude is None or altitude > geofence["maxAltitude"]:
                return False
        geometry = geofence["geometry"]
        if geofence["shapeType"] == "circle":
            center_lon, center_lat = geometry["center"]
            return HistoryStore._distance_m(lat, lon, center_lat, center_lon) <= geometry["radiusMeters"]
        return _point_in_polygon(lon, lat, geometry["coordinates"][0])

    @staticmethod
    def _json_list(value):
        try:
            parsed = json.loads(value)
            return parsed if isinstance(parsed, list) else []
        except (TypeError, ValueError, json.JSONDecodeError):
            return []

    @staticmethod
    def _row_to_incident(row, include_evidence=False):
        evidence_json = row["evidence_json"] or "{}"
        try:
            evidence = json.loads(evidence_json)
        except (TypeError, ValueError, json.JSONDecodeError):
            evidence = None
        lab_loopback = bool(
            isinstance(evidence, dict)
            and isinstance(evidence.get("drone"), dict)
            and evidence["drone"].get("labLoopback") is True
        )
        simulated = bool(
            isinstance(evidence, dict)
            and isinstance(evidence.get("drone"), dict)
            and evidence["drone"].get("simulated") is True
        ) or lab_loopback
        result = {
            "id": row["id"],
            "flightId": row["flight_id"],
            "stationId": row["station_id"],
            "mac": row["mac"],
            "model": row["model"] or "未知机型",
            "uasId": row["uas_id"],
            "status": row["status"],
            "riskScore": row["risk_score"],
            "riskLevel": row["risk_level"],
            "riskReasons": HistoryStore._json_list(row["risk_reasons_json"]),
            "geofenceIds": HistoryStore._json_list(row["geofence_ids_json"]),
            "firstSeen": row["first_seen_at"],
            "lastSeen": row["last_seen_at"],
            "createdAt": row["created_at"],
            "updatedAt": row["updated_at"],
            "simulated": simulated,
            "labLoopback": lab_loopback,
            "evidenceSha256": hashlib.sha256(evidence_json.encode("utf-8")).hexdigest(),
        }
        if include_evidence:
            result["evidence"] = evidence
        return result

    def _upsert_incident_locked(self, conn, snapshot, drone, flight_id, score, level,
                                reasons, geofence_ids, geofence_snapshots, observed_ms, wall_ms):
        evidence = {
            "capturedAt": observed_ms,
            "stationId": snapshot.get("stationId") or "local",
            "stationName": snapshot.get("stationName"),
            "flightId": flight_id,
            "drone": {key: drone.get(key) for key in (
                "mac", "model", "id", "proto", "protocol", "lat", "lon", "alt", "spd",
                "heading", "rssi", "olat", "olon", "simulated", "labLoopback"
            ) if drone.get(key) is not None},
            "riskScore": score,
            "riskLevel": level,
            "riskReasons": list(reasons),
            "geofenceIds": list(geofence_ids),
            "geofences": list(geofence_snapshots),
        }
        evidence_json = json.dumps(evidence, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
        existing = conn.execute("SELECT * FROM incidents WHERE flight_id=?", (flight_id,)).fetchone()
        if existing is None:
            cursor = conn.execute(
                """
                INSERT INTO incidents(
                    flight_id,station_id,mac,model,uas_id,status,risk_score,risk_level,
                    risk_reasons_json,geofence_ids_json,evidence_json,first_seen_at,last_seen_at,
                    created_at,updated_at
                ) VALUES (?,?,?,?,?,'open',?,?,?,?,?,?,?,?,?)
                """,
                (flight_id, snapshot.get("stationId") or "local", drone["mac"], drone.get("model"),
                 drone.get("id"), score, level,
                 json.dumps(reasons, ensure_ascii=False, separators=(",", ":")),
                 json.dumps(geofence_ids, separators=(",", ":")), evidence_json,
                 observed_ms, observed_ms, wall_ms, wall_ms),
            )
            return cursor.lastrowid

        merged_reasons = self._json_list(existing["risk_reasons_json"])
        for reason in reasons:
            if reason not in merged_reasons:
                merged_reasons.append(reason)
        merged_geofence_ids = self._json_list(existing["geofence_ids_json"])
        for geofence_id in geofence_ids:
            if geofence_id not in merged_geofence_ids:
                merged_geofence_ids.append(geofence_id)
        max_score = max(existing["risk_score"], score)
        max_level = _risk_level(max_score)
        best_evidence = evidence_json if score > existing["risk_score"] else existing["evidence_json"]
        if existing["status"] == "resolved":
            conn.execute(
                "UPDATE incidents SET status='open',updated_at=? WHERE id=?",
                (wall_ms, existing["id"]),
            )
            conn.execute(
                """
                INSERT INTO incident_actions(
                    incident_id,action,from_status,to_status,note,actor,created_at
                ) VALUES (?,'reopened','resolved','open','风险条件再次出现','system',?)
                """,
                (existing["id"], wall_ms),
            )
        conn.execute(
            """
            UPDATE incidents SET station_id=?,mac=?,model=COALESCE(?,model),
                uas_id=COALESCE(?,uas_id),risk_score=?,risk_level=?,risk_reasons_json=?,
                geofence_ids_json=?,evidence_json=?,last_seen_at=MAX(last_seen_at,?),updated_at=?
            WHERE id=?
            """,
            (snapshot.get("stationId") or "local", drone["mac"], drone.get("model"), drone.get("id"),
             max_score, max_level,
             json.dumps(merged_reasons, ensure_ascii=False, separators=(",", ":")),
             json.dumps(merged_geofence_ids, separators=(",", ":")), best_evidence,
             observed_ms, wall_ms, existing["id"]),
        )
        return existing["id"]

    def _evaluate_drone_locked(self, conn, snapshot, drone, flight_id, geofences,
                               observed_ms, wall_ms):
        score = 0
        reasons = []
        geofence_ids = []
        geofence_snapshots = []
        lat = _number(drone.get("lat"))
        lon = _number(drone.get("lon"))
        altitude = _number(drone.get("alt"))
        for geofence in geofences:
            if self._geofence_contains(geofence, lat, lon, altitude):
                points = GEOFENCE_SEVERITY_SCORES[geofence["severity"]]
                score += points
                geofence_ids.append(geofence["id"])
                geofence_snapshots.append(geofence)
                reasons.append(f"进入{geofence['severity']}围栏：{geofence['name']}")
        if not drone.get("id"):
            score += 20
            reasons.append("缺少UAS ID")
        if _number(drone.get("olat")) is None or _number(drone.get("olon")) is None:
            score += 10
            reasons.append("缺少操作者坐标")
        speed = _number(drone.get("spd"))
        if speed is not None and speed > 20:
            score += 15
            reasons.append("速度超过20m/s")
        score = min(100, score)
        level = _risk_level(score)
        incident_id = None
        if score >= 25:
            incident_id = self._upsert_incident_locked(
                conn, snapshot, drone, flight_id, score, level, reasons, geofence_ids,
                geofence_snapshots, observed_ms, wall_ms,
            )
        drone["riskScore"] = score
        drone["riskLevel"] = level
        drone["riskReasons"] = reasons
        drone["geofenceIds"] = geofence_ids
        drone["incidentId"] = incident_id

    def prune(self, now_ms=None):
        """Delete completed sessions older than retention; SQLite reuses freed pages."""
        wall_now = int(time.time() * 1000) if now_ms is None else int(now_ms)
        self._next_prune_at = wall_now + 24 * 60 * 60 * 1000
        if self.retention_days <= 0:
            return 0
        cutoff = wall_now - self.retention_days * 24 * 60 * 60 * 1000
        with self._lock, self._db() as conn:
            cursor = conn.execute(
                "DELETE FROM flights WHERE active=0 AND COALESCE(ended_at,last_seen_at) < ? "
                "AND NOT EXISTS (SELECT 1 FROM incidents WHERE incidents.flight_id=flights.id)",
                (cutoff,),
            )
            return cursor.rowcount

    def expire_sessions(self, now_ms=None):
        now_ms = int(time.time() * 1000) if now_ms is None else int(now_ms)
        with self._lock, self._db() as conn:
            self._expire_locked(conn, now_ms)

    def ingest_snapshot(self, snapshot, now_ms=None):
        """Persist one full snapshot. Coordinates remain raw RID WGS-84."""
        received_ms = int(time.time() * 1000)
        now_ms = received_ms if now_ms is None else int(now_ms)
        captured_ms = _integer(snapshot.get("capturedAt")) if isinstance(snapshot, dict) else None
        if captured_ms is not None and captured_ms > 0:
            # Any future station time is untrusted; clamp it to receipt time so it cannot
            # block later real-time samples for the same station.
            now_ms = min(captured_ms, received_ms)
        drones = snapshot.get("drones") if isinstance(snapshot, dict) else None
        if not isinstance(drones, list):
            raise ValueError("快照缺少 drones 数组")

        if received_ms >= self._next_prune_at:
            self.prune(received_ms)

        station_id = str(snapshot.get("stationId") or "local").strip()[:128] or "local"
        station_name = str(snapshot.get("stationName") or "").strip()[:256] or None
        with self._lock, self._db() as conn:
            self._expire_locked(conn, now_ms)
            geofences = [
                self._row_to_geofence(row)
                for row in conn.execute(
                    "SELECT * FROM geofences WHERE enabled=1 ORDER BY id"
                ).fetchall()
            ]
            station_latest = conn.execute(
                "SELECT MAX(last_seen_at) FROM flights WHERE station_id=?",
                (station_id,),
            ).fetchone()[0]
            if station_latest is not None and now_ms < station_latest - self.flight_gap_ms:
                return False
            for drone in drones:
                if not isinstance(drone, dict):
                    continue
                mac = str(drone.get("mac") or "").strip().upper()[:64]
                if not mac:
                    continue

                model = str(drone.get("model") or "").strip()[:256] or None
                uas_id = str(drone.get("id") or "").strip()[:256] or None
                proto = _integer(drone.get("proto"))
                altitude = _number(drone.get("alt"))
                speed = _number(drone.get("spd"))
                rssi = _integer(drone.get("rssi"))
                lat = _number(drone.get("lat"))
                lon = _number(drone.get("lon"))
                operator_lat = _number(drone.get("olat"))
                operator_lon = _number(drone.get("olon"))

                if lat is not None and not -90 <= lat <= 90:
                    lat = None
                if lon is not None and not -180 <= lon <= 180:
                    lon = None

                key = (station_id, mac)
                state = self._active.get(key)
                if state is not None and now_ms < state["last_seen"] - self.flight_gap_ms:
                    # Gateways upload FIFO. A sample this stale is an out-of-order duplicate or
                    # a station-id collision; ignoring it avoids corrupting the active session.
                    continue
                if state is None:
                    recent = conn.execute(
                        "SELECT id,last_seen_at,active FROM flights "
                        "WHERE station_id=? AND mac=? ORDER BY last_seen_at DESC,id DESC LIMIT 1",
                        (station_id, mac),
                    ).fetchone()
                    delta = now_ms - recent["last_seen_at"] if recent is not None else None
                    if recent is not None and abs(delta) < self.flight_gap_ms:
                        conn.execute(
                            "UPDATE flights SET active=1,ended_at=NULL WHERE id=?",
                            (recent["id"],),
                        )
                        last = conn.execute(
                            "SELECT recorded_at,lat,lon FROM track_points "
                            "WHERE flight_id=? ORDER BY recorded_at DESC,id DESC LIMIT 1",
                            (recent["id"],),
                        ).fetchone()
                        state = {
                            "id": recent["id"],
                            "last_seen": recent["last_seen_at"],
                            "last_point": ({"t": last["recorded_at"], "lat": last["lat"], "lon": last["lon"]}
                                           if last is not None else None),
                        }
                    elif recent is None or delta >= 0:
                        cur = conn.execute(
                            """
                            INSERT INTO flights(
                                station_id, station_name, mac, model, uas_id, proto, started_at, last_seen_at,
                                max_altitude, max_speed, min_rssi, max_rssi, active
                            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1)
                            """,
                            (station_id, station_name, mac, model, uas_id, proto, now_ms, now_ms,
                             altitude, speed, rssi, rssi),
                        )
                        state = {"id": cur.lastrowid, "last_seen": now_ms, "last_point": None}
                    else:
                        # Older than the latest session by a full gap: reject out-of-order input.
                        continue
                    self._active[key] = state

                has_point = lat is not None and lon is not None
                last_point = state["last_point"]
                if has_point and last_point is not None:
                    elapsed = now_ms - last_point["t"]
                    moved = self._distance_m(last_point["lat"], last_point["lon"], lat, lon)
                    has_point = elapsed >= self.max_point_interval_ms or moved >= self.min_point_meters
                if has_point:
                    conn.execute(
                        """
                        INSERT INTO track_points(
                            flight_id, station_id, recorded_at, lat, lon, altitude, speed, rssi,
                            operator_lat, operator_lon
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (state["id"], station_id, now_ms, lat, lon, altitude, speed, rssi,
                         operator_lat, operator_lon),
                    )
                    if last_point is None or now_ms >= last_point["t"]:
                        state["last_point"] = {"t": now_ms, "lat": lat, "lon": lon}

                conn.execute(
                    """
                    UPDATE flights SET
                        model=COALESCE(NULLIF(?, ''), model),
                        station_name=COALESCE(NULLIF(?, ''), station_name),
                        uas_id=COALESCE(NULLIF(?, ''), uas_id),
                        proto=COALESCE(?, proto),
                        started_at=CASE WHEN ? < started_at THEN ? ELSE started_at END,
                        last_seen_at=CASE WHEN ? > last_seen_at THEN ? ELSE last_seen_at END,
                        max_altitude=CASE
                            WHEN ? IS NULL THEN max_altitude
                            WHEN max_altitude IS NULL OR ? > max_altitude THEN ?
                            ELSE max_altitude END,
                        max_speed=CASE
                            WHEN ? IS NULL THEN max_speed
                            WHEN max_speed IS NULL OR ? > max_speed THEN ?
                            ELSE max_speed END,
                        min_rssi=CASE
                            WHEN ? IS NULL THEN min_rssi
                            WHEN min_rssi IS NULL OR ? < min_rssi THEN ?
                            ELSE min_rssi END,
                        max_rssi=CASE
                            WHEN ? IS NULL THEN max_rssi
                            WHEN max_rssi IS NULL OR ? > max_rssi THEN ?
                            ELSE max_rssi END,
                        point_count=point_count+?
                    WHERE id=?
                    """,
                    (model, station_name, uas_id, proto, now_ms, now_ms, now_ms, now_ms,
                     altitude, altitude, altitude,
                     speed, speed, speed,
                     rssi, rssi, rssi,
                     rssi, rssi, rssi,
                     1 if has_point else 0, state["id"]),
                )
                state["last_seen"] = max(state["last_seen"], now_ms)
                self._evaluate_drone_locked(
                    conn, snapshot, drone, state["id"], geofences, now_ms, received_ms,
                )
        return True

    def finish_all(self):
        with self._lock, self._db() as conn:
            for state in self._active.values():
                self._finish(conn, state["id"], state["last_seen"])
            self._active.clear()

    @staticmethod
    def _row_to_flight(row):
        end = row["ended_at"]
        last_seen = row["last_seen_at"]
        start = row["started_at"]
        active = bool(row["active"])
        return {
            "id": row["id"],
            "stationId": row["station_id"],
            "stationName": row["station_name"],
            "mac": row["mac"],
            "model": row["model"] or "未知机型",
            "uasId": row["uas_id"],
            "proto": row["proto"],
            "start": start,
            "end": end,
            "lastSeen": last_seen,
            "status": "active" if active else "completed",
            "active": active,
            "durationMs": max(0, (end if end is not None else last_seen) - start),
            "n": row["point_count"],
            "maxAlt": row["max_altitude"],
            "maxSpd": row["max_speed"],
            "minRssi": row["min_rssi"],
            "maxRssi": row["max_rssi"],
        }

    @staticmethod
    def _filters(params):
        clauses = []
        values = []

        q = (params.get("q", [""])[0] or "").strip()[:128]
        if q:
            q = q.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
            clauses.append("(station_id LIKE ? ESCAPE '\\' OR station_name LIKE ? ESCAPE '\\' "
                           "OR mac LIKE ? ESCAPE '\\' OR model LIKE ? ESCAPE '\\' OR uas_id LIKE ? ESCAPE '\\')")
            values.extend([f"%{q}%"] * 5)

        station_id = (params.get("station_id", [""])[0] or "").strip()
        if station_id:
            clauses.append("station_id=?")
            values.append(station_id)

        start_from = _parse_time(params.get("from", [None])[0])
        end_to = _parse_time(params.get("to", [None])[0])
        if start_from is not None:
            clauses.append("last_seen_at >= ?")
            values.append(start_from)
        if end_to is not None:
            clauses.append("started_at <= ?")
            values.append(end_to)

        status = (params.get("status", ["all"])[0] or "all").lower()
        if status not in {"all", "active", "completed"}:
            raise ValueError("status 必须是 all、active 或 completed")
        if status == "active":
            clauses.append("active=1")
        elif status == "completed":
            clauses.append("active=0")

        raw_duration = params.get("min_duration", [None])[0]
        if raw_duration not in (None, ""):
            minimum = _number(raw_duration)
            if minimum is None or minimum < 0:
                raise ValueError("min_duration 必须是非负秒数")
            clauses.append("(COALESCE(ended_at, last_seen_at)-started_at) >= ?")
            values.append(int(minimum * 1000))

        raw_altitude = params.get("min_altitude", [None])[0]
        if raw_altitude not in (None, ""):
            minimum = _number(raw_altitude)
            if minimum is None:
                raise ValueError("min_altitude 必须是数字")
            clauses.append("max_altitude >= ?")
            values.append(minimum)

        return (" WHERE " + " AND ".join(clauses)) if clauses else "", values

    def list_flights(self, params, paginate=True):
        self.expire_sessions()
        where, values = self._filters(params)
        try:
            page = max(1, int(params.get("page", ["1"])[0]))
            page_size = max(1, min(200, int(params.get("page_size", ["50"])[0])))
        except (TypeError, ValueError) as exc:
            raise ValueError("page/page_size 必须是整数") from exc

        with self._lock, self._db() as conn:
            total = conn.execute("SELECT COUNT(*) FROM flights" + where, values).fetchone()[0]
            sql = "SELECT * FROM flights" + where + " ORDER BY started_at DESC, id DESC"
            query_values = list(values)
            if paginate:
                sql += " LIMIT ? OFFSET ?"
                query_values.extend([page_size, (page - 1) * page_size])
            rows = conn.execute(sql, query_values).fetchall()

        items = [self._row_to_flight(row) for row in rows]
        return {
            "items": items,
            "total": total,
            "page": page,
            "page_size": page_size,
            "pages": math.ceil(total / page_size) if total else 0,
        }

    def get_flight(self, flight_id):
        self.expire_sessions()
        with self._lock, self._db() as conn:
            row = conn.execute("SELECT * FROM flights WHERE id=?", (flight_id,)).fetchone()
            if row is None:
                return None
            point_rows = conn.execute(
                "SELECT * FROM track_points WHERE flight_id=? ORDER BY recorded_at, id",
                (flight_id,),
            ).fetchall()

        flight = self._row_to_flight(row)
        flight["points"] = [
            {
                "t": point["recorded_at"],
                "lat": point["lat"],
                "lon": point["lon"],
                "alt": point["altitude"],
                "spd": point["speed"],
                "rssi": point["rssi"],
                "olat": point["operator_lat"],
                "olon": point["operator_lon"],
            }
            for point in point_rows
        ]
        return flight

    @staticmethod
    def _incident_filters(params):
        clauses = []
        values = []
        status = (params.get("status", ["all"])[0] or "all").strip().lower()
        if status != "all":
            if status not in INCIDENT_STATUSES:
                raise ValueError("status 参数无效")
            clauses.append("status=?")
            values.append(status)
        severity = (params.get("severity", ["all"])[0] or "all").strip().lower()
        if severity != "all":
            if severity not in set(GEOFENCE_SEVERITY_SCORES) | {"normal"}:
                raise ValueError("severity 参数无效")
            clauses.append("risk_level=?")
            values.append(severity)
        station_id = (params.get("station_id", [""])[0] or "").strip()[:128]
        if station_id:
            clauses.append("station_id=?")
            values.append(station_id)
        q = (params.get("q", [""])[0] or "").strip()[:128]
        if q:
            q = q.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
            clauses.append(
                "(mac LIKE ? ESCAPE '\\' OR model LIKE ? ESCAPE '\\' OR "
                "uas_id LIKE ? ESCAPE '\\' OR CAST(flight_id AS TEXT) LIKE ? ESCAPE '\\')"
            )
            values.extend([f"%{q}%"] * 4)
        start_from = _parse_time(params.get("from", [None])[0])
        end_to = _parse_time(params.get("to", [None])[0])
        if start_from is not None:
            clauses.append("last_seen_at>=?")
            values.append(start_from)
        if end_to is not None:
            clauses.append("first_seen_at<=?")
            values.append(end_to)
        return (" WHERE " + " AND ".join(clauses)) if clauses else "", values

    def list_incidents(self, params, paginate=True):
        where, values = self._incident_filters(params)
        try:
            page = max(1, int(params.get("page", ["1"])[0]))
            page_size = max(1, min(200, int(params.get("page_size", ["50"])[0])))
        except (TypeError, ValueError) as exc:
            raise ValueError("page/page_size 必须是整数") from exc
        with self._lock, self._db() as conn:
            total = conn.execute("SELECT COUNT(*) FROM incidents" + where, values).fetchone()[0]
            summary_row = conn.execute(
                """
                SELECT COALESCE(SUM(status='open'),0) AS open_count,
                       COALESCE(SUM(status='acknowledged'),0) AS acknowledged_count,
                       COALESCE(SUM(status='resolved'),0) AS resolved_count,
                       COALESCE(SUM(status='dismissed'),0) AS dismissed_count,
                       COALESCE(SUM(
                           risk_level IN ('high','critical')
                           AND status NOT IN ('resolved','dismissed')
                       ),0) AS critical_count,
                       COALESCE(SUM(risk_level='high'),0) AS high_count,
                       COALESCE(SUM(risk_level='medium'),0) AS medium_count,
                       COALESCE(SUM(risk_level='low'),0) AS low_count
                FROM incidents
                """ + where,
                values,
            ).fetchone()
            sql = "SELECT * FROM incidents" + where + " ORDER BY last_seen_at DESC,id DESC"
            query_values = list(values)
            if paginate:
                sql += " LIMIT ? OFFSET ?"
                query_values.extend([page_size, (page - 1) * page_size])
            rows = conn.execute(sql, query_values).fetchall()
        return {
            "items": [self._row_to_incident(row) for row in rows],
            "total": total,
            "page": page,
            "page_size": page_size,
            "pages": math.ceil(total / page_size) if total else 0,
            "summary": {
                "open": summary_row["open_count"],
                "acknowledged": summary_row["acknowledged_count"],
                "resolved": summary_row["resolved_count"],
                "dismissed": summary_row["dismissed_count"],
                "critical": summary_row["critical_count"],
                "high": summary_row["high_count"],
                "medium": summary_row["medium_count"],
                "low": summary_row["low_count"],
            },
        }

    def get_incident(self, incident_id):
        with self._lock, self._db() as conn:
            row = conn.execute("SELECT * FROM incidents WHERE id=?", (int(incident_id),)).fetchone()
            if row is None:
                return None
            actions = conn.execute(
                "SELECT * FROM incident_actions WHERE incident_id=? ORDER BY created_at,id",
                (int(incident_id),),
            ).fetchall()
        incident = self._row_to_incident(row, include_evidence=True)
        incident["actions"] = [{
            "id": action["id"],
            "action": action["action"],
            "fromStatus": action["from_status"],
            "toStatus": action["to_status"],
            "note": action["note"],
            "actor": action["actor"],
            "createdAt": action["created_at"],
        } for action in actions]
        return incident

    def get_incident_evidence_bytes(self, incident_id):
        with self._lock, self._db() as conn:
            row = conn.execute(
                "SELECT evidence_json FROM incidents WHERE id=?", (int(incident_id),)
            ).fetchone()
        return None if row is None else row["evidence_json"].encode("utf-8")

    def set_incident_status(self, incident_id, status, note=None, actor=None):
        status = str(status or "").strip().lower()
        if status not in INCIDENT_STATUSES:
            raise ValueError("status 必须是 open、acknowledged、resolved 或 dismissed")
        if note is not None and not isinstance(note, str):
            raise ValueError("note 必须是字符串")
        note = _clean_text(note, 1000) if note is not None else None
        actor = _clean_text(actor, 128)
        now_ms = int(time.time() * 1000)
        with self._lock, self._db() as conn:
            row = conn.execute(
                "SELECT status FROM incidents WHERE id=?", (int(incident_id),)
            ).fetchone()
            if row is None:
                return None
            previous = row["status"]
            if previous != status or note is not None:
                action = "note" if previous == status else "set_status"
                conn.execute(
                    "UPDATE incidents SET status=?,updated_at=? WHERE id=?",
                    (status, now_ms, int(incident_id)),
                )
                conn.execute(
                    """
                    INSERT INTO incident_actions(
                        incident_id,action,from_status,to_status,note,actor,created_at
                    ) VALUES (?,?,?,?,?,?,?)
                    """,
                    (int(incident_id), action, previous, status, note, actor, now_ms),
                )
        return self.get_incident(incident_id)

    def export_incidents_csv(self, params):
        result = self.list_incidents(params, paginate=False)
        out = io.StringIO(newline="")
        writer = csv.writer(out)
        writer.writerow([
            "incident_id", "flight_id", "station_id", "mac", "model", "uas_id", "status",
            "risk_score", "risk_level", "risk_reasons", "geofence_ids", "first_seen_ms",
            "last_seen_ms", "simulated", "evidence_sha256", "created_at_ms", "updated_at_ms",
        ])
        for item in result["items"]:
            writer.writerow([
                item["id"], item["flightId"], _csv_safe(item["stationId"]),
                _csv_safe(item["mac"]), _csv_safe(item["model"]), _csv_safe(item["uasId"]),
                _csv_safe(item["status"]), item["riskScore"], _csv_safe(item["riskLevel"]),
                _csv_safe(" | ".join(item["riskReasons"])),
                ",".join(str(value) for value in item["geofenceIds"]),
                item["firstSeen"], item["lastSeen"], item["simulated"], item["evidenceSha256"],
                item["createdAt"], item["updatedAt"],
            ])
        return "\ufeff" + out.getvalue()

    def status(self):
        self.expire_sessions()
        with self._lock, self._db() as conn:
            row = conn.execute(
                """
                SELECT COUNT(*) AS flights,
                       COALESCE(SUM(active), 0) AS active,
                       COALESCE(SUM(point_count), 0) AS points,
                       COUNT(DISTINCT station_id) AS stations,
                       MIN(started_at) AS first_seen,
                       MAX(last_seen_at) AS last_seen
                FROM flights
                """
            ).fetchone()
            geofence_row = conn.execute(
                "SELECT COUNT(*) AS total,COALESCE(SUM(enabled),0) AS enabled FROM geofences"
            ).fetchone()
            incident_row = conn.execute(
                """
                SELECT COUNT(*) AS total,
                       COALESCE(SUM(status='open'),0) AS open,
                       COALESCE(SUM(status='acknowledged'),0) AS acknowledged,
                       COALESCE(SUM(status IN ('open','acknowledged')),0) AS active
                FROM incidents
                """
            ).fetchone()
        database_bytes = sum(
            os.path.getsize(candidate) for candidate in (self.path, self.path + "-wal", self.path + "-shm")
            if os.path.exists(candidate)
        )
        return {
            "database": os.path.basename(self.path),
            "databaseBytes": database_bytes,
            "flights": row["flights"],
            "activeFlights": row["active"],
            "trackPoints": row["points"],
            "stations": row["stations"],
            "firstSeen": row["first_seen"],
            "lastSeen": row["last_seen"],
            "flightGapSeconds": self.flight_gap_ms / 1000,
            "minPointMeters": self.min_point_meters,
            "maxPointIntervalSeconds": self.max_point_interval_ms / 1000,
            "retentionDays": self.retention_days,
            "geofences": geofence_row["total"],
            "enabledGeofences": geofence_row["enabled"],
            "incidents": incident_row["total"],
            "openIncidents": incident_row["open"],
            "acknowledgedIncidents": incident_row["acknowledged"],
            "activeIncidents": incident_row["active"],
        }

    def export_csv(self, params):
        result = self.list_flights(params, paginate=False)
        out = io.StringIO(newline="")
        writer = csv.writer(out)
        writer.writerow([
            "flight_id", "station_id", "station_name", "model", "mac", "uas_id", "status", "start_ms", "end_ms",
            "duration_seconds", "track_points", "max_altitude_m", "max_speed_mps",
            "min_rssi_dbm", "max_rssi_dbm",
        ])
        for item in result["items"]:
            writer.writerow([
                item["id"], _csv_safe(item["stationId"]), _csv_safe(item["stationName"]),
                _csv_safe(item["model"]), _csv_safe(item["mac"]), _csv_safe(item["uasId"]),
                _csv_safe(item["status"]),
                item["start"], item["end"], round(item["durationMs"] / 1000, 3), item["n"],
                item["maxAlt"], item["maxSpd"], item["minRssi"], item["maxRssi"],
            ])
        return "\ufeff" + out.getvalue()

    def track_geojson(self, flight_id):
        flight = self.get_flight(flight_id)
        if flight is None:
            return None
        points = flight.pop("points")
        coordinates = [[point["lon"], point["lat"]] for point in points]
        if len(coordinates) >= 2:
            geometry = {"type": "LineString", "coordinates": coordinates}
        elif coordinates:
            geometry = {"type": "Point", "coordinates": coordinates[0]}
        else:
            geometry = None
        return {"type": "Feature", "id": flight_id, "geometry": geometry, "properties": flight}


# ---------------------------------------------------------------
# 串口/演示数据源
# ---------------------------------------------------------------
def publish_snapshot(snapshot, raise_on_history_error=False):
    """Persist a raw station snapshot, then broadcast a merged live snapshot."""
    global LATEST_SNAPSHOT, LATEST_SNAPSHOT_AT, SNAPSHOT_SEQUENCE
    try:
        snapshot = normalize_snapshot(snapshot)
    except ValueError as exc:
        if raise_on_history_error:
            raise
        print(f"[ingest] 已拒绝无效快照: {exc}")
        return False
    try:
        accepted = HISTORY.ingest_snapshot(snapshot)
    except Exception as exc:
        print(f"[history] 写入失败: {exc}")
        if raise_on_history_error:
            raise
        accepted = False
    if not accepted:
        return False

    wall_now = int(time.time() * 1000)
    captured_at = _integer(snapshot.get("capturedAt"))
    if captured_at is not None and captured_at > wall_now:
        captured_at = wall_now
    # Backfilled FIFO data belongs in history but must not rewind the live map.
    if captured_at is not None and wall_now - captured_at > LIVE_MAX_AGE_MS:
        return True

    station_id = str(snapshot.get("stationId") or "local").strip()[:128] or "local"
    station_name = str(snapshot.get("stationName") or "").strip()[:256] or None
    with LOCK:
        STATION_LIVE[station_id] = {
            "id": station_id,
            "name": station_name,
            "receivedAt": wall_now,
            "capturedAt": captured_at or wall_now,
            "snapshot": snapshot,
        }
        SNAPSHOT_SEQUENCE += 1
        merged = _merged_snapshot_locked(wall_now, SNAPSHOT_SEQUENCE)
        text = json.dumps(merged, ensure_ascii=False, separators=(",", ":"))
        LATEST_SNAPSHOT = text
        LATEST_SNAPSHOT_AT = wall_now
    broadcast(text)
    return True


def _merged_snapshot_locked(now_ms, sequence=None):
    for station_id, state in list(STATION_LIVE.items()):
        if now_ms - state["receivedAt"] >= STATION_TIMEOUT_MS:
            del STATION_LIVE[station_id]

    selected = {}
    stations = []
    newest_state = None
    for state in STATION_LIVE.values():
        source = state["snapshot"]
        source_drones = source.get("drones") or []
        stations.append({
            "id": state["id"], "name": state["name"],
            "lastSeen": state["receivedAt"], "capturedAt": state["capturedAt"],
            "sourceType": source.get("sourceType"),
            "sourceTransport": source.get("sourceTransport"),
            "hardwareConnected": source.get("hardwareConnected") is True,
            "labLoopback": source.get("labLoopback") is True,
            "drones": len(source_drones),
        })
        if newest_state is None or state["receivedAt"] > newest_state["receivedAt"]:
            newest_state = state
        for raw in source_drones:
            if not isinstance(raw, dict) or not raw.get("mac"):
                continue
            drone = dict(raw)
            drone["mac"] = str(drone["mac"]).strip().upper()[:64]
            if not drone["mac"]:
                continue
            drone["stationId"] = state["id"]
            drone["stationName"] = state["name"]
            drone["observedAt"] = state["capturedAt"]
            current = selected.get(drone["mac"])
            new_rssi = _number(drone.get("rssi"))
            old_rssi = _number(current.get("rssi")) if current else None
            stronger = current is None or (new_rssi is not None and (old_rssi is None or new_rssi > old_rssi))
            if stronger or (new_rssi == old_rssi and state["capturedAt"] > current.get("observedAt", 0)):
                selected[drone["mac"]] = drone

    newest = newest_state["snapshot"] if newest_state else {}
    drones = list(selected.values())
    return {
        "t": "snap",
        "n": len(drones),
        "ch": newest.get("ch"),
        "bat": newest.get("bat", -1),
        "serverTime": now_ms,
        "serverId": SERVER_INSTANCE_ID,
        "seq": SNAPSHOT_SEQUENCE if sequence is None else sequence,
        "stationCount": len(stations),
        "hardwareConnected": any(item["hardwareConnected"] for item in stations),
        "labLoopback": any(item["labLoopback"] for item in stations),
        "stations": sorted(stations, key=lambda item: item["id"]),
        "drones": drones,
    }


def station_maintenance_worker():
    """Emit a new full snapshot when a station times out, even if every gateway is offline."""
    global LATEST_SNAPSHOT, LATEST_SNAPSHOT_AT, SNAPSHOT_SEQUENCE
    while True:
        time.sleep(1)
        now_ms = int(time.time() * 1000)
        with LOCK:
            before = len(STATION_LIVE)
            merged = _merged_snapshot_locked(now_ms)
            if len(STATION_LIVE) == before:
                continue
            SNAPSHOT_SEQUENCE += 1
            merged["seq"] = SNAPSHOT_SEQUENCE
            text = json.dumps(merged, ensure_ascii=False, separators=(",", ":"))
            LATEST_SNAPSHOT = text
            LATEST_SNAPSHOT_AT = now_ms
        broadcast(text)


def serial_worker(port: str, baud: int):
    import serial
    try:
        from serial.tools import list_ports
        available = [item.device for item in list_ports.comports()]
        print(f"[serial] 目标串口 {port} @ {baud}")
        if port not in available:
            print(f"[serial] ⚠ 未检测到 {port}! 当前可用串口: {available if available else '(无)'}")
            print("[serial]   请确认设备已插好，并用 --port 指定正确串口")
    except Exception:
        pass
    print("[serial] 等待/重试连接中... (Ctrl+C 退出)")
    while True:
        try:
            with serial.Serial(port, baud, timeout=1.0) as serial_port:
                print(f"[serial] 已连接 {port} (等待固件数据...)")
                while True:
                    line = serial_port.readline()
                    if not line:
                        continue
                    text = line.decode("utf-8", errors="ignore").strip()
                    if not text.startswith("{"):
                        continue
                    try:
                        snapshot = json.loads(text)
                    except json.JSONDecodeError:
                        continue
                    if snapshot.get("t") == "snap":
                        snapshot["sourceType"] = DEFAULT_INGEST_SOURCE_TYPE
                        snapshot["sourceTransport"] = (
                            "usb-serial-loopback" if snapshot.get("labLoopback") is True
                            else "usb-serial"
                        )
                        snapshot["hardwareConnected"] = True
                        publish_snapshot(snapshot)
        except serial.SerialException as exc:
            print(f"[serial] 连接失败: {exc} -- 3 秒后重试...")
            time.sleep(3)
        except Exception as exc:
            print(f"[serial] 异常: {exc}")
            time.sleep(1)


DEMO_DRONES = [
    # mac, model, uasId, lat, lon, alt, spd, heading, rssi
    ("AA:BB:CC:00:11:22", "DJI Mavic 3", "1581F45QK9C2D12", 39.9042, 116.4074, 120.5, 8.2, 30, -55),
    ("AA:BB:CC:33:44:55", "DJI Mini 5 Pro", "1581FANL1M5P000", 39.9137, 116.4320, 60.0, 5.1, 120, -58),
    ("AA:BB:CC:66:77:88", "DJI Mavic 3 Pro", "1581F67Q3PRO000", 39.8894, 116.3847, 85.0, 15.4, 210, -62),
    ("AA:BB:CC:99:AA:BB", "DJI Air 3S", "1581F895AIR3S00", 39.9280, 116.4124, 150.0, 9.8, 80, -67),
    ("AA:BB:CC:CC:DD:EE", "DJI Neo", "1581F8A1NEODR0N0", 39.8971, 116.4446, 30.0, 3.1, 350, -71),
    ("18:D7:93:6A:0B:0C", "道通无人机", "AUTEL-EVO2-0101", 39.8999, 116.3936, 95.0, 6.5, 160, -76),
    ("6C:DF:FB:EA:00:01", "飞米无人机", "FIMI-X8SE-0001", 39.9174, 116.4176, 40.0, 4.2, 20, -82),
    ("AA:BB:CC:FF:00:11", "DJI Inspire 3", "1581F578INSPIRE3", 39.9069, 116.3781, 200.0, 2.0, 270, -88),
]
DEMO_OPS = [
    (drone[3] + random.uniform(-0.005, 0.005), drone[4] + random.uniform(-0.005, 0.005))
    for drone in DEMO_DRONES
]
DEMO_STATE = [dict(lat=drone[3], lon=drone[4], rssi=drone[8]) for drone in DEMO_DRONES]


def demo_worker(interval: float):
    print(f"[demo] 演示模式: {len(DEMO_DRONES)} 架模拟无人机，每 {interval}s 推送")
    while True:
        snapshot = {
            "t": "snap", "n": len(DEMO_DRONES), "ch": random.choice([1, 6, 11]),
            "bat": random.randint(70, 92), "simulated": True,
            "sourceType": SERVER_SIMULATOR_SOURCE_TYPE,
            "sourceTransport": "process", "hardwareConnected": False, "drones": [],
        }
        for index, drone in enumerate(DEMO_DRONES):
            state = DEMO_STATE[index]
            heading = random.uniform(0, 360) * math.pi / 180
            state["lat"] += math.cos(heading) * 0.00008
            state["lon"] += math.sin(heading) * 0.00008
            state["rssi"] = max(-95, min(-40, state["rssi"] + random.randint(-2, 2)))
            operator = DEMO_OPS[index]
            snapshot["drones"].append({
                "mac": drone[0], "model": drone[1], "id": drone[2], "rssi": state["rssi"],
                "lat": round(state["lat"], 6), "lon": round(state["lon"], 6),
                "alt": drone[5], "spd": drone[6],
                "olat": round(operator[0], 6), "olon": round(operator[1], 6), "proto": 0,
                "simulated": True,
            })
        publish_snapshot(snapshot)
        time.sleep(interval)


# ---------------------------------------------------------------
# WebSocket 服务
# ---------------------------------------------------------------
async def ws_handler(ws, *args):
    request = getattr(ws, "request", None)
    headers = getattr(request, "headers", None) or getattr(ws, "request_headers", {})
    origin = headers.get("Origin", "")
    host = headers.get("Host", "")
    if origin:
        parsed_origin = urlparse(origin)
        origin_ok = parsed_origin.scheme in {"http", "https"} and bool(parsed_origin.hostname)
        if CLOUD_MODE:
            origin_ok = origin_ok and parsed_origin.netloc.lower() == host.lower()
        else:
            host_name = host.rsplit(":", 1)[0].strip("[]").lower()
            origin_ok = origin_ok and parsed_origin.hostname.lower() == host_name
        if not origin_ok:
            await ws.close(code=1008, reason="origin not allowed")
            return
    session = None
    if SESSION_SECRET:
        session = session_from_cookie(headers.get("Cookie", ""))
        if session is None:
            await ws.close(code=1008, reason="authentication required")
            return
    with LOCK:
        CLIENTS.add(ws)
        latest = LATEST_SNAPSHOT
        if latest is None:
            latest = json.dumps(
                _merged_snapshot_locked(int(time.time() * 1000)),
                ensure_ascii=False,
                separators=(",", ":"),
            )
        client_count = len(CLIENTS)
    print(f"[ws] 客户端接入 ({client_count} 个)")
    try:
        await ws.send(latest)
        if session:
            while True:
                remaining = max(0, session["exp"] - time.time())
                if remaining == 0:
                    await ws.close(code=1008, reason="session expired")
                    break
                try:
                    await asyncio.wait_for(ws.recv(), timeout=remaining)
                except asyncio.TimeoutError:
                    await ws.close(code=1008, reason="session expired")
                    break
        else:
            async for _ in ws:
                pass
    except Exception:
        pass
    finally:
        with LOCK:
            CLIENTS.discard(ws)
            client_count = len(CLIENTS)
        print(f"[ws] 客户端断开 ({client_count} 个)")


def broadcast(text: str):
    with LOCK:
        clients = list(CLIENTS)
        loop = MAIN_LOOP
    if not clients or loop is None or loop.is_closed():
        return
    for ws in clients:
        try:
            asyncio.run_coroutine_threadsafe(ws.send(text), loop)
        except Exception:
            pass


async def ws_server(host: str, port: int):
    global MAIN_LOOP
    import websockets
    MAIN_LOOP = asyncio.get_running_loop()
    async with websockets.serve(ws_handler, host, port):
        print(f"[ws] WebSocket 服务: ws://{host}:{port}")
        await asyncio.Future()


# ---------------------------------------------------------------
# HTTP 静态托管 + 历史 API
# ---------------------------------------------------------------
class Handler(SimpleHTTPRequestHandler):
    server_version = "RIDMonitor/2.0"
    sys_version = ""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=HERE, **kwargs)

    def log_message(self, fmt, *args):
        pass

    def end_headers(self):
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("X-Frame-Options", "DENY")
        self.send_header("Referrer-Policy", "no-referrer")
        self.send_header("Permissions-Policy", "camera=(), microphone=(), geolocation=()")
        self.send_header(
            "Content-Security-Policy",
            "default-src 'self'; object-src 'none'; base-uri 'self'; frame-ancestors 'none'; "
            "form-action 'self'; script-src 'self' 'unsafe-inline' 'unsafe-eval' https://*.amap.com; "
            "style-src 'self' 'unsafe-inline' https://*.amap.com https://*.alicdn.com; "
            "img-src 'self' data: blob: https://*.amap.com https://*.alicdn.com https://*.autonavi.com; "
            "font-src 'self' data: https://*.alicdn.com; "
            "worker-src 'self' blob:; "
            "connect-src 'self' ws: wss: https://*.amap.com https://*.autonavi.com",
        )
        super().end_headers()

    def _send_bytes(self, status, body, content_type, filename=None, headers=None):
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        if filename:
            self.send_header("Content-Disposition", f'attachment; filename="{filename}"')
        for key, value in (headers or {}).items():
            self.send_header(key, value)
        self.end_headers()
        self.wfile.write(body)

    def _send_json(self, status, payload, headers=None):
        body = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        self._send_bytes(status, body, "application/json; charset=utf-8", headers=headers)

    def _client_ip(self):
        real_ip = self.headers.get("X-Real-IP", "").strip()
        return real_ip or self.client_address[0]

    def _authenticated(self):
        return not SESSION_SECRET or session_from_cookie(self.headers.get("Cookie", "")) is not None

    def _read_json_body(self, max_bytes=128 * 1024):
        try:
            content_length = int(self.headers.get("Content-Length", ""))
        except ValueError:
            self._send_json(411, {"error": "content_length_required"})
            return None
        if content_length <= 0 or content_length > max_bytes:
            self._send_json(413, {"error": "payload_too_large"})
            return None
        try:
            return json.loads(self.rfile.read(content_length).decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            self._send_json(400, {"error": "invalid_json"})
            return None

    def _login(self):
        client_ip = self._client_ip()
        if _login_limited(client_ip):
            self._send_json(429, {"error": "too_many_attempts"}, {"Retry-After": "300"})
            return
        try:
            content_length = int(self.headers.get("Content-Length", ""))
        except ValueError:
            self._send_json(411, {"error": "content_length_required"})
            return
        if content_length <= 0 or content_length > 8192:
            self._send_json(413, {"error": "payload_too_large"})
            return
        try:
            credentials = json.loads(self.rfile.read(content_length).decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            self._send_json(400, {"error": "invalid_json"})
            return
        username = str(credentials.get("username") or "") if isinstance(credentials, dict) else ""
        password = str(credentials.get("password") or "") if isinstance(credentials, dict) else ""
        try:
            valid = (hmac.compare_digest(username.encode("utf-8"), (ADMIN_USER or "").encode("utf-8"))
                     and hmac.compare_digest(password.encode("utf-8"), (ADMIN_PASSWORD or "").encode("utf-8")))
        except UnicodeEncodeError:
            valid = False
        if not valid:
            _record_login_failure(client_ip)
            self._send_json(401, {"error": "invalid_credentials"})
            return
        _clear_login_failures(client_ip)
        token = create_session(username)
        cookie = (f"rid_session={token}; Path=/; HttpOnly; SameSite=Strict; "
                  f"Max-Age={SESSION_TTL_SECONDS}")
        if COOKIE_SECURE:
            cookie += "; Secure"
        self._send_json(200, {"ok": True, "user": username}, {"Set-Cookie": cookie})

    def _logout(self):
        cookie = "rid_session=; Path=/; HttpOnly; SameSite=Strict; Max-Age=0"
        if COOKIE_SECURE:
            cookie += "; Secure"
        self._send_json(200, {"ok": True}, {"Set-Cookie": cookie})

    def _proxy_uom_wms(self, params):
        request_params = dict(params or {})
        revision_values = request_params.pop("_ridv", None)
        token, credential_revision = _uom_wms_request_credential()
        if revision_values is not None:
            active_revision = credential_revision
            if (not isinstance(revision_values, list) or len(revision_values) != 1
                    or not isinstance(revision_values[0], str)
                    or not hmac.compare_digest(revision_values[0], active_revision)):
                self._send_json(409, {
                    "error": "stale_uom_wms_revision",
                    "message": "UOM WMS 凭据已更新，请重新加载图层",
                })
                return
        try:
            normalized = normalize_uom_wms_request(request_params)
        except ValueError as exc:
            self._send_json(400, {"error": "invalid_uom_wms_request", "message": str(exc)})
            return
        if not token:
            self._send_json(503, {
                "error": "uom_wms_not_configured",
                "message": "UOM WMS 服务端令牌尚未配置",
                "missing": ["RID_UOM_WMS_TOKEN"],
            })
            return
        try:
            image = fetch_uom_wms(normalized, token, credential_revision)
        except UomWmsUpstreamError as exc:
            _record_uom_wms_result(
                False, exc.upstream_status, exc.failure_kind, credential_revision,
            )
            payload = {
                "error": "uom_wms_upstream_error",
                "message": "UOM WMS 上游服务暂不可用或凭据已失效",
            }
            if exc.upstream_status is not None:
                payload["upstreamStatus"] = exc.upstream_status
            self._send_json(502, payload)
            return
        if not _record_uom_wms_result(
                True, credential_revision=credential_revision):
            self._send_json(409, {"error": "stale_uom_wms_revision"})
            return
        self._send_bytes(
            200, image, "image/png", headers={"Cache-Control": "private, no-store"},
        )

    def _api(self, parsed):
        path = parsed.path.rstrip("/") or "/"
        params = parse_qs(parsed.query, keep_blank_values=True)

        if path == "/api/auth/me":
            session = session_from_cookie(self.headers.get("Cookie", ""))
            self._send_json(200, {"authenticated": True,
                                  "user": session["u"] if session else (ADMIN_USER or "local")})
            return

        if path == "/api/status":
            status = HISTORY.status()
            with LOCK:
                now_ms = int(time.time() * 1000)
                station_details = []
                for state in sorted(STATION_LIVE.values(), key=lambda item: item["id"]):
                    captured_at = state["capturedAt"]
                    station_details.append({
                        "id": state["id"],
                        "name": state["name"],
                        "sourceType": state["snapshot"].get("sourceType"),
                        "sourceTransport": state["snapshot"].get("sourceTransport"),
                        "hardwareConnected": state["snapshot"].get("hardwareConnected") is True,
                        "labLoopback": state["snapshot"].get("labLoopback") is True,
                        "lastSeen": state["receivedAt"],
                        "capturedAt": captured_at,
                        "drones": len(state["snapshot"].get("drones") or []),
                        "latencyMs": max(0, state["receivedAt"] - captured_at),
                        "status": "online" if now_ms - state["receivedAt"] < STATION_TIMEOUT_MS else "offline",
                    })
                status.update({
                    "websocketClients": len(CLIENTS),
                    "latestSnapshotAt": LATEST_SNAPSHOT_AT,
                    "serverTime": now_ms,
                    "liveStations": len(STATION_LIVE),
                    "hardwareConnected": any(
                        state["snapshot"].get("hardwareConnected") is True
                        for state in STATION_LIVE.values()
                    ),
                    "labLoopback": any(
                        state["snapshot"].get("labLoopback") is True
                        for state in STATION_LIVE.values()
                    ),
                    "liveStationDetails": station_details,
                    "stationTimeoutSeconds": STATION_TIMEOUT_MS / 1000,
                    "cloudMode": CLOUD_MODE,
                    "ingestEnabled": bool(INGEST_TOKEN),
                    "authEnabled": bool(SESSION_SECRET),
                })
            self._send_json(200, status)
            return

        if path == "/api/airspace/status":
            self._send_json(200, HISTORY.airspace_status(airspace_sync_configuration()))
            return

        if path == "/api/airspace/catalog":
            self._send_json(200, airspace_catalog_summary())
            return

        if path == "/api/airspace/uom-status":
            self._send_json(200, uom_wms_status())
            return

        if path == "/api/airspace/uom-wms":
            self._proxy_uom_wms(params)
            return

        if path == "/api/airspace/zones":
            self._send_json(200, HISTORY.list_airspace_zones(params))
            return

        if path == "/api/geofences":
            self._send_json(200, HISTORY.list_geofences())
            return

        if path == "/api/incidents":
            self._send_json(200, HISTORY.list_incidents(params))
            return

        if path == "/api/incidents/export.csv":
            body = HISTORY.export_incidents_csv(params).encode("utf-8")
            self._send_bytes(200, body, "text/csv; charset=utf-8", "rid_incidents.csv")
            return

        match = re.fullmatch(r"/api/incidents/(\d+)/evidence\.json", path)
        if match:
            incident_id = int(match.group(1))
            body = HISTORY.get_incident_evidence_bytes(incident_id)
            if body is None:
                self._send_json(404, {"error": "incident_not_found"})
            else:
                self._send_bytes(
                    200, body, "application/json; charset=utf-8",
                    f"rid_incident_{incident_id}_evidence.json",
                    {"X-Evidence-SHA256": hashlib.sha256(body).hexdigest()},
                )
            return

        match = re.fullmatch(r"/api/incidents/(\d+)", path)
        if match:
            incident = HISTORY.get_incident(int(match.group(1)))
            if incident is None:
                self._send_json(404, {"error": "incident_not_found"})
            else:
                self._send_json(200, incident)
            return

        if path == "/api/flights":
            self._send_json(200, HISTORY.list_flights(params))
            return

        if path == "/api/flights/export.csv":
            body = HISTORY.export_csv(params).encode("utf-8")
            self._send_bytes(200, body, "text/csv; charset=utf-8", "rid_flights.csv")
            return

        match = re.fullmatch(r"/api/flights/(\d+)", path)
        if match:
            flight = HISTORY.get_flight(int(match.group(1)))
            if flight is None:
                self._send_json(404, {"error": "flight_not_found"})
            else:
                self._send_json(200, flight)
            return

        match = re.fullmatch(r"/api/flights/(\d+)/track\.geojson", path)
        if match:
            feature = HISTORY.track_geojson(int(match.group(1)))
            if feature is None:
                self._send_json(404, {"error": "flight_not_found"})
            else:
                self._send_json(200, feature)
            return

        self._send_json(404, {"error": "api_not_found"})

    def do_POST(self):
        parsed = urlparse(self.path)
        path = parsed.path.rstrip("/")
        if path == "/api/auth/login":
            if not SESSION_SECRET:
                self._send_json(503, {"error": "auth_not_configured"})
            else:
                self._login()
            return
        if path == "/api/auth/logout":
            self._logout()
            return
        if path == "/api/airspace/uom-token":
            if not self._authenticated():
                self._send_json(401, {"error": "authentication_required"})
                return
            if self.headers.get("Sec-Fetch-Site", "").strip().lower() == "cross-site":
                self._send_json(403, {"error": "cross_site_request_rejected"})
                return
            content_type = self.headers.get("Content-Type", "").split(";", 1)[0].strip().lower()
            if content_type != "application/json":
                self._send_json(415, {"error": "json_content_type_required"})
                return
            payload = self._read_json_body(4096)
            if payload is None:
                return
            try:
                result = store_uom_wms_token(
                    payload.get("token") if isinstance(payload, dict) else None
                )
            except ValueError as exc:
                self._send_json(400, {
                    "error": "invalid_uom_wms_token", "message": str(exc),
                })
                return
            except OSError as exc:
                print(f"[uom-wms] credential store failed: {type(exc).__name__}")
                self._send_json(500, {"error": "uom_wms_token_store_failed"})
                return
            self._send_json(200, result)
            return
        if path == "/api/airspace/import":
            if not self._authenticated():
                self._send_json(401, {"error": "authentication_required"})
                return
            payload = self._read_json_body(MAX_AIRSPACE_IMPORT_BYTES)
            if payload is None:
                return
            try:
                result = HISTORY.import_airspace(payload)
            except ValueError as exc:
                self._send_json(400, {"error": "invalid_airspace_import", "message": str(exc)})
                return
            except Exception as exc:
                print(f"[airspace] 导入失败: {exc}")
                self._send_json(500, {
                    "error": "airspace_import_failed", "message": "空域数据写入失败",
                })
                return
            self._send_json(200 if result["idempotent"] else 201, result)
            return
        if path == "/api/airspace/sync":
            if not self._authenticated():
                self._send_json(401, {"error": "authentication_required"})
                return
            payload = {}
            raw_length = self.headers.get("Content-Length", "").strip()
            if raw_length not in {"", "0"}:
                payload = self._read_json_body(16 * 1024)
                if payload is None:
                    return
                if not isinstance(payload, dict):
                    self._send_json(400, {"error": "invalid_airspace_sync", "message": "请求体必须是对象"})
                    return
            source_value = payload.get("source", "uom")
            if isinstance(source_value, dict):
                source_value = source_value.get("slug", "uom")
            config = airspace_sync_configuration()
            try:
                recorded = HISTORY.record_airspace_sync_unavailable(
                    config["status"], config["message"], source_value,
                )
            except ValueError as exc:
                self._send_json(400, {"error": "invalid_airspace_sync", "message": str(exc)})
                return
            error = ("airspace_sync_not_configured" if config["status"] == "unconfigured"
                     else "airspace_sync_adapter_unsupported")
            self._send_json(503, {
                "ok": False,
                "configured": config["configured"],
                "syncSupported": config["supported"],
                "error": error,
                "status": config["status"],
                "message": config["message"],
                "missing": config["missing"],
                "runId": recorded["runId"],
                "source": recorded["source"],
            })
            return
        if path == "/api/geofences":
            if not self._authenticated():
                self._send_json(401, {"error": "authentication_required"})
                return
            payload = self._read_json_body()
            if payload is None:
                return
            try:
                geofence = HISTORY.create_geofence(payload)
            except ValueError as exc:
                self._send_json(400, {"error": "invalid_geofence", "message": str(exc)})
                return
            self._send_json(201, geofence)
            return
        match = re.fullmatch(r"/api/incidents/(\d+)/status", path)
        if match:
            session = session_from_cookie(self.headers.get("Cookie", ""))
            if SESSION_SECRET and session is None:
                self._send_json(401, {"error": "authentication_required"})
                return
            payload = self._read_json_body(16 * 1024)
            if payload is None:
                return
            if not isinstance(payload, dict):
                self._send_json(400, {"error": "invalid_incident_action"})
                return
            try:
                incident = HISTORY.set_incident_status(
                    int(match.group(1)), payload.get("status"), payload.get("note"),
                    session["u"] if session else (ADMIN_USER or "local"),
                )
            except ValueError as exc:
                self._send_json(400, {"error": "invalid_incident_action", "message": str(exc)})
                return
            if incident is None:
                self._send_json(404, {"error": "incident_not_found"})
            else:
                self._send_json(200, incident)
            return
        if path != "/api/ingest":
            self._send_json(404, {"error": "api_not_found"})
            return
        if not INGEST_TOKEN:
            self._send_json(503, {"error": "ingest_not_configured"})
            return

        authorization = self.headers.get("Authorization", "")
        supplied = authorization[7:].strip() if authorization.lower().startswith("bearer ") else ""
        if not supplied:
            supplied = self.headers.get("X-Ingest-Token", "")
        if not hmac.compare_digest(supplied.encode("utf-8"), INGEST_TOKEN.encode("utf-8")):
            self._send_json(401, {"error": "unauthorized"})
            return

        try:
            content_length = int(self.headers.get("Content-Length", ""))
        except ValueError:
            self._send_json(411, {"error": "content_length_required"})
            return
        if content_length <= 0 or content_length > MAX_INGEST_BYTES:
            self._send_json(413, {"error": "payload_too_large"})
            return

        try:
            snapshot = json.loads(self.rfile.read(content_length).decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            self._send_json(400, {"error": "invalid_json"})
            return
        if (not isinstance(snapshot, dict) or snapshot.get("t") != "snap"
                or not isinstance(snapshot.get("drones"), list)):
            self._send_json(400, {"error": "invalid_snapshot"})
            return
        if len(snapshot["drones"]) > 1024:
            self._send_json(400, {"error": "too_many_drones"})
            return

        source_type = _clean_text(snapshot.get("sourceType"), 48)
        user_agent = self.headers.get("User-Agent", "")
        if source_type == SERVER_SIMULATOR_SOURCE_TYPE or user_agent.startswith(SERVER_SIMULATOR_UA):
            self._send_json(403, {"error": "server_simulator_disabled"})
            return
        simulated_payload = snapshot.get("simulated") is True or snapshot.get("labLoopback") is True or any(
            isinstance(item, dict) and item.get("simulated") is True
            for item in snapshot.get("drones", [])
        )
        if simulated_payload and snapshot.get("hardwareConnected") is not True:
            self._send_json(403, {"error": "hardware_source_required"})
            return

        try:
            accepted = publish_snapshot(snapshot, raise_on_history_error=True)
        except ValueError:
            self._send_json(400, {"error": "invalid_snapshot"})
            return
        except Exception:
            self._send_json(500, {"error": "history_write_failed"})
            return
        self._send_json(202, {
            "ok": True,
            "accepted": accepted,
            "receivedAt": LATEST_SNAPSHOT_AT,
            "drones": len(snapshot["drones"]),
        })

    def do_PUT(self):
        parsed = urlparse(self.path)
        path = parsed.path.rstrip("/")
        match = re.fullmatch(r"/api/geofences/(\d+)", path)
        if match is None:
            self._send_json(404, {"error": "api_not_found"})
            return
        if not self._authenticated():
            self._send_json(401, {"error": "authentication_required"})
            return
        payload = self._read_json_body()
        if payload is None:
            return
        try:
            geofence = HISTORY.update_geofence(int(match.group(1)), payload)
        except ValueError as exc:
            self._send_json(400, {"error": "invalid_geofence", "message": str(exc)})
            return
        if geofence is None:
            self._send_json(404, {"error": "geofence_not_found"})
        else:
            self._send_json(200, geofence)

    def do_DELETE(self):
        parsed = urlparse(self.path)
        path = parsed.path.rstrip("/")
        match = re.fullmatch(r"/api/geofences/(\d+)", path)
        if match is None:
            self._send_json(404, {"error": "api_not_found"})
            return
        if not self._authenticated():
            self._send_json(401, {"error": "authentication_required"})
            return
        if HISTORY.delete_geofence(int(match.group(1))):
            self._send_json(200, {"ok": True})
        else:
            self._send_json(404, {"error": "geofence_not_found"})

    def do_GET(self):
        parsed = urlparse(self.path)
        if parsed.path == "/healthz":
            self._send_json(200, {"ok": True, "serverTime": int(time.time() * 1000)})
            return
        if parsed.path == "/runtime-config.js":
            ws_path = os.environ.get("RID_WS_PATH", "/ws").strip() or "/ws"
            if not ws_path.startswith("/"):
                ws_path = "/" + ws_path
            monitor_lat, monitor_lon = _monitor_location()
            config = {
                "amapKey": os.environ.get("AMAP_KEY", ""),
                "amapSecurityCode": os.environ.get("AMAP_SECURITY_CODE", ""),
                "wsPath": ws_path,
                "wsPort": None if CLOUD_MODE else WS_PORT,
                "apiBase": "/api",
                "monitorLat": monitor_lat,
                "monitorLon": monitor_lon,
            }
            script = ("window.RID_RUNTIME_CONFIG="
                      + json.dumps(config, ensure_ascii=False, separators=(",", ":"))
                      + ";\n")
            self._send_bytes(200, script.encode("utf-8"), "application/javascript; charset=utf-8")
            return
        if parsed.path == "/api" or parsed.path.startswith("/api/"):
            if not self._authenticated():
                self._send_json(401, {"error": "authentication_required"})
                return
            try:
                self._api(parsed)
            except ValueError as exc:
                self._send_json(400, {"error": "invalid_query", "message": str(exc)})
            except Exception as exc:
                print(f"[http] API 异常: {exc}")
                self._send_json(500, {"error": "internal_error"})
            return
        if parsed.path == "/":
            self.send_response(302)
            self.send_header("Location", "/dashboard.html")
            self.end_headers()
            return
        if parsed.path in {"/dashboard.html", "/demo.html"}:
            self.path = parsed.path
            super().do_GET()
            return
        self._send_json(404, {"error": "not_found"})

    def do_HEAD(self):
        parsed = urlparse(self.path)
        if parsed.path == "/":
            self.send_response(302)
            self.send_header("Location", "/dashboard.html")
            self.end_headers()
            return
        if parsed.path in {"/dashboard.html", "/demo.html"}:
            self.path = parsed.path
            super().do_HEAD()
            return
        self.send_response(404)
        self.send_header("Content-Length", "0")
        self.end_headers()

class BoundedThreadingHTTPServer(ThreadingHTTPServer):
    """Bound request threads so a tile burst cannot exhaust the container PID cap."""

    daemon_threads = True
    request_queue_size = 128

    def __init__(self, server_address, request_handler_class, max_workers=HTTP_MAX_WORKERS):
        self._worker_slots = threading.BoundedSemaphore(max_workers)
        super().__init__(server_address, request_handler_class)

    def process_request(self, request, client_address):
        self._worker_slots.acquire()
        try:
            super().process_request(request, client_address)
        except BaseException:
            self._worker_slots.release()
            raise

    def process_request_thread(self, request, client_address):
        try:
            super().process_request_thread(request, client_address)
        finally:
            self._worker_slots.release()


def http_server(host: str, port: int):
    server = BoundedThreadingHTTPServer((host, port), Handler)
    print(f"[http] 大屏页面: http://localhost:{port}/dashboard.html")
    print(f"[http] 历史 API: http://localhost:{port}/api/flights")
    server.serve_forever()


def main():
    global ADMIN_PASSWORD, ADMIN_USER, CLOUD_MODE, COOKIE_SECURE, LIVE_MAX_AGE_MS
    global HISTORY, INGEST_TOKEN, SESSION_SECRET, SESSION_TTL_SECONDS, STATION_TIMEOUT_MS, WS_PORT
    global UOM_WMS_TOKEN_PATH
    parser = argparse.ArgumentParser(description="RID 监测智慧大屏数据服务")
    parser.add_argument("--port", default="COM5", help="串口号 (默认 COM5)")
    parser.add_argument("--baud", type=int, default=115200, help="波特率 (默认 115200)")
    parser.add_argument("--http", type=int, default=8080, help="页面 HTTP 端口 (默认 8080)")
    parser.add_argument("--ws", type=int, default=8765, help="WebSocket 端口 (默认 8765)")
    parser.add_argument("--bind", default="127.0.0.1", help="HTTP/WS 监听地址 (默认 127.0.0.1)")
    parser.add_argument("--demo", action="store_true", help="演示模式(无串口，模拟无人机数据)")
    parser.add_argument("--cloud", action="store_true", help="云端接收模式，不打开本地串口")
    parser.add_argument("--ingest-token", default=os.environ.get("RID_INGEST_TOKEN"),
                        help="云端上报 token；也可使用 RID_INGEST_TOKEN 环境变量")
    parser.add_argument("--admin-user", default=os.environ.get("RID_ADMIN_USER", "admin"),
                        help="大屏服务端登录用户名 (默认 admin)")
    parser.add_argument("--admin-password", default=os.environ.get("RID_ADMIN_PASSWORD"),
                        help="大屏服务端登录密码；推荐用 RID_ADMIN_PASSWORD")
    parser.add_argument("--session-secret", default=os.environ.get("RID_SESSION_SECRET"),
                        help="Cookie 签名密钥；推荐用 RID_SESSION_SECRET")
    parser.add_argument("--session-ttl", type=int,
                        default=int(os.environ.get("RID_SESSION_TTL", str(12 * 60 * 60))),
                        help="登录会话有效秒数 (默认 12 小时)")
    parser.add_argument("--cookie-secure", action="store_true",
                        default=os.environ.get("RID_COOKIE_SECURE", "").lower() in {"1", "true", "yes"},
                        help="给登录 Cookie 增加 Secure；HTTPS 部署必须启用")
    parser.add_argument("--interval", type=float, default=1.0, help="演示推送间隔秒 (默认 1.0)")
    parser.add_argument("--db", default=DEFAULT_DB, help="SQLite 历史库路径")
    parser.add_argument("--flight-gap", type=float, default=15.0,
                        help="信号中断多久后结束一次飞行会话，单位秒 (默认 15)")
    parser.add_argument("--point-distance", type=float, default=2.0,
                        help="移动至少多少米写入新航点 (默认 2)")
    parser.add_argument("--point-interval", type=float, default=5.0,
                        help="即使静止也至少多久写一个航点，单位秒 (默认 5)")
    parser.add_argument("--retention-days", type=int, default=30,
                        help="历史保留天数，0 表示不自动删除 (默认 30)")
    parser.add_argument("--station-timeout", type=float, default=15.0,
                        help="实时层多久未收到哨站数据后剔除，单位秒 (默认 15)")
    parser.add_argument("--live-max-age", type=float, default=30.0,
                        help="补传数据超过多久只入历史、不进入实时层，单位秒 (默认 30)")
    args = parser.parse_args()

    if args.interval <= 0:
        parser.error("--interval 必须大于 0")
    if args.flight_gap <= 0:
        parser.error("--flight-gap 必须大于 0")
    if args.point_distance < 0 or args.point_interval <= 0:
        parser.error("航点采样参数无效")
    if args.retention_days < 0:
        parser.error("--retention-days 不能小于 0")
    if args.station_timeout <= 0 or args.live_max_age <= 0:
        parser.error("实时哨站超时参数必须大于 0")
    if args.cloud and args.demo:
        parser.error("--cloud 与 --demo 不能同时使用")
    if args.cloud and not args.ingest_token:
        parser.error("--cloud 必须配置 --ingest-token 或 RID_INGEST_TOKEN")
    if bool(args.admin_password) != bool(args.session_secret):
        parser.error("RID_ADMIN_PASSWORD 与 RID_SESSION_SECRET 必须同时配置")
    if args.cloud and (not args.admin_password or not args.session_secret):
        parser.error("--cloud 必须配置 RID_ADMIN_PASSWORD 与 RID_SESSION_SECRET")
    if args.cloud and len(args.ingest_token) < 24:
        parser.error("RID_INGEST_TOKEN 至少 24 个字符")
    if args.admin_password and len(args.admin_password) < 6:
        parser.error("RID_ADMIN_PASSWORD 至少 6 个字符")
    if args.session_secret and len(args.session_secret) < 32:
        parser.error("RID_SESSION_SECRET 至少 32 个字符")
    if args.session_ttl < 300:
        parser.error("--session-ttl 不能少于 300 秒")
    if args.cloud and not args.cookie_secure:
        parser.error("--cloud 必须启用 RID_COOKIE_SECURE=1 / --cookie-secure")
    loopback = args.bind in {"127.0.0.1", "::1", "localhost"}
    if not args.cloud and not args.admin_password and not loopback:
        parser.error("非回环监听必须显式配置 RID_ADMIN_PASSWORD 与 RID_SESSION_SECRET")
    local_secret = None
    if not args.admin_password:
        args.admin_user = "admin"
        args.admin_password = "admin123"
        local_secret = secrets.token_bytes(32)

    CLOUD_MODE = args.cloud
    INGEST_TOKEN = args.ingest_token
    ADMIN_USER = args.admin_user
    ADMIN_PASSWORD = args.admin_password
    SESSION_SECRET = args.session_secret.encode("utf-8") if args.session_secret else local_secret
    SESSION_TTL_SECONDS = args.session_ttl
    COOKIE_SECURE = args.cookie_secure
    WS_PORT = args.ws
    STATION_TIMEOUT_MS = int(args.station_timeout * 1000)
    LIVE_MAX_AGE_MS = int(args.live_max_age * 1000)
    configured_token_path = os.environ.get(UOM_WMS_TOKEN_FILE_ENV, "").strip()
    UOM_WMS_TOKEN_PATH = os.path.abspath(
        configured_token_path
        or os.path.join(os.path.dirname(os.path.abspath(args.db)), "uom_wms_token")
    )
    HISTORY = HistoryStore(
        args.db, args.flight_gap, args.point_distance, args.point_interval, args.retention_days,
        seed_builtin_airspace=True, seed_packaged_airspace=True,
    )
    print("[airspace] 已确保北京规则参考与 UOM WMS 派生全国快照存在")
    print("=" * 60)
    print("  RID 监测智慧大屏 · 数据服务")
    print("=" * 60)
    print(f"[history] SQLite: {HISTORY.path}")

    workers = []
    if args.cloud:
        print("[cloud] 已启用 token 认证的 POST /api/ingest")
    elif args.demo:
        workers.append(threading.Thread(target=demo_worker, args=(args.interval,), daemon=True))
    else:
        workers.append(threading.Thread(target=serial_worker, args=(args.port, args.baud), daemon=True))
    workers.append(threading.Thread(target=http_server, args=(args.bind, args.http), daemon=True))
    workers.append(threading.Thread(target=station_maintenance_worker, daemon=True))
    for worker in workers:
        worker.start()

    try:
        asyncio.run(ws_server(args.bind, args.ws))
    except KeyboardInterrupt:
        print("\n已停止。")
    finally:
        HISTORY.finish_all()


if __name__ == "__main__":
    main()
