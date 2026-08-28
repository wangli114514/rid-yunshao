#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
智慧大屏页面自检脚本
=====================
	对本仓库页面做静态断言 + 内联 JS 语法检查,
确认关键功能(登录门控/风险事件/地理围栏/轨迹回放/高德修复)未在编辑中丢失。

用法:
    python verify.py          # 全部检查, 全绿退出码 0

依赖: 无(仅标准库); JS 语法检查需要 node(可选, 无 node 时跳过)
"""
import os
import re
import subprocess
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
NODE_CANDIDATES = [
    r"C:\Users\19389\AppData\Local\hermes\node\node.exe",
    "node",
]
fails = []


def check(name, cond, extra=""):
    print(("  ok: " if cond else "FAIL: ") + name + ((" " + extra) if extra else ""))
    if not cond:
        fails.append(name)


def js_syntax(label, html):
    """提取内联 <script> 块, 逐个 node --check 语法检查(无 node 则跳过)"""
    def available(candidate):
        try:
            return subprocess.run([candidate, "--version"], capture_output=True).returncode == 0
        except OSError:
            return False

    node = next((n for n in NODE_CANDIDATES if available(n)), None)
    if node is None:
        print("  -- 未找到 node, 跳过 JS 语法检查 --")
        return
    scripts = re.findall(r"<script>(.*?)</script>", html, re.S)
    ok = True
    for i, js in enumerate(scripts):
        if not js.strip():
            continue
        fd, path = tempfile_path(".js")
        try:
            open(path, "w", encoding="utf-8").write(js)
            r = subprocess.run([node, "--check", path], capture_output=True, text=True, timeout=30)
            if r.returncode != 0:
                check("%s script#%d JS 语法" % (label, i + 1), False, r.stderr.strip()[:150])
                ok = False
        finally:
            os.remove(path)
    check("%s 内联 JS 语法(%d 块)" % (label, len(scripts)), ok)


def tempfile_path(suffix):
    import tempfile
    fd, path = tempfile.mkstemp(suffix=suffix, prefix="hermes-verify-")
    os.close(fd)
    return fd, path


def main():
    dash = open(os.path.join(HERE, "dashboard.html"), encoding="utf-8").read()
    demo = open(os.path.join(HERE, "demo.html"), encoding="utf-8").read()
    srv = open(os.path.join(HERE, "server.py"), encoding="utf-8").read()
    gateway = open(os.path.join(HERE, "gateway.py"), encoding="utf-8").read()

    print("== dashboard.html ==")
    check("登录 DOM 齐全", all('id="%s"' % x in dash for x in
                               ("loginOverlay", "loginUser", "loginPass", "loginBtn", "loginErr")))
    check("服务端 Cookie 登录/恢复/退出",
          all(x in dash for x in ('/auth/login', '/auth/me', '/auth/logout',
                                  'credentials:"same-origin"')) and
          "const AUTH" not in dash and 'sessionStorage.getItem("rid_auth")' not in dash)
    check("鉴权成功后才连数据",
          "connectWS();" in dash.split("function enterApp")[1].split("function connectWS")[0] and
          dash.count("connectWS();") == 1)
    check("运行时配置(地图/API/WS)",
          'src="/runtime-config.js"' in dash and
          all(x in dash for x in ("RID_RUNTIME_CONFIG", "amapSecurityCode", "API_BASE", "wsPath")))
    check("WS 同源 /ws + 本地 ?ws= 兼容",
          'RID_CONFIG.wsPath || "/ws"' in dash and
          'new URLSearchParams(location.search).get("ws")' in dash and
          'location.host' in dash)
    check("登录后才加载高德 + onload 初始化地图",
          "loadAMapScript();" in dash.split("function enterApp")[1].split("function connectWS")[0] and
          "s.onload = () => initMap();" in dash)
    check("高德配置缺失检测(key/jscode)", "amapCfgIssue" in dash and 'return "jscode"' in dash)
    check("WS hostname 兜底(file:// 修复)", 'location.hostname || "localhost"' in dash)
    check("WS 失败指引", "python server.py --port COMx" in dash)
    check("WS 会话过期与快照防倒序",
          'event.code === 1008' in dash and "lastSnapshotServerId" in dash and
          "sequence <= lastSnapshotSeq" in dash)
    check("遥测输出转义 + BLE 协议",
          '${esc(d.rssi ?? "--")}' in dash and 'data-mac="${esc(mac)}"' in dash and
          'proto === 2 ? "BLE"' in dash)
    check("警报核心(检测/触发/红闪/声音/按钮)",
          all(x in dash for x in ("checkAlerts", "triggerAlert", "stopAlert",
                                  'body.classList.add("alert")', "playAlarm",
                                  'getElementById("alertAuth").onclick')) and
          "#alertBanner{position:relative;flex:none" in dash and
          "#alertBanner{position:fixed;left:7px" in dash)
    check("白名单(持久化/增删/UI)",
          'localStorage.getItem("rid_whitelist")' in dash and
          all(x in dash for x in ("whitelistAdd", "whitelistRemove",
                                  'id="wlAdd"', 'id="wlRemove"', 'class="shield"')))
    check("高德容错 + fitView 无参修复", "amapTry" in dash and "map.setFitView()" in dash)
    check("无 Key Canvas 态势图",
          all(x in dash for x in ('id="mapCanvas"', "drawCanvasMap", "canvasRecordPath",
                                  "canvasReplayPoint", "initCanvasMap")))
    check("云端历史筛选/分页/降级",
          all(x in dash for x in ('"/flights?"', "historyParams", "localFlightRows",
                                  "historyStation", "changeHistoryPage")))
    check("事件中心/处置/证据完整性",
          all(x in dash for x in ('id="securityView"', 'id="incidentRows"',
                                  "loadIncidents", "updateIncidentStatus",
                                  "downloadIncidentEvidence", "crypto.subtle.digest",
                                  "X-Evidence-SHA256", 'id="incidentModal"')))
    check("围栏 CRUD + 双地图绘制",
          all(x in dash for x in ('id="geofenceForm"', "loadGeofences",
                                  "saveGeofence", "toggleGeofence", "deleteGeofence",
                                  "drawCanvasGeofences", "new AMap.Circle",
                                  "new AMap.Polygon")))
    check("独立地图圈选 + WGS/GCJ 回填",
          all(x in dash for x in ('id="geofenceMapModal"', "openGeofenceMapEditor",
                                  "undoGeofenceMapEdit", "clearGeofenceMapEdit",
                                  "cancelGeofenceMapEditor", "finishGeofenceMapEditor",
                                  "AMap.CircleEditor", "AMap.PolygonEditor", "gcjToWgs")) and
          "100dvh" in dash and "geofence-map-open" in dash)
    check("空域参考状态/导入/bbox 双地图图层",
          all(x in dash for x in ('id="toggleAirspace"', 'id="airspaceAdmin"',
                                  'id="airspaceImportFile"', "loadAirspaceStatus",
                                  "importAirspaceFile", "syncUomAirspace",
                                  "airspaceZonesPath", "drawCanvasAirspace",
                                  "registerAirspaceMapSurface", "coverageCatalog",
                                  "airspaceCatalogSummary")) and
          all(x in dash for x in ("UOM 实时适飞栅格", "非适飞 / 需核验",
                                  "UOM 实时适飞栅格：服务端未配置")))
    check("实时风险研判字段",
          all(x in dash for x in ("riskScoreOf", "riskLevelOf", "riskReasons",
                                  "geofenceIds", "incidentId", "riskPillHTML")))
    check("回放暂停/拖动/倍速",
          all(x in dash for x in ('id="replaySeek"', 'id="replaySpeed"',
                                  "toggleReplay", "setReplayPoint", "setReplaySpeed")))
    check("移动端底栏/抽屉/档案卡片",
          all(x in dash for x in ("MOBILE_LAYOUT_QUERY", 'id="mobileLiveActions"',
                                  'id="mobileSheetBackdrop"', "openMobileSheet",
                                  "historyFilterToggle", ".flightTable tbody tr{display:grid",
                                  "syncMobileAlertLayout", "--mobile-alert-actions-bottom",
                                  "selectionChanged", 'detailBody").scrollTop = 0',
                                  ".historyHead{position:absolute;z-index:520")))
    check("地图触控与手动浏览保护",
          all(x in dash for x in ("dragEnable: true", "touchZoom: true",
                                  "suspendAutoFollow", 'map.on("dragstart",',
                                  'mapCanvas.addEventListener("pointermove"',
                                  'mapCanvas.addEventListener("wheel"', "zoomCanvasAt",
                                  "panCanvasByPixels", "touch-action:none")))
    js_syntax("dash", dash)

    print("== demo.html ==")
    check("登录 DOM + AUTH", all('id="%s"' % x in demo for x in
                                 ("loginOverlay", "loginUser", "loginPass", "loginBtn")) and
          'pass: "admin123"' in demo)
    check("登录门控(load 不直接启动)", "enterApp();" in demo.split("window.addEventListener(\"load\"")[1] and
          "tickData();" not in demo.split("window.addEventListener(\"load\"")[1])
    check("警报全套(与正式版一致)", all(x in demo for x in
                                        ("checkAlerts", "triggerAlert", "stopAlert", "playAlarm")))
    check("tickData 接入警报", "checkAlerts(new Set(drones.keys()))" in demo)
    check("白名单 UI", all(x in demo for x in ("whitelistAdd", "whitelistRemove",
                                               'id="wlAdd"', 'id="wlRemove"', 'class="shield"')))
    check("高德分支保留", all(x in demo for x in ("tryInitAMap", "amap.setFitView()")))
    js_syntax("demo", demo)

    print("== server.py ==")
    check("列可用 COM 端口", "list_ports.comports()" in srv)
    check("未检测到端口提示", "未检测到" in srv and "当前可用串口" in srv)
    check("重试逻辑", "3 秒后重试" in srv)
    check("SQLite 历史 + 轨迹表", all(x in srv for x in
          ("CREATE TABLE IF NOT EXISTS flights", "CREATE TABLE IF NOT EXISTS track_points")))
    check("SQLite 围栏/事件/处置表", all(x in srv for x in
          ("CREATE TABLE IF NOT EXISTS geofences", "CREATE TABLE IF NOT EXISTS incidents",
           "CREATE TABLE IF NOT EXISTS incident_actions")))
    check("历史 API + 云端鉴权上报", all(x in srv for x in
          ('/api/flights', '/api/status', '/api/ingest', 'Authorization', 'compare_digest')))
    check("航点采样 + 保留策略", all(x in srv for x in
          ("min_point_meters", "max_point_interval_ms", "retention_days", "def prune")))
    check("动态运行配置", "/runtime-config.js" in srv and "AMAP_SECURITY_CODE" in srv)
    check("云端遥测规范化 + 有序快照",
          "def normalize_snapshot" in srv and "MAC_PATTERN.fullmatch" in srv and
          "SERVER_INSTANCE_ID" in srv and "SNAPSHOT_SEQUENCE" in srv)
    check("服务端风险/围栏/事件 API",
          all(x in srv for x in ("normalize_geofence_payload", "_evaluate_drone_locked",
                                 "list_incidents", "set_incident_status",
                                 '"/api/geofences"', '"/api/incidents"')))
    check("空域版本库/导入/查询/同步状态 API",
          all(x in srv for x in ("CREATE TABLE IF NOT EXISTS airspace_sources",
                                 "CREATE TABLE IF NOT EXISTS airspace_datasets",
                                 "CREATE TABLE IF NOT EXISTS airspace_zones",
                                 "CREATE TABLE IF NOT EXISTS airspace_sync_runs",
                                 "normalize_airspace_import_payload", "import_airspace",
                                 "list_airspace_zones", "airspace_status",
                                 '"/api/airspace/import"', '"/api/airspace/sync"')))
    check("空域来源边界 + UOM 不伪连接",
          all(x in srv for x in ("sync_mode", "manual_import", "authorized_sync",
                                 "authoritative", "airspace_sync_adapter_unsupported",
                                 "未发起网络请求", "flyableAirspace")))
    check("全国空域台账 + 北京 110000 参考面",
          all(x in srv for x in ("airspace_catalog.json", "airspace_catalog_summary",
                                 "builtin_beijing_airspace_payload", "ensure_builtin_airspace",
                                 '"/api/airspace/catalog"', "seed_builtin_airspace")))
    check("证据原文 SHA-256 下载",
          "X-Evidence-SHA256" in srv and "evidence_bytes" in srv and
          'evidence.json' in srv and "hashlib.sha256" in srv)
    check("python 语法", subprocess.run([sys.executable, "-m", "py_compile",
                                         os.path.join(HERE, "server.py")],
                                        capture_output=True).returncode == 0)

    print("== gateway.py ==")
    check("串口/标准输入测试源", "collect_serial" in gateway and '"--stdin"' in gateway)
    check("持久化断网队列", "pending_snapshots" in gateway and "class Spool" in gateway)
    check("HTTPS Bearer 上传", "Authorization" in gateway and "urlopen" in gateway)
    check("RF-disabled lab loopback contract",
          "labLoopback" in gateway and "usb-serial-loopback" in gateway and
          "labLoopback" in srv and "RF-disabled" in open(
              os.path.join(HERE, "LAB_LOOPBACK.md"), encoding="utf-8").read())
    check("lab controller python syntax", subprocess.run([sys.executable, "-m", "py_compile",
                                                           os.path.join(HERE, "lab_replay.py")],
                                                          capture_output=True).returncode == 0)
    check("gateway python 语法", subprocess.run([sys.executable, "-m", "py_compile",
                                                 os.path.join(HERE, "gateway.py")],
                                                capture_output=True).returncode == 0)

    print("\n" + ("全部通过" if not fails else "失败项: " + ", ".join(fails)))
    sys.exit(0 if not fails else 1)


if __name__ == "__main__":
    main()
