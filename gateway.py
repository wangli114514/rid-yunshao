#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Local RID gateway: COM JSON snapshots -> authenticated cloud HTTPS ingest.

Snapshots are first committed to a bounded SQLite spool. Network, DNS, TLS, or
cloud outages therefore do not block serial collection and survive a PC restart.
"""
import argparse
import json
import os
import socket
import sqlite3
import ssl
import sys
import threading
import time
import urllib.error
import urllib.request
from contextlib import contextmanager

HERE = os.path.dirname(os.path.abspath(__file__))
DEFAULT_SPOOL = os.path.join(HERE, "data", "gateway_spool.db")


class Spool:
    def __init__(self, path, max_items=10000):
        self.path = os.path.abspath(path)
        self.max_items = max(100, int(max_items))
        self._lock = threading.RLock()
        os.makedirs(os.path.dirname(self.path), exist_ok=True)
        with self._db() as conn:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS pending_snapshots (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    created_at INTEGER NOT NULL,
                    payload TEXT NOT NULL,
                    attempts INTEGER NOT NULL DEFAULT 0,
                    next_attempt_at INTEGER NOT NULL DEFAULT 0,
                    last_error TEXT
                )
                """
            )

    @contextmanager
    def _db(self):
        conn = sqlite3.connect(self.path, timeout=10)
        try:
            conn.execute("PRAGMA busy_timeout=5000")
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def enqueue(self, snapshot):
        payload = json.dumps(snapshot, ensure_ascii=False, separators=(",", ":"))
        now_ms = int(time.time() * 1000)
        with self._lock, self._db() as conn:
            cursor = conn.execute(
                "INSERT INTO pending_snapshots(created_at,payload) VALUES (?,?)",
                (now_ms, payload),
            )
            count = conn.execute("SELECT COUNT(*) FROM pending_snapshots").fetchone()[0]
            dropped = max(0, count - self.max_items)
            if dropped:
                conn.execute(
                    "DELETE FROM pending_snapshots WHERE id IN "
                    "(SELECT id FROM pending_snapshots ORDER BY id LIMIT ?)",
                    (dropped,),
                )
            return cursor.lastrowid, count - dropped, dropped

    def first(self):
        with self._lock, self._db() as conn:
            row = conn.execute(
                "SELECT id,payload,attempts,next_attempt_at FROM pending_snapshots ORDER BY id LIMIT 1"
            ).fetchone()
        if row is None:
            return None
        return {"id": row[0], "payload": row[1], "attempts": row[2], "next": row[3]}

    def acknowledge(self, item_id):
        with self._lock, self._db() as conn:
            conn.execute("DELETE FROM pending_snapshots WHERE id=?", (item_id,))

    def retry(self, item_id, attempts, delay_seconds, error):
        next_at = int((time.time() + delay_seconds) * 1000)
        with self._lock, self._db() as conn:
            conn.execute(
                "UPDATE pending_snapshots SET attempts=?,next_attempt_at=?,last_error=? WHERE id=?",
                (attempts, next_at, str(error)[:500], item_id),
            )

    def count(self):
        with self._lock, self._db() as conn:
            return conn.execute("SELECT COUNT(*) FROM pending_snapshots").fetchone()[0]


class CloudSender(threading.Thread):
    def __init__(self, spool, url, token, timeout=10.0, ca_file=None):
        super().__init__(daemon=True)
        self.spool = spool
        self.url = url
        self.token = token
        self.timeout = timeout
        self.wakeup = threading.Event()
        self.context = ssl.create_default_context(cafile=ca_file)
        self.sent = 0

    def notify(self):
        self.wakeup.set()

    def _post(self, payload):
        request = urllib.request.Request(
            self.url,
            data=payload.encode("utf-8"),
            method="POST",
            headers={
                "Authorization": "Bearer " + self.token,
                "Content-Type": "application/json",
                "User-Agent": "RID-Gateway/2.0",
            },
        )
        with urllib.request.urlopen(request, timeout=self.timeout, context=self.context) as response:
            if response.status < 200 or response.status >= 300:
                raise RuntimeError(f"HTTP {response.status}")
            response.read(4096)

    def run(self):
        while True:
            item = self.spool.first()
            if item is None:
                self.wakeup.wait(2)
                self.wakeup.clear()
                continue

            wait_ms = item["next"] - int(time.time() * 1000)
            if wait_ms > 0:
                self.wakeup.wait(min(wait_ms / 1000, 5))
                self.wakeup.clear()
                continue

            try:
                self._post(item["payload"])
                self.spool.acknowledge(item["id"])
                self.sent += 1
                if self.sent == 1 or self.sent % 60 == 0:
                    print(f"[cloud] 上传成功，累计 {self.sent}，待传 {self.spool.count()}")
            except urllib.error.HTTPError as exc:
                # Bad payload cannot recover; keep auth/rate/server failures for operator repair/retry.
                if exc.code in {400, 404, 413, 422}:
                    print(f"[cloud] 丢弃云端拒绝的快照 #{item['id']}: HTTP {exc.code}")
                    self.spool.acknowledge(item["id"])
                    continue
                attempts = item["attempts"] + 1
                delay = 60 if exc.code in {401, 403} else min(60, 2 ** min(attempts, 6))
                self.spool.retry(item["id"], attempts, delay, f"HTTP {exc.code}")
                print(f"[cloud] HTTP {exc.code}，{delay}s 后重试，待传 {self.spool.count()}")
            except (urllib.error.URLError, TimeoutError, OSError, RuntimeError) as exc:
                attempts = item["attempts"] + 1
                delay = min(60, 2 ** min(attempts, 6))
                self.spool.retry(item["id"], attempts, delay, exc)
                if attempts == 1 or attempts % 5 == 0:
                    print(f"[cloud] 连接失败，{delay}s 后重试: {exc}")


def enqueue_line(text, args, spool, sender):
    text = text.strip()
    if not text.startswith("{"):
        return False
    try:
        snapshot = json.loads(text)
    except json.JSONDecodeError:
        return False
    if not isinstance(snapshot, dict) or snapshot.get("t") != "snap":
        return False
    # Fixed before spooling: delayed delivery and retries retain acquisition time.
    snapshot.setdefault("capturedAt", int(time.time() * 1000))
    snapshot["stationId"] = args.station_id
    if args.station_name:
        snapshot["stationName"] = args.station_name
    # The cloud accepts live telemetry only from this USB gateway contract.  The
    # hardware may still set simulated=true when its local RID simulator or
    # software frame loopback is selected; sourceType distinguishes that from a
    # server-side demo producer.  Loopback is deliberately kept visible in the
    # transport field so a lab capture cannot be mistaken for RF reception.
    lab_loopback = snapshot.get("labLoopback") is True or any(
        isinstance(item, dict) and item.get("labLoopback") is True
        for item in snapshot.get("drones", [])
    )
    if lab_loopback:
        snapshot["labLoopback"] = True
        snapshot["simulated"] = True
    snapshot["sourceType"] = "hardware-usb"
    snapshot["sourceTransport"] = "usb-serial-loopback" if lab_loopback else "usb-serial"
    snapshot["hardwareConnected"] = True
    _, queued, dropped = spool.enqueue(snapshot)
    if dropped:
        print(f"[queue] 队列达到上限，丢弃最旧 {dropped} 条")
    sender.notify()
    if queued == 1 or queued % 100 == 0:
        print(f"[queue] 待传 {queued}")
    return True


def collect_serial(args, spool, sender):
    import serial
    try:
        from serial.tools import list_ports
        available = [item.device for item in list_ports.comports()]
        print(f"[serial] 目标 {args.port} @ {args.baud}; 当前端口: {available or '(无)'}")
    except Exception:
        pass

    while True:
        try:
            with serial.Serial(args.port, args.baud, timeout=1.0) as serial_port:
                print(f"[serial] 已连接 {args.port}")
                if args.lab_loopback:
                    time.sleep(1.0)
                    serial_port.reset_input_buffer()
                    command = "frame loopback on %s %d\n" % (
                        args.lab_kind, args.lab_interval,
                    )
                    serial_port.write(command.encode("ascii"))
                    serial_port.flush()
                    print("[lab] RF-disabled loopback armed: %s/%dms" % (
                        args.lab_kind, args.lab_interval,
                    ))
                while True:
                    line = serial_port.readline()
                    if not line:
                        continue
                    text = line.decode("utf-8", errors="ignore").strip()
                    enqueue_line(text, args, spool, sender)
        except serial.SerialException as exc:
            print(f"[serial] 连接失败: {exc}；3s 后重试")
            time.sleep(3)
        except Exception as exc:
            print(f"[serial] 异常: {exc}；1s 后重试")
            time.sleep(1)


def main():
    parser = argparse.ArgumentParser(description="RID 本地串口到云端的可靠上传网关")
    parser.add_argument("--port", default="COM5", help="T-Display-S3 串口")
    parser.add_argument("--baud", type=int, default=115200)
    parser.add_argument("--url", default=os.environ.get("RID_CLOUD_INGEST_URL"),
                        help="完整云端地址，如 https://rid.example.com/api/ingest")
    parser.add_argument("--token", default=os.environ.get("RID_INGEST_TOKEN"),
                        help="上报 token；也可用 RID_INGEST_TOKEN")
    parser.add_argument("--station-id", default=socket.gethostname(), help="哨站唯一 ID")
    parser.add_argument("--station-name", default="", help="哨站显示名称")
    parser.add_argument("--spool", default=DEFAULT_SPOOL, help="断网队列 SQLite 路径")
    parser.add_argument("--max-queue", type=int, default=10000, help="最多缓存快照数")
    parser.add_argument("--timeout", type=float, default=10.0, help="HTTPS 请求超时秒")
    parser.add_argument("--ca-file", help="私有 CA PEM；公网证书无需配置")
    parser.add_argument("--stdin", action="store_true",
                        help="从标准输入逐行读取 snap JSON（无硬件端到端测试）")
    parser.add_argument("--lab-loopback", action="store_true",
                        help="arm the firmware RF-disabled software loopback after opening serial")
    parser.add_argument("--lab-kind", choices=("pack", "beacon", "nan", "all"), default="all")
    parser.add_argument("--lab-interval", type=int, default=1000)
    args = parser.parse_args()
    if not 100 <= args.lab_interval <= 60_000:
        parser.error("--lab-interval must be between 100 and 60000 ms")
    if args.lab_loopback and args.stdin:
        parser.error("--lab-loopback requires a serial port and cannot be combined with --stdin")
    if not args.url or not args.token:
        parser.error("必须配置 --url/--token 或对应环境变量")
    if not args.url.lower().startswith("https://"):
        print("[cloud] 警告: 当前 URL 不是 HTTPS，token 和轨迹会明文传输")
    if args.max_queue < 100 or args.timeout <= 0:
        parser.error("队列或超时参数无效")

    spool = Spool(args.spool, args.max_queue)
    sender = CloudSender(spool, args.url, args.token, args.timeout, args.ca_file)
    sender.start()
    sender.notify()
    print(f"[gateway] 站点 {args.station_id}; 待传 {spool.count()}; spool={spool.path}")
    try:
        if args.stdin:
            accepted = sum(1 for line in sys.stdin if enqueue_line(line, args, spool, sender))
            print(f"[stdin] 已入队 {accepted} 条；等待上传完成")
            while spool.count() and sender.is_alive():
                time.sleep(0.1)
        else:
            collect_serial(args, spool, sender)
    except KeyboardInterrupt:
        print("\n已停止。本地 spool 中未上传的数据会在下次启动续传。")


if __name__ == "__main__":
    main()
