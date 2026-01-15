#!/usr/bin/env python3
import os
import sys
import json
import gzip
import time
from io import BytesIO
from datetime import datetime, timezone
from urllib.parse import urlencode

import requests

# --------------------------------------------------------
# Konfiguration
# --------------------------------------------------------
BASE_URL = "https://verkehr.autobahn.de"
TIMEOUT = int(os.environ.get("HTTP_TIMEOUT", "15"))
RETRIES = int(os.environ.get("HTTP_RETRIES", "3"))

# HDFS (WebHDFS)
# Must include /webhdfs/v1, e.g. http://hdfs-namenode.default.svc.cluster.local:9870/webhdfs/v1
HDFS_WEBHDFS_URL = os.getenv("HDFS_WEBHDFS_URL", "").rstrip("/")
HDFS_USER = os.getenv("HDFS_USER", "hdfs")

# Zielpfad in HDFS (ohne trailing slash ok)
# Ergebnis: /datalake/bronze/autobahn/YYYY/MM/DD/HH/autobahn_warning_<ts>.jsonl.gz
HDFS_AUTOBAHN_DIR = os.getenv("HDFS_AUTOBAHN_DIR", "/datalake/bronze/autobahn")

# Optional: lokales Fallback (nur wenn HDFS nicht gesetzt ist)
SCRAPER_DATA_DIR = os.environ.get("SCRAPER_DATA_DIR", "/data")
LOCAL_DATA_ROOT = os.path.join(SCRAPER_DATA_DIR, "autobahn")

# --------------------------------------------------------
# Hilfsfunktionen
# --------------------------------------------------------
def utc_now():
    return datetime.now(timezone.utc)

def ts_for_filename(dt_utc: datetime) -> str:
    return dt_utc.strftime("%Y%m%dT%H%M%SZ")

def hdfs_subdir_for(dt_utc: datetime) -> str:
    # HDFS path: <root>/YYYY/MM/DD/HH
    return str(
        f"{HDFS_AUTOBAHN_DIR.rstrip('/')}/"
        f"{dt_utc.strftime('%Y')}/"
        f"{dt_utc.strftime('%m')}/"
        f"{dt_utc.strftime('%d')}/"
        f"{dt_utc.strftime('%H')}"
    )

def local_subdir_for(dt_utc: datetime) -> str:
    # local path: <root>/YYYY/MM/DD/HH
    return os.path.join(
        LOCAL_DATA_ROOT,
        dt_utc.strftime("%Y"),
        dt_utc.strftime("%m"),
        dt_utc.strftime("%d"),
        dt_utc.strftime("%H"),
    )

def get_json(url: str) -> dict:
    """HTTP GET mit Retry und JSON-Payload."""
    last_exc = None
    for attempt in range(1, RETRIES + 1):
        try:
            r = requests.get(
                url,
                timeout=TIMEOUT,
                headers={"accept": "application/json"},
            )
            if r.status_code >= 500:
                time.sleep(min(2 ** attempt, 10))
                continue
            r.raise_for_status()
            return {
                "ok": True,
                "json": r.json(),
                "status": r.status_code,
                "headers": dict(r.headers),
            }
        except Exception as e:
            last_exc = e
            time.sleep(min(2 ** attempt, 10))

    return {"ok": False, "error": str(last_exc) if last_exc else "unknown error", "url": url}

def _require_hdfs():
    if not HDFS_WEBHDFS_URL:
        raise RuntimeError(
            "HDFS_WEBHDFS_URL ist nicht gesetzt. Beispiel:\n"
            "  http://hdfs-namenode.default.svc.cluster.local:9870/webhdfs/v1"
        )

def _webhdfs_url(hdfs_path: str, op: str, **params) -> str:
    if not hdfs_path.startswith("/"):
        hdfs_path = "/" + hdfs_path
    qp = {"op": op, "user.name": HDFS_USER}
    qp.update(params)
    return f"{HDFS_WEBHDFS_URL}{hdfs_path}?{urlencode(qp)}"

def hdfs_mkdirs(hdfs_dir: str) -> None:
    _require_hdfs()
    # MKDIRS is idempotent
    r = requests.put(_webhdfs_url(hdfs_dir, "MKDIRS"), timeout=60)
    r.raise_for_status()

def hdfs_put_bytes(hdfs_file: str, data: bytes, overwrite: bool = True) -> None:
    _require_hdfs()
    # Two-step WebHDFS CREATE: first get redirect, then upload to redirect URL
    r1 = requests.put(
        _webhdfs_url(hdfs_file, "CREATE", overwrite="true" if overwrite else "false"),
        allow_redirects=False,
        timeout=60,
    )
    if r1.status_code not in (307, 201):
        # Some setups return 201 directly; others 307 redirect
        try:
            r1.raise_for_status()
        except Exception as e:
            raise RuntimeError(f"WebHDFS CREATE failed for {hdfs_file}: {r1.text}") from e

    if r1.status_code == 201:
        return

    loc = r1.headers.get("Location")
    if not loc:
        raise RuntimeError(f"WebHDFS CREATE redirect missing Location header for {hdfs_file}")

    r2 = requests.put(loc, data=data, timeout=300)
    r2.raise_for_status()

def write_jsonl_gz_to_hdfs(hdfs_dir: str, filename: str, rows: list[dict]) -> str:
    hdfs_mkdirs(hdfs_dir)
    buf = BytesIO()
    with gzip.GzipFile(fileobj=buf, mode="wb") as gz:
        for row in rows:
            line = (json.dumps(row, ensure_ascii=False) + "\n").encode("utf-8")
            gz.write(line)
    hdfs_path = f"{hdfs_dir.rstrip('/')}/{filename}"
    hdfs_put_bytes(hdfs_path, buf.getvalue(), overwrite=True)
    return hdfs_path

def write_jsonl_gz_local(local_dir: str, filename: str, rows: list[dict]) -> str:
    os.makedirs(local_dir, exist_ok=True)
    out_file = os.path.join(local_dir, filename)
    with gzip.open(out_file, "wt", encoding="utf-8") as gz:
        for row in rows:
            gz.write(json.dumps(row, ensure_ascii=False))
            gz.write("\n")
    return out_file

# --------------------------------------------------------
# Hauptlogik — EINZIGER RUN (perfekt für CronJob)
# --------------------------------------------------------
def main() -> int:
    dt = utc_now()
    ts = ts_for_filename(dt)

    # 1) Index: roadIds discovern
    idx_url = f"{BASE_URL}/o/autobahn/"
    idx_res = get_json(idx_url)
    if not idx_res["ok"]:
        err_row = {
            "ts_utc": dt.isoformat(),
            "request_url": idx_url,
            "ok": False,
            "error": idx_res.get("error"),
            "stage": "index",
        }
        filename = f"autobahn_warning_{ts}.jsonl.gz"

        # Prefer HDFS if configured, else local
        if HDFS_WEBHDFS_URL:
            out_dir = hdfs_subdir_for(dt)
            out_path = write_jsonl_gz_to_hdfs(out_dir, filename, [err_row])
        else:
            out_dir = local_subdir_for(dt)
            out_path = write_jsonl_gz_local(out_dir, filename, [err_row])

        print(f"[{ts}] ERROR index fetch failed: {idx_res.get('error')}", file=sys.stderr)
        print(f"[{ts}] wrote 1 error row -> {out_path}")
        return 1

    idx_payload = idx_res["json"]

    # roadIds extrahieren
    if isinstance(idx_payload, dict) and isinstance(idx_payload.get("roads"), list):
        road_ids = idx_payload["roads"]
    elif isinstance(idx_payload, list):
        road_ids = idx_payload
    else:
        print(f"[{ts}] ERROR: unexpected index payload shape", file=sys.stderr)
        road_ids = []

    print(f"[{ts}] discovered {len(road_ids)} roadIds")

    results = []
    total_warning_items = 0

    # 2) Für jede roadId warnings holen
    for rid in road_ids:
        warn_url = f"{BASE_URL}/o/autobahn/{rid}/services/warning"
        res = get_json(warn_url)

        entry = {
            "ts_utc": dt.isoformat(),
            "road_id": rid,
            "request_url": warn_url,
            "ok": res.get("ok"),
            "status": res.get("status"),
            "headers": res.get("headers"),
            "error": res.get("error"),
            "payload": res.get("json"),
        }

        payload = res.get("json")
        if isinstance(payload, dict):
            items = payload.get("warning") or payload.get("warnings") or payload.get("events")
            if isinstance(items, list):
                entry["item_count"] = len(items)
                total_warning_items += len(items)

        results.append(entry)

    # 3) Schreiben als JSONL.GZ
    filename = f"autobahn_warning_{ts}.jsonl.gz"

    if HDFS_WEBHDFS_URL:
        out_dir = hdfs_subdir_for(dt)
        out_path = write_jsonl_gz_to_hdfs(out_dir, filename, results)
    else:
        out_dir = local_subdir_for(dt)
        out_path = write_jsonl_gz_local(out_dir, filename, results)

    ok_count = sum(1 for r in results if r.get("ok"))
    print(f"[{ts}] wrote {len(results)} responses ({ok_count} ok, {total_warning_items} items) -> {out_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
