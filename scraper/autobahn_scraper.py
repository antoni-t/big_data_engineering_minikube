#!/usr/bin/env python3
import os
import sys
import json
import gzip
import time
from datetime import datetime, timezone

import requests

# --------------------------------------------------------
# Konfiguration
# --------------------------------------------------------

# Ordner dieses Skripts
APP_DIR = os.path.dirname(os.path.abspath(__file__))

# Ist in config file ausgelagert Config/autobahn_config.json
BASE_URL = "https://verkehr.autobahn.de"
TIMEOUT = int(15)
RETRIES = int(3)

# BASISPFAD FÜR DIE DATEN — keine /bronze, nur SCRAPER_DATA_DIR
# Default: /data (oder was dein Deployment per ENV setzt)
SCRAPER_DATA_DIR = os.environ.get("SCRAPER_DATA_DIR", "/data")

# Spezifischer Ordner für diesen Scraper:
# Ergebnis: /data/autobahn/YYYY/MM/DD/HH/*.jsonl.gz
DATA_ROOT = os.path.join(SCRAPER_DATA_DIR, "autobahn")

# --------------------------------------------------------
# Hilfsfunktionen
# --------------------------------------------------------
def utc_now():
    return datetime.now(timezone.utc)

def ts_for_filename(dt_utc: datetime) -> str:
    return dt_utc.strftime("%Y%m%dT%H%M%SZ")

def subdir_for(dt_utc: datetime) -> str:
    """Erstellt Pfad: DATA_ROOT/YYYY/MM/DD/HH"""
    return os.path.join(
        DATA_ROOT,
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
            # 5xx → Retry
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

    return {
        "ok": False,
        "error": str(last_exc) if last_exc else "unknown error",
        "url": url,
    }

# --------------------------------------------------------
# Hauptlogik — EINZIGER RUN (perfekt für CronJob)
# --------------------------------------------------------
def main() -> int:
    dt = utc_now()
    ts = ts_for_filename(dt)
    out_dir = subdir_for(dt)
    os.makedirs(out_dir, exist_ok=True)

    # 1) Index: roadIds discovern
    idx_url = f"{BASE_URL}/o/autobahn/"
    idx_res = get_json(idx_url)
    if not idx_res["ok"]:
        print(f"[{ts}] ERROR index fetch failed: {idx_res.get('error')}", file=sys.stderr)

        out_file = os.path.join(out_dir, f"autobahn_warning_{ts}.jsonl.gz")
        with gzip.open(out_file, "wt", encoding="utf-8") as gz:
            gz.write(json.dumps({
                "ts_utc": dt.isoformat(),
                "request_url": idx_url,
                "ok": False,
                "error": idx_res.get("error"),
                "stage": "index"
            }) + "\n")

        print(f"[{ts}] wrote 1 error row -> {out_file}")
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
    out_file = os.path.join(out_dir, f"autobahn_warning_{ts}.jsonl.gz")
    with gzip.open(out_file, "wt", encoding="utf-8") as gz:
        for row in results:
            gz.write(json.dumps(row, ensure_ascii=False))
            gz.write("\n")

    ok_count = sum(1 for r in results if r.get("ok"))
    print(
        f"[{ts}] wrote {len(results)} responses ({ok_count} ok, {total_warning_items} items) -> {out_file}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
