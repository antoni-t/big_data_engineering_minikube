#!/usr/bin/env python3
import os
import time
import gzip
import json
from io import BytesIO
import datetime as dt
from urllib.parse import urlencode

import pandas as pd
import openmeteo_requests
import requests_cache
import requests
from retry_requests import retry

######################################################################
# Open-Meteo Wetter-Scraper → HDFS (Bronze Layer)
#
# Zweck
#   Dieses Skript ruft stündliche Wetterdaten (z. B. Temperatur, Niederschlag,
#   Luftfeuchtigkeit, Schneefall) für ein fest definiertes räumliches Raster
#   in Deutschland über die Open-Meteo-API ab und persistiert die Rohdaten
#   strukturiert und komprimiert im Bronze-Layer eines Data Lakes.
#
# Funktionaler Ablauf
#   1) Laden eines festen Gitters von Zellzentren (lat/lon, row/col)
#   2) Bestimmung der letzten vollständig abgeschlossenen Stunde (UTC)
#   3) Abfrage der stündlichen Wetterparameter je Rasterzelle
#      unter Berücksichtigung von API-Limits (Retry, Backoff, Throttling)
#   4) Normalisierung der Antworten in zeilenbasierte Records
#   5) Persistenz als gzip-komprimierte JSONL-Datei
#
# Persistenz & Struktur
#   - Bevorzugtes Ziel: HDFS über WebHDFS
#   - Optionales lokales Fallback bei fehlender HDFS-Konfiguration
#   - Zeitbasierte Ordnerstruktur:
#       <root>/YYYY/MM/DD/HH/openmeteo_<timestamp>.jsonl.gz
#
# Robustheit
#   - HTTP-Caching und Retry-Mechanismus für stabile API-Nutzung
#   - Explizite Behandlung von Open-Meteo Rate-Limits
#   - Schutz vor Überlastung durch MAX_REQUESTS_PER_DAY
#
# Einsatzkontext
#   - Einsatz als Kubernetes CronJob (ein Lauf pro Stunde)
#   - Datenbasis für nachgelagerte Streaming-, ML- und Forecast-Pipelines
######################################################################


# ============================================
# Konfiguration
# ============================================
APP_DIR = os.path.dirname(os.path.abspath(__file__))

# erwartet Spalten: id,row,col,lat,lon
CSV_PATH = os.path.join(APP_DIR, "de_grid_cell_centers.csv")

# HDFS (WebHDFS)
HDFS_WEBHDFS_URL = os.getenv("HDFS_WEBHDFS_URL", "").rstrip("/")
HDFS_USER = os.getenv("HDFS_USER", "hdfs")
HDFS_WEATHER_DIR = os.getenv("HDFS_WEATHER_DIR", "/datalake/bronze/weather_hist")

# Optional lokales Fallback
SCRAPER_DATA_DIR = os.environ.get("SCRAPER_DATA_DIR", "/data")
LOCAL_BASE_OUT_DIR = os.path.join(SCRAPER_DATA_DIR, "wetter")

# Open-Meteo API
OPENMETEO_URL = "https://api.open-meteo.com/v1/forecast"

HOURLY_VARIABLES = [
    "temperature_2m",
    "snowfall",
    "showers",
    "rain",
    "relative_humidity_2m",
]

MAX_REQUESTS_PER_DAY = int(os.getenv("MAX_REQUESTS_PER_DAY", "9000"))
REQUEST_INTERVAL_SEC = float(os.getenv("REQUEST_INTERVAL_SEC", "1.5"))

# ============================================
# Open-Meteo Client mit Cache & Retry
# ============================================
cache_session = requests_cache.CachedSession(".cache", expire_after=3600)
retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
openmeteo = openmeteo_requests.Client(session=retry_session)

# ============================================
# WebHDFS Helpers
# ============================================
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
    r = requests.put(_webhdfs_url(hdfs_dir, "MKDIRS"), timeout=60)
    r.raise_for_status()

def hdfs_put_bytes(hdfs_file: str, data: bytes, overwrite: bool = True) -> None:
    _require_hdfs()
    r1 = requests.put(
        _webhdfs_url(hdfs_file, "CREATE", overwrite="true" if overwrite else "false"),
        allow_redirects=False,
        timeout=60,
    )
    if r1.status_code not in (307, 201):
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

def hdfs_subdir_for(target_hour: dt.datetime) -> str:
    # HDFS path: <root>/YYYY/MM/DD/HH
    return str(
        f"{HDFS_WEATHER_DIR.rstrip('/')}/"
        f"{target_hour.year:04d}/"
        f"{target_hour.month:02d}/"
        f"{target_hour.day:02d}/"
        f"{target_hour.hour:02d}"
    )

# ============================================
# Hilfsfunktionen
# ============================================
def load_coords(csv_path: str) -> pd.DataFrame:
    df = pd.read_csv(csv_path)
    df["row"] = df["row"].astype(int)
    df["col"] = df["col"].astype(int)
    return df

def get_last_full_hour_utc() -> dt.datetime:
    now = dt.datetime.utcnow().replace(minute=0, second=0, microsecond=0)
    target = now - dt.timedelta(hours=1)
    return target

def make_local_output_path(base_dir: str, target_hour: dt.datetime) -> str:
    year = f"{target_hour.year:04d}"
    month = f"{target_hour.month:02d}"
    day = f"{target_hour.day:02d}"
    hour = f"{target_hour.hour:02d}"

    dir_path = os.path.join(base_dir, year, month, day, hour)
    os.makedirs(dir_path, exist_ok=True)

    filename = f"openmeteo_{target_hour.strftime('%Y%m%d%H')}.jsonl.gz"
    return os.path.join(dir_path, filename)

def build_time_series(hourly) -> list[dt.datetime]:
    start_ts = pd.to_datetime(hourly.Time(), unit="s", utc=True)
    interval_sec = hourly.Interval()
    n_steps = len(hourly.Variables(0).ValuesAsNumpy())
    times = [start_ts + i * pd.Timedelta(seconds=interval_sec) for i in range(n_steps)]
    return times

def fetch_hour_for_coord(lat: float, lon: float, target_hour: dt.datetime):
    start_hour_iso = target_hour.strftime("%Y-%m-%dT%H:00")
    end_hour_iso = (target_hour + dt.timedelta(hours=1)).strftime("%Y-%m-%dT%H:00")

    params = {
        "latitude": lat,
        "longitude": lon,
        "hourly": HOURLY_VARIABLES,
        "start_hour": start_hour_iso,
        "end_hour": end_hour_iso,
        "timezone": "UTC",
        "cell_selection": "land",
    }

    while True:
        try:
            responses = openmeteo.weather_api(OPENMETEO_URL, params=params)
            return responses[0]
        except Exception as e:
            msg = str(e)
            if "Minutely API request limit exceeded" in msg:
                print("Minutely-Limit erreicht → warte 70 Sekunden ...")
                time.sleep(70)
                continue
            elif "Hourly API request limit exceeded" in msg:
                print("Hourly-Limit erreicht → warte 3600 Sekunden (1 Stunde) ...")
                time.sleep(3600)
                continue
            else:
                print("Unerwarteter Fehler im API-Request:")
                print(msg)
                raise

def write_records_jsonl_gz_hdfs(hdfs_dir: str, filename: str, records: list[dict]) -> str:
    hdfs_mkdirs(hdfs_dir)
    buf = BytesIO()
    with gzip.GzipFile(fileobj=buf, mode="wb") as gz:
        for rec in records:
            gz.write((json.dumps(rec) + "\n").encode("utf-8"))
    hdfs_path = f"{hdfs_dir.rstrip('/')}/{filename}"
    hdfs_put_bytes(hdfs_path, buf.getvalue(), overwrite=True)
    return hdfs_path

# ============================================
# Ein Durchlauf: alle Koordinaten abfragen und speichern
# ============================================
def run_once():
    coords_df = load_coords(CSV_PATH)
    n_points = len(coords_df)
    print(f"Anzahl Koordinaten im Grid: {n_points}")

    if n_points > MAX_REQUESTS_PER_DAY:
        raise ValueError(
            f"Zu viele Koordinaten ({n_points}) für MAX_REQUESTS_PER_DAY={MAX_REQUESTS_PER_DAY}. "
            "Bitte Limit/Strategie anpassen."
        )

    target_hour = get_last_full_hour_utc()
    print(f"Hole Daten für Stunde (UTC): {target_hour.strftime('%Y-%m-%d %H:%M')}")

    all_records = []
    requests_made = 0

    for idx, row in coords_df.iterrows():
        station_id = row["id"]
        r = int(row["row"])
        c = int(row["col"])
        lat = float(row["lat"])
        lon = float(row["lon"])

        print(f"[{idx+1}/{n_points}] Request für {station_id} (lat={lat}, lon={lon}) ...")

        response = fetch_hour_for_coord(lat, lon, target_hour)
        requests_made += 1

        resp_lat = response.Latitude()
        resp_lon = response.Longitude()
        hourly = response.Hourly()

        arrays = [hourly.Variables(i).ValuesAsNumpy() for i in range(len(HOURLY_VARIABLES))]
        times = build_time_series(hourly)

        for t_idx, ts in enumerate(times):
            rec = {
                "time": ts.isoformat(),
                "id": station_id,
                "row": r,
                "col": c,
                "latitude": float(resp_lat),
                "longitude": float(resp_lon),
            }
            for v_name, arr in zip(HOURLY_VARIABLES, arrays):
                rec[v_name] = float(arr[t_idx])
            all_records.append(rec)

        time.sleep(REQUEST_INTERVAL_SEC)

    print(f"Gesamtanzahl Records: {len(all_records)}")
    print(f"Anzahl Requests in diesem Lauf: {requests_made}")

    filename = f"openmeteo_{target_hour.strftime('%Y%m%d%H')}.jsonl.gz"

    # Prefer HDFS if configured, else local
    if HDFS_WEBHDFS_URL:
        hdfs_dir = hdfs_subdir_for(target_hour)
        out_path = write_records_jsonl_gz_hdfs(hdfs_dir, filename, all_records)
    else:
        out_path = make_local_output_path(LOCAL_BASE_OUT_DIR, target_hour)
        print(f"Schreibe Datei: {out_path}")
        with gzip.open(out_path, "wt", encoding="utf-8") as f:
            for rec in all_records:
                f.write(json.dumps(rec) + "\n")

    print(f"Fertig. Output: {out_path}")

# ============================================
# CronJob-Variante: EIN Lauf, dann Exit
# ============================================
if __name__ == "__main__":
    try:
        run_once()
    except Exception as e:
        print(f"Fehler in run_once(): {e}")
        raise