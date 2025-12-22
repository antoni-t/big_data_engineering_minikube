#!/usr/bin/env python3
import os
import time
import gzip
import json
import datetime as dt

import pandas as pd
import openmeteo_requests
import requests_cache
from retry_requests import retry

# ============================================
# Konfiguration
# ============================================

# Verzeichnis dieses Skripts
APP_DIR = os.path.dirname(os.path.abspath(__file__))

# CSV-Pfad relativ zum Skript
# erwartet Spalten: id,row,col,lat,lon
CSV_PATH = os.path.join(APP_DIR, "de_grid_cell_centers.csv")

# Basisverzeichnis aus ENV (z.B. /data), darunter "wetter"
SCRAPER_DATA_DIR = os.environ.get("SCRAPER_DATA_DIR", "/data")
BASE_OUT_DIR = os.path.join(SCRAPER_DATA_DIR, "wetter")

# Open-Meteo API
OPENMETEO_URL = "https://api.open-meteo.com/v1/forecast"

# Hourly Variablen (Reihenfolge wichtig!)
HOURLY_VARIABLES = [
    "temperature_2m",
    "snowfall",
    "showers",
    "rain",
    "relative_humidity_2m",
]

# Rate-Limit / Safety
MAX_REQUESTS_PER_DAY = 9000      # nur als Sicherheitslimit
REQUEST_INTERVAL_SEC = 1.5       # Pause zwischen Requests


# ============================================
# Open-Meteo Client mit Cache & Retry
# ============================================
cache_session = requests_cache.CachedSession(".cache", expire_after=3600)
retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
openmeteo = openmeteo_requests.Client(session=retry_session)


# ============================================
# Hilfsfunktionen
# ============================================
def load_coords(csv_path: str) -> pd.DataFrame:
    """
    Lädt das CSV mit Koordinaten.
    erwartet Spalten: id,row,col,lat,lon
    """
    df = pd.read_csv(csv_path)
    df["row"] = df["row"].astype(int)
    df["col"] = df["col"].astype(int)
    return df


def get_last_full_hour_utc() -> dt.datetime:
    """
    Letzte volle UTC-Stunde.
    Beispiel: 10:07 UTC -> 09:00 UTC
    """
    now = dt.datetime.utcnow().replace(minute=0, second=0, microsecond=0)
    target = now - dt.timedelta(hours=1)
    return target


def make_output_path(base_dir: str, target_hour: dt.datetime) -> str:
    """
    Erzeugt Pfad:
      BASE_OUT_DIR/YYYY/MM/DD/HH/openmeteo_YYYYMMDDHH.jsonl.gz
    Beispiel:
      /data/wetter/2025/03/06/14/openmeteo_2025030614.jsonl.gz
    """
    year = f"{target_hour.year:04d}"
    month = f"{target_hour.month:02d}"
    day = f"{target_hour.day:02d}"
    hour = f"{target_hour.hour:02d}"

    dir_path = os.path.join(base_dir, year, month, day, hour)
    os.makedirs(dir_path, exist_ok=True)

    filename = f"openmeteo_{target_hour.strftime('%Y%m%d%H')}.jsonl.gz"
    return os.path.join(dir_path, filename)


def build_time_series(hourly) -> list[dt.datetime]:
    """
    Baut eine Liste von Timestamps auf Basis von
    hourly.Time(), hourly.Interval() und der Länge einer Variablen.
    Vermeidet Probleme mit date_range/inclusive.
    """
    start_ts = pd.to_datetime(hourly.Time(), unit="s", utc=True)
    interval_sec = hourly.Interval()

    n_steps = len(hourly.Variables(0).ValuesAsNumpy())

    times = [
        start_ts + i * pd.Timedelta(seconds=interval_sec)
        for i in range(n_steps)
    ]
    return times


def fetch_hour_for_coord(lat: float, lon: float, target_hour: dt.datetime):
    """
    Holt für eine Koordinate die Daten der letzten vollen Stunde (UTC)
    mit Error-Handling für Rate-Limits.
    Gibt das Open-Meteo Response-Objekt zurück.
    """
    # Zeitfenster: genau eine Stunde ab target_hour
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


# ============================================
# Ein Durchlauf: alle Koordinaten abfragen und speichern
# ============================================
def run_once():
    # 1) Koordinaten laden
    coords_df = load_coords(CSV_PATH)
    n_points = len(coords_df)
    print(f"Anzahl Koordinaten im Grid: {n_points}")

    if n_points > MAX_REQUESTS_PER_DAY:
        raise ValueError(
            f"Zu viele Koordinaten ({n_points}) für MAX_REQUESTS_PER_DAY={MAX_REQUESTS_PER_DAY}. "
            "Bitte Limit/Strategie anpassen."
        )

    # 2) Zielstunde bestimmen (letzte volle Stunde UTC)
    target_hour = get_last_full_hour_utc()
    print(f"Hole Daten für Stunde (UTC): {target_hour.strftime('%Y-%m-%d %H:%M')}")

    all_records = []
    requests_made = 0

    # 3) Für jede Koordinate einen Request
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

        arrays = [
            hourly.Variables(i).ValuesAsNumpy()
            for i in range(len(HOURLY_VARIABLES))
        ]

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

    # 4) JSONL.GZ-Datei schreiben
    out_path = make_output_path(BASE_OUT_DIR, target_hour)
    print(f"Schreibe Datei: {out_path}")

    with gzip.open(out_path, "wt", encoding="utf-8") as f:
        for rec in all_records:
            f.write(json.dumps(rec) + "\n")

    print("Fertig.")


# ============================================
# CronJob-Variante: EIN Lauf, dann Exit
# ============================================
if __name__ == "__main__":
    try:
        run_once()
    except Exception as e:
        print(f"Fehler in run_once(): {e}")
        # Bei Fehler nicht in einer Endlosschleife bleiben,
        # sondern mit Fehlercode beenden (für CronJob sichtbar).
        raise
