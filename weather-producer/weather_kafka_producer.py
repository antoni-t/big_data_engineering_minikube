#!/usr/bin/env python3
import json
import os
import time
from datetime import datetime, timezone
from zoneinfo import ZoneInfo  # für Europe/Berlin

import pandas as pd
from kafka import KafkaProducer
import openmeteo_requests
import requests_cache
from retry_requests import retry

# -------------------------------------------------------------------
# 0) Konfiguration über Umgebungsvariablen
# -------------------------------------------------------------------
# Diese Werte kannst du im Dockerfile oder im Kubernetes-Deployment setzen.
# Falls nichts gesetzt ist, greifen die Defaults (gut für lokalen Test).
GRID_PATH = os.getenv("GRID_PATH", "de_grid_cell_centers.csv")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "weather-raw")
POLL_INTERVAL_SECONDS = int(os.getenv("POLL_INTERVAL_SECONDS", "3600"))

print("=== Weather Kafka Producer Konfiguration ===")
print(f"Kafka Bootstrap: {KAFKA_BOOTSTRAP}")
print(f"Kafka Topic:     {TOPIC}")
print(f"Grid Path:       {GRID_PATH}")
print(f"Poll Interval:   {POLL_INTERVAL_SECONDS} s")
print("============================================")

# -------------------------------------------------------------------
# 1) Setup Open-Meteo API client (laut offizieller Doku)
# -------------------------------------------------------------------
cache_session = requests_cache.CachedSession(".cache", expire_after=3600)
retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
openmeteo = openmeteo_requests.Client(session=retry_session)

# -------------------------------------------------------------------
# 2) Load grid (Testgrid mit 2 Punkten oder später 400er-Grid)
# -------------------------------------------------------------------
# test_grid.csv:
# id,row,col,lat,lon
# pt_0_0,0,0,49.4,8.4
# pt_0_1,0,1,49.4,8.5

try:
    grid = pd.read_csv(GRID_PATH)
except FileNotFoundError:
    raise SystemExit(f"FEHLER: Grid-CSV nicht gefunden unter Pfad: {GRID_PATH}")

print(f"Loaded grid with {len(grid)} cells from {GRID_PATH}")

# -------------------------------------------------------------------
# 3) Configure Kafka producer
# -------------------------------------------------------------------
producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BOOTSTRAP],
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda k: k.encode("utf-8"),
)

print(f"Connected to Kafka at {KAFKA_BOOTSTRAP}, streaming into topic '{TOPIC}'")


# -------------------------------------------------------------------
# Helper: Slot berechnen D1/D2/D3 + Hxx
# - Tag relativ zu lokalem Datum (Europe/Berlin)
# - Stunde aus lokaler deutscher Zeit
# -------------------------------------------------------------------
def compute_slot(ts_local: pd.Timestamp, today_local_date) -> tuple[int, str]:
    """
    Erzeugt Slot wie D1H10 basierend auf:
      - Day (D1/D2/D3) relativ zum heutigen Datum in Europe/Berlin
      - Hour (Hxx) aus der lokalen Stunde (Europe/Berlin)
    """
    # D1 = heute lokal, D2 = morgen lokal, D3 = übermorgen lokal
    day_offset = (ts_local.date() - today_local_date).days
    day_index = day_offset + 1  # heute → 1, morgen → 2, übermorgen → 3

    # Lokale Stunde Deutschlands (0–23)
    hour_local = ts_local.hour

    slot_str = f"D{day_index}H{hour_local:02d}"
    return day_index, slot_str


# -------------------------------------------------------------------
# 4) Request weather forecast for a single cell (offizielle Doku-Logik)
# -------------------------------------------------------------------
def fetch_forecast(lat: float, lon: float):
    """
    Ruft den 3-Tage-Stunden-Forecast für eine Koordinate ab und gibt
    (times, temperature_2m, rain, showers, snowfall, relative_humidity_2m, resp_lat, resp_lon) zurück.
    """
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": lat,
        "longitude": lon,
        "hourly": ["temperature_2m", "relative_humidity_2m", "rain", "showers", "snowfall"],
        "timezone": "Europe/Berlin",
        "forecast_days": 3,
    }

    responses = openmeteo.weather_api(url, params=params)
    response = responses[0]

    hourly = response.Hourly()
    hourly_temperature_2m = hourly.Variables(0).ValuesAsNumpy()
    hourly_relative_humidity_2m = hourly.Variables(1).ValuesAsNumpy()
    hourly_rain = hourly.Variables(2).ValuesAsNumpy()
    hourly_showers = hourly.Variables(3).ValuesAsNumpy()
    hourly_snowfall = hourly.Variables(4).ValuesAsNumpy()

    # times als UTC-Timestamps (wie in der offiziellen Doku)
    times = pd.date_range(
        start=pd.to_datetime(hourly.Time(), unit="s", utc=True),
        end=pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True),
        freq=pd.Timedelta(seconds=hourly.Interval()),
        inclusive="left",
    )

    return (
        times,
        hourly_temperature_2m,
        hourly_rain,
        hourly_showers,
        hourly_snowfall,
        hourly_relative_humidity_2m,
        float(response.Latitude()),
        float(response.Longitude()),
    )


# -------------------------------------------------------------------
# 5) Main loop: alle Gridpunkte abfragen und je Stunde ein Event senden
# -------------------------------------------------------------------
while True:
    # Lokale Zeit in Deutschland (Europe/Berlin)
    now_local = datetime.now(ZoneInfo("Europe/Berlin"))
    # Gleicher Zeitpunkt in UTC (für ts_produced_utc)
    now_utc = now_local.astimezone(timezone.utc)
    batch_ts_str = now_utc.isoformat()
    today_local_date = now_local.date()

    print(
        f"\n[{batch_ts_str}] Fetching forecasts for all grid cells "
        f"(today_local={today_local_date})..."
    )

    for _, row in grid.iterrows():
        cell_id = row["id"]  # z. B. "pt_0_0"
        lat = float(row["lat"])
        lon = float(row["lon"])

        try:
            (
                times,
                temps,
                rain,
                showers,
                snowfall,
                rhs,
                resp_lat,
                resp_lon,
            ) = fetch_forecast(lat, lon)

            # Für jede Stunde ein separates Kafka-Event
            for i in range(len(times)):
                ts_hour_utc = times[i]              # pandas.Timestamp (tz=UTC)
                ts_hour_local = ts_hour_utc.tz_convert("Europe/Berlin")

                # 1) Nur zukünftige Stunden (lokal) – keine Vergangenheit
                if ts_hour_local <= now_local:
                    continue

                # 2) Slot basierend auf lokalem Datum + lokaler Stunde
                day_index, slot_key = compute_slot(ts_hour_local, today_local_date)

                # Nur Tage D1–D3 zulassen (heute, morgen, übermorgen)
                if day_index < 1 or day_index > 3:
                    continue

                # Kafka-Key: z. B. D1H10Lat49.4000Lon8.4000
                kafka_key = f"{slot_key}Lat{resp_lat:.4f}Lon{resp_lon:.4f}"

                message = {
                    "ts_produced_utc": batch_ts_str,                    # Zeitpunkt des Sends (UTC)
                    "slot": slot_key,                                   # D1Hxx
                    "cell_id": cell_id,                                 # aus CSV (pt_0_0, pt_0_1, ...)
                    "latitude": resp_lat,
                    "longitude": resp_lon,
                    "timestamp_hour_local": ts_hour_local.isoformat(),  # lokaler Timestamp
                    # "timestamp_hour_utc": ts_hour_utc.isoformat(),    # optional
                    "temperature_2m": float(temps[i]),
                    "rain": float(rain[i]),
                    "showers": float(showers[i]),
                    "snowfall": float(snowfall[i]),
                    "relative_humidity_2m": float(rhs[i]),
                    "source": "open-meteo",
                }

                producer.send(TOPIC, key=kafka_key, value=message)
                print(f"Sent {kafka_key} → {message['timestamp_hour_local']}")

        except Exception as e:
            print(f"Error for cell {cell_id}: {e}")

    producer.flush()
    print(f"[{batch_ts_str}] → Successfully streamed hourly weather for all grid cells.")

    # Konfigurierbare Pause, dann neuen Forecast ziehen
    print(f"Sleeping for {POLL_INTERVAL_SECONDS} seconds before next batch...")
    time.sleep(POLL_INTERVAL_SECONDS)