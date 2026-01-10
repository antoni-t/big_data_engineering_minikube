#!/usr/bin/env python3
import os
import json
import time
import re
from typing import Optional

import numpy as np
import pandas as pd

from kafka import KafkaConsumer
import mlflow
import mlflow.sklearn

from sqlalchemy import create_engine, text


def get_env(name: str, default: str | None = None) -> str:
    v = os.getenv(name, default)
    if v is None or v == "":
        raise RuntimeError(f"Missing required env var: {name}")
    return v


def load_run_id(path: str) -> str:
    with open(path, "r", encoding="utf-8") as f:
        return f.read().strip()


# --- DB schema we target (must match mariadb init) ---
# forecast_predictions(
#   hour INT NOT NULL,
#   row INT NOT NULL,
#   col INT NOT NULL,
#   predicted_events INT NOT NULL,
#   timestamp DATETIME NOT NULL,
#   PRIMARY KEY(hour,row,col)
# )


def ensure_table(engine) -> None:
    ddl = """
    CREATE TABLE IF NOT EXISTS forecast_predictions (
      hour INT NOT NULL,
      row INT NOT NULL,
      col INT NOT NULL,
      predicted_events INT NOT NULL,
      timestamp DATETIME NOT NULL,
      PRIMARY KEY (hour, row, col)
    );
    """
    with engine.begin() as conn:
        conn.execute(text(ddl))


# -----------------------------
# Slot -> stable hour mapping
# -----------------------------
# Producer sends slot like "D1H10", "D2H03", "D3H23"
SLOT_RE = re.compile(r"^D(?P<d>[1-3])H(?P<h>\d{2})$")


def hour_from_slot(slot: object) -> Optional[int]:
    if not isinstance(slot, str):
        return None
    m = SLOT_RE.match(slot)
    if not m:
        return None
    d = int(m.group("d"))
    h = int(m.group("h"))
    if d < 1 or d > 3 or h < 0 or h > 23:
        return None
    # D1H00 -> 1, D1H23 -> 24, D2H00 -> 25, D3H23 -> 72
    return (d - 1) * 24 + h + 1


def coerce_timestamp_utc(df: pd.DataFrame) -> pd.Series:
    """
    Parse forecast timestamp robustly and normalize to tz-aware UTC.
    Supported candidates (your pipeline):
      - timestamp_hour_local (producer) e.g. "2026-01-10T14:00:00+01:00"
      - timestamp
      - time
    Returns dtype datetime64[ns, UTC]
    """
    candidates = ["timestamp_hour_local", "timestamp", "time"]
    for c in candidates:
        if c in df.columns:
            return pd.to_datetime(df[c], errors="coerce", utc=True)
    return pd.to_datetime(pd.Series([pd.NaT] * len(df)), errors="coerce", utc=True)


UPSERT_SQL = text(
    """
INSERT INTO forecast_predictions (hour, row, col, predicted_events, timestamp)
VALUES (:hour, :row, :col, :predicted_events, :timestamp)
ON DUPLICATE KEY UPDATE
  predicted_events = VALUES(predicted_events),
  timestamp = VALUES(timestamp);
"""
)


def upsert_predictions(engine, df: pd.DataFrame) -> None:
    """
    Expects df columns: hour,row,col,predicted_events,timestamp (timestamp tz-aware UTC ok)
    Writes MariaDB DATETIME as naive UTC.
    """
    if df.empty:
        return

    df = df.copy()

    # Enforce numeric
    df["hour"] = pd.to_numeric(df["hour"], errors="coerce").astype("Int64")
    df["row"] = pd.to_numeric(df["row"], errors="coerce").astype("Int64")
    df["col"] = pd.to_numeric(df["col"], errors="coerce").astype("Int64")
    df["predicted_events"] = (
        pd.to_numeric(df["predicted_events"], errors="coerce")
        .fillna(0)
        .astype(int)
    )

    # Parse timestamp (ensure tz-aware UTC if possible)
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce", utc=True)

    df = df.dropna(subset=["hour", "row", "col", "timestamp"]).copy()
    if df.empty:
        return

    rows: list[dict] = []
    for r in df[["hour", "row", "col", "predicted_events", "timestamp"]].itertuples(index=False):
        ts = pd.Timestamp(r.timestamp)  # tz-aware UTC (because utc=True above)

        # Convert to naive UTC for MariaDB DATETIME
        ts_naive_utc = ts.tz_convert("UTC").tz_localize(None).to_pydatetime()

        rows.append(
            {
                "hour": int(r.hour),
                "row": int(r.row),
                "col": int(r.col),
                "predicted_events": int(r.predicted_events),
                "timestamp": ts_naive_utc,
            }
        )

    if not rows:
        return

    with engine.begin() as conn:
        conn.execute(UPSERT_SQL, rows)


def main():
    kafka_bootstrap = get_env("KAFKA_BOOTSTRAP_SERVERS")
    topic = get_env("KAFKA_TOPIC_INPUT", "weather-raw")
    group_id = get_env("KAFKA_GROUP_ID", "jupyter-consumer-group")

    mlflow_uri = get_env("MLFLOW_TRACKING_URI", "file:/mlruns")
    run_id_path = get_env("RUN_ID_FILE", "/mlruns/LATEST_RUN_ID")

    mariadb_url = get_env("MARIADB_URL")

    # Batching
    BATCH_SIZE = int(os.getenv("BATCH_SIZE", "200"))
    FLUSH_SEC = int(os.getenv("FLUSH_SEC", "5"))

    print("=== jupyter-consumer starting ===")
    print("Kafka:", kafka_bootstrap, "Topic:", topic, "Group:", group_id)
    print("MLflow:", mlflow_uri, "RUN_ID_FILE:", run_id_path)
    print("MariaDB:", mariadb_url)
    print("BATCH_SIZE:", BATCH_SIZE, "FLUSH_SEC:", FLUSH_SEC)

    # MLflow model load
    mlflow.set_tracking_uri(mlflow_uri)
    run_id = load_run_id(run_id_path)
    model_uri = f"runs:/{run_id}/model"
    print("Loading model:", model_uri)
    model = mlflow.sklearn.load_model(model_uri)

    # Feature order from model (preferred)
    if hasattr(model, "feature_names_in_"):
        feature_cols = list(model.feature_names_in_)
    else:
        feature_cols = [
            "temperature_2m",
            "relative_humidity_2m",
            "rain",
            "snowfall",
            "row",
            "col",
        ]
    print("Feature cols:", feature_cols)

    # DB engine + ensure table
    engine = create_engine(mariadb_url, pool_pre_ping=True)
    ensure_table(engine)

    # Kafka consumer
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=[kafka_bootstrap],
        group_id=group_id,
        enable_auto_commit=True,
        auto_offset_reset="latest",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        key_deserializer=lambda k: k.decode("utf-8") if k else None,
        consumer_timeout_ms=1000,  # iterator may end if idle -> we loop
    )

    print("KafkaConsumer created. Subscribing to topic:", topic)
    consumer.poll(timeout_ms=2000)

    try:
        print("Topic partitions:", consumer.partitions_for_topic(topic))
    except Exception as e:
        print("WARN: partitions_for_topic error:", repr(e))

    try:
        print("Initial assignment:", consumer.assignment())
    except Exception as e:
        print("WARN: assignment error:", repr(e))

    try:
        print("Subscription:", consumer.subscription())
    except Exception as e:
        print("WARN: subscription error:", repr(e))

    batch: list[dict] = []
    last_flush = time.time()

    def flush(reason: str):
        nonlocal batch, last_flush
        if not batch:
            return

        df = pd.DataFrame(batch)
        print(f"\nFLUSH ({reason}) batch_size={len(batch)} df_shape={df.shape} cols={list(df.columns)}")

        # Debug sample
        try:
            print("Incoming head(2):", df.head(2).to_dict(orient="records"))
        except Exception as e:
            print("WARN: cannot print head:", repr(e))

        # Required keys
        required_cols = {"row", "col", "slot"}
        missing_req = [c for c in required_cols if c not in df.columns]
        if missing_req:
            print("WARN: missing required cols -> skipping batch:", missing_req)
            batch = []
            last_flush = time.time()
            return

        missing_feat = [c for c in feature_cols if c not in df.columns]
        if missing_feat:
            print("WARN: missing feature cols -> skipping batch:", missing_feat)
            batch = []
            last_flush = time.time()
            return

        # Timestamp (UTC)
        df["timestamp"] = coerce_timestamp_utc(df)
        nat_cnt = int(df["timestamp"].isna().sum())
        if nat_cnt:
            print(f"WARN: timestamp NaT count: {nat_cnt}/{len(df)}")

        # Stable hour from slot
        df["hour"] = df["slot"].apply(hour_from_slot)
        bad_hour = int(pd.isna(df["hour"]).sum())
        if bad_hour:
            print(f"WARN: bad slot/hour count: {bad_hour}/{len(df)}")
            # show a few bad slots
            try:
                bad_slots = df.loc[df["hour"].isna(), "slot"].astype(str).head(5).tolist()
                print("Bad slot examples:", bad_slots)
            except Exception:
                pass

        # Drop bad rows
        df = df.dropna(subset=["timestamp", "hour", "row", "col"]).copy()
        if df.empty:
            print("WARN: empty after dropping bad timestamp/hour/row/col -> skip batch")
            batch = []
            last_flush = time.time()
            return

        df["hour"] = df["hour"].astype(int)

        # Keep only 1..72 to match 3-day forecast horizon
        df = df[(df["hour"] >= 1) & (df["hour"] <= 72)].copy()
        if df.empty:
            print("WARN: all rows filtered out (hour not in 1..72)")
            batch = []
            last_flush = time.time()
            return

        # Predict
        try:
            X = df[feature_cols].astype(float)
        except Exception as e:
            print("WARN: cannot build feature matrix:", repr(e))
            batch = []
            last_flush = time.time()
            return

        pred = model.predict(X)
        pred = np.clip(pred, 0, None)
        df["predicted_events"] = np.rint(pred).astype(int)

        # Prepare DB write
        df_db = df[["hour", "row", "col", "predicted_events", "timestamp"]].copy()

        print(
            "Prepared df_db:",
            df_db.shape,
            "hour_min/max=",
            (int(df_db["hour"].min()), int(df_db["hour"].max())),
            "ts_min/max=",
            (str(df_db["timestamp"].min()), str(df_db["timestamp"].max())),
        )

        upsert_predictions(engine, df_db)
        print(f"Upserted {len(df_db)} rows into forecast_predictions (PK=hour,row,col)")

        batch = []
        last_flush = time.time()

    print("Entering consume loop...")

    while True:
        got_message = False

        for msg in consumer:
            got_message = True
            payload = msg.value

            # Minimal debug for first messages per flush
            if len(batch) < 3 and isinstance(payload, dict):
                sample_keys = [
                    "slot",
                    "timestamp_hour_local",
                    "timestamp",
                    "time",
                    "row",
                    "col",
                    "temperature_2m",
                    "relative_humidity_2m",
                    "rain",
                    "snowfall",
                ]
                sample = {k: payload.get(k) for k in sample_keys if k in payload}
                print(
                    "GOT MSG sample:",
                    "partition=", msg.partition,
                    "offset=", msg.offset,
                    "key=", msg.key,
                    "payload=", sample,
                )

            batch.append(payload)

            now = time.time()
            if len(batch) >= BATCH_SIZE or (now - last_flush) >= FLUSH_SEC:
                flush(reason="size/time")

        if not got_message:
            # consumer iterator can end if idle; keep process alive
            time.sleep(2)

        now = time.time()
        if batch and (now - last_flush) >= FLUSH_SEC:
            flush(reason="idle-timeout")


if __name__ == "__main__":
    main()
