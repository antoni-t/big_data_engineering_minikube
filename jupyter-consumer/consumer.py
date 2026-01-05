#!/usr/bin/env python3
import os
import json
import time
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


def _coerce_timestamp(df: pd.DataFrame) -> pd.Series:
    """
    Find and parse the timestamp column from incoming payloads.
    Accepts typical keys from your pipeline:
      - timestamp_hour_local (current)
      - timestamp
      - time
    Returns pandas datetime64[ns] (naive, suitable for MariaDB DATETIME).
    """
    candidates = ["timestamp_hour_local", "timestamp", "time"]
    for c in candidates:
        if c in df.columns:
            # parse ISO like "2026-01-04T00:00" -> datetime
            ts = pd.to_datetime(df[c], errors="coerce")
            return ts
    # no candidate found
    return pd.to_datetime(pd.Series([pd.NaT] * len(df)), errors="coerce")


def add_hour_column(df: pd.DataFrame, ts_col_name: str = "timestamp") -> pd.DataFrame:
    """
    Add hour=1..72 based on timestamps.
    Base is 00:00 of the first day present in the batch (floor('D')).
    """
    out = df.copy()
    out[ts_col_name] = _coerce_timestamp(out)

    # Drop rows where timestamp couldn't be parsed
    out = out.dropna(subset=[ts_col_name]).copy()

    if out.empty:
        return out

    base = out[ts_col_name].min().floor("D")  # 00:00 of day-1
    out["hour"] = ((out[ts_col_name] - base) / pd.Timedelta(hours=1)).astype(int) + 1

    # Keep only 1..72 if you strictly want 3 days * 24 hours
    out = out[(out["hour"] >= 1) & (out["hour"] <= 72)].copy()

    return out


UPSERT_SQL = text("""
INSERT INTO forecast_predictions (hour, row, col, predicted_events, timestamp)
VALUES (:hour, :row, :col, :predicted_events, :timestamp)
ON DUPLICATE KEY UPDATE
  predicted_events = VALUES(predicted_events),
  timestamp = VALUES(timestamp);
""")


def upsert_predictions(engine, df: pd.DataFrame) -> None:
    """
    Expects df columns: hour,row,col,predicted_events,timestamp
    """
    if df.empty:
        return

    # Ensure proper dtypes
    df = df.copy()
    df["hour"] = pd.to_numeric(df["hour"], errors="coerce").astype("Int64")
    df["row"] = pd.to_numeric(df["row"], errors="coerce").astype("Int64")
    df["col"] = pd.to_numeric(df["col"], errors="coerce").astype("Int64")
    df["predicted_events"] = pd.to_numeric(df["predicted_events"], errors="coerce").fillna(0).astype(int)

    df = df.dropna(subset=["hour", "row", "col", "timestamp"]).copy()
    if df.empty:
        return

    # Convert to python-native values for SQLAlchemy executemany
    rows = []
    for r in df[["hour", "row", "col", "predicted_events", "timestamp"]].itertuples(index=False):
        rows.append(
            {
                "hour": int(r.hour),
                "row": int(r.row),
                "col": int(r.col),
                "predicted_events": int(r.predicted_events),
                # MariaDB DATETIME: pass naive datetime
                "timestamp": pd.Timestamp(r.timestamp).to_pydatetime(),
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

    mariadb_url = get_env("MARIADB_URL")  # mysql+pymysql://root:root@mariadb.default.svc.cluster.local:3306/predictions

    print("=== jupyter-consumer starting ===")
    print("Kafka:", kafka_bootstrap, "Topic:", topic, "Group:", group_id)
    print("MLflow:", mlflow_uri, "RUN_ID_FILE:", run_id_path)
    print("MariaDB:", mariadb_url)

    # MLflow setup + model load
    mlflow.set_tracking_uri(mlflow_uri)
    run_id = load_run_id(run_id_path)
    model_uri = f"runs:/{run_id}/model"
    print("Loading model:", model_uri)
    model = mlflow.sklearn.load_model(model_uri)

    # DB engine + table
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
        consumer_timeout_ms=0,
    )

    # Feature order from model (best)
    if hasattr(model, "feature_names_in_"):
        feature_cols = list(model.feature_names_in_)
    else:
        feature_cols = ["temperature_2m", "relative_humidity_2m", "rain", "snowfall", "row", "col"]

    print("Feature cols:", feature_cols)

    batch: list[dict] = []
    BATCH_SIZE = int(os.getenv("BATCH_SIZE", "200"))
    FLUSH_SEC = int(os.getenv("FLUSH_SEC", "5"))
    last_flush = time.time()

    def flush():
        nonlocal batch, last_flush
        if not batch:
            return

        df = pd.DataFrame(batch)

        # --- basic required cols from weather-producer payload ---
        # row/col must exist for PK
        if "row" not in df.columns or "col" not in df.columns:
            print("WARN: missing row/col -> skipping batch")
            batch = []
            last_flush = time.time()
            return

        # Validate + predict
        missing = [c for c in feature_cols if c not in df.columns]
        if missing:
            print("WARN: missing feature cols -> skipping batch:", missing)
            batch = []
            last_flush = time.time()
            return

        # Predict
        X = df[feature_cols].astype(float)
        pred = model.predict(X)
        pred = np.clip(pred, 0, None)

        # We store integer prediction for DB
        df["predicted_events"] = np.rint(pred).astype(int)

        # Add hour + timestamp column
        df2 = add_hour_column(df, ts_col_name="timestamp")

        if df2.empty:
            print("WARN: could not compute hour/timestamp -> skipping batch")
            batch = []
            last_flush = time.time()
            return

        # Keep only needed columns for upsert
        df_db = df2[["hour", "row", "col", "predicted_events", "timestamp"]].copy()

        # Upsert
        upsert_predictions(engine, df_db)
        print(f"Upserted {len(df_db)} rows into forecast_predictions (hour,row,col PK)")

        batch = []
        last_flush = time.time()

    for msg in consumer:
        payload = msg.value
        batch.append(payload)

        now = time.time()
        if len(batch) >= BATCH_SIZE or (now - last_flush) >= FLUSH_SEC:
            flush()

    # never reached normally
    flush()


if __name__ == "__main__":
    main()
