#!/usr/bin/env python3
import os
import sys
import json
import gzip
import urllib.parse
from io import BytesIO
from pathlib import Path, PurePosixPath

import urllib.parse

import pandas as pd
import numpy as np
import requests

from datetime import datetime
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score

import mlflow
import mlflow.sklearn

######################################################################
# Training Job: Autobahn-Event-Prognose mit Wetterdaten (MLflow + HDFS)
#
# Zweck
#   Dieses Skript trainiert ein Regressionsmodell zur Vorhersage der Anzahl
#   von Autobahnereignissen pro Rasterzelle und Stunde, indem historische
#   Autobahn-Warnmeldungen mit korrespondierenden Wetterdaten kombiniert
#   und das resultierende Modell samt Metriken in MLflow versioniert wird.
#
# Datenquellen
#   - Autobahn-Daten (*.jsonl.gz) aus HDFS (WebHDFS, hierarchische Struktur)
#   - Historische Wetterdaten (*.jsonl.gz) aus HDFS
#   - Statisches Deutschland-Gitter (CSV) zur räumlichen Aggregation
#
# Verarbeitungsschritte
#   1) Laden der Daten aus HDFS per WebHDFS (inkl. Streaming & Safety Limits)
#   2) Feature Engineering:
#      - Extraktion und Normalisierung von Autobahnereignissen
#      - Mapping von Events auf räumliche Rasterzellen und Stunden
#      - Aggregation (Counts, Labels, Geschwindigkeitsmetriken)
#   3) Join von Wetter- und Autobahndaten auf Zelle + Stunde
#   4) Training eines RandomForestRegressor (Sklearn)
#   5) Evaluation (MAE, RMSE, R²) auf Testdaten
#
# MLOps / Output
#   - Logging von Parametern, Metriken und Feature Importances in MLflow
#   - Persistenz des trainierten Modells als MLflow-Artefakt
#   - Schreiben der aktuellen RUN_ID in eine Datei (LATEST_RUN_ID),
#     die von nachgelagerten Inferenz-Services genutzt wird
######################################################################


# -----------------------------------------------------------------------------
# Configuration
# -----------------------------------------------------------------------------
# WebHDFS base (must include /webhdfs/v1, like: http://hdfs-namenode:9870/webhdfs/v1)
HDFS_WEBHDFS_URL = os.getenv("HDFS_WEBHDFS_URL", "").rstrip("/")
HDFS_USER = os.getenv("HDFS_USER", "hdfs")

# HDFS data roots
HDFS_AUTOBAHN_DIR = os.getenv("HDFS_AUTOBAHN_DIR", "/datalake/autobahn")
HDFS_WEATHER_DIR  = os.getenv("HDFS_WEATHER_DIR", "/datalake/weather_hist")

# local grid file inside container image
GRID_FILE = Path(os.getenv("GRID_FILE", "/app/de_grid_sym_400.csv"))

MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "file:/mlruns")
LATEST_RUN_ID_FILE  = Path(os.getenv("LATEST_RUN_ID_FILE", "/mlruns/LATEST_RUN_ID"))

# Performance / safety knobs
# - Limit how many files we load to avoid OOM in small clusters
MAX_AUTOBAHN_FILES = int(os.getenv("MAX_AUTOBAHN_FILES", "0"))  # 0 = no limit
MAX_WEATHER_FILES  = int(os.getenv("MAX_WEATHER_FILES", "0"))   # 0 = no limit

# - Optional row downsampling after load (0..1); 1.0 means keep all
AUTOBAHN_SAMPLE_FRAC = float(os.getenv("AUTOBAHN_SAMPLE_FRAC", "1.0"))
WEATHER_SAMPLE_FRAC  = float(os.getenv("WEATHER_SAMPLE_FRAC", "1.0"))

# - Requests timeouts
HTTP_TIMEOUT_S = int(os.getenv("HTTP_TIMEOUT_S", "60"))
HTTP_STREAM_TIMEOUT_S = int(os.getenv("HTTP_STREAM_TIMEOUT_S", "300"))

# -----------------------------------------------------------------------------
# WebHDFS helpers
# -----------------------------------------------------------------------------
def _webhdfs_url(hdfs_path: str, op: str, **params) -> str:
    """
    Build a WebHDFS URL.
    hdfs_path must start with "/" (e.g. "/datalake/autobahn")
    """
    if not hdfs_path.startswith("/"):
        hdfs_path = "/" + hdfs_path
    qp = {"op": op, "user.name": HDFS_USER}
    qp.update(params)
    return f"{HDFS_WEBHDFS_URL}{hdfs_path}?{urllib.parse.urlencode(qp)}"


def _require_hdfs_config():
    if not HDFS_WEBHDFS_URL:
        raise RuntimeError(
            "HDFS_WEBHDFS_URL is not set. Example:\n"
            "  http://hdfs-namenode.default.svc.cluster.local:9870/webhdfs/v1"
        )


def hdfs_list_recursive(root_dir: str) -> list[str]:
    """
    Recursively list all files under root_dir (HDFS path).
    Returns a list of absolute HDFS paths.
    """
    _require_hdfs_config()
    out: list[str] = []
    stack = [root_dir]

    while stack:
        d = stack.pop()
        r = requests.get(_webhdfs_url(d, "LISTSTATUS"), timeout=HTTP_TIMEOUT_S)
        r.raise_for_status()
        statuses = r.json().get("FileStatuses", {}).get("FileStatus", [])

        for s in statuses:
            suffix = s.get("pathSuffix", "")
            p = str(PurePosixPath(d) / suffix) if suffix else d
            if s.get("type") == "DIRECTORY":
                stack.append(p)
            else:
                out.append(p)

    return out



HDFS_DATANODE_SERVICE = os.getenv("HDFS_DATANODE_SERVICE", "hdfs-datanode.default.svc.cluster.local:9864")

def _rewrite_datanode_location(loc: str) -> str:
    """
    WebHDFS redirects OPEN/CREATE to a DataNode URL, often with host=<podname>.
    Pod hostnames are not resolvable in this cluster -> rewrite host to the DataNode Service.
    """
    u = urllib.parse.urlparse(loc)

    # keep scheme; force netloc to the service
    # (netloc includes host:port)
    new_u = u._replace(netloc=HDFS_DATANODE_SERVICE)
    return urllib.parse.urlunparse(new_u)

def hdfs_open_stream(hdfs_file: str):
    # Step 1: ask NameNode for OPEN, but do NOT follow redirect
    r1 = requests.get(
        _webhdfs_url(hdfs_file, "OPEN"),
        allow_redirects=False,
        stream=True,
        timeout=60,
    )

    # NameNode typically responds 307 with Location to DataNode
    if r1.status_code in (307, 302):
        loc = r1.headers.get("Location")
        if not loc:
            raise RuntimeError("WebHDFS OPEN redirect without Location header")

        loc2 = _rewrite_datanode_location(loc)

        # Step 2: stream from DataNode service
        r2 = requests.get(loc2, stream=True, timeout=HTTP_STREAM_TIMEOUT_S)
        r2.raise_for_status()
        return r2

    # Some setups may directly return 200
    r1.raise_for_status()
    return r1


# -----------------------------------------------------------------------------
# Data loading (HDFS -> pandas)
# -----------------------------------------------------------------------------
def list_jsonl_gz_by_structure_hdfs(root_dir: str) -> list[str]:
    """
    Expects HDFS structure: root/YYYY/MM/DD/HH/*.jsonl.gz
    We list recursively and filter *.jsonl.gz
    """
    all_files = hdfs_list_recursive(root_dir)
    files = [p for p in all_files if p.endswith(".jsonl.gz")]
    files.sort()
    return files


def load_jsonl_gz_structured_hdfs(root_dir: str, max_files: int = 0, sample_frac: float = 1.0) -> pd.DataFrame:
    files = list_jsonl_gz_by_structure_hdfs(root_dir)
    if not files:
        raise FileNotFoundError(f"No .jsonl.gz files found under HDFS: {root_dir}")

    if max_files and max_files > 0:
        files = files[:max_files]

    out = []
    for i, hdfs_file in enumerate(files, 1):
        resp = hdfs_open_stream(hdfs_file)
        # Pandas can read gzipped jsonl from bytes; easiest: read all, then decompress
        raw = resp.content
        try:
            with gzip.GzipFile(fileobj=BytesIO(raw)) as gz:
                df = pd.read_json(gz, lines=True)
        except Exception as e:
            raise RuntimeError(f"Failed to read {hdfs_file} as .jsonl.gz: {e}") from e

        out.append(df)

        if i % 50 == 0:
            rows = sum(len(x) for x in out)
            print(f"loaded {i}/{len(files)} files; current rows={rows}")

    merged = pd.concat(out, ignore_index=True)

    if 0.0 < sample_frac < 1.0 and len(merged) > 0:
        merged = merged.sample(frac=sample_frac, random_state=42).reset_index(drop=True)

    return merged


# -----------------------------------------------------------------------------
# Feature engineering
# -----------------------------------------------------------------------------

# Wandelt rohe Autobahnmeldungen in strukturierte, zeit- und ortsbezogene Ereignisdaten mit Stau-Labeln um (per DataFrame).
def build_events_from_autobahn(autobahn_raw: pd.DataFrame) -> pd.DataFrame:
    df = autobahn_raw.dropna(subset=["payload"]).copy()
    df["warnings"] = df["payload"].apply(lambda x: x.get("warning", []) if isinstance(x, dict) else [])
    df = df[df["warnings"].map(len) > 0].copy()
    df_exp = df.explode("warnings").reset_index(drop=True)
    warn_flat = pd.json_normalize(df_exp["warnings"])

    events = pd.concat(
        [df_exp[["ts_utc", "road_id"]].reset_index(drop=True), warn_flat.reset_index(drop=True)],
        axis=1
    )

    events["ts_utc"] = pd.to_datetime(events["ts_utc"], utc=True, errors="coerce")
    events["event_time"] = events["ts_utc"]
    events["event_hour"] = events["event_time"].dt.floor("h")

    if "coordinate.lat" not in events.columns or "coordinate.long" not in events.columns:
        raise ValueError("Missing columns coordinate.lat / coordinate.long in Autobahn payload")

    events["lat"] = pd.to_numeric(events["coordinate.lat"], errors="coerce")
    events["lon"] = pd.to_numeric(events["coordinate.long"], errors="coerce")
    events = events.dropna(subset=["lat", "lon"]).copy()

    if "averageSpeed" in events.columns:
        events["avg_speed_raw"] = pd.to_numeric(events["averageSpeed"], errors="coerce")
    else:
        events["avg_speed_raw"] = np.nan

    congested_types = {"SLOW_TRAFFIC", "STATIONARY_TRAFFIC", "QUEUING_TRAFFIC"}
    if "abnormalTrafficType" in events.columns:
        events["label"] = events["abnormalTrafficType"].isin(congested_types).astype(int)
    else:
        events["label"] = 0

    return events

# Ordnet Autobahnereignisse räumlich Rasterzellen zu und aggregiert sie stündlich pro Zelle zu Ereignis- und Staukennzahlen
def map_events_to_cells(events: pd.DataFrame, grid_points_csv: Path) -> pd.DataFrame:
    pts = pd.read_csv(grid_points_csv)  # id,row,col,lat,lon (corner points)
    pts["lat"] = pts["lat"].astype(float)
    pts["lon"] = pts["lon"].astype(float)

    lat_pts = np.sort(pts["lat"].unique())
    lon_pts = np.sort(pts["lon"].unique())
    dlat = float(np.median(np.diff(lat_pts)))
    dlon = float(np.median(np.diff(lon_pts)))
    lat0 = float(lat_pts.min())
    lon0 = float(lon_pts.min())

    n_cell_rows = len(lat_pts) - 1
    n_cell_cols = len(lon_pts) - 1

    ev = events.copy()
    ev["cell_row"] = np.floor((ev["lat"] - lat0) / dlat).astype("int64")
    ev["cell_col"] = np.floor((ev["lon"] - lon0) / dlon).astype("int64")

    mask = (
        (ev["cell_row"] >= 0) & (ev["cell_row"] < n_cell_rows) &
        (ev["cell_col"] >= 0) & (ev["cell_col"] < n_cell_cols)
    )
    ev = ev.loc[mask].copy()

    ev["cell_id"] = "cell_" + ev["cell_row"].astype(str) + "_" + ev["cell_col"].astype(str)
    ev["event_hour"] = ev["event_time"].dt.floor("h")

    grid_hour_agg = (
        ev.groupby(["event_hour", "cell_id"], as_index=False)
          .agg(
              event_count=("cell_id", "size"),
              congested_count=("label", "sum"),
              label=("label", "max"),
              avg_speed_min=("avg_speed_raw", "min"),
              avg_speed_mean=("avg_speed_raw", "mean"),
              row=("cell_row", "first"),
              col=("cell_col", "first"),
          )
    )
    return grid_hour_agg


# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------
def main():
    print("=== TRAINING JOB START ===")
    print("HDFS_WEBHDFS_URL:", HDFS_WEBHDFS_URL)
    print("HDFS_AUTOBAHN_DIR:", HDFS_AUTOBAHN_DIR)
    print("HDFS_WEATHER_DIR:", HDFS_WEATHER_DIR)
    print("GRID_FILE:", GRID_FILE)
    print("MLFLOW_TRACKING_URI:", MLFLOW_TRACKING_URI)

    _require_hdfs_config()

    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    mlflow.set_experiment("autobahn_event_forecast_rf")

    # 1) Autobahn laden (HDFS)
    autobahn_raw = load_jsonl_gz_structured_hdfs(
        HDFS_AUTOBAHN_DIR,
        max_files=MAX_AUTOBAHN_FILES,
        sample_frac=AUTOBAHN_SAMPLE_FRAC,
    )
    print("autobahn_raw shape:", autobahn_raw.shape)

    # 2) Events -> cell-hour Aggregation
    events = build_events_from_autobahn(autobahn_raw)
    grid_hour_agg = map_events_to_cells(events, GRID_FILE)

    print("grid_hour_agg shape:", grid_hour_agg.shape)
    print("time range:", grid_hour_agg["event_hour"].min(), "->", grid_hour_agg["event_hour"].max())

    # 3) Historische Wetterdaten aus HDFS
    weather = load_jsonl_gz_structured_hdfs(
        HDFS_WEATHER_DIR,
        max_files=MAX_WEATHER_FILES,
        sample_frac=WEATHER_SAMPLE_FRAC,
    )

    # expected: time,id,row,col,temperature_2m,rain,snowfall,relative_humidity_2m...
    if "time" in weather.columns:
        weather["time"] = pd.to_datetime(weather["time"], utc=True, errors="coerce")
    print("weather shape:", weather.shape)

    # 4) Merge weather (by hour+cell) with labels
    # weather uses id=time cell_id? (Format: id + time)
    # ensure cell id exists
    if "id" not in weather.columns and ("row" in weather.columns and "col" in weather.columns):
        weather["id"] = "cell_" + weather["row"].astype(int).astype(str) + "_" + weather["col"].astype(int).astype(str)

    merged = pd.merge(
        weather,
        grid_hour_agg,
        left_on=["id", "time"],
        right_on=["cell_id", "event_hour"],
        how="left"
    )

    # ensure row/col exist after merge (handle suffixes)
    if "row" not in merged.columns or "col" not in merged.columns:
        # Prefer weather side if present
        if "row_x" in merged.columns and "col_x" in merged.columns:
            merged["row"] = merged["row_x"]
            merged["col"] = merged["col_x"]
        # Else fallback to autobahn agg side
        elif "row_y" in merged.columns and "col_y" in merged.columns:
            merged["row"] = merged["row_y"]
            merged["col"] = merged["col_y"]

    # If still missing, derive from id like "cell_<row>_<col>"
    if ("row" not in merged.columns or "col" not in merged.columns) and ("id" in merged.columns):
        rc = merged["id"].astype(str).str.extract(r"cell_(\d+)_(\d+)")
        merged["row"] = pd.to_numeric(rc[0], errors="coerce")
        merged["col"] = pd.to_numeric(rc[1], errors="coerce")

    merged["row"] = pd.to_numeric(merged["row"], errors="coerce")
    merged["col"] = pd.to_numeric(merged["col"], errors="coerce")

    # Fill Autobahn columns
    EVENT_COLS = ["event_count", "congested_count", "label", "avg_speed_min", "avg_speed_mean"]
    for col in EVENT_COLS:
        if col in merged.columns:
            merged[col] = merged[col].fillna(0)

    merged["event_count"] = merged["event_count"].astype(int)

    # Debug prints
    print("merged columns:", sorted(list(merged.columns))[:60])
    print("row/col preview:", merged[["id", "row", "col"]].head(3))

    # 5) Train RF
    feature_cols = ["temperature_2m", "relative_humidity_2m", "rain", "snowfall", "row", "col"]
    merged = merged.dropna(subset=feature_cols + ["event_count"]).copy()

    X = merged[feature_cols].astype(float)
    y = merged["event_count"].astype(int)

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    reg = RandomForestRegressor(
        n_estimators=400,
        max_depth=20,
        min_samples_leaf=5,
        random_state=42,
        n_jobs=-1
    )
    reg.fit(X_train, y_train)
    pred = np.clip(reg.predict(X_test), 0, None)

    mae = mean_absolute_error(y_test, pred)
    rmse = np.sqrt(mean_squared_error(y_test, pred))
    r2 = r2_score(y_test, pred)

    fi = (
        pd.DataFrame({"feature": feature_cols, "importance": reg.feature_importances_})
          .sort_values("importance", ascending=False)
    )

    # 6) Log to MLflow and write RUN_ID file
    with mlflow.start_run(run_name="rf_event_count_regression") as run:
        mlflow.log_params({
            "model": "RandomForestRegressor",
            "n_estimators": 400,
            "max_depth": 20,
            "min_samples_leaf": 5,
            "features": ",".join(feature_cols)
        })
        mlflow.log_metrics({"MAE": mae, "RMSE": rmse, "R2": r2})

        mlflow.sklearn.log_model(
            sk_model=reg,
            artifact_path="model",
            input_example=X_train.head(5)
        )

        fi_path = "/tmp/feature_importances.csv"
        fi.to_csv(fi_path, index=False)
        mlflow.log_artifact(fi_path)

        run_id = run.info.run_id
        print("MLflow run_id:", run_id)

        LATEST_RUN_ID_FILE.parent.mkdir(parents=True, exist_ok=True)
        LATEST_RUN_ID_FILE.write_text(run_id, encoding="utf-8")
        print("Wrote LATEST_RUN_ID to:", str(LATEST_RUN_ID_FILE))

    print("=== TRAINING JOB DONE ===")
    return 0


if __name__ == "__main__":
    sys.exit(main())
