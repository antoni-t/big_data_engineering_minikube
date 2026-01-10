#!/usr/bin/env python3
import os
import sys
from pathlib import Path
import pandas as pd
import numpy as np

from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score

import mlflow
import mlflow.sklearn

AUTOBAHN_DIR = Path(os.getenv("AUTOBAHN_DIR", "/autobahn_data"))
WEATHER_DIR  = Path(os.getenv("WEATHER_DIR", "/weather_hist")) 
GRID_FILE    = Path(os.getenv("GRID_FILE", "/app/de_grid_sym_400.csv"))

MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "file:/mlruns")
LATEST_RUN_ID_FILE  = Path(os.getenv("LATEST_RUN_ID_FILE", "/mlruns/LATEST_RUN_ID"))

# -----------------------------
# Helpers
# -----------------------------
def load_jsonl_gz_recursive(root: Path) -> pd.DataFrame:
    files = list(root.rglob("*.jsonl.gz"))
    if not files:
        raise FileNotFoundError(f"No .jsonl.gz files found under: {root}")

    out = []
    for i, f in enumerate(files, 1):
        df = pd.read_json(f, lines=True, compression="gzip")
        out.append(df)
        if i % 50 == 0:
            print(f"loaded {i}/{len(files)} files; current rows={sum(len(x) for x in out)}")

    return pd.concat(out, ignore_index=True)

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

def main():
    print("=== TRAINING JOB START ===")
    print("AUTOBAHN_DIR:", AUTOBAHN_DIR)
    print("GRID_FILE:", GRID_FILE)
    print("MLFLOW_TRACKING_URI:", MLFLOW_TRACKING_URI)

    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    mlflow.set_experiment("autobahn_event_forecast_rf")

    # 1) Autobahn laden (gemountet)
    autobahn_raw = load_jsonl_gz_recursive(AUTOBAHN_DIR)
    print("autobahn_raw shape:", autobahn_raw.shape)

    # 2) Events -> cell-hour Aggregation
    events = build_events_from_autobahn(autobahn_raw)
    grid_hour_agg = map_events_to_cells(events, GRID_FILE)

    print("grid_hour_agg shape:", grid_hour_agg.shape)
    print("time range:", grid_hour_agg["event_hour"].min(), "->", grid_hour_agg["event_hour"].max())

    # 3) Historische Wetterdaten:
    #    Du wolltest sie im Training-Pod über API abfragen.
    #    Damit das jetzt erstmal “läuft”, gehen wir pragmatisch vor:
    #    -> Falls du Weather noch nicht im Pod erzeugst, kannst du WEATHER_DIR mounten.
    #    -> Oder du ergänzt später hier den Open-Meteo Archive-Call (du hattest bereits ein gutes Script).
    if WEATHER_DIR.exists() and list(WEATHER_DIR.rglob("*.jsonl.gz")):
        weather = load_jsonl_gz_recursive(WEATHER_DIR)
        # weather columns expected: time,id,row,col,temperature_2m,rain,snowfall,relative_humidity_2m...
        if "time" in weather.columns:
            weather["time"] = pd.to_datetime(weather["time"], utc=True, errors="coerce")
        print("weather shape:", weather.shape)
    else:
        raise RuntimeError(
            f"No historical weather found at {WEATHER_DIR}. "
            "For now: mount your local historical weather folder to /weather_hist. "
            "Next step: integrate archive API fetch inside this job."
        )

    # 4) Merge weather (by hour+cell) with labels
    #    weather uses id=time cell_id? (dein Format: id + time)
    #    Wir brauchen cell_id: aus row/col bauen falls nicht vorhanden
    if "id" not in weather.columns and ("row" in weather.columns and "col" in weather.columns):
        weather["id"] = "cell_" + weather["row"].astype(int).astype(str) + "_" + weather["col"].astype(int).astype(str)

    merged = pd.merge(
        weather,
        grid_hour_agg,
        left_on=["id", "time"],
        right_on=["cell_id", "event_hour"],
        how="left"
    )

    # --- ensure row/col exist after merge (handle suffixes) ---
    # pandas adds suffixes when both sides have same column names
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
    if "row" not in merged.columns or "col" not in merged.columns:
        if "id" in merged.columns:
            rc = merged["id"].astype(str).str.extract(r"cell_(\d+)_(\d+)")
            merged["row"] = pd.to_numeric(rc[0], errors="coerce")
            merged["col"] = pd.to_numeric(rc[1], errors="coerce")

    # finally enforce numeric
    merged["row"] = pd.to_numeric(merged["row"], errors="coerce")
    merged["col"] = pd.to_numeric(merged["col"], errors="coerce")

    # Fill Autobahn columns
    EVENT_COLS = ["event_count","congested_count","label","avg_speed_min","avg_speed_mean"]
    for col in EVENT_COLS:
        if col in merged.columns:
            merged[col] = merged[col].fillna(0)

    merged["event_count"] = merged["event_count"].astype(int)

    #folgende zwei prints sind für debugging
    print("merged columns:", sorted(list(merged.columns))[:60])
    print("row/col preview:", merged[["id","row","col"]].head(3))


    # 5) Train RF
    feature_cols = ["temperature_2m","relative_humidity_2m","rain","snowfall","row","col"]
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

    fi = pd.DataFrame({"feature": feature_cols, "importance": reg.feature_importances_}) \
           .sort_values("importance", ascending=False)

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

        #mlflow.sklearn.log_model(
        #    sk_model=reg,
        #    name="model",
        #    input_example=X_train.head(5)
        #)

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
