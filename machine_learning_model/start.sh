#!/usr/bin/env bash
set -e

# Arbeitsverzeichnis
cd /app

# Jupyter starten
exec jupyter lab \
  --ip=0.0.0.0 \
  --port=8888 \
  --no-browser \
  --ServerApp.token="" \
  --ServerApp.password="" \
  --ServerApp.allow_origin="*" \
  --ServerApp.allow_remote_access=True \
  --ServerApp.root_dir=/app
