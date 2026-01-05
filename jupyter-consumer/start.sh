#!/usr/bin/env bash
set -euo pipefail

RUN_ID_FILE="${RUN_ID_FILE:-/mlruns/LATEST_RUN_ID}"

echo "Waiting for RUN_ID_FILE: ${RUN_ID_FILE}"
until [ -s "${RUN_ID_FILE}" ]; do
  echo "RUN_ID_FILE not ready yet... sleeping 5s"
  sleep 5
done

echo "RUN_ID found:"
cat "${RUN_ID_FILE}"
echo

exec python /app/consumer.py
