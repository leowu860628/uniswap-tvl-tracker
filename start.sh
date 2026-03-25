#!/bin/bash
set -e
echo "[start] Running migrations..."
rm -f "${DATA_DIR}/.seeded"
python3 scripts/migrate.py
echo "[start] Starting scheduler in background..."
python3 run.py &
echo "[start] Starting dashboard..."
exec streamlit run dashboard/app.py --server.port=$PORT --server.address=0.0.0.0 --server.headless=true
