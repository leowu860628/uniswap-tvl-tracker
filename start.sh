#!/bin/bash
set -e
echo "[start] Running migrations..."
python3 scripts/migrate.py
echo "[start] Starting dashboard..."
exec streamlit run dashboard/app.py --server.port=$PORT --server.address=0.0.0.0 --server.headless=true
