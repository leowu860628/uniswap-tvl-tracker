#!/bin/bash
set -e
echo "[start] Running migrations..."
# Remove old flags so the new versioned migration always gets a chance to run
rm -f "${DATA_DIR}/.seeded"
python3 scripts/migrate.py
echo "[start] Migrations done. Starting dashboard..."
exec streamlit run dashboard/app.py --server.port=$PORT --server.address=0.0.0.0 --server.headless=true
