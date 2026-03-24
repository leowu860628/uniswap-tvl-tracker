#!/bin/bash
set -e

if [ -n "$DB_SEED_URL" ]; then
  mkdir -p "$DATA_DIR"
  echo "[start] Seeding database from $DB_SEED_URL ..."
  curl -fsSL "$DB_SEED_URL" -o "$DATA_DIR/tvl.db"
  echo "[start] Database seeded ($(du -h $DATA_DIR/tvl.db | cut -f1))"
  # Clear the env var so future restarts don't re-seed
  unset DB_SEED_URL
fi

exec streamlit run dashboard/app.py --server.port=$PORT --server.address=0.0.0.0 --server.headless=true
