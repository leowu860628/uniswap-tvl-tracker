#!/bin/bash
echo "[start] Container starting..."
echo "[start] DATA_DIR=$DATA_DIR"
echo "[start] DB_SEED_URL=${DB_SEED_URL:0:40}..."

if [ -n "$DB_SEED_URL" ]; then
  echo "[start] Seeding database..."
  mkdir -p "$DATA_DIR"
  curl -fsSL -o "$DATA_DIR/tvl.db" "$DB_SEED_URL"
  echo "[start] Done. Size: $(du -h $DATA_DIR/tvl.db | cut -f1)"
else
  echo "[start] No DB_SEED_URL set, skipping seed."
fi

exec streamlit run dashboard/app.py --server.port=$PORT --server.address=0.0.0.0 --server.headless=true
