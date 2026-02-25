#!/bin/bash
set -e

echo "[INFO] Applying schema migration..."
docker exec -i postgres psql \
  -U "${POSTGRES_USER:-admin}" \
  -d "${POSTGRES_DB:-clickstream}" < scripts/migrate_schema.sql
echo "[OK] Migration applied."
