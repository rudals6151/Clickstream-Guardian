#!/bin/bash
set -e

docker compose -f docker/docker-compose.yml up -d
python scripts/smoke_test.py --base-url http://localhost:8000
