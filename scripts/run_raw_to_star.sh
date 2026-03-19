#!/bin/bash
set -euo pipefail

docker compose exec -T spark /opt/spark/bin/spark-submit /opt/app/jobs/raw_to_star.py
