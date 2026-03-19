#!/bin/bash
set -euo pipefail

docker compose exec -T spark /opt/spark/bin/spark-submit /opt/app/jobs/star_to_clickhouse_reports.py
