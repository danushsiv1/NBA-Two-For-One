#!/usr/bin/env bash
# Full ingest (2010–2025 playoffs per config.py) then report. Can take hours.
set -euo pipefail
cd "$(dirname "$0")"
echo "=== Ingestion (no --max-games) ==="
python3 data_ingestion.py 2>&1 | tee ingest_full.log
echo "=== Report ==="
python3 spark_analyzer.py --engine spark 2>&1 | tee report_run.log
echo "=== Done. Report: reports/two_for_one_report.md ==="
cat reports/two_for_one_report.md
