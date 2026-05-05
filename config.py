"""
Configuration for NBA Play-by-Play 2-for-1 strategy audit (playoffs 2010–2025).
"""

import os
from pathlib import Path

# --- Java (PySpark): Temurin 17 installed without sudo (see README). ---
JAVA_17_HOME = (
    Path.home()
    / "Library/Java/JavaVirtualMachines/temurin-17-aarch64.jdk/Contents/Home"
)

# --- Seasons: "playoff year" Y = June finals year (2010 = 2009–10 playoffs). Inclusive. ---
SEASON_START = 2010
SEASON_END = 2025

# --- Paths (all under project root by default) ---
PROJECT_ROOT = Path(__file__).resolve().parent
DATA_DIR = PROJECT_ROOT / "data"
RAW_JSON_DIR = DATA_DIR / "raw_json"
PARQUET_DIR = DATA_DIR / "playbyplay_parquet"
REPORT_PATH = PROJECT_ROOT / "reports" / "two_for_one_report.md"

# --- 2-for-1 window: FGA with this many seconds *left in the quarter* (inclusive) ---
POSSESSION_WINDOW_HIGH_SEC = 38
POSSESSION_WINDOW_LOW_SEC = 28

# --- Patient / late FGA (clock left in quarter): 3–27s always; 0–3s included only if not a long heave ---
MIN_LATE_FGA_SECONDS = 3  # below this, apply shot-distance rule; from here to 28, all FGAs count
# 0–3s FGAs with shot_distance *strictly greater* than this (feet) are excluded (typical end-quarter heaves)
HEAVE_EXCLUDE_DISTANCE_FT = 35.0

# --- Regulation quarters only (set False to include OT) ---
REGULATION_PERIODS_ONLY = True

# --- Ingestion ---
REQUEST_SLEEP_SECONDS = 0.6
MAX_RETRIES = 5
RETRY_BACKOFF_BASE = 2.0
# Persist raw API JSON (large); set True for audit trail
SAVE_RAW_JSON = False

# --- Spark / cluster ---
SPARK_APP_NAME = "NBA_2for1_Auditor"
# local[*] for laptop; set SPARK_MASTER=yarn / spark://... / k8s://... for Hadoop YARN / standalone / K8s
SPARK_MASTER = os.environ.get("SPARK_MASTER", "local[*]")
# Mixed Parquet (int64 vs double scores) breaks Spark's vectorized reader; keep false for robust reads
SPARK_PARQUET_VECTORIZED = os.environ.get("SPARK_PARQUET_VECTORIZED", "false").lower() in (
    "1",
    "true",
    "yes",
)
# Per-file Parquet unions build a deep Catalyst plan; raise driver heap if you still see OOM.
SPARK_DRIVER_MEMORY = os.environ.get("SPARK_DRIVER_MEMORY", "4g")
# Truncates lineage after union (written under this directory; can be large).
SPARK_LOCAL_CHECKPOINT_DIR = PROJECT_ROOT / ".spark_local_checkpoint"
