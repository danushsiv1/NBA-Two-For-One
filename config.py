"""
Configuration for NBA Play-by-Play 2-for-1 strategy audit (playoffs 2010–2025).
"""

from pathlib import Path

# --- Seasons (inclusive). NBA season labels: e.g. 2019 = 2019-20 playoffs. ---
SEASON_START = 2010
SEASON_END = 2025

# --- Paths (all under project root by default) ---
PROJECT_ROOT = Path(__file__).resolve().parent
DATA_DIR = PROJECT_ROOT / "data"
RAW_JSON_DIR = DATA_DIR / "raw_json"
PARQUET_DIR = DATA_DIR / "playbyplay_parquet"

# --- 2-for-1 / possession window (seconds left in quarter) ---
POSSESSION_WINDOW_HIGH_SEC = 38
POSSESSION_WINDOW_LOW_SEC = 28

# --- Early shot: FGA with more than this many seconds on the *game* clock ---
EARLY_SHOT_GAME_CLOCK_THRESHOLD_SEC = 24

# --- API / ingestion ---
REQUEST_SLEEP_SECONDS = 0.6  # tune for nba_api rate limits
MAX_RETRIES = 5
RETRY_BACKOFF_BASE = 2.0

# --- Spark ---
SPARK_APP_NAME = "NBA_2for1_Auditor"
