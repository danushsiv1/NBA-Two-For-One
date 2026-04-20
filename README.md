# NBA Play-by-Play 2-for-1 Strategy Auditor

> **Status:** Unfinished. This repository is a **skeleton / work in progress**. Ingestion, Parquet shaping, and Spark analytics contain explicit `TODO`s and are not yet wired end-to-end. Use it as a starting point rather than a production pipeline.

## Purpose

Evaluate how often teams execute a **2-for-1** shot-clock strategy in **NBA playoff** games and compare outcomes (e.g. net score change from the late-quarter window to the buzzer) against more “patient” single-possession endings.

**Intended data scope:** Playoffs from **2010 through 2025** (season labels and API parameters should be aligned with [nba_api](https://github.com/swar/nba_api) conventions).

## Project layout

| Path | Role |
|------|------|
| `config.py` | Season range, directories, 2-for-1 window (38s–28s), early-shot threshold (24s), API backoff, Spark app name. |
| `data_ingestion.py` | Fetch `PlayByPlayV2` via `nba_api`, store raw JSON, convert to Parquet partitioned by `Season` and `Game_ID`. |
| `spark_analyzer.py` | Local Spark session, `PCTIMESTRING` preprocessing, window functions for quarter-level logic, stubs for 2-for-1 metrics. |
| `data/` | Created at runtime (gitignored if you add a `.gitignore`): `raw_json/`, `playbyplay_parquet/`. |

## Requirements

- **Python** 3.10+ recommended (tested conceptually with 3.12).
- **Packages**

  ```bash
  python3 -m pip install nba_api pandas pyarrow pyspark
  ```

  - `nba_api` — stats endpoints (e.g. `PlayByPlayV2`).
  - `pandas` + `pyarrow` — Parquet writes in ingestion.
  - `pyspark` — local analysis in `spark_analyzer.py`.

- **Java** — PySpark’s local mode needs a JDK Spark can use (often already present on dev machines; install if Spark fails to start).

## Configuration

Edit `config.py` to change:

- `SEASON_START` / `SEASON_END` — inclusive playoff seasons to target.
- `DATA_DIR`, `RAW_JSON_DIR`, `PARQUET_DIR` — where JSON and Parquet live (defaults under this project).
- `POSSESSION_WINDOW_HIGH_SEC` / `POSSESSION_WINDOW_LOW_SEC` — possession start window (default **38** and **28** seconds left in the quarter).
- `EARLY_SHOT_GAME_CLOCK_THRESHOLD_SEC` — “early” FGA threshold (default **24** seconds; validate against your clock column definitions).
- `REQUEST_SLEEP_SECONDS`, `MAX_RETRIES`, `RETRY_BACKOFF_BASE` — polite use of the NBA stats API.

## How to run (when implementation is complete)

From the project root (the directory that contains `config.py`):

1. **Ingest**

   ```bash
   python3 data_ingestion.py
   ```

   Expected flow: resolve playoff `GAME_ID`s per season → fetch play-by-play → write `data/raw_json/{season}/{game_id}.json` → build `data/playbyplay_parquet/` with partitions `Season`, `Game_ID`.

2. **Analyze**

   ```bash
   python3 spark_analyzer.py
   ```

   Expected flow: read Parquet → add `seconds_remaining` / `period_seconds_remaining` → window logic and metrics (see below).

Until the `TODO`s are filled in, these commands may no-op, error on empty Parquet, or skip games entirely.

## What is implemented vs. still open

**In place (scaffolding):**

- Central settings in `config.py`.
- Retry + sleep wrapper around `PlayByPlayV2`, raw JSON paths, and a Parquet write **structure** in `data_ingestion.py`.
- Spark bootstrap, `PCTIMESTRING` → numeric seconds, and window definitions aligned with the spec (`partitionBy` `game_id` + `period`, `orderBy` `seconds_remaining` descending) in `spark_analyzer.py`.

**Still to finish (non-exhaustive):**

- **`iter_playoff_game_ids`** — list playoff games per season (e.g. `LeagueGameFinder` or schedule endpoints) and yield real `GAME_ID`s.
- **`json_to_parquet_partitioned`** — map `nba_api` JSON `resultSets` into flat rows; align column names (`GAME_ID`, `PERIOD`, `PCTIMESTRING`, `EVENTMSGTYPE`, team/score fields) with what Spark expects.
- **Possession boundaries** — define possession starts/stops from events; filter rows in the **38s–28s** window.
- **2-for-1 rules** — early FGA flag, `LEAD` / ordering for “same team took last shot of quarter,” and **net score change** from the window start to quarter end for 2-for-1 vs patient possessions.
- **Validation** — confirm sort order matches real time within each period; NBA `PCTIMESTRING` is usually time remaining in the period; game-clock vs period-clock must match the spec you use for “more than 24 seconds.”

## Data directories

After a full ingest:

- **`data/raw_json/{season}/{game_id}.json`** — verbatim API-style payloads for auditability and reprocessing.
- **`data/playbyplay_parquet/`** — Hive-style partitions, e.g. `Season=2019/Game_ID=0041900401/…`, for Spark.

## API etiquette

The public stats API is unofficial and rate-sensitive. Keep `REQUEST_SLEEP_SECONDS` conservative, use retries with backoff, and avoid parallel bursts across many games until you confirm stability.

## License / data

This project is a personal/educational scaffold. NBA data is fetched from public endpoints via `nba_api`; respect [NBA.com](https://www.nba.com) terms of use and do not rely on this for commercial redistribution without your own legal review.

---

*Last note: treat this README as describing **intent** and **layout**. Behavior is incomplete until the items under “Still to finish” are implemented and tested.*
