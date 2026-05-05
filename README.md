# NBA Play-by-Play 2-for-1 Strategy Auditor

End-to-end pipeline: download **NBA playoff** play-by-play with [`nba_api`](https://github.com/swar/nba_api), store **partitioned Parquet** (`Season`, `Game_ID`), then compare **2-for-1 window** field goal attempts (**28–38 seconds** left in the quarter) vs **patient late** attempts (**3–27 seconds**, plus short-clock non-heaves). Output: `reports/two_for_one_report.md`.

---

## What you get

- **`data_ingestion.py`** — `LeagueGameFinder` (playoffs) + **`PlayByPlayV3`** (**`PlayByPlayV2`** is deprecated and often empty). Writes `data/playbyplay_parquet/`. Failed games are skipped with a warning after retries.
- **`spark_analyzer.py`** — **Apache Spark (PySpark)** by default (`SPARK_MASTER` for clusters). **`--engine ray`** runs the same logic per game file in parallel. **`--engine pandas`** for quick validation.
- **`reports/two_for_one_report.md`** — FGA counts and mean/median **net margin change** for the shooting team from the FGA to the quarter buzzer.

> **Causal caution:** This is **observational**—who shoots early vs late is not randomized. The report states that explicitly.

---

## Analysis (what a full run showed)

Numbers refresh every time you run the analyzer; the **latest** figures are always in `reports/two_for_one_report.md`. On a **complete** ingest through **2010–2025** playoffs (~**1,336** games, ~**14.7k** FGA rows in the two buckets), results looked like:

| Bucket | Rough FGA count | Mean net margin Δ (pts)* |
|--------|-----------------|---------------------------|
| patient_late | ~10.6k | ~−0.55 |
| two_for_one_window (28–38s) | ~4.1k | ~−0.36 |

\*Shooting team: (margin at quarter end) − (margin at shot). **Higher is better.**

**Reading:** The **2-for-1 clock band** had a **less negative** average margin swing to the buzzer than the **patient late** band—that is a **descriptive** association in this sample only. It does **not** prove that “going 2-for-1” *causes* better outcomes (skill, score, fouls, and shot quality differ systematically).

---

## Adding or removing seasons

1. Edit **`config.py`**: set **`SEASON_START`** and **`SEASON_END`** to the playoff **calendar years** you want (the **Finals year**, e.g. `2010` = 2009–10 playoffs).
2. Run ingestion so Parquet matches that range:
   - **Incremental:** `python3 data_ingestion.py` — skips games that already have `part-0.parquet`.
   - **Force refresh:** `python3 data_ingestion.py --no-skip-existing`.
3. Run **`python3 spark_analyzer.py`** (or `./run_full_pipeline.sh` for ingest + report).

Clock windows and heave logic live in the same file (`POSSESSION_WINDOW_*`, `MIN_LATE_FGA_SECONDS`, `HEAVE_EXCLUDE_DISTANCE_FT`).

---

## Why Spark—and why it worked well here

**Why Spark**

- **Batch analytics on many Parquet files** — playoff seasons mean **hundreds of games** and **millions of rows** of play-by-play; Spark reads partitioned Parquet, joins, filters, and aggregates in **SQL/DataFrame** form without holding everything in one pandas table.
- **Same stack as “big data” jobs** — Parquet layout matches what you’d put on **HDFS** or cloud storage; set **`SPARK_MASTER`** to **`yarn`**, a **standalone** URL, or **Kubernetes** and the same script path works on a cluster.

**Why it worked well in this repo**

- **Real Parquet is messy** — different games had **INT64 vs DOUBLE** score columns. A single multi-file read can fail schema merge; this code **reads each game file**, **casts** to a common schema, **unions**, then **`localCheckpoint`** so the driver doesn’t run out of memory on a huge logical plan.
- **Tuning** — **`SPARK_PARQUET_VECTORIZED=false`** (default) avoids vectorized reader edge cases; **`SPARK_DRIVER_MEMORY`** (default **4g**, env-overridable) helps large unions. Optional **Ray** (`--engine ray`) scales the **same** per-game logic across cores/machines.

---

## Requirements

```bash
python3 -m pip install -r requirements.txt
```

### Java 17 for PySpark (macOS)

PySpark **3.5+** needs **Java 17+**. If Temurin 17 is installed here, `spark_analyzer.py` sets **`JAVA_HOME`** automatically:

`~/Library/Java/JavaVirtualMachines/temurin-17-aarch64.jdk/Contents/Home`

**Apple Silicon one-liner** (download + unpack Temurin 17):

```bash
mkdir -p ~/Library/Java/JavaVirtualMachines && cd /tmp && \
  curl -sL "https://api.adoptium.net/v3/binary/latest/17/ga/mac/aarch64/jdk/hotspot/normal/eclipse?project=jdk" -o jdk17.tar.gz && \
  tar xzf jdk17.tar.gz && rm -f jdk17.tar.gz && \
  rm -rf ~/Library/Java/JavaVirtualMachines/temurin-17-aarch64.jdk && \
  mv jdk-17* ~/Library/Java/JavaVirtualMachines/temurin-17-aarch64.jdk && \
  ~/Library/Java/JavaVirtualMachines/temurin-17-aarch64.jdk/Contents/Home/bin/java -version
```

- **Homebrew:** `brew install --cask temurin@17` (may prompt for `sudo`).
- **Fallback:** `python3 spark_analyzer.py --engine pandas` if Spark won’t start locally.

---

## Configuration (`config.py`)

| Setting | Meaning |
|--------|---------|
| `SEASON_START` / `SEASON_END` | Playoff year range (Finals calendar year). Default **2010–2025**. |
| `POSSESSION_WINDOW_LOW_SEC` / `HIGH_SEC` | 2-for-1 FGA window (default **28–38**). |
| `MIN_LATE_FGA_SECONDS`, `HEAVE_EXCLUDE_DISTANCE_FT` | Patient vs heave rule for sub-3s clock. |
| `REGULATION_PERIODS_ONLY` | `True` → Q1–Q4 only. |
| `REQUEST_SLEEP_SECONDS` | API pacing. |
| `SPARK_MASTER` (env) | Default **`local[*]`**; use **`yarn`** / **`spark://…`** / K8s on a cluster. |
| `SPARK_PARQUET_VECTORIZED` (env) | Default **`false`** — safer on mixed Parquet. |
| `SPARK_DRIVER_MEMORY` (env) | Default **4g**; raise if the driver OOMs (e.g. **`8g`**). |

### Engines (resume-friendly)

| Command | One-liner for interviews |
|--------|--------------------------|
| `python3 spark_analyzer.py` | PySpark on partitioned Parquet; **`SPARK_MASTER`** for YARN / K8s. |
| `python3 spark_analyzer.py --engine ray` | Ray tasks per game file; same metrics. |
| `python3 spark_analyzer.py --engine pandas` | Single-node check only. |

---

## Run

```bash
chmod +x run_full_pipeline.sh
./run_full_pipeline.sh          # full ingest (slow) + Spark report
```

Step by step:

```bash
python3 data_ingestion.py       # add --max-games N while testing
python3 spark_analyzer.py       # default Spark; --engine ray | pandas
python3 data_ingestion.py --no-skip-existing   # force re-fetch / rewrite Parquet
```

### Why does ingest look like “only 2025”?

1. **Order:** Newest playoff **year first** (2025 → 2010).
2. **`--max-games`:** Stops after N **writes**—often before older seasons appear.
3. **`skip_existing`:** Already-ingested games are skipped.

---

## Project layout

| File | Role |
|------|------|
| `config.py` | Seasons, paths, clocks, Spark-related defaults. |
| `data_ingestion.py` | Playoffs → `PlayByPlayV3` → Parquet. |
| `spark_analyzer.py` | Spark / Ray / pandas + markdown report. |
| `requirements.txt` | Dependencies. |
| `data/` | Gitignored Parquet (and optional raw JSON). |
| `reports/` | Gitignored `two_for_one_report.md`. |

### Methodology (short)

1. Quarter-end score = last play of the period (max `action_number`).
2. Shooting-team margin at FGA and at buzzer from `location` + forward-filled scores.
3. **Net margin Δ** = margin at buzzer − margin at shot.
4. Buckets: **two_for_one_window** (28–38 s), **patient_late** (3–27 s + heave-filtered sub-3s).

---

## Troubleshooting

### Spark: `PARQUET_COLUMN_DATA_TYPE_MISMATCH` / mixed INT64 vs DOUBLE

Keep **`SPARK_PARQUET_VECTORIZED=false`**. Ingestion writes **float64** for new files; **re-ingest** old shards with `--no-skip-existing` if needed.

### Corrupt Parquet (`FAILED_READ_FILE`, truncated file)

Delete the broken `part-0.parquet` (and any `.partial`), then **`python3 data_ingestion.py`**. The analyzer **skips** unreadable files and lists paths in the report. New writes use **temp file + atomic replace**.

Gitignored local Spark checkpoint: **`.spark_local_checkpoint/`** (lineage materialization).

---

## License / data

NBA data via `nba_api`. Respect [NBA.com](https://www.nba.com) terms of use.
