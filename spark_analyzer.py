"""
Compare late-quarter FGAs in the 2-for-1 clock window (28–38s left) vs. patient late
FGAs (3–27s left, plus 0–3s non-heaves). Net outcome = shooter's scoring margin at
quarter end minus margin at the shot.

**Default engine: Apache Spark (PySpark)** — tuned for mixed Parquet types and
`SPARK_MASTER` for YARN / K8s. **Ray** runs the same per-game logic in parallel.
**pandas** is optional (`--engine pandas`) for debugging.
"""

from __future__ import annotations

import argparse
import os
import re
import sys
from pathlib import Path

import numpy as np
import pandas as pd

import config


def _ensure_java_for_spark() -> None:
    """Use project JDK 17 if present so PySpark works (system Java may be 16)."""
    home = config.JAVA_17_HOME
    java_bin = home / "bin" / "java"
    if java_bin.is_file():
        os.environ["JAVA_HOME"] = str(home)
        path = os.environ.get("PATH", "")
        os.environ["PATH"] = str(home / "bin") + os.pathsep + path


def build_spark_session():
    """SparkSession tuned for heterogeneous Parquet (int/float score columns) and local or YARN/K8s master."""
    _ensure_java_for_spark()
    from pyspark.sql import SparkSession

    b = (
        SparkSession.builder.appName(config.SPARK_APP_NAME)
        .master(config.SPARK_MASTER)
        .config("spark.sql.shuffle.partitions", os.environ.get("SPARK_SHUFFLE_PARTITIONS", "16"))
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.parquet.mergeSchema", "true")
    )
    if not config.SPARK_PARQUET_VECTORIZED:
        # Avoid PARQUET_COLUMN_DATA_TYPE_MISMATCH (INT64 vs DOUBLE) across different game files
        b = b.config("spark.sql.parquet.enableVectorizedReader", "false")
    b = b.config("spark.driver.memory", config.SPARK_DRIVER_MEMORY)
    return b.getOrCreate()


def _normalize_spark_parquet_schema(df):
    """Unify types across shards so aggregations match pandas (scores as double, ids as long)."""
    from pyspark.sql import functions as F

    for c in (
        "score_home",
        "score_away",
        "score_home_ff",
        "score_away_ff",
        "seconds_remaining",
        "shot_distance",
    ):
        if c in df.columns:
            df = df.withColumn(c, F.col(c).cast("double"))
    for c in ("action_number", "period", "team_id", "person_id", "Season"):
        if c in df.columns:
            df = df.withColumn(c, F.col(c).cast("long"))
    return df


def _read_parquet_paths_union(spark, paths: list[Path]):
    """
    Read many game Parquet files and union them. A single multi-path read fails with
    CANNOT_MERGE_SCHEMAS when some files store scores as BIGINT and others as DOUBLE.
    Per-file read + cast in _normalize_spark_parquet_schema avoids that.
    """
    df_all = None
    for p in paths:
        d = spark.read.parquet(str(p))
        if "Game_ID" in d.columns:
            if "game_id" in d.columns:
                d = d.drop("Game_ID")
            else:
                d = d.withColumnRenamed("Game_ID", "game_id")
        d = _normalize_spark_parquet_schema(d)
        df_all = d if df_all is None else df_all.unionByName(d, allowMissingColumns=True)
    if df_all is None:
        raise ValueError("No Parquet paths to read")
    # Avoid thousands of tiny partitions from per-file unions on local runs
    n = int(os.environ.get("SPARK_POST_UNION_COALESCE", "64"))
    if n > 0:
        df_all = df_all.coalesce(n)
    return df_all


def _coverage_from_paths(good: list[Path], bad: list[Path]) -> dict:
    """Season bounds from partition paths (matches games on disk)."""
    seasons: list[int] = []
    for p in good:
        m = re.search(r"Season=(\d+)", str(p))
        if m:
            seasons.append(int(m.group(1)))
    cov: dict = {
        "n_games": len(good),
        "season_from": min(seasons) if seasons else None,
        "season_to": max(seasons) if seasons else None,
        "n_seasons": len(set(seasons)) if seasons else 0,
    }
    if bad:
        cov["skipped_corrupt_parquet"] = len(bad)
        cov["skipped_corrupt_sample"] = [str(p) for p in bad[:8]]
    return cov


def _parquet_file_is_readable(path: Path) -> bool:
    """Detect truncated/corrupt Parquet (e.g. interrupted writes) without a full table scan."""
    try:
        import pyarrow.parquet as pq

        pf = pq.ParquetFile(str(path))
        if pf.num_row_groups < 1:
            return False
        col = pf.schema_arrow.names[0]
        pf.read_row_group(0, columns=[col])
        return True
    except Exception:
        return False


def _filter_readable_parquet_paths() -> tuple[list[Path], list[Path]]:
    """Return (readable_paths, corrupt_paths). Corrupt files are skipped for Spark/pandas."""
    all_files = sorted(config.PARQUET_DIR.rglob("*.parquet"))
    good: list[Path] = []
    bad: list[Path] = []
    for p in all_files:
        if _parquet_file_is_readable(p):
            good.append(p)
        else:
            bad.append(p)
    for p in bad:
        print(f"WARN: skipping unreadable Parquet (re-run ingestion for this game): {p}", file=sys.stderr)
    return good, bad


def _load_parquet_pandas() -> tuple[pd.DataFrame, list[Path], list[Path]]:
    good, bad = _filter_readable_parquet_paths()
    if not good:
        raise FileNotFoundError(
            f"No readable Parquet files under {config.PARQUET_DIR}. "
            f"Corrupt files: {len(bad)}. Delete them and run data_ingestion.py again."
        )
    frames = [pd.read_parquet(f, engine="pyarrow") for f in good]
    return pd.concat(frames, ignore_index=True), good, bad


def _labeled_frame_pandas(df: pd.DataFrame) -> pd.DataFrame:
    """Same logic as Spark: join quarter-end scores, FGA filter, buckets."""
    for c in (
        "action_number",
        "period",
        "seconds_remaining",
        "score_home_ff",
        "score_away_ff",
        "is_field_goal",
        "action_type",
        "location",
    ):
        if c not in df.columns:
            raise ValueError(f"Parquet missing column {c!r}; re-run ingestion.")

    if config.REGULATION_PERIODS_ONLY:
        df = df[df["period"] <= 4].copy()

    df = df.sort_values(["game_id", "period", "action_number"], kind="mergesort")
    ends = (
        df.groupby(["game_id", "period"], as_index=False)
        .tail(1)[["game_id", "period", "score_home_ff", "score_away_ff"]]
        .rename(columns={"score_home_ff": "end_home", "score_away_ff": "end_away"})
    )
    base = df.merge(ends, on=["game_id", "period"], how="inner")

    sec = pd.to_numeric(base["seconds_remaining"], errors="coerce")
    sh = pd.to_numeric(base["score_home_ff"], errors="coerce")
    sa = pd.to_numeric(base["score_away_ff"], errors="coerce")
    eh = pd.to_numeric(base["end_home"], errors="coerce")
    ea = pd.to_numeric(base["end_away"], errors="coerce")

    is_fga = (
        (base["is_field_goal"] == 1)
        & base["action_type"].isin(["Made Shot", "Missed Shot"])
        & sec.notna()
        & base["location"].isin(["h", "v"])
        & sh.notna()
        & sa.notna()
        & eh.notna()
        & ea.notna()
    )
    fg = base.loc[is_fga].copy()
    fg["seconds_remaining"] = sec.loc[is_fga].to_numpy()

    sh_f = pd.to_numeric(fg["score_home_ff"], errors="coerce")
    sa_f = pd.to_numeric(fg["score_away_ff"], errors="coerce")
    eh_f = pd.to_numeric(fg["end_home"], errors="coerce")
    ea_f = pd.to_numeric(fg["end_away"], errors="coerce")

    margin_now = np.where(fg["location"].eq("h"), sh_f - sa_f, sa_f - sh_f)
    margin_end = np.where(fg["location"].eq("h"), eh_f - ea_f, ea_f - eh_f)

    fg["margin_at_fga"] = margin_now
    fg["margin_end_of_period"] = margin_end
    fg["net_pts_rest_of_quarter"] = fg["margin_end_of_period"] - fg["margin_at_fga"]

    lo = float(config.POSSESSION_WINDOW_LOW_SEC)
    hi = float(config.POSSESSION_WINDOW_HIGH_SEC)
    mn = float(config.MIN_LATE_FGA_SECONDS)
    heave_ft = float(config.HEAVE_EXCLUDE_DISTANCE_FT)
    s = fg["seconds_remaining"].astype(float)
    if "shot_distance" in fg.columns:
        dist = pd.to_numeric(fg["shot_distance"], errors="coerce")
    else:
        dist = pd.Series(np.nan, index=fg.index, dtype=float)

    bucket = pd.Series("other", index=fg.index, dtype=object)
    bucket.loc[(s >= lo) & (s <= hi)] = "two_for_one_window"
    patient_main = (s >= mn) & (s < lo)
    buzzer_non_heave = (s >= 0) & (s < mn) & (dist.isna() | (dist <= heave_ft))
    bucket.loc[patient_main | buzzer_non_heave] = "patient_late"
    fg["shot_bucket"] = bucket
    return fg[fg["shot_bucket"] != "other"]


def _summarize_pandas(labeled: pd.DataFrame) -> list[dict]:
    rows = []
    for name, g in labeled.groupby("shot_bucket", sort=True):
        x = g["net_pts_rest_of_quarter"].astype(float)
        rows.append(
            {
                "shot_bucket": name,
                "fga_count": len(g),
                "mean_net_margin_delta": float(x.mean()),
                "std_net_margin_delta": float(x.std(ddof=0)),
                "median_net_margin_delta": float(x.median()),
            }
        )
    return rows


def ray_label_one_game_parquet(path: str) -> pd.DataFrame:
    """Ray task: one Parquet file == one game — same `_labeled_frame_pandas` logic as Spark SQL."""
    return _labeled_frame_pandas(pd.read_parquet(path, engine="pyarrow"))


def run_analysis_ray() -> tuple[str, str]:
    """Embarrassingly parallel path: Ray schedules one task per game file; numerically matches Spark/pandas."""
    import ray  # type: ignore[import-untyped]

    good, bad = _filter_readable_parquet_paths()
    if not good:
        raise FileNotFoundError(
            f"No readable Parquet under {config.PARQUET_DIR}. Corrupt: {len(bad)}. "
            "Delete bad part-0.parquet and re-ingest."
        )
    ray.init(ignore_reinit_error=True)
    try:
        remote_fn = ray.remote(ray_label_one_game_parquet)
        parts: list[pd.DataFrame] = ray.get([remote_fn.remote(str(p)) for p in good])
        nonempty = [p for p in parts if p is not None and len(p) > 0]
        if not nonempty:
            raise ValueError("No FGA rows in any game after labeling; check data and filters.")
        labeled = pd.concat(nonempty, ignore_index=True)
        rows = _summarize_pandas(labeled)
        total = int(labeled.shape[0])
        coverage = _coverage_from_paths(good, bad)
        return (
            format_report_md(rows, total, backend="Ray (parallel tasks, same logic as Spark)", coverage=coverage),
            "Ray",
        )
    finally:
        ray.shutdown()


def run_analysis_pandas() -> tuple[str, str]:
    df, good, bad = _load_parquet_pandas()
    coverage = _coverage_from_paths(good, bad)
    labeled = _labeled_frame_pandas(df)
    rows = _summarize_pandas(labeled)
    total = int(labeled.shape[0])
    return format_report_md(rows, total, backend="pandas (single-node)", coverage=coverage), "pandas"


def _bucket_table_label(internal_key: str) -> str:
    """Human-readable bucket label with clock rules (matches default config windows)."""
    mn = config.MIN_LATE_FGA_SECONDS
    lo = config.POSSESSION_WINDOW_LOW_SEC
    hi = config.POSSESSION_WINDOW_HIGH_SEC
    heave = config.HEAVE_EXCLUDE_DISTANCE_FT
    if heave == int(heave):
        heave_s = str(int(heave))
    else:
        heave_s = f"{heave:g}"
    patient_hi = lo - 1
    if internal_key == "two_for_one_window":
        return f"two_for_one_window ({lo}-{hi})"
    if internal_key == "patient_late":
        # "0-{mn}" = clock below MIN_LATE_FGA_SECONDS (heave rule); "{mn}-{patient_hi}" = patient main band
        return f"patient_late (0-{mn} ≤ {heave_s} ft, {mn}-{patient_hi})"
    return internal_key


def _results_table_md(summary_rows: list) -> list[str]:
    """Aligned markdown table so long bucket labels line up with numeric columns in plain-text editors."""
    headers = ["Bucket", "FGA count", "Mean net margin Δ (pts)", "Std dev", "Median"]
    body: list[list[str]] = []
    for r in summary_rows:
        med = r.get("median_net_margin_delta")
        std = r.get("std_net_margin_delta") or 0.0
        med_s = f"{float(med):.4f}" if med is not None and med == med else "nan"
        body.append(
            [
                _bucket_table_label(r["shot_bucket"]),
                str(int(r["fga_count"])),
                f"{float(r['mean_net_margin_delta']):.4f}",
                f"{float(std):.4f}",
                med_s,
            ]
        )
    grid = [headers] + body
    widths = [max(len(grid[row][col]) for row in range(len(grid))) for col in range(5)]
    num_cols = {1, 2, 3, 4}

    def fmt_row(cells: list[str]) -> str:
        parts = []
        for i in range(5):
            w = widths[i]
            parts.append(cells[i].rjust(w) if i in num_cols else cells[i].ljust(w))
        return "| " + " | ".join(parts) + " |"

    sep = "| " + " | ".join("-" * max(3, widths[i]) for i in range(5)) + " |"
    return [fmt_row(headers), sep] + [fmt_row(row) for row in body]


def format_report_md(
    summary_rows: list,
    total_fga: int,
    backend: str,
    coverage: dict | None = None,
) -> str:
    """Build markdown report from collected summary rows (dicts)."""
    lines = [
        "# NBA Playoffs 2-for-1 vs. patient late FGA (report)",
        "",
        f"- **Engine:** {backend}",
        "",
    ]
    if coverage:
        sf, st = coverage.get("season_from"), coverage.get("season_to")
        season_line = (
            f"{sf}–{st} ({coverage.get('n_seasons', 0)} distinct playoff years)"
            if sf is not None and st is not None
            else "unknown (Season column missing)"
        )
        lines.extend(
            [
                "## Dataset coverage (Parquet on disk)",
                "",
                f"- **Unique games:** {coverage.get('n_games', 0)}",
                f"- **Playoff years in data:** {season_line}",
                f"- **Configured ingest range:** {config.SEASON_START}–{config.SEASON_END} (see `config.py`). "
                "If years are missing, ingestion may still be running or older seasons failed the API.",
                "",
            ]
        )
        n_skip = coverage.get("skipped_corrupt_parquet")
        if n_skip:
            sample = coverage.get("skipped_corrupt_sample") or []
            lines.extend(
                [
                    f"- **Skipped corrupt / unreadable Parquet files:** {n_skip} "
                    "(delete these paths and re-run `data_ingestion.py` for those games).",
                    "",
                ]
            )
            for s in sample:
                lines.append(f"  - `{s}`")
            lines.append("")
    lines.extend(
        [
        "## Methodology",
        "",
        "- **Data:** Play-by-play from `nba_api` `PlayByPlayV3` (stored as Parquet). "
        "`PlayByPlayV2` is not used (deprecated / empty responses).",
        "- **2-for-1 window:** field goal attempts with **"
        f"{config.POSSESSION_WINDOW_LOW_SEC}–{config.POSSESSION_WINDOW_HIGH_SEC} seconds** "
        "remaining in the quarter (inclusive).",
        "- **Patient late:** FGA with **"
        f"{config.MIN_LATE_FGA_SECONDS}–{config.POSSESSION_WINDOW_LOW_SEC - 1} seconds** "
        "remaining, **plus** FGA with **0–"
        f"{config.MIN_LATE_FGA_SECONDS - 1} seconds** left only if **shot distance is missing or ≤ "
        f"{config.HEAVE_EXCLUDE_DISTANCE_FT:g} ft** (long heaves beyond that excluded).",
        "- **Outcome:** For the **shooting team**, *net margin change rest of quarter* = "
        "(margin at quarter end) − (margin at shot), using score columns forward-filled within each period. "
        "Positive = the shooter's team outscored the opponent from that shot to the buzzer.",
        "- **Scope:** "
        + ("Regulation quarters only (Q1–Q4)." if config.REGULATION_PERIODS_ONLY else "All periods including OT."),
        "",
        f"- **FGAs in comparison buckets (this run):** {total_fga}",
        "",
        "## Results",
        "",
    ]
    )
    lines.extend(_results_table_md(summary_rows))

    lines.extend(
        [
            "",
            "## Interpretation",
            "",
            "This is an **observational** comparison: late FGA timing is not randomized, so differences "
            "do not by themselves prove the 2-for-1 *causes* better outcomes. Strength of schedule, "
            "game script, and shot quality are not adjusted here.",
            "",
            "If **mean net margin Δ** is higher for `two_for_one_window` than `patient_late`, quick shots "
            "in that clock band are associated with better *team* margin movement to the quarter end "
            "in this sample; the opposite suggests the opposite association.",
            "",
        ]
    )
    return "\n".join(lines)


def run_analysis_spark():
    from pyspark.sql import functions as F

    spark = build_spark_session()

    try:
        if not config.PARQUET_DIR.is_dir() or not any(config.PARQUET_DIR.rglob("*.parquet")):
            raise FileNotFoundError(
                f"No Parquet files under {config.PARQUET_DIR}. Run data_ingestion.py first."
            )
        good_parquet, bad_parquet = _filter_readable_parquet_paths()
        if not good_parquet:
            raise FileNotFoundError(
                f"No readable Parquet under {config.PARQUET_DIR}; corrupt count={len(bad_parquet)}. "
                "Delete broken part-0.parquet files and re-ingest."
            )
        df = _read_parquet_paths_union(spark, good_parquet)
        # Deep union plans can exhaust driver memory during analysis; materialize once locally.
        ck = config.SPARK_LOCAL_CHECKPOINT_DIR
        ck.mkdir(parents=True, exist_ok=True)
        spark.sparkContext.setCheckpointDir(str(ck))
        df = df.localCheckpoint(eager=True)

        for c in (
            "action_number",
            "period",
            "seconds_remaining",
            "score_home_ff",
            "score_away_ff",
            "is_field_goal",
        ):
            if c not in df.columns:
                raise ValueError(f"Parquet missing column {c!r}; re-run ingestion.")

        coverage = _coverage_from_paths(good_parquet, bad_parquet)

        if config.REGULATION_PERIODS_ONLY:
            df = df.filter(F.col("period") <= 4)

        ends = (
            df.groupBy("game_id", "period")
            .agg(
                F.max(F.struct("action_number", "score_home_ff", "score_away_ff")).alias("st"),
            )
            .select(
                "game_id",
                "period",
                F.col("st.score_home_ff").cast("double").alias("end_home"),
                F.col("st.score_away_ff").cast("double").alias("end_away"),
            )
        )

        base = df.join(ends, on=["game_id", "period"], how="inner")
        if config.REGULATION_PERIODS_ONLY:
            base = base.filter(F.col("period") <= 4)

        sec = F.col("seconds_remaining").cast("double")
        fgas = base.filter(
            (F.col("is_field_goal") == 1)
            & (F.col("action_type").isin("Made Shot", "Missed Shot"))
            & F.col("seconds_remaining").isNotNull()
            & F.col("location").isin("h", "v")
            & F.col("score_home_ff").cast("double").isNotNull()
            & F.col("score_away_ff").cast("double").isNotNull()
            & F.col("end_home").isNotNull()
            & F.col("end_away").isNotNull()
        )

        if "shot_distance" in fgas.columns:
            dist = F.col("shot_distance").cast("double")
        else:
            dist = F.lit(None).cast("double")

        mn = float(config.MIN_LATE_FGA_SECONDS)
        lo = float(config.POSSESSION_WINDOW_LOW_SEC)
        hi = float(config.POSSESSION_WINDOW_HIGH_SEC)
        heave_ft = float(config.HEAVE_EXCLUDE_DISTANCE_FT)
        patient_main = (sec >= F.lit(mn)) & (sec < F.lit(lo))
        buzzer_non_heave = (sec >= F.lit(0)) & (sec < F.lit(mn)) & (
            dist.isNull() | (dist <= F.lit(heave_ft))
        )

        sh = F.col("score_home_ff").cast("double")
        sa = F.col("score_away_ff").cast("double")
        eh = F.col("end_home")
        ea = F.col("end_away")

        margin_now = F.when(F.col("location") == "h", sh - sa).otherwise(sa - sh)
        margin_end = F.when(F.col("location") == "h", eh - ea).otherwise(ea - eh)

        labeled = (
            fgas.withColumn("margin_at_fga", margin_now)
            .withColumn("margin_end_of_period", margin_end)
            .withColumn(
                "net_pts_rest_of_quarter",
                F.col("margin_end_of_period") - F.col("margin_at_fga"),
            )
            .withColumn(
                "shot_bucket",
                F.when((sec >= F.lit(lo)) & (sec <= F.lit(hi)), F.lit("two_for_one_window"))
                .when(patient_main | buzzer_non_heave, F.lit("patient_late"))
                .otherwise(F.lit("other")),
            )
        ).filter(F.col("shot_bucket") != "other")

        summary = (
            labeled.groupBy("shot_bucket")
            .agg(
                F.count("*").alias("fga_count"),
                F.avg("net_pts_rest_of_quarter").alias("mean_net_margin_delta"),
                F.stddev_pop("net_pts_rest_of_quarter").alias("std_net_margin_delta"),
                F.expr("percentile_approx(net_pts_rest_of_quarter, 0.5)").alias("median_net_margin_delta"),
            )
            .orderBy("shot_bucket")
        )

        rows = [r.asDict() for r in summary.collect()]
        total = sum(int(r["fga_count"]) for r in rows)
        return (
            format_report_md(rows, total, backend="Apache Spark (PySpark)", coverage=coverage),
            "PySpark",
        )
    finally:
        spark.stop()


def main() -> None:
    parser = argparse.ArgumentParser(description="2-for-1 vs patient late FGA report.")
    parser.add_argument(
        "--output",
        type=str,
        default=str(config.REPORT_PATH),
        help="Markdown report path",
    )
    parser.add_argument(
        "--engine",
        choices=["spark", "ray", "pandas"],
        default="spark",
        help="spark=PySpark (default); ray=parallel per-game tasks; pandas=single-node fallback",
    )
    args = parser.parse_args()

    if args.engine == "pandas":
        md, eng = run_analysis_pandas()
    elif args.engine == "ray":
        md, eng = run_analysis_ray()
    else:
        md, eng = run_analysis_spark()

    out = Path(args.output)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(md, encoding="utf-8")
    print(md)
    print(f"\nWrote {out} (engine={eng})")


if __name__ == "__main__":
    main()
