"""
PySpark analysis: preprocess clock columns, identify 2-for-1 window possessions,
flag early FGA, LEAD-based bonus possession, net score change metrics.
"""

from __future__ import annotations

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

import config


def build_spark() -> SparkSession:
    return (
        SparkSession.builder.appName(config.SPARK_APP_NAME)
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )


def seconds_from_pctimestring(col_name: str = "PCTIMESTRING"):
    """
    MM:SS -> seconds (NBA PCTIMESTRING is typically time remaining in the period).

    Map into seconds_remaining and period_seconds_remaining per spec (tune if you split game vs period clock).
    """
    c = F.col(col_name)
    parts = F.split(c, ":")
    mm = parts.getItem(0).cast("int")
    ss = parts.getItem(1).cast("int")
    return mm * 60 + ss


def load_playbyplay(spark: SparkSession):
    """Read partitioned Parquet written by data_ingestion."""
    return spark.read.parquet(str(config.PARQUET_DIR))


def add_preprocess_columns(df):
    sec = seconds_from_pctimestring()
    return df.withColumn("seconds_remaining", sec).withColumn("period_seconds_remaining", sec)


def possession_starts_in_2for1_window(df):
    """
    Identify possessions whose start falls between 38s and 28s left in the quarter.

    Window: partitionBy game_id, period; orderBy seconds_remaining descending
    (clock counting down: larger remaining first in desc order = earlier in real time... tune to your row ordering).
    """
    w = Window.partitionBy("game_id", "period").orderBy(F.desc("seconds_remaining"))
    # TODO: define "possession start" rows (e.g. period start, rebound, turnover) and filter
    # df = df.withColumn("is_possession_start", ...)
    return df.withColumn("row_in_period_clock", F.row_number().over(w))


def flag_early_fga(df):
    """FGA with more than 24 seconds on game clock (column name may differ in your schema)."""
    # TODO: join or compute game_clock_seconds if different from period clock
    return df.withColumn(
        "is_early_fga",
        F.when(
            (F.col("EVENTMSGTYPE") == 2) & (F.col("seconds_remaining") > config.EARLY_SHOT_GAME_CLOCK_THRESHOLD_SEC),
            F.lit(1),
        ).otherwise(F.lit(0)),
    )


def add_lead_final_shot_team(df):
    """
    Use LEAD to see if the same team took the final FGA of the quarter (bonus possession check).
    Adjust EVENTMSGTYPE / team column names to match play-by-play schema.
    """
    w = Window.partitionBy("game_id", "period").orderBy(F.desc("seconds_remaining"))
    # Last event in quarter: orderBy desc(seconds_remaining) -> first row is last chronologically if
    # seconds_remaining decreases as game progresses; verify ordering against your data.
    df = df.withColumn("next_team_id", F.lead("PLAYER1_TEAM_ID").over(w))
    # TODO: derive is_final_fga_of_period and same_team_final_shot
    return df


def net_score_change_metrics(df):
    """
    Net score change from ~38s mark to quarter buzzer: compare 2-for-1 attempts vs patient possessions.
    """
    # TODO: filter to possessions in [28, 38], classify 2-for-1 vs patient, aggregate SCOREMARGIN / HOME/AWAY PTS
    return df


def main() -> None:
    spark = build_spark()
    try:
        df = load_playbyplay(spark)
        df = add_preprocess_columns(df)
        df = possession_starts_in_2for1_window(df)
        df = flag_early_fga(df)
        df = add_lead_final_shot_team(df)
        df = net_score_change_metrics(df)
        df.printSchema()
        # df.select(...).show(20, truncate=False)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
