"""
Fetch playoff play-by-play via nba_api PlayByPlayV3 (V2 is deprecated/broken),
optionally save raw JSON, write partitioned Parquet (Season, Game_ID).

Play-by-play year Y refers to the June finals year (e.g. Y=2010 → season 2009-10).
"""

from __future__ import annotations

import argparse
import json
import os
import re
import time
from pathlib import Path
from typing import Any, Iterator

import pandas as pd

import config

try:
    from nba_api.stats.endpoints import leaguegamefinder
    from nba_api.stats.endpoints import playbyplayv3
except ImportError:
    leaguegamefinder = None  # type: ignore
    playbyplayv3 = None  # type: ignore

_CLOCK_RE = re.compile(r"PT(?:(\d+)M)?([\d.]+)S")


def playoff_year_to_season_nullable(playoff_year: int) -> str:
    """2010 -> '2009-10', 2025 -> '2024-25'."""
    y0 = playoff_year - 1
    y1_suffix = str(playoff_year)[-2:]
    return f"{y0}-{y1_suffix}"


def parse_clock_to_seconds_remaining(clock: str | None) -> float | None:
    """Parse NBA Stats clock like PT11M43.00S or PT00M39.20S -> seconds left in period."""
    if clock is None or (isinstance(clock, float) and pd.isna(clock)):
        return None
    s = str(clock).strip()
    m = _CLOCK_RE.match(s)
    if not m:
        return None
    minutes = int(m.group(1) or 0)
    secs = float(m.group(2))
    return minutes * 60 + secs


def ensure_dirs() -> None:
    config.PARQUET_DIR.mkdir(parents=True, exist_ok=True)
    if config.SAVE_RAW_JSON:
        config.RAW_JSON_DIR.mkdir(parents=True, exist_ok=True)


def _sleep_for_rate_limit() -> None:
    time.sleep(config.REQUEST_SLEEP_SECONDS)


def iter_playoff_game_ids(playoff_year: int) -> Iterator[str]:
    """Unique playoff GAME_ID strings for a given playoff year."""
    if leaguegamefinder is None:
        raise RuntimeError("Install nba_api: pip install nba_api")

    season = playoff_year_to_season_nullable(playoff_year)
    last_err: Exception | None = None
    for attempt in range(config.MAX_RETRIES):
        try:
            _sleep_for_rate_limit()
            finder = leaguegamefinder.LeagueGameFinder(
                season_nullable=season,
                season_type_nullable="Playoffs",
            )
            df = finder.get_data_frames()[0]
            if df is None or df.empty:
                return
            for gid in df["GAME_ID"].dropna().unique():
                yield str(int(gid)).zfill(10)
            return
        except Exception as e:
            last_err = e
            time.sleep(config.RETRY_BACKOFF_BASE**attempt)
    raise RuntimeError(f"LeagueGameFinder failed for season {season}") from last_err


def fetch_playbyplay_dataframe(game_id: str) -> pd.DataFrame | None:
    if playbyplayv3 is None:
        raise RuntimeError("Install nba_api: pip install nba_api")

    last_err: Exception | None = None
    for attempt in range(config.MAX_RETRIES):
        try:
            _sleep_for_rate_limit()
            pbp = playbyplayv3.PlayByPlayV3(game_id=game_id)
            dfs = pbp.get_data_frames()
            if not dfs:
                return None
            return dfs[0]
        except Exception as e:
            last_err = e
            time.sleep(config.RETRY_BACKOFF_BASE**attempt)
    print(f"WARN: skipping game_id={game_id} after retries: {last_err}")
    return None


def normalize_playbyplay_to_rows(
    df: pd.DataFrame, game_id: str, playoff_year: int
) -> pd.DataFrame:
    """Map PlayByPlayV3 columns to snake_case Parquet schema."""
    if df.empty:
        return pd.DataFrame()

    out = pd.DataFrame(
        {
            "game_id": game_id,
            "Season": int(playoff_year),
            "action_number": pd.to_numeric(df.get("actionNumber"), errors="coerce"),
            "period": pd.to_numeric(df.get("period"), errors="coerce"),
            "clock": df.get("clock"),
            "team_id": pd.to_numeric(df.get("teamId"), errors="coerce"),
            "team_tricode": df.get("teamTricode"),
            "person_id": pd.to_numeric(df.get("personId"), errors="coerce"),
            "is_field_goal": pd.to_numeric(df.get("isFieldGoal"), errors="coerce").fillna(0).astype(int),
            "action_type": df.get("actionType"),
            "shot_result": df.get("shotResult"),
            "score_home": df.get("scoreHome"),
            "score_away": df.get("scoreAway"),
            "location": df.get("location"),
            "description": df.get("description"),
            "shot_distance": pd.to_numeric(df.get("shotDistance"), errors="coerce"),
        }
    )

    out["seconds_remaining"] = out["clock"].map(parse_clock_to_seconds_remaining)

    # Forward-fill scores within period for margin calculations downstream
    out = out.sort_values(["period", "action_number"], kind="mergesort")
    for col in ("score_home", "score_away"):
        out[col] = pd.to_numeric(out[col], errors="coerce")
    out["score_home_ff"] = out.groupby(["game_id", "period"], sort=False)["score_home"].ffill()
    out["score_away_ff"] = out.groupby(["game_id", "period"], sort=False)["score_away"].ffill()

    # Spark merges many Parquet parts into one schema; int64 vs float64 on scores breaks vectorized read.
    for col in ("score_home", "score_away", "score_home_ff", "score_away_ff", "seconds_remaining", "shot_distance"):
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce").astype("float64")

    return out


def save_raw_json(game_id: str, playoff_year: int, payload: dict[str, Any]) -> Path:
    out_dir = config.RAW_JSON_DIR / str(playoff_year)
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / f"{game_id}.json"
    path.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
    return path


def write_game_parquet(df: pd.DataFrame) -> Path | None:
    if df.empty:
        return None
    season = int(df["Season"].iloc[0])
    gid = str(df["game_id"].iloc[0])
    part_dir = config.PARQUET_DIR / f"Season={season}" / f"Game_ID={gid}"
    part_dir.mkdir(parents=True, exist_ok=True)
    out_file = part_dir / "part-0.parquet"
    tmp = part_dir / "part-0.parquet.partial"
    df.to_parquet(tmp, index=False, engine="pyarrow")
    os.replace(tmp, out_file)
    return out_file


def ingest_playoffs(
    max_games: int | None = None,
    skip_existing: bool = True,
) -> int:
    """
    Download all playoff games in [SEASON_START, SEASON_END], write Parquet per game.
    Returns number of games written.
    """
    ensure_dirs()
    written = 0
    cap = max_games if max_games is not None and max_games > 0 else None

    # Newest seasons first: API responses for very old playoff games are sometimes empty.
    # NOTE: With --max-games N, the first N *new* writes almost always come from 2025 only,
    # because we walk 2025→2010 and there are more than N playoff games in recent years.
    years = list(range(config.SEASON_START, config.SEASON_END + 1))
    years.reverse()
    for playoff_year in years:
        if cap is not None and written >= cap:
            break
        seen_any = False
        for game_id in iter_playoff_game_ids(playoff_year):
            if not seen_any:
                print(f"Playoff year {playoff_year} ({config.SEASON_START}–{config.SEASON_END} range): ingesting…")
                seen_any = True
            if cap is not None and written >= cap:
                break
            existing = (
                config.PARQUET_DIR / f"Season={playoff_year}" / f"Game_ID={game_id}" / "part-0.parquet"
            )
            if skip_existing and existing.is_file():
                continue

            raw_df = fetch_playbyplay_dataframe(game_id)
            if raw_df is None or raw_df.empty:
                continue
            if config.SAVE_RAW_JSON:
                # minimal raw dump
                save_raw_json(
                    game_id,
                    playoff_year,
                    {"gameId": game_id, "rowCount": len(raw_df)},
                )

            norm = normalize_playbyplay_to_rows(raw_df, game_id, playoff_year)
            path = write_game_parquet(norm)
            if path:
                written += 1
                print(f"Wrote {path} ({written} games)")

        if not seen_any:
            print(
                f"WARN: Playoff year {playoff_year}: no game IDs returned "
                "(LeagueGameFinder empty or API error); skipping year."
            )

    return written


def main() -> None:
    parser = argparse.ArgumentParser(description="Ingest NBA playoff play-by-play to Parquet.")
    parser.add_argument(
        "--max-games",
        type=int,
        default=None,
        help="Cap successful writes (testing). Fills newest season first, so small N may be 2025-only. "
        "Omit for full 2010–2025.",
    )
    parser.add_argument(
        "--no-skip-existing",
        action="store_true",
        help="Re-fetch even if part-0.parquet already exists.",
    )
    args = parser.parse_args()

    n = ingest_playoffs(max_games=args.max_games, skip_existing=not args.no_skip_existing)
    print(f"Done. Games written this run: {n}")


if __name__ == "__main__":
    main()
