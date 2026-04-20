"""
Fetch PlayByPlayV2 for NBA playoff games (configured seasons), persist raw JSON,
and write partitioned Parquet (Season, Game_ID).
"""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any, Iterator

import config

# Optional: install with `pip install nba_api pyarrow`
try:
    from nba_api.stats.endpoints import playbyplayv2
except ImportError:
    playbyplayv2 = None  # type: ignore


def ensure_dirs() -> None:
    config.RAW_JSON_DIR.mkdir(parents=True, exist_ok=True)
    config.PARQUET_DIR.mkdir(parents=True, exist_ok=True)


def _sleep_for_rate_limit() -> None:
    time.sleep(config.REQUEST_SLEEP_SECONDS)


def fetch_playbyplay_json(game_id: str, season: int) -> dict[str, Any]:
    """
    Call nba_api PlayByPlayV2 for one game. Retries with exponential backoff.
    Returns the API payload (or normalized dict) suitable for JSON dump.
    """
    if playbyplayv2 is None:
        raise RuntimeError("Install nba_api: pip install nba_api")

    last_err: Exception | None = None
    for attempt in range(config.MAX_RETRIES):
        try:
            _sleep_for_rate_limit()
            # game_id format from league game finder is typically e.g. "0041900401"
            pbp = playbyplayv2.PlayByPlayV2(game_id=game_id)
            # get_json() returns a JSON string; parse for a consistent Python dict
            return json.loads(pbp.get_json())
        except Exception as e:
            last_err = e
            time.sleep(config.RETRY_BACKOFF_BASE**attempt)
    raise RuntimeError(f"Failed PlayByPlayV2 for game_id={game_id}") from last_err


def save_raw_json(game_id: str, season: int, payload: dict[str, Any]) -> Path:
    """Write one game's raw response under raw_json/{season}/{game_id}.json"""
    out_dir = config.RAW_JSON_DIR / str(season)
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / f"{game_id}.json"
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return path


def iter_playoff_game_ids(season: int) -> Iterator[tuple[str, int]]:
    """
    Yield (game_id, season) for playoff games in the given season.

    Implement using nba_api (e.g. leaguegamefinder.LeagueGameFinder with
    season_type_playoffs, or schedule endpoints). Stub returns empty.
    """
    # TODO: resolve playoff game IDs per season via nba_api
    # Example sketch (adjust params to match nba_api API):
    # from nba_api.stats.endpoints import leaguegamefinder
    # finder = leaguegamefinder.LeagueGameFinder(
    #     season_nullable=f"{season}-{str(season + 1)[-2:]}",
    #     season_type_nullable="Playoffs",
    # )
    # df = finder.get_data_frames()[0]
    # for gid in df["GAME_ID"].unique():
    #     yield str(gid), season
    yield from ()


def json_to_parquet_partitioned() -> None:
    """
    Read raw JSON from disk, normalize rows, write Parquet partitioned by
    Season and Game_ID under config.PARQUET_DIR.
    """
    try:
        import pandas as pd
    except ImportError as e:
        raise RuntimeError("Install pandas and pyarrow: pip install pandas pyarrow") from e

    rows: list[dict[str, Any]] = []
    for season_dir in sorted(config.RAW_JSON_DIR.glob("*")):
        if not season_dir.is_dir():
            continue
        try:
            season_val = int(season_dir.name)
        except ValueError:
            continue
        for jf in season_dir.glob("*.json"):
            game_id = jf.stem
            payload = json.loads(jf.read_text(encoding="utf-8"))
            # TODO: map payload to list of play rows; structure depends on nba_api response
            # result_sets = payload.get("resultSets", [])
            # ... extract headers + rows into dicts, add Season + Game_ID columns
            _ = (season_val, game_id, payload)
            # rows.extend(...)

    if not rows:
        # No data yet — create empty scaffold or skip write
        return

    df = __import__("pandas").DataFrame(rows)
    df.to_parquet(
        config.PARQUET_DIR,
        partition_cols=["Season", "Game_ID"],
        index=False,
        engine="pyarrow",
        existing_data_behavior="overwrite_or_ignore",
    )


def main() -> None:
    ensure_dirs()
    for season in range(config.SEASON_START, config.SEASON_END + 1):
        for game_id, _ in iter_playoff_game_ids(season):
            payload = fetch_playbyplay_json(game_id, season)
            save_raw_json(game_id, season, payload)
    json_to_parquet_partitioned()


if __name__ == "__main__":
    main()
