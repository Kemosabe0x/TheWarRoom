import os
from typing import Optional, Dict, Any, List
import nflreadpy as nfl
from pathlib import Path

import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class NFLDataWrapper:
    """
    A comprehensive Python wrapper for loading, processing, and analyzing NFL data using nflreadpy and Polars.
    """

    def __init__(self, cache_dir: str = "nfl_data_cache", use_cache: bool = True):
        """
        Initialize the NFLDataWrapper with optional caching.

        Args:
            cache_dir: Directory to store cached data.
            use_cache: Whether to use caching for data loading.
        """
        self.cache_dir = Path(cache_dir)
        self.use_cache = use_cache

        # Create cache directory if it doesn't exist
        if not self.cache_dir.exists():
            self.cache_dir.mkdir(parents=True, exist_ok=True)
            logger.info(f"Created cache directory at {self.cache_dir}")

    # Core Loading Functions
    def load_play_by_play(self, seasons: Optional[List[int]] = None, **kwargs) -> pl.DataFrame:
        """Load play-by-play data."""
        logger.info(f"Loading play-by-play data for seasons: {seasons}")
        return load_pbp(seasons=seasons, cache=self.use_cache, cache_dir=self.cache_dir, **kwargs)

    def load_player_stats(self, stat_type: str = "season", **kwargs) -> pl.DataFrame:
        """Load player game or season statistics."""
        logger.info(f"Loading player {stat_type} statistics")
        return load_player_stats(stat_type=stat_type, cache=self.use_cache, cache_dir=self.cache_dir, **kwargs)

    def load_team_stats(self, stat_type: str = "season", **kwargs) -> pl.DataFrame:
        """Load team game or season statistics."""
        logger.info(f"Loading team {stat_type} statistics")
        return load_team_stats(stat_type=stat_type, cache=self.use_cache, cache_dir=self.cache_dir, **kwargs)

    def load_schedules(self, seasons: Optional[List[int]] = None, **kwargs) -> pl.DataFrame:
        """Load game schedules and results."""
        logger.info(f"Loading schedules for seasons: {seasons}")
        return load_schedules(seasons=seasons, cache=self.use_cache, cache_dir=self.cache_dir, **kwargs)

    def load_players(self, **kwargs) -> pl.DataFrame:
        """Load player information."""
        logger.info("Loading player information")
        return load_players(cache=self.use_cache, cache_dir=self.cache_dir, **kwargs)

    def load_rosters(self, **kwargs) -> pl.DataFrame:
        """Load team rosters."""
        logger.info("Loading team rosters")
        return load_rosters(cache=self.use_cache, cache_dir=self.cache_dir, **kwargs)

    def load_rosters_weekly(self, seasons: Optional[List[int]] = None, **kwargs) -> pl.DataFrame:
        """Load team rosters by season-week."""
        logger.info(f"Loading weekly rosters for seasons: {seasons}")
        return load_rosters_weekly(seasons=seasons, cache=self.use_cache, cache_dir=self.cache_dir, **kwargs)

    def load_snap_counts(self, seasons: Optional[List[int]] = None, **kwargs) -> pl.DataFrame:
        """Load snap counts."""
        logger.info(f"Loading snap counts for seasons: {seasons}")
        return load_snap_counts(seasons=seasons, cache=self.use_cache, cache_dir=self.cache_dir, **kwargs)

    def load_nextgen_stats(self, stat_type: str = "passing", **kwargs) -> pl.DataFrame:
        """Load advanced stats from nextgenstats.nfl.com."""
        logger.info(f"Loading NextGen stats for {stat_type}")
        return load_nextgen_stats(stat_type=stat_type, cache=self.use_cache, cache_dir=self.cache_dir, **kwargs)

    def load_ftn_charting(self, **kwargs) -> pl.DataFrame:
        """Load charted stats from ftnfantasy.com/data."""
        logger.info("Loading FTN charting data")
        return load_ftn_charting(cache=self.use_cache, cache_dir=self.cache_dir, **kwargs)

    def load_participation(self, seasons: Optional[List[int]] = None, **kwargs) -> pl.DataFrame:
        """Load participation data (historical)."""
        logger.info(f"Loading participation data for seasons: {seasons}")
        return load_participation(seasons=seasons, cache=self.use_cache, cache_dir=self.cache_dir, **kwargs)

    def load_draft_picks(self, **kwargs) -> pl.DataFrame:
        """Load NFL draft picks."""
        logger.info("Loading draft picks")
        return load_draft_picks(cache=self.use_cache, cache_dir=self.cache_dir, **kwargs)

    def load_injuries(self, seasons: Optional[List[int]] = None, **kwargs) -> pl.DataFrame:
        """Load injury statuses and practice participation."""
        logger.info(f"Loading injuries for seasons: {seasons}")
        return load_injuries(seasons=seasons, cache=self.use_cache, cache_dir=self.cache_dir, **kwargs)

    def load_contracts(self, **kwargs) -> pl.DataFrame:
        """Load historical contract data from OTC."""
        logger.info("Loading contract data")
        return load_contracts(cache=self.use_cache, cache_dir=self.cache_dir, **kwargs)

    def load_officials(self, seasons: Optional[List[int]] = None, **kwargs) -> pl.DataFrame:
        """Load officials for each game."""
        logger.info(f"Loading officials for seasons: {seasons}")
        return load_officials(seasons=seasons, cache=self.use_cache, cache_dir=self.cache_dir, **kwargs)

    def load_combine(self, **kwargs) -> pl.DataFrame:
        """Load NFL combine results."""
        logger.info("Loading combine results")
        return load_combine(cache=self.use_cache, cache_dir=self.cache_dir, **kwargs)

    def load_depth_charts(self, seasons: Optional[List[int]] = None, **kwargs) -> pl.DataFrame:
        """Load depth charts."""
        logger.info(f"Loading depth charts for seasons: {seasons}")
        return load_depth_charts(seasons=seasons, cache=self.use_cache, cache_dir=self.cache_dir, **kwargs)

    def load_trades(self, **kwargs) -> pl.DataFrame:
        """Load trades."""
        logger.info("Loading trades")
        return load_trades(cache=self.use_cache, cache_dir=self.cache_dir, **kwargs)

    def load_ff_playerids(self, **kwargs) -> pl.DataFrame:
        """Load ffverse/dynastyprocess player IDs."""
        logger.info("Loading fantasy football player IDs")
        return load_ff_playerids(cache=self.use_cache, cache_dir=self.cache_dir, **kwargs)

    def load_ff_rankings(self, **kwargs) -> pl.DataFrame:
        """Load fantasypros rankings."""
        logger.info("Loading fantasy football rankings")
        return load_ff_rankings(cache=self.use_cache, cache_dir=self.cache_dir, **kwargs)

    def load_ff_opportunity(self, **kwargs) -> pl.DataFrame:
        """Load expected yards, touchdowns, and fantasy points."""
        logger.info("Loading fantasy football opportunity data")
        return load_ff_opportunity(cache=self.use_cache, cache_dir=self.cache_dir, **kwargs)

    # Utility Functions
    def clear_cache(self) -> None:
        """Clear cached data."""
        logger.info("Clearing cache")
        clear_cache(cache_dir=self.cache_dir)

    def get_current_season(self) -> int:
        """Get the current NFL season."""
        return get_current_season()

    def get_current_week(self) -> int:
        """Get the current NFL week."""
        return get_current_week()

    # Custom Processing Functions
    def calculate_fantasy_points(self, pbp_df: pl.DataFrame) -> pl.DataFrame:
        """
        Calculate fantasy points for each play in the play-by-play data.
        Customize the scoring rules as needed.
        """
        fantasy_rules = {
            'passing_yards': 0.04,
            'passing_tds': 4,
            'interceptions': -2,
            'rushing_yards': 0.1,
            'rushing_tds': 6,
            'receiving_yards': 0.1,
            'receiving_tds': 6,
            'fumbles_lost': -2,
        }

        logger.info("Calculating fantasy points")
        pbp_df = pbp_df.with_columns(
            (
                (pl.col("passing_yards") * fantasy_rules['passing_yards']) +
                (pl.col("passing_tds") * fantasy_rules['passing_tds']) +
                (pl.col("interceptions") * fantasy_rules['interceptions']) +
                (pl.col("rushing_yards") * fantasy_rules['rushing_yards']) +
                (pl.col("rushing_tds") * fantasy_rules['rushing_tds']) +
                (pl.col("receiving_yards") * fantasy_rules['receiving_yards']) +
                (pl.col("receiving_tds") * fantasy_rules['receiving_tds']) +
                (pl.col("fumbles_lost") * fantasy_rules['fumbles_lost'])
            ).alias("fantasy_points")
        )
        return pbp_df

    def aggregate_player_stats(self, pbp_df: pl.DataFrame, player_df: pl.DataFrame) -> pl.DataFrame:
        """
        Aggregate player statistics from play-by-play data.
        Customize the aggregation logic as needed.
        """
        logger.info("Aggregating player statistics")
        player_stats = (
            pbp_df
            .group_by(["player_id", "player_name"])
            .agg(
                pl.col("fantasy_points").sum().alias("total_fantasy_points"),
                pl.col("passing_yards").sum().alias("total_passing_yards"),
                pl.col("rushing_yards").sum().alias("total_rushing_yards"),
                pl.col("receiving_yards").sum().alias("total_receiving_yards"),
                pl.count().alias("plays_involved"),
            )
            .join(player_df, on="player_id", how="left")
        )
        return player_stats

# Example Usage
if __name__ == "__main__":
    # Initialize the wrapper
    nfl_wrapper = NFLDataWrapper(cache_dir="nfl_data_cache", use_cache=True)

    # Load data
    pbp_df = nfl_wrapper.load_play_by_play(seasons=[2022, 2023])
    player_df = nfl_wrapper.load_players()

    # Calculate fantasy points
    pbp_df = nfl_wrapper.calculate_fantasy_points(pbp_df)

    # Aggregate player stats
    player_stats = nfl_wrapper.aggregate_player_stats(pbp_df, player_df)

    # Show the first few rows
    print(player_stats.head())
