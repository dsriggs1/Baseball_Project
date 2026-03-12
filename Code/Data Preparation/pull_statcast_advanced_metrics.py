"""
Pull game-level Statcast metrics from Baseball Savant and insert into MariaDB.

This script retrieves pitch-level Statcast data and aggregates it to game level:
- Exit velocity and barrel metrics per game
- Expected stats (xBA, xwOBA) per game
- Hard hit rates per game
- Pitch characteristics per game

Data is inserted into the retrosheet MariaDB database with partitioning by year.
Includes both MLB game_pk and Retrosheet GAME_ID for joining with existing tables.
"""

import argparse
import getpass
import logging
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, Dict, Tuple

import pandas as pd
import numpy as np
import sqlalchemy
from pybaseball import statcast, cache, playerid_reverse_lookup

# Add parent directory to path to import Database class
sys.path.append(str(Path(__file__).parent.parent / 'Production'))
from create_model_ready import Database


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'statcast_game_pull_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


# Team code mapping from MLB to Retrosheet format
MLB_TO_RETROSHEET = {
    "LAA": "ANA", "HOU": "HOU", "OAK": "OAK", "TOR": "TOR",
    "ATL": "ATL", "MIL": "MIL", "STL": "SLN", "CHC": "CHN",
    "ARI": "ARI", "LAD": "LAN", "SF": "SFN", "CLE": "CLE",
    "SEA": "SEA", "MIA": "MIA", "NYM": "NYN", "WSH": "WAS",
    "BAL": "BAL", "SD": "SDN", "PHI": "PHI", "PIT": "PIT",
    "TEX": "TEX", "TB": "TBA", "BOS": "BOS", "CIN": "CIN",
    "COL": "COL", "KC": "KCA", "DET": "DET", "MIN": "MIN",
    "CWS": "CHA", "NYY": "NYA"
}


class StatcastGameLevelLoader:
    """Pull pitch-level Statcast data and aggregate to game level for MariaDB."""

    def __init__(self, uid: str, pwd: str, host: str, db: str, port: int,
                 enable_cache: bool = True):
        """
        Initialize the database loader.

        Args:
            uid: Database username
            pwd: Database password
            host: Database host
            db: Database name
            port: Database port
            enable_cache: Whether to enable pybaseball caching (recommended)
        """
        self.db_helper = Database(uid, pwd, host, db, port)
        self.engine = self.db_helper.db_connect()

        if enable_cache:
            cache.enable()
            logger.info("Pybaseball caching enabled")

    def create_tables(self) -> None:
        """Create partitioned tables for storing game-level Statcast metrics."""
        logger.info("Creating database tables with partitioning...")

        tables = {
            'statcast_batter_game': """
                CREATE TABLE IF NOT EXISTS statcast_batter_game (
                    game_pk BIGINT NOT NULL,
                    game_id VARCHAR(12),
                    game_date DATE NOT NULL,
                    year_id SMALLINT NOT NULL,
                    batter_id INT NOT NULL,
                    bat_id VARCHAR(8),
                    batter_name VARCHAR(100),
                    team VARCHAR(3),
                    opponent VARCHAR(3),
                    home_away VARCHAR(4),
                    pa INT DEFAULT 0,
                    ab INT DEFAULT 0,
                    batted_balls INT DEFAULT 0,
                    avg_launch_speed DECIMAL(5,2),
                    max_launch_speed DECIMAL(5,2),
                    avg_launch_angle DECIMAL(5,2),
                    barrels INT DEFAULT 0,
                    barrel_rate DECIMAL(5,3),
                    hard_hit INT DEFAULT 0,
                    hard_hit_rate DECIMAL(5,3),
                    sweet_spot INT DEFAULT 0,
                    sweet_spot_rate DECIMAL(5,3),
                    avg_xba DECIMAL(5,3),
                    avg_xwoba DECIMAL(5,3),
                    avg_xslg DECIMAL(5,3),
                    PRIMARY KEY (year_id, game_pk, batter_id),
                    INDEX idx_game_date (game_date),
                    INDEX idx_batter (batter_id),
                    INDEX idx_bat_id (bat_id),
                    INDEX idx_game_id (game_id),
                    INDEX idx_game_pk_batter (game_pk, batter_id),
                    INDEX idx_batter_year (batter_id, year_id)
                ) ENGINE=InnoDB
                PARTITION BY RANGE (year_id) (
                    PARTITION p2015 VALUES LESS THAN (2016),
                    PARTITION p2016 VALUES LESS THAN (2017),
                    PARTITION p2017 VALUES LESS THAN (2018),
                    PARTITION p2018 VALUES LESS THAN (2019),
                    PARTITION p2019 VALUES LESS THAN (2020),
                    PARTITION p2020 VALUES LESS THAN (2021),
                    PARTITION p2021 VALUES LESS THAN (2022),
                    PARTITION p2022 VALUES LESS THAN (2023),
                    PARTITION p2023 VALUES LESS THAN (2024),
                    PARTITION p2024 VALUES LESS THAN (2025),
                    PARTITION p2025 VALUES LESS THAN (2026),
                    PARTITION pmax VALUES LESS THAN MAXVALUE
                )
            """,

            'statcast_pitcher_game': """
                CREATE TABLE IF NOT EXISTS statcast_pitcher_game (
                    game_pk BIGINT NOT NULL,
                    game_id VARCHAR(12),
                    game_date DATE NOT NULL,
                    year_id SMALLINT NOT NULL,
                    pitcher_id INT NOT NULL,
                    pit_id VARCHAR(8),
                    pitcher_name VARCHAR(100),
                    team VARCHAR(3),
                    opponent VARCHAR(3),
                    home_away VARCHAR(4),
                    pitches INT DEFAULT 0,
                    batters_faced INT DEFAULT 0,
                    batted_balls_against INT DEFAULT 0,
                    avg_release_speed DECIMAL(5,2),
                    max_release_speed DECIMAL(5,2),
                    avg_spin_rate DECIMAL(6,1),
                    avg_launch_speed_against DECIMAL(5,2),
                    max_launch_speed_against DECIMAL(5,2),
                    avg_launch_angle_against DECIMAL(5,2),
                    barrels_against INT DEFAULT 0,
                    barrel_rate_against DECIMAL(5,3),
                    hard_hit_against INT DEFAULT 0,
                    hard_hit_rate_against DECIMAL(5,3),
                    avg_xba_against DECIMAL(5,3),
                    avg_xwoba_against DECIMAL(5,3),
                    PRIMARY KEY (year_id, game_pk, pitcher_id),
                    INDEX idx_game_date (game_date),
                    INDEX idx_pitcher (pitcher_id),
                    INDEX idx_pit_id (pit_id),
                    INDEX idx_game_id (game_id),
                    INDEX idx_game_pk_pitcher (game_pk, pitcher_id),
                    INDEX idx_pitcher_year (pitcher_id, year_id)
                ) ENGINE=InnoDB
                PARTITION BY RANGE (year_id) (
                    PARTITION p2015 VALUES LESS THAN (2016),
                    PARTITION p2016 VALUES LESS THAN (2017),
                    PARTITION p2017 VALUES LESS THAN (2018),
                    PARTITION p2018 VALUES LESS THAN (2019),
                    PARTITION p2019 VALUES LESS THAN (2020),
                    PARTITION p2020 VALUES LESS THAN (2021),
                    PARTITION p2021 VALUES LESS THAN (2022),
                    PARTITION p2022 VALUES LESS THAN (2023),
                    PARTITION p2023 VALUES LESS THAN (2024),
                    PARTITION p2024 VALUES LESS THAN (2025),
                    PARTITION p2025 VALUES LESS THAN (2026),
                    PARTITION pmax VALUES LESS THAN MAXVALUE
                )
            """
        }

        with self.engine.connect() as conn:
            for table_name, create_sql in tables.items():
                try:
                    conn.execute(sqlalchemy.text(create_sql))
                    conn.commit()
                    logger.info(f"  Created/verified table: {table_name}")
                except Exception as e:
                    logger.error(f"  Error creating table {table_name}: {e}")
                    raise

        logger.info("All tables created successfully")

    def map_mlb_to_retrosheet_ids(self, mlb_ids: list) -> dict:
        """
        Map MLB (MLBAM) player IDs to Retrosheet player IDs.

        Args:
            mlb_ids: List of MLB player IDs

        Returns:
            Dictionary mapping MLB ID to Retrosheet ID
        """
        if not mlb_ids:
            return {}

        try:
            # Remove duplicates and NaN values
            unique_ids = [int(pid) for pid in set(mlb_ids) if pd.notna(pid)]

            if not unique_ids:
                return {}

            logger.info(f"    Mapping {len(unique_ids)} unique player IDs to Retrosheet format...")

            # Use pybaseball's reverse lookup function
            # key_type='mlbam' means we're passing MLB IDs
            lookup_df = playerid_reverse_lookup(unique_ids, key_type='mlbam')

            # Create mapping dictionary: mlb_id -> retrosheet_id
            mapping = {}
            if lookup_df is not None and not lookup_df.empty:
                for _, row in lookup_df.iterrows():
                    mlb_id = row.get('key_mlbam')
                    retro_id = row.get('key_retro')
                    if pd.notna(mlb_id) and pd.notna(retro_id):
                        mapping[int(mlb_id)] = retro_id

                logger.info(f"    Successfully mapped {len(mapping)}/{len(unique_ids)} players to Retrosheet IDs")
            else:
                logger.warning(f"    No Retrosheet mappings found for provided MLB IDs")

            return mapping

        except Exception as e:
            logger.warning(f"    Error mapping player IDs to Retrosheet format: {e}")
            return {}

    def create_retrosheet_game_ids(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Create Retrosheet-format GAME_ID for all games in dataframe.

        Format: TTTYYYYMMDDG
        - TTT: Home team 3-letter Retrosheet code
        - YYYY: Year (4 digits)
        - MM: Month (2 digits with leading zero)
        - DD: Day (2 digits with leading zero)
        - G: Game number (0 for single game, 1/2 for doubleheader games)

        Args:
            df: DataFrame with home_team, game_date, game_pk columns

        Returns:
            DataFrame with game_id column added
        """
        try:
            # Convert home team to Retrosheet format
            df['home_team_rs'] = df['home_team'].map(
                lambda x: MLB_TO_RETROSHEET.get(x, x)
            )

            # Format date as YYYYMMDD
            df['date_str'] = pd.to_datetime(df['game_date']).dt.strftime('%Y%m%d')

            # Detect doubleheaders: multiple games same date, same home team
            # First, count games per (home_team, date)
            game_counts = df.groupby(['home_team_rs', 'date_str'])['game_pk'].transform('nunique')

            # For each (home_team, date) with multiple games, assign game numbers
            df['game_num'] = 0  # Default to 0 for single games

            # Identify doubleheaders (more than 1 game per home team per date)
            doubleheader_mask = game_counts > 1

            if doubleheader_mask.any():
                # For doubleheaders, sort by game_pk and assign 1, 2, etc.
                df.loc[doubleheader_mask, 'game_num'] = (
                    df[doubleheader_mask]
                    .groupby(['home_team_rs', 'date_str'])['game_pk']
                    .rank(method='dense')
                    .astype(int)
                )

            # Create GAME_ID: TTTYYYYMMDDG
            df['game_id'] = df['home_team_rs'] + df['date_str'] + df['game_num'].astype(str)

            # Clean up temporary columns
            df = df.drop(columns=['home_team_rs', 'date_str', 'game_num'])

            return df

        except Exception as e:
            logger.error(f"Error creating GAME_IDs: {e}")
            # Return with None game_ids if error
            df['game_id'] = None
            return df

    def aggregate_batter_game_stats(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Aggregate pitch-level data to game level for batters.

        Args:
            df: Pitch-level Statcast data

        Returns:
            Game-level batter statistics
        """
        logger.info("  Aggregating batter statistics to game level...")

        # Filter to batted balls only for some metrics
        batted_df = df[df['type'] == 'X'].copy()  # type='X' means ball in play

        # Create barrel indicator (>=98mph exit velo AND 26-30° launch angle)
        # Fill NA values with 0 before converting to int
        if 'launch_speed' in batted_df.columns and 'launch_angle' in batted_df.columns:
            batted_df['is_barrel'] = (
                (batted_df['launch_speed'].fillna(0) >= 98) &
                (batted_df['launch_angle'].fillna(0) >= 26) &
                (batted_df['launch_angle'].fillna(0) <= 30)
            ).fillna(False).astype(int)

            # Hard hit: >= 95 mph
            batted_df['is_hard_hit'] = (
                batted_df['launch_speed'].fillna(0) >= 95
            ).fillna(False).astype(int)

            # Sweet spot: 8-32° launch angle
            batted_df['is_sweet_spot'] = (
                (batted_df['launch_angle'].fillna(0) >= 8) &
                (batted_df['launch_angle'].fillna(0) <= 32)
            ).fillna(False).astype(int)

        # Aggregate batted ball metrics
        # Note: Group by batter ID only, not player_name, to avoid duplicates from inconsistent names
        batted_agg = batted_df.groupby(['game_pk', 'game_date', 'batter']).agg({
            'player_name': 'first',  # Take first player name for this batter
            'launch_speed': ['mean', 'max', 'count'],
            'launch_angle': 'mean',
            'is_barrel': 'sum',
            'is_hard_hit': 'sum',
            'is_sweet_spot': 'sum',
            'estimated_ba_using_speedangle': 'mean',
            'estimated_woba_using_speedangle': 'mean',
            'estimated_slg_using_speedangle': 'mean',
            'home_team': 'first',
            'away_team': 'first'
        }).reset_index()

        # Flatten column names
        batted_agg.columns = [
            'game_pk', 'game_date', 'batter_id', 'batter_name',
            'avg_launch_speed', 'max_launch_speed', 'batted_balls',
            'avg_launch_angle', 'barrels', 'hard_hit', 'sweet_spot',
            'avg_xba', 'avg_xwoba', 'avg_xslg',
            'home_team', 'away_team'
        ]

        # Fill NaN values in integer columns to prevent database insertion errors
        int_columns = ['batted_balls', 'barrels', 'hard_hit', 'sweet_spot']
        for col in int_columns:
            batted_agg[col] = batted_agg[col].fillna(0).astype(int)

        # Count PA and AB from full dataset
        pa_counts = df.groupby(['game_pk', 'batter']).agg({
            'description': 'count',  # Total pitches seen
        }).reset_index()

        # Count actual plate appearances (last pitch of each AB)
        pa_actual = df[df['pitch_number'].notna()].groupby(['game_pk', 'batter']).agg({
            'at_bat_number': 'nunique'
        }).reset_index()
        pa_actual.columns = ['game_pk', 'batter_id', 'pa']

        # Merge PA counts
        batted_agg = batted_agg.merge(
            pa_actual,
            on=['game_pk', 'batter_id'],
            how='left'
        )

        # Fill NaN in pa column
        batted_agg['pa'] = batted_agg['pa'].fillna(0).astype(int)

        # Calculate rates (handle division by zero)
        batted_agg['barrel_rate'] = (batted_agg['barrels'] / batted_agg['batted_balls']).fillna(0)
        batted_agg['hard_hit_rate'] = (batted_agg['hard_hit'] / batted_agg['batted_balls']).fillna(0)
        batted_agg['sweet_spot_rate'] = (batted_agg['sweet_spot'] / batted_agg['batted_balls']).fillna(0)

        # Add year_id
        batted_agg['year_id'] = pd.to_datetime(batted_agg['game_date']).dt.year

        # Determine team and opponent for batter
        batted_agg['team'] = None
        batted_agg['opponent'] = None
        batted_agg['home_away'] = None

        # Add AB count (roughly PA minus walks/HBP, but simplified here)
        batted_agg['ab'] = batted_agg['pa']  # Simplified (pa is already int from above)

        # Create Retrosheet GAME_ID
        batted_agg = self.create_retrosheet_game_ids(batted_agg)

        # Map MLB batter IDs to Retrosheet bat_id
        batter_id_mapping = self.map_mlb_to_retrosheet_ids(batted_agg['batter_id'].tolist())
        batted_agg['bat_id'] = batted_agg['batter_id'].map(batter_id_mapping)

        # Select and rename columns to match schema
        result = batted_agg[[
            'game_pk', 'game_id', 'game_date', 'year_id', 'batter_id', 'bat_id', 'batter_name',
            'team', 'opponent', 'home_away', 'pa', 'ab', 'batted_balls',
            'avg_launch_speed', 'max_launch_speed', 'avg_launch_angle',
            'barrels', 'barrel_rate', 'hard_hit', 'hard_hit_rate',
            'sweet_spot', 'sweet_spot_rate', 'avg_xba', 'avg_xwoba', 'avg_xslg'
        ]].copy()

        logger.info(f"    Aggregated to {len(result)} game-batter combinations")
        return result

    def aggregate_pitcher_game_stats(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Aggregate pitch-level data to game level for pitchers.

        Args:
            df: Pitch-level Statcast data

        Returns:
            Game-level pitcher statistics
        """
        logger.info("  Aggregating pitcher statistics to game level...")

        # First, get base pitcher info from full dataset (ensures game_date, pitcher_name always present)
        base_pitcher_info = df.groupby(['game_pk', 'pitcher']).agg({
            'game_date': 'first',
            'player_name': 'first',
            'home_team': 'first',
            'away_team': 'first'
        }).reset_index()

        base_pitcher_info.columns = ['game_pk', 'pitcher_id', 'game_date', 'pitcher_name', 'home_team', 'away_team']

        # Filter to batted balls for contact metrics
        batted_df = df[df['type'] == 'X'].copy()

        # Create barrel and hard hit indicators
        # Fill NA values with 0 before converting to int
        if 'launch_speed' in batted_df.columns and 'launch_angle' in batted_df.columns:
            batted_df['is_barrel'] = (
                (batted_df['launch_speed'].fillna(0) >= 98) &
                (batted_df['launch_angle'].fillna(0) >= 26) &
                (batted_df['launch_angle'].fillna(0) <= 30)
            ).fillna(False).astype(int)
            batted_df['is_hard_hit'] = (
                batted_df['launch_speed'].fillna(0) >= 95
            ).fillna(False).astype(int)

        # Aggregate batted ball metrics against (only if there are batted balls)
        if len(batted_df) > 0:
            batted_agg = batted_df.groupby(['game_pk', 'pitcher']).agg({
                'launch_speed': ['mean', 'max', 'count'],
                'launch_angle': 'mean',
                'is_barrel': 'sum',
                'is_hard_hit': 'sum',
                'estimated_ba_using_speedangle': 'mean',
                'estimated_woba_using_speedangle': 'mean'
            }).reset_index()

            batted_agg.columns = [
                'game_pk', 'pitcher_id',
                'avg_launch_speed_against', 'max_launch_speed_against', 'batted_balls_against',
                'avg_launch_angle_against', 'barrels_against', 'hard_hit_against',
                'avg_xba_against', 'avg_xwoba_against'
            ]

            # Fill NaN values in integer columns
            int_columns = ['batted_balls_against', 'barrels_against', 'hard_hit_against']
            for col in int_columns:
                batted_agg[col] = batted_agg[col].fillna(0).astype(int)
        else:
            # No batted balls in this batch - create empty dataframe with correct columns
            batted_agg = pd.DataFrame(columns=[
                'game_pk', 'pitcher_id', 'avg_launch_speed_against', 'max_launch_speed_against',
                'batted_balls_against', 'avg_launch_angle_against', 'barrels_against',
                'hard_hit_against', 'avg_xba_against', 'avg_xwoba_against'
            ])

        # Aggregate pitch characteristics
        pitch_agg = df.groupby(['game_pk', 'pitcher']).agg({
            'release_speed': ['mean', 'max', 'count'],
            'release_spin_rate': 'mean',
            'batter': 'nunique'
        }).reset_index()

        pitch_agg.columns = [
            'game_pk', 'pitcher_id',
            'avg_release_speed', 'max_release_speed', 'pitches',
            'avg_spin_rate', 'batters_faced'
        ]

        # Start with base pitcher info, then merge in pitch stats and batted ball stats
        # Using left merge ensures all pitchers keep their game_date, pitcher_name, etc.
        result = base_pitcher_info.merge(pitch_agg, on=['game_pk', 'pitcher_id'], how='left')
        result = result.merge(batted_agg, on=['game_pk', 'pitcher_id'], how='left')

        # Fill NaN values in integer columns after merge
        int_columns_after_merge = ['pitches', 'batters_faced', 'batted_balls_against', 'barrels_against', 'hard_hit_against']
        for col in int_columns_after_merge:
            if col in result.columns:
                result[col] = result[col].fillna(0).astype(int)

        # Calculate rates (handle division by zero)
        result['barrel_rate_against'] = (result['barrels_against'] / result['batted_balls_against']).fillna(0)
        result['hard_hit_rate_against'] = (result['hard_hit_against'] / result['batted_balls_against']).fillna(0)

        # Add year_id
        result['year_id'] = pd.to_datetime(result['game_date']).dt.year

        # Add team and opponent
        result['team'] = None
        result['opponent'] = None
        result['home_away'] = None

        # Create Retrosheet GAME_ID
        result = self.create_retrosheet_game_ids(result)

        # Map MLB pitcher IDs to Retrosheet pit_id
        pitcher_id_mapping = self.map_mlb_to_retrosheet_ids(result['pitcher_id'].tolist())
        result['pit_id'] = result['pitcher_id'].map(pitcher_id_mapping)

        # Select columns to match schema
        result = result[[
            'game_pk', 'game_id', 'game_date', 'year_id', 'pitcher_id', 'pit_id', 'pitcher_name',
            'team', 'opponent', 'home_away', 'pitches', 'batters_faced', 'batted_balls_against',
            'avg_release_speed', 'max_release_speed', 'avg_spin_rate',
            'avg_launch_speed_against', 'max_launch_speed_against', 'avg_launch_angle_against',
            'barrels_against', 'barrel_rate_against', 'hard_hit_against', 'hard_hit_rate_against',
            'avg_xba_against', 'avg_xwoba_against'
        ]].copy()

        logger.info(f"    Aggregated to {len(result)} game-pitcher combinations")
        return result

    def pull_and_aggregate_date_range(self, start_date: str, end_date: str) -> tuple:
        """
        Pull pitch-level data for date range and aggregate to game level.

        Args:
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)

        Returns:
            Tuple of (batter_df, pitcher_df)
        """
        try:
            logger.info(f"  Pulling pitch-level data for {start_date} to {end_date}...")
            df = statcast(start_dt=start_date, end_dt=end_date)

            if df is None or df.empty:
                logger.warning(f"    No data returned for {start_date} to {end_date}")
                return None, None

            logger.info(f"    Retrieved {len(df)} pitches")

            # Aggregate to game level
            batter_stats = self.aggregate_batter_game_stats(df)
            pitcher_stats = self.aggregate_pitcher_game_stats(df)

            return batter_stats, pitcher_stats

        except Exception as e:
            logger.error(f"    Error pulling/aggregating data: {e}")
            return None, None

    def insert_data(self, df: pd.DataFrame, table_name: str, date_range: str) -> None:
        """
        Insert data into database table.

        Args:
            df: DataFrame to insert
            table_name: Target table name
            date_range: Date range string for logging
        """
        if df is None or df.empty:
            logger.warning(f"    No data to insert for {table_name}")
            return

        try:
            # Insert data (append mode)
            self.db_helper.db_insert(df, table_name, self.engine)
            logger.info(f"    Inserted {len(df)} rows into {table_name} for {date_range}")

        except Exception as e:
            logger.error(f"    Error inserting into {table_name}: {e}")
            raise

    def pull_and_insert_year(self, year: int, batch_days: int = 7, rate_limit_delay: float = 0.5) -> Dict:
        """
        Pull and insert data for an entire year in batches.

        Args:
            year: Year to pull
            batch_days: Number of days per batch (default 7)
            rate_limit_delay: Seconds to wait between API calls (default 0.5)

        Returns:
            Dictionary with timing and statistics
        """
        year_start_time = time.time()

        logger.info(f"\n{'='*60}")
        logger.info(f"Processing year: {year}")
        logger.info(f"{'='*60}")

        start_date = datetime(year, 3, 1)  # MLB season typically starts late March
        end_date = datetime(year, 11, 30)  # Season ends in November

        current_date = start_date
        batches_processed = 0
        batches_failed = 0
        total_batter_rows = 0
        total_pitcher_rows = 0

        while current_date <= end_date:
            batch_end = min(current_date + timedelta(days=batch_days - 1), end_date)

            start_str = current_date.strftime('%Y-%m-%d')
            end_str = batch_end.strftime('%Y-%m-%d')

            logger.info(f"\nProcessing batch: {start_str} to {end_str}")

            try:
                # Pull and aggregate
                batter_df, pitcher_df = self.pull_and_aggregate_date_range(start_str, end_str)

                # Insert batters
                if batter_df is not None:
                    self.insert_data(batter_df, 'statcast_batter_game', f"{start_str} to {end_str}")
                    total_batter_rows += len(batter_df)

                # Insert pitchers
                if pitcher_df is not None:
                    self.insert_data(pitcher_df, 'statcast_pitcher_game', f"{start_str} to {end_str}")
                    total_pitcher_rows += len(pitcher_df)

                batches_processed += 1

                # Rate limiting between API calls
                time.sleep(rate_limit_delay)

            except Exception as e:
                logger.error(f"  Batch {start_str} to {end_str} failed: {e}")
                batches_failed += 1

            current_date = batch_end + timedelta(days=1)

        year_end_time = time.time()
        year_duration = year_end_time - year_start_time

        # Log summary for this year
        logger.info(f"\n{'='*60}")
        logger.info(f"Year {year} Complete")
        logger.info(f"  Duration: {year_duration:.2f} seconds ({year_duration/60:.2f} minutes)")
        logger.info(f"  Batches processed: {batches_processed}")
        logger.info(f"  Batches failed: {batches_failed}")
        logger.info(f"  Total batter rows inserted: {total_batter_rows:,}")
        logger.info(f"  Total pitcher rows inserted: {total_pitcher_rows:,}")
        logger.info(f"{'='*60}")

        return {
            'year': year,
            'duration_seconds': year_duration,
            'batches_processed': batches_processed,
            'batches_failed': batches_failed,
            'batter_rows': total_batter_rows,
            'pitcher_rows': total_pitcher_rows
        }

    def pull_and_insert_all_years(self, start_year: int, end_year: int, batch_days: int = 7,
                                  max_workers: int = 5, rate_limit_delay: float = 0.5) -> None:
        """
        Pull and insert data for multiple years in parallel.

        Args:
            start_year: First year to pull
            end_year: Last year to pull (inclusive)
            batch_days: Days per batch
            max_workers: Number of parallel workers (default 5)
            rate_limit_delay: Seconds to wait between API calls per worker (default 0.5)
        """
        total_start_time = time.time()

        years = list(range(start_year, end_year + 1))

        logger.info("\n" + "="*60)
        logger.info(f"Starting parallel processing of {len(years)} years")
        logger.info(f"  Workers: {max_workers}")
        logger.info(f"  Batch size: {batch_days} days")
        logger.info(f"  Rate limit delay: {rate_limit_delay}s per worker")
        logger.info("="*60)

        # Track results
        year_results = []

        # Process years in parallel using ThreadPoolExecutor
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all year processing tasks
            future_to_year = {
                executor.submit(self.pull_and_insert_year, year, batch_days, rate_limit_delay): year
                for year in years
            }

            # Process completed tasks as they finish
            for future in as_completed(future_to_year):
                year = future_to_year[future]
                try:
                    result = future.result()
                    year_results.append(result)
                    logger.info(f"\n✓ Year {year} completed successfully")
                except Exception as e:
                    logger.error(f"\n✗ Year {year} failed with error: {e}")
                    year_results.append({
                        'year': year,
                        'duration_seconds': 0,
                        'batches_processed': 0,
                        'batches_failed': 0,
                        'batter_rows': 0,
                        'pitcher_rows': 0,
                        'error': str(e)
                    })

        total_end_time = time.time()
        total_duration = total_end_time - total_start_time

        # Sort results by year for final summary
        year_results.sort(key=lambda x: x['year'])

        # Calculate totals
        total_batches = sum(r['batches_processed'] for r in year_results)
        total_failures = sum(r['batches_failed'] for r in year_results)
        total_batter_rows = sum(r['batter_rows'] for r in year_results)
        total_pitcher_rows = sum(r['pitcher_rows'] for r in year_results)

        # Log final summary
        logger.info("\n" + "="*60)
        logger.info("FINAL SUMMARY - All Years Complete")
        logger.info("="*60)
        logger.info(f"Total duration: {total_duration:.2f} seconds ({total_duration/60:.2f} minutes)")
        logger.info(f"Years processed: {len(years)}")
        logger.info(f"Total batches processed: {total_batches}")
        logger.info(f"Total batches failed: {total_failures}")
        logger.info(f"Total batter rows inserted: {total_batter_rows:,}")
        logger.info(f"Total pitcher rows inserted: {total_pitcher_rows:,}")
        logger.info("")
        logger.info("Per-Year Breakdown:")
        logger.info("-" * 60)

        for result in year_results:
            year = result['year']
            duration = result['duration_seconds']
            batches = result['batches_processed']
            failures = result['batches_failed']
            batter_rows = result['batter_rows']
            pitcher_rows = result['pitcher_rows']

            status = "✓" if 'error' not in result else "✗"
            logger.info(f"{status} {year}: {duration:.1f}s | Batches: {batches} | "
                       f"Failed: {failures} | Batters: {batter_rows:,} | Pitchers: {pitcher_rows:,}")

        logger.info("="*60)


def main():
    """Main execution function."""
    parser = argparse.ArgumentParser(
        description='Pull game-level Statcast metrics and insert into MariaDB'
    )
    parser.add_argument(
        '--uid', type=str, required=True,
        help='Database username'
    )
    parser.add_argument(
        '--pwd', type=str, required=False,
        help='Database password (will prompt if not provided)'
    )
    parser.add_argument(
        '--host', type=str, default='localhost',
        help='Database host (default: localhost)'
    )
    parser.add_argument(
        '--db', type=str, default='retrosheet',
        help='Database name (default: retrosheet)'
    )
    parser.add_argument(
        '--port', type=int, default=3306,
        help='Database port (default: 3306)'
    )
    parser.add_argument(
        '--start-year', type=int, default=2015,
        help='First season to pull (default: 2015)'
    )
    parser.add_argument(
        '--end-year', type=int, default=datetime.now().year - 1,
        help='Last season to pull (default: previous year)'
    )
    parser.add_argument(
        '--batch-days', type=int, default=7,
        help='Number of days per batch request (default: 7)'
    )
    parser.add_argument(
        '--no-cache', action='store_true',
        help='Disable pybaseball caching (not recommended)'
    )
    parser.add_argument(
        '--create-tables-only', action='store_true',
        help='Only create tables, do not pull data'
    )
    parser.add_argument(
        '--parallel-workers', type=int, default=5,
        help='Number of parallel workers for year-level processing (default: 5)'
    )
    parser.add_argument(
        '--rate-limit-delay', type=float, default=0.5,
        help='Seconds to wait between API calls per worker (default: 0.5)'
    )

    args = parser.parse_args()

    # Prompt for password if not provided via command line
    if args.pwd is None:
        args.pwd = getpass.getpass(f"Enter password for {args.uid}@{args.host}: ")

    # Log configuration
    logger.info("="*60)
    logger.info("Statcast Game-Level Metrics Loader")
    logger.info("="*60)
    logger.info(f"Database: {args.db}@{args.host}:{args.port}")
    logger.info(f"Years: {args.start_year} - {args.end_year}")
    logger.info(f"Batch size: {args.batch_days} days")
    logger.info(f"Parallel workers: {args.parallel_workers}")
    logger.info(f"Rate limit delay: {args.rate_limit_delay}s per worker")
    logger.info(f"Caching: {'Disabled' if args.no_cache else 'Enabled'}")
    logger.info("="*60)

    # Initialize loader
    loader = StatcastGameLevelLoader(
        uid=args.uid,
        pwd=args.pwd,
        host=args.host,
        db=args.db,
        port=args.port,
        enable_cache=not args.no_cache
    )

    # Create tables
    loader.create_tables()

    # Pull and insert data (unless create-tables-only flag is set)
    if not args.create_tables_only:
        loader.pull_and_insert_all_years(
            start_year=args.start_year,
            end_year=args.end_year,
            batch_days=args.batch_days,
            max_workers=args.parallel_workers,
            rate_limit_delay=args.rate_limit_delay
        )
    else:
        logger.info("\n--create-tables-only flag set, skipping data pull")

    logger.info("\n" + "="*60)
    logger.info("COMPLETE!")
    logger.info("="*60)


if __name__ == '__main__':
    main()
