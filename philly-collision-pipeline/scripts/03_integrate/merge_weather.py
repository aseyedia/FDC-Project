"""
Weather-Crash Data Integration Script

Merges NOAA weather data with crash records by date.
Handles temporal matching and creates weather-enriched datasets.

Author: FDC Project
Date: 2025-10-26
"""

import sys
from pathlib import Path
import pandas as pd
from typing import Optional
from datetime import datetime, timedelta

# Add parent directory to path for imports
sys.path.append(str(Path(__file__).parent.parent))

from config import (
    RAW_DATA_DIR,
    PROCESSED_DATA_DIR,
    METADATA_DIR
)
from utils.logging_utils import setup_logger, log_dataframe_info

# Initialize logger
logger = setup_logger(__name__)


class WeatherCrashIntegrator:
    """
    Integrates weather data with crash records.
    """
    
    def __init__(self):
        """Initialize integrator."""
        self.logger = logger
        self.weather_df = None
        
        self.stats = {
            'total_crashes': 0,
            'crashes_with_dates': 0,
            'crashes_matched': 0,
            'crashes_unmatched': 0
        }
        
        self.logger.info("WeatherCrashIntegrator initialized")
    
    def load_weather_data(self) -> pd.DataFrame:
        """
        Load NOAA weather data.
        
        Returns:
            Weather dataframe
        """
        weather_file = RAW_DATA_DIR / "noaa_weather_philly.parquet"
        
        if not weather_file.exists():
            self.logger.error(f"Weather data not found: {weather_file}")
            self.logger.error("Run stage 1 (acquisition) first!")
            raise FileNotFoundError(f"Weather data required: {weather_file}")
        
        self.logger.info(f"Loading weather data from {weather_file.name}")
        df = pd.read_parquet(weather_file)
        
        # Ensure date column is datetime
        df['date'] = pd.to_datetime(df['date'])
        
        # Sort by date
        df = df.sort_values('date').reset_index(drop=True)
        
        self.logger.info(f"Loaded weather data: {len(df)} days")
        self.logger.info(f"Date range: {df['date'].min()} to {df['date'].max()}")
        log_dataframe_info(df, "Weather data")
        
        self.weather_df = df
        return df
    
    def prepare_crash_dates(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Extract and standardize crash dates.
        
        Crash data has CRASH_YEAR, CRASH_MONTH, DAY_OF_WEEK columns.
        Need to construct actual date for matching.
        
        Args:
            df: Crash dataframe
            
        Returns:
            DataFrame with crash_date column
        """
        self.logger.info("Preparing crash dates")
        
        # Check required columns
        date_cols = ['CRASH_YEAR', 'CRASH_MONTH']
        missing = [col for col in date_cols if col not in df.columns]
        
        if missing:
            self.logger.error(f"Missing date columns: {missing}")
            self.logger.error(f"Available columns: {', '.join(df.columns[:20])}...")
            return df
        
        # Try to construct date
        # If we have CRASH_DAY column, use it
        if 'CRASH_DAY' in df.columns:
            df['crash_date'] = pd.to_datetime(
                df[['CRASH_YEAR', 'CRASH_MONTH', 'CRASH_DAY']].rename(
                    columns={'CRASH_YEAR': 'year', 'CRASH_MONTH': 'month', 'CRASH_DAY': 'day'}
                ),
                errors='coerce'
            )
        else:
            # Use first day of month as fallback
            self.logger.warning("No CRASH_DAY column, using first day of month")
            df['CRASH_DAY'] = 1
            df['crash_date'] = pd.to_datetime(
                df[['CRASH_YEAR', 'CRASH_MONTH', 'CRASH_DAY']].rename(
                    columns={'CRASH_YEAR': 'year', 'CRASH_MONTH': 'month', 'CRASH_DAY': 'day'}
                ),
                errors='coerce'
            )
        
        self.stats['total_crashes'] = len(df)
        self.stats['crashes_with_dates'] = df['crash_date'].notna().sum()
        
        self.logger.info(f"Crash dates prepared:")
        self.logger.info(f"  Total crashes: {self.stats['total_crashes']:,}")
        self.logger.info(f"  With valid dates: {self.stats['crashes_with_dates']:,}")
        
        if df['crash_date'].notna().any():
            self.logger.info(f"  Date range: {df['crash_date'].min()} to {df['crash_date'].max()}")
        
        return df
    
    def merge_weather(self, crash_df: pd.DataFrame) -> pd.DataFrame:
        """
        Merge weather data with crash data by date.
        
        Args:
            crash_df: Crash dataframe with crash_date column
            
        Returns:
            Merged dataframe
        """
        self.logger.info("Merging weather data with crashes")
        
        if self.weather_df is None:
            self.load_weather_data()
        
        # Ensure crash dates are prepared
        if 'crash_date' not in crash_df.columns:
            crash_df = self.prepare_crash_dates(crash_df)
        
        # Extract just the date part (no time)
        crash_df['crash_date_only'] = crash_df['crash_date'].dt.date
        weather_date_only = self.weather_df['date'].dt.date
        
        # Create temporary column for merge
        weather_merge = self.weather_df.copy()
        weather_merge['crash_date_only'] = weather_date_only
        
        # Merge on date
        initial_count = len(crash_df)
        
        merged = crash_df.merge(
            weather_merge.drop('date', axis=1),
            on='crash_date_only',
            how='left',
            suffixes=('', '_weather')
        )
        
        # Clean up temporary column
        merged = merged.drop('crash_date_only', axis=1)
        
        # Count matches
        self.stats['crashes_matched'] = merged['temp_avg_c'].notna().sum()
        self.stats['crashes_unmatched'] = merged['temp_avg_c'].isna().sum()
        
        self.logger.info(f"Weather merge complete:")
        self.logger.info(f"  Input crashes: {initial_count:,}")
        self.logger.info(f"  Matched with weather: {self.stats['crashes_matched']:,}")
        self.logger.info(f"  No weather data: {self.stats['crashes_unmatched']:,}")
        
        return merged
    
    def add_weather_derived_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Add derived weather features.
        
        Creates useful categorical and binary features:
        - Precipitation category (none, light, moderate, heavy)
        - Temperature category (cold, cool, mild, warm, hot)
        - Adverse weather flag
        - Extreme temperature flag
        
        Args:
            df: Dataframe with weather columns
            
        Returns:
            DataFrame with additional features
        """
        self.logger.info("Adding derived weather features")
        
        if 'precipitation_mm' in df.columns:
            # Precipitation categories
            df['precip_category'] = pd.cut(
                df['precipitation_mm'],
                bins=[-float('inf'), 0.1, 2.5, 10, float('inf')],
                labels=['none', 'light', 'moderate', 'heavy']
            )
            
            # Binary adverse weather flag
            df['adverse_weather'] = (
                (df['precipitation_mm'] > 2.5) |  # Moderate+ precipitation
                (df['wind_speed_max_ms'] > 15) |  # High winds (>30 mph)
                (df['snowfall_mm'] > 0)            # Any snow
            ).astype(int)
        
        if 'temp_avg_c' in df.columns:
            # Temperature categories (Celsius)
            df['temp_category'] = pd.cut(
                df['temp_avg_c'],
                bins=[-float('inf'), 0, 10, 20, 30, float('inf')],
                labels=['cold', 'cool', 'mild', 'warm', 'hot']
            )
            
            # Extreme temperature flag
            df['extreme_temp'] = (
                (df['temp_avg_c'] < -5) |   # Very cold
                (df['temp_avg_c'] > 35)      # Very hot
            ).astype(int)
        
        self.logger.info("Derived features created")
        
        return df
    
    def process_crash_category(self, category: str = "CRASH") -> Optional[pd.DataFrame]:
        """
        Process crash data with weather integration.
        
        Args:
            category: Category to process (typically CRASH)
            
        Returns:
            Merged dataframe or None
        """
        self.logger.info("=" * 80)
        self.logger.info(f"Processing {category} with weather integration")
        self.logger.info("=" * 80)
        
        # Load geographic-filtered crash data
        input_file = PROCESSED_DATA_DIR / f"{category.lower()}_geographic.parquet"
        
        if not input_file.exists():
            self.logger.error(f"Geographic data not found: {input_file}")
            self.logger.error("Run geographic filtering first!")
            return None
        
        self.logger.info(f"Loading {input_file.name}")
        df = pd.read_parquet(input_file)
        
        self.logger.info(f"Loaded {len(df):,} records")
        log_dataframe_info(df, f"Input {category}")
        
        # Load weather data
        self.load_weather_data()
        
        # Prepare dates and merge
        df = self.prepare_crash_dates(df)
        df = self.merge_weather(df)
        df = self.add_weather_derived_features(df)
        
        # Save integrated data
        output_file = PROCESSED_DATA_DIR / f"{category.lower()}_weather_integrated.parquet"
        
        self.logger.info(f"Saving to {output_file.name}")
        df.to_parquet(output_file, index=False, engine='pyarrow')
        
        file_size = output_file.stat().st_size / (1024 * 1024)
        self.logger.info(f"Saved {len(df):,} records ({file_size:.2f} MB)")
        
        # Save statistics
        stats_file = METADATA_DIR / f"{category.lower()}_weather_stats.json"
        import json
        # Convert numpy types to Python types for JSON serialization
        stats_json = {k: int(v) if hasattr(v, 'item') else v for k, v in self.stats.items()}
        with open(stats_file, 'w') as f:
            json.dump(stats_json, f, indent=2)
        
        return df


def main():
    """Main execution function."""
    logger.info("=" * 80)
    logger.info("WEATHER-CRASH INTEGRATION SCRIPT")
    logger.info("=" * 80)
    
    integrator = WeatherCrashIntegrator()
    df = integrator.process_crash_category("CRASH")
    
    if df is not None:
        logger.info("=" * 80)
        logger.info("WEATHER INTEGRATION COMPLETE")
        logger.info("=" * 80)
        logger.info(f"Final dataset: {len(df):,} records")
        logger.info(f"Weather-matched: {integrator.stats['crashes_matched']:,}")
    else:
        logger.error("Weather integration failed!")
        return 1
    
    return 0


if __name__ == "__main__":
    import sys
    sys.exit(main())
