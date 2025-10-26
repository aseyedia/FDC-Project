"""
NOAA Weather Data Acquisition Script

Downloads weather data from NOAA's Climate Data Online (CDO) API
for Philadelphia International Airport weather station.

Retrieves daily weather observations including:
- Temperature (min, max, average)
- Precipitation
- Wind speed
- Visibility (if available)

Data period: 2005-2024 to match crash data temporal coverage

NOAA API Documentation: https://www.ncdc.noaa.gov/cdo-web/webservices/v2
Token required: https://www.ncdc.noaa.gov/cdo-web/token

Author: Arta Seyedian
Date: October 2025
"""

import requests
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta
from typing import List, Dict, Optional
import time
from tqdm import tqdm

import sys
sys.path.append(str(Path(__file__).parent.parent))

from config import (
    NOAA_API_TOKEN,
    NOAA_STATION_ID,
    START_YEAR,
    END_YEAR,
    RAW_DATA_DIR,
    OUTPUT_FORMAT
)
from utils.logging_utils import setup_logger, log_dataframe_info


logger = setup_logger("download_noaa")


# NOAA CDO API endpoint
NOAA_BASE_URL = "https://www.ncei.noaa.gov/cdo-web/api/v2"

# Data types we want to retrieve
NOAA_DATATYPES = [
    "TMAX",  # Maximum temperature
    "TMIN",  # Minimum temperature
    "TAVG",  # Average temperature
    "PRCP",  # Precipitation
    "AWND",  # Average wind speed
    "WSF2",  # Fastest 2-minute wind speed
    "SNOW",  # Snowfall
    "SNWD",  # Snow depth
]


class NOAADataDownloader:
    """Handler for NOAA Climate Data Online API requests."""
    
    def __init__(self, api_token: str):
        """
        Initialize NOAA downloader.
        
        Args:
            api_token: NOAA CDO API token
        """
        if not api_token:
            raise ValueError("NOAA API token is required. Set NOAA_API_TOKEN in .env file")
        
        self.api_token = api_token
        self.headers = {"token": api_token}
        self.session = requests.Session()
        self.session.headers.update(self.headers)
        
        logger.info("NOAA Data Downloader initialized")
    
    def _make_request(self, endpoint: str, params: dict, max_retries: int = 3) -> Optional[dict]:
        """
        Make API request with retry logic.
        
        Args:
            endpoint: API endpoint (e.g., 'data')
            params: Query parameters
            max_retries: Maximum retry attempts
            
        Returns:
            JSON response or None if failed
        """
        url = f"{NOAA_BASE_URL}/{endpoint}"
        
        for attempt in range(max_retries):
            try:
                response = self.session.get(url, params=params, timeout=30)
                
                # Check for rate limiting
                if response.status_code == 429:
                    wait_time = 60
                    logger.warning(f"Rate limited. Waiting {wait_time} seconds...")
                    time.sleep(wait_time)
                    continue
                
                response.raise_for_status()
                return response.json()
                
            except requests.exceptions.RequestException as e:
                logger.warning(f"Request attempt {attempt + 1} failed: {e}")
                if attempt < max_retries - 1:
                    time.sleep((attempt + 1) * 5)
                else:
                    logger.error(f"Failed after {max_retries} attempts")
                    return None
        
        return None
    
    def get_station_info(self, station_id: str) -> Optional[dict]:
        """
        Get information about a weather station.
        
        Args:
            station_id: NOAA station ID
            
        Returns:
            Station information dictionary
        """
        logger.info(f"Fetching station info for {station_id}")
        
        response = self._make_request("stations", {"stationid": station_id})
        
        if response and "results" in response:
            station = response["results"][0]
            logger.info(f"Station: {station.get('name', 'Unknown')}")
            logger.info(f"Location: {station.get('latitude', 'N/A')}, {station.get('longitude', 'N/A')}")
            logger.info(f"Elevation: {station.get('elevation', 'N/A')} m")
            return station
        
        return None
    
    def download_daily_data(
        self,
        station_id: str,
        start_date: str,
        end_date: str,
        datatypes: List[str] = None
    ) -> pd.DataFrame:
        """
        Download daily weather data for a date range.
        
        Args:
            station_id: NOAA station ID
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)
            datatypes: List of data types to retrieve
            
        Returns:
            DataFrame with weather data
        """
        if datatypes is None:
            datatypes = NOAA_DATATYPES
        
        logger.info(f"Downloading data from {start_date} to {end_date}")
        logger.info(f"Data types: {', '.join(datatypes)}")
        
        all_results = []
        offset = 1
        limit = 1000  # NOAA API limit per request
        
        params = {
            "datasetid": "GHCND",  # Global Historical Climatology Network Daily
            "stationid": station_id,
            "startdate": start_date,
            "enddate": end_date,
            "units": "metric",
            "limit": limit,
            "offset": offset
        }
        
        # Add datatypes if specified
        if datatypes:
            params["datatypeid"] = ",".join(datatypes)
        
        with tqdm(desc=f"Downloading {start_date} to {end_date}", unit="records") as pbar:
            while True:
                params["offset"] = offset
                response = self._make_request("data", params)
                
                if not response or "results" not in response:
                    break
                
                results = response["results"]
                all_results.extend(results)
                pbar.update(len(results))
                
                # Check if more data available
                if len(results) < limit:
                    break
                
                offset += limit
                time.sleep(0.5)  # Be nice to the API
        
        logger.info(f"Retrieved {len(all_results)} total records")
        
        # Convert to DataFrame
        if all_results:
            df = pd.DataFrame(all_results)
            return df
        else:
            logger.warning("No data retrieved")
            return pd.DataFrame()
    
    def download_year_data(self, station_id: str, year: int) -> pd.DataFrame:
        """
        Download weather data for a full year.
        
        Args:
            station_id: NOAA station ID
            year: Year to download
            
        Returns:
            DataFrame with year's weather data
        """
        logger.info(f"Downloading weather data for year {year}")
        
        start_date = f"{year}-01-01"
        end_date = f"{year}-12-31"
        
        return self.download_daily_data(station_id, start_date, end_date)


def process_weather_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Process raw NOAA data into analysis-ready format.
    
    Args:
        df: Raw NOAA data DataFrame
        
    Returns:
        Processed DataFrame with one row per date
    """
    if df.empty:
        logger.warning("Empty DataFrame provided for processing")
        return df
    
    logger.info("Processing weather data")
    log_dataframe_info(df, "Raw NOAA data")
    
    # Parse date column
    df['date'] = pd.to_datetime(df['date'])
    
    # Pivot so each datatype becomes a column
    df_pivot = df.pivot_table(
        index='date',
        columns='datatype',
        values='value',
        aggfunc='first'
    ).reset_index()
    
    # Rename columns to be more descriptive
    column_mapping = {
        'TMAX': 'temp_max_c',
        'TMIN': 'temp_min_c',
        'TAVG': 'temp_avg_c',
        'PRCP': 'precipitation_mm',
        'AWND': 'wind_speed_avg_ms',
        'WSF2': 'wind_speed_max_ms',
        'SNOW': 'snowfall_mm',
        'SNWD': 'snow_depth_mm'
    }
    
    df_pivot = df_pivot.rename(columns=column_mapping)
    
    # Convert temperature from tenths of degrees to degrees
    for col in ['temp_max_c', 'temp_min_c', 'temp_avg_c']:
        if col in df_pivot.columns:
            df_pivot[col] = df_pivot[col] / 10.0
    
    # Sort by date
    df_pivot = df_pivot.sort_values('date')
    
    logger.info(f"Processed data shape: {df_pivot.shape}")
    logger.info(f"Date range: {df_pivot['date'].min()} to {df_pivot['date'].max()}")
    
    return df_pivot


def download_all_weather_data(
    station_id: str = NOAA_STATION_ID,
    start_year: int = START_YEAR,
    end_year: int = END_YEAR
) -> pd.DataFrame:
    """
    Download all weather data for the specified time period.
    
    Args:
        station_id: NOAA weather station ID
        start_year: First year to download
        end_year: Last year to download
        
    Returns:
        Combined DataFrame with all weather data
    """
    downloader = NOAADataDownloader(NOAA_API_TOKEN)
    
    # Get station info
    downloader.get_station_info(station_id)
    
    all_data = []
    
    for year in range(start_year, end_year + 1):
        logger.info(f"Processing year {year}")
        
        df_year = downloader.download_year_data(station_id, year)
        
        if not df_year.empty:
            all_data.append(df_year)
        
        # Be nice to the API
        time.sleep(1)
    
    # Combine all years
    if all_data:
        df_combined = pd.concat(all_data, ignore_index=True)
        logger.info(f"Combined data: {len(df_combined)} total records")
        
        # Process into analysis-ready format
        df_processed = process_weather_data(df_combined)
        
        return df_processed
    else:
        logger.error("No weather data downloaded")
        return pd.DataFrame()


def save_weather_data(df: pd.DataFrame, filename: str = "noaa_weather_philly"):
    """
    Save weather data to file.
    
    Args:
        df: Weather data DataFrame
        filename: Output filename (without extension)
    """
    if df.empty:
        logger.error("Cannot save empty DataFrame")
        return
    
    output_path = RAW_DATA_DIR / f"{filename}.{OUTPUT_FORMAT}"
    
    logger.info(f"Saving weather data to {output_path}")
    
    if OUTPUT_FORMAT == "parquet":
        df.to_parquet(output_path, index=False)
    else:
        df.to_csv(output_path, index=False)
    
    logger.info(f"Weather data saved successfully")
    log_dataframe_info(df, "Saved weather data")


def main():
    """Main execution function."""
    logger.info("NOAA Weather Data Acquisition Script Started")
    logger.info(f"Station: {NOAA_STATION_ID}")
    logger.info(f"Years: {START_YEAR}-{END_YEAR}")
    
    # Check for API token
    if not NOAA_API_TOKEN:
        logger.error("NOAA API token not found!")
        logger.error("Please set NOAA_API_TOKEN in .env file")
        logger.error("Get a token at: https://www.ncdc.noaa.gov/cdo-web/token")
        return None
    
    # Download data
    df_weather = download_all_weather_data()
    
    if not df_weather.empty:
        # Save to file
        save_weather_data(df_weather)
        
        logger.info("NOAA Weather Data Acquisition Script Completed Successfully")
        return df_weather
    else:
        logger.error("NOAA Weather Data Acquisition Script Failed")
        return None


if __name__ == "__main__":
    main()
