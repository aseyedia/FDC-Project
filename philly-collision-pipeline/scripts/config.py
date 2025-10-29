"""
Configuration management for the Philadelphia Collision Pipeline.
Loads environment variables and provides centralized configuration access.
"""

import os
from pathlib import Path
from typing import Tuple
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Project paths
PROJECT_ROOT = Path(__file__).parent.parent
DATA_DIR = PROJECT_ROOT / "data"
RAW_DATA_DIR = DATA_DIR / "raw"
PROCESSED_DATA_DIR = DATA_DIR / "processed"
FINAL_DATA_DIR = DATA_DIR / "final"
METADATA_DIR = PROJECT_ROOT / "metadata"
LOGS_DIR = PROJECT_ROOT / "logs"

# Ensure directories exist
for dir_path in [RAW_DATA_DIR, PROCESSED_DATA_DIR, FINAL_DATA_DIR, METADATA_DIR, LOGS_DIR]:
    dir_path.mkdir(parents=True, exist_ok=True)

# Data collection configuration
START_YEAR = int(os.getenv("START_YEAR", "2005"))
END_YEAR = int(os.getenv("END_YEAR", "2024"))
YEARS = list(range(START_YEAR, END_YEAR + 1))

# PennDOT data categories
PENNDOT_CATEGORIES = [
    "CRASH",
    "COMMVEH",
    "CYCLE",
    "FLAG",
    "PERSON",
    "ROADWAY",
    "TRAILVEH",
    "VEHICLE"
]

# PennDOT URL template
PENNDOT_URL_TEMPLATE = "https://gis.penndot.gov/gishub/crashZip/County/Philadelphia/Philadelphia_{year}.zip"

# Philadelphia geographic bounds (for validation)
PHILLY_BOUNDS = {
    "lat_min": float(os.getenv("PHILLY_LAT_MIN", "39.867")),
    "lat_max": float(os.getenv("PHILLY_LAT_MAX", "40.138")),
    "lon_min": float(os.getenv("PHILLY_LON_MIN", "-75.280")),
    "lon_max": float(os.getenv("PHILLY_LON_MAX", "-74.956"))
}

# NOAA configuration
NOAA_API_TOKEN = os.getenv("NOAA_API_TOKEN", "")
NOAA_STATION_ID = os.getenv("NOAA_STATION_ID", "GHCND:USW00013739")  # Philadelphia Int'l Airport

# Logging configuration
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

# Output format
OUTPUT_FORMAT = os.getenv("OUTPUT_FORMAT", "parquet")

# Coordinate Reference System
CRS_WGS84 = "EPSG:4326"  # Standard WGS84 lat/lon


def get_raw_data_path(year: int, category: str = None) -> Path:
    """Get path for raw data files."""
    if category:
        return RAW_DATA_DIR / f"{category}_{year}.csv"
    return RAW_DATA_DIR / f"Philadelphia_{year}.zip"


def get_processed_data_path(category: str, suffix: str = "") -> Path:
    """Get path for processed data files."""
    ext = f".{OUTPUT_FORMAT}"
    if suffix:
        return PROCESSED_DATA_DIR / f"{category}_{suffix}{ext}"
    return PROCESSED_DATA_DIR / f"{category}{ext}"


def get_final_data_path(dataset_name: str) -> Path:
    """Get path for final analysis-ready datasets."""
    ext = f".{OUTPUT_FORMAT}"
    return FINAL_DATA_DIR / f"{dataset_name}{ext}"
