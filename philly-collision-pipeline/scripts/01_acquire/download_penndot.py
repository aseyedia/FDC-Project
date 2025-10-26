"""
PennDOT Data Acquisition Script

Downloads crash data from PennDOT's GIS Open Data Portal for Philadelphia
for all years from 2005-2024. Handles all 8 data categories:
- CRASH: Main crash records
- COMMVEH: Commercial vehicle involvement
- CYCLE: Bicycle involvement
- FLAG: Flag person details
- PERSON: Person-level details
- ROADWAY: Road characteristics
- TRAILVEH: Trailer vehicle details
- VEHICLE: Vehicle details

Based on the R script: philly-crash-stats/code/download_preprocess.R

Author: Arta Seyedian
Date: October 2025
"""

import requests
import zipfile
from pathlib import Path
from typing import List, Tuple
import time
from tqdm import tqdm
import pandas as pd

# Import from parent directory
import sys
sys.path.append(str(Path(__file__).parent.parent))

from config import (
    YEARS,
    PENNDOT_CATEGORIES,
    PENNDOT_URL_TEMPLATE,
    RAW_DATA_DIR,
    get_raw_data_path
)
from utils.logging_utils import setup_logger, log_dataframe_info


# Initialize logger
logger = setup_logger("download_penndot")


def download_file(url: str, dest_path: Path, max_retries: int = 3) -> bool:
    """
    Download a file from URL to destination path with retry logic.
    
    Args:
        url: Source URL
        dest_path: Destination file path
        max_retries: Maximum number of retry attempts
        
    Returns:
        bool: True if successful, False otherwise
    """
    for attempt in range(max_retries):
        try:
            logger.info(f"Downloading {url} (attempt {attempt + 1}/{max_retries})")
            
            response = requests.get(url, stream=True, timeout=60)
            response.raise_for_status()
            
            # Get file size if available
            total_size = int(response.headers.get('content-length', 0))
            
            # Download with progress bar
            with open(dest_path, 'wb') as f:
                if total_size:
                    with tqdm(total=total_size, unit='B', unit_scale=True, desc=dest_path.name) as pbar:
                        for chunk in response.iter_content(chunk_size=8192):
                            f.write(chunk)
                            pbar.update(len(chunk))
                else:
                    for chunk in response.iter_content(chunk_size=8192):
                        f.write(chunk)
            
            logger.info(f"Successfully downloaded {dest_path}")
            return True
            
        except requests.exceptions.RequestException as e:
            logger.warning(f"Download attempt {attempt + 1} failed: {e}")
            if attempt < max_retries - 1:
                wait_time = (attempt + 1) * 5  # Exponential backoff
                logger.info(f"Waiting {wait_time} seconds before retry...")
                time.sleep(wait_time)
            else:
                logger.error(f"Failed to download {url} after {max_retries} attempts")
                return False
    
    return False


def extract_zip(zip_path: Path, extract_dir: Path) -> List[Path]:
    """
    Extract ZIP file to directory.
    
    Args:
        zip_path: Path to ZIP file
        extract_dir: Directory to extract to
        
    Returns:
        List of extracted file paths
    """
    extracted_files = []
    
    try:
        logger.info(f"Extracting {zip_path}")
        
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            # Get list of files
            file_list = zip_ref.namelist()
            logger.info(f"ZIP contains {len(file_list)} files")
            
            # Extract all files
            zip_ref.extractall(extract_dir)
            
            # Return paths of extracted files
            for filename in file_list:
                extracted_path = extract_dir / filename
                if extracted_path.exists():
                    extracted_files.append(extracted_path)
            
        logger.info(f"Extracted {len(extracted_files)} files from {zip_path}")
        return extracted_files
        
    except zipfile.BadZipFile as e:
        logger.error(f"Bad ZIP file {zip_path}: {e}")
        return []
    except Exception as e:
        logger.error(f"Error extracting {zip_path}: {e}")
        return []


def download_year_data(year: int) -> Tuple[bool, List[Path]]:
    """
    Download and extract data for a single year.
    
    Args:
        year: Year to download
        
    Returns:
        Tuple of (success, list of extracted CSV paths)
    """
    logger.info(f"Processing year {year}")
    
    # Create URL and paths
    url = PENNDOT_URL_TEMPLATE.format(year=year)
    zip_path = get_raw_data_path(year)
    
    # Download ZIP file
    if not download_file(url, zip_path):
        return False, []
    
    # Extract ZIP
    extracted_files = extract_zip(zip_path, RAW_DATA_DIR)
    
    # Filter for CSV files
    csv_files = [f for f in extracted_files if f.suffix.lower() == '.csv']
    logger.info(f"Extracted {len(csv_files)} CSV files for year {year}")
    
    return True, csv_files


def validate_extracted_files(year: int, csv_files: List[Path]) -> bool:
    """
    Validate that all expected data categories were extracted.
    
    Args:
        year: Year being validated
        csv_files: List of extracted CSV files
        
    Returns:
        bool: True if all categories present
    """
    # Get category names from filenames
    extracted_categories = set()
    for csv_file in csv_files:
        # Extract category from filename (e.g., "CRASH_2020.csv" -> "CRASH")
        for category in PENNDOT_CATEGORIES:
            if category in csv_file.name.upper():
                extracted_categories.add(category)
                break
    
    missing_categories = set(PENNDOT_CATEGORIES) - extracted_categories
    
    if missing_categories:
        logger.warning(f"Year {year} missing categories: {missing_categories}")
        return False
    else:
        logger.info(f"Year {year} has all {len(PENNDOT_CATEGORIES)} categories")
        return True


def download_all_penndot_data(years: List[int] = None) -> dict:
    """
    Download PennDOT crash data for all specified years.
    
    Args:
        years: List of years to download (default: all years from config)
        
    Returns:
        Dictionary with download statistics
    """
    if years is None:
        years = YEARS
    
    logger.info(f"Starting PennDOT data download for {len(years)} years: {years[0]}-{years[-1]}")
    logger.info(f"Data will be saved to: {RAW_DATA_DIR}")
    
    stats = {
        'total_years': len(years),
        'successful_downloads': 0,
        'failed_downloads': 0,
        'total_csv_files': 0,
        'failed_years': []
    }
    
    for year in years:
        success, csv_files = download_year_data(year)
        
        if success:
            stats['successful_downloads'] += 1
            stats['total_csv_files'] += len(csv_files)
            
            # Validate
            validate_extracted_files(year, csv_files)
        else:
            stats['failed_downloads'] += 1
            stats['failed_years'].append(year)
        
        # Be nice to the server
        time.sleep(2)
    
    # Log summary
    logger.info("=" * 60)
    logger.info("DOWNLOAD SUMMARY")
    logger.info("=" * 60)
    logger.info(f"Total years processed: {stats['total_years']}")
    logger.info(f"Successful downloads: {stats['successful_downloads']}")
    logger.info(f"Failed downloads: {stats['failed_downloads']}")
    logger.info(f"Total CSV files extracted: {stats['total_csv_files']}")
    
    if stats['failed_years']:
        logger.error(f"Failed years: {stats['failed_years']}")
    else:
        logger.info("All years downloaded successfully!")
    
    return stats


def main():
    """Main execution function."""
    logger.info("PennDOT Data Acquisition Script Started")
    logger.info(f"Years to download: {YEARS[0]}-{YEARS[-1]}")
    logger.info(f"Categories: {', '.join(PENNDOT_CATEGORIES)}")
    
    # Download all data
    stats = download_all_penndot_data()
    
    logger.info("PennDOT Data Acquisition Script Completed")
    
    return stats


if __name__ == "__main__":
    main()
