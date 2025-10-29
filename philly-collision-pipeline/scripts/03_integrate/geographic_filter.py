"""
Geographic Filtering Script

Filters collision data to Philadelphia boundaries and validates geographic data quality.
Addresses known issues from R analysis:
- County miscoding (York county appearing in Philadelphia data)
- Coordinate precision inconsistencies
- Invalid lat/lon values
- CRS standardization

Uses geopandas for spatial operations and boundary filtering.

Author: FDC Project
Date: 2025-10-26
"""

import sys
from pathlib import Path
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point, Polygon
from typing import Optional, Tuple
import numpy as np

# Add parent directory to path for imports
sys.path.append(str(Path(__file__).parent.parent))

from config import (
    PROCESSED_DATA_DIR,
    PHILLY_BOUNDS,
    METADATA_DIR
)
from utils.logging_utils import setup_logger, log_dataframe_info

# Initialize logger
logger = setup_logger(__name__)

# Philadelphia boundary coordinates (approximate)
# For production, load official city boundary shapefile
PHILLY_BOUNDARY_COORDS = [
    (-75.280, 39.867),  # Southwest
    (-75.280, 40.138),  # Northwest
    (-74.956, 40.138),  # Northeast
    (-74.956, 39.867),  # Southeast
    (-75.280, 39.867),  # Close polygon
]


class GeographicFilter:
    """
    Filters and validates geographic data for Philadelphia collisions.
    """
    
    def __init__(self):
        """Initialize geographic filter."""
        self.logger = logger
        self.philly_bounds = PHILLY_BOUNDS
        self.philly_boundary = None
        
        # Statistics
        self.stats = {
            'total_records': 0,
            'records_with_coords': 0,
            'invalid_coords': 0,
            'outside_bounds': 0,
            'county_mismatches': 0,
            'final_records': 0
        }
        
        self.logger.info("GeographicFilter initialized")
    
    def create_philly_boundary(self) -> Polygon:
        """
        Create Philadelphia boundary polygon.
        
        For production, this should load official city boundary shapefile.
        For now, uses bounding box.
        
        Returns:
            Shapely Polygon of Philadelphia boundary
        """
        polygon = Polygon(PHILLY_BOUNDARY_COORDS)
        self.philly_boundary = polygon
        
        self.logger.info("Created Philadelphia boundary polygon")
        self.logger.info(f"Bounds: {self.philly_bounds}")
        
        return polygon
    
    def validate_coordinates(self, df: pd.DataFrame, 
                           lat_col: str = 'DEC_LATITUDE', 
                           lon_col: str = 'DEC_LONGITUDE') -> pd.DataFrame:
        """
        Validate and clean geographic coordinates.
        
        Addresses issues:
        - Missing coordinates
        - Coordinates of (0, 0) or (0.0, 0.0)
        - Out of range values
        - Coordinate precision inconsistencies
        
        Args:
            df: Input dataframe
            lat_col: Latitude column name
            lon_col: Longitude column name
            
        Returns:
            DataFrame with validated coordinates
        """
        self.logger.info("Validating geographic coordinates")
        
        initial_count = len(df)
        self.stats['total_records'] = initial_count
        
        # Check if coordinate columns exist
        if lat_col not in df.columns or lon_col not in df.columns:
            self.logger.warning(f"Coordinate columns not found: {lat_col}, {lon_col}")
            self.logger.warning("Available columns: " + ", ".join(df.columns[:10]) + "...")
            return df
        
        # Convert to numeric, coerce errors to NaN
        df[lat_col] = pd.to_numeric(df[lat_col], errors='coerce')
        df[lon_col] = pd.to_numeric(df[lon_col], errors='coerce')
        
        # Count records with coordinates
        has_coords = df[lat_col].notna() & df[lon_col].notna()
        self.stats['records_with_coords'] = has_coords.sum()
        
        # Flag invalid coordinates
        invalid_coords = (
            (df[lat_col] == 0) & (df[lon_col] == 0) |  # Zero coordinates
            (df[lat_col].isna()) | (df[lon_col].isna()) |  # Missing
            (df[lat_col] < -90) | (df[lat_col] > 90) |  # Out of range latitude
            (df[lon_col] < -180) | (df[lon_col] > 180)  # Out of range longitude
        )
        
        self.stats['invalid_coords'] = invalid_coords.sum()
        
        if invalid_coords.any():
            self.logger.warning(f"Found {invalid_coords.sum()} records with invalid coordinates")
            df.loc[invalid_coords, 'COORD_QUALITY_FLAG'] = 'INVALID'
        
        # Standardize coordinate precision to 6 decimal places (~0.1m precision)
        # Addresses coordinate precision inconsistencies from R analysis
        df.loc[has_coords, lat_col] = df.loc[has_coords, lat_col].round(6)
        df.loc[has_coords, lon_col] = df.loc[has_coords, lon_col].round(6)
        
        self.logger.info(f"Coordinate validation complete:")
        self.logger.info(f"  Total records: {initial_count:,}")
        self.logger.info(f"  With coordinates: {self.stats['records_with_coords']:,}")
        self.logger.info(f"  Invalid: {self.stats['invalid_coords']:,}")
        
        return df
    
    def filter_to_philadelphia(self, df: pd.DataFrame,
                              lat_col: str = 'DEC_LATITUDE',
                              lon_col: str = 'DEC_LONGITUDE') -> pd.DataFrame:
        """
        Filter data to Philadelphia geographic boundaries.
        
        Args:
            df: Input dataframe
            lat_col: Latitude column name
            lon_col: Longitude column name
            
        Returns:
            DataFrame filtered to Philadelphia
        """
        self.logger.info("Filtering to Philadelphia boundaries")
        
        initial_count = len(df)
        
        # Create boundary if not exists
        if self.philly_boundary is None:
            self.create_philly_boundary()
        
        # Filter using bounding box (fast first pass)
        in_bbox = (
            (df[lat_col] >= self.philly_bounds['lat_min']) &
            (df[lat_col] <= self.philly_bounds['lat_max']) &
            (df[lon_col] >= self.philly_bounds['lon_min']) &
            (df[lon_col] <= self.philly_bounds['lon_max'])
        )
        
        # Count records outside bounding box
        outside_bbox = ~in_bbox & df[lat_col].notna()
        self.stats['outside_bounds'] = outside_bbox.sum()
        
        if outside_bbox.any():
            self.logger.info(f"Found {outside_bbox.sum()} records outside Philadelphia bounding box")
            df.loc[outside_bbox, 'COORD_QUALITY_FLAG'] = 'OUTSIDE_PHILLY'
        
        # Keep only records inside Philadelphia
        df_filtered = df[in_bbox | df[lat_col].isna()].copy()
        
        self.stats['final_records'] = len(df_filtered)
        
        self.logger.info(f"Geographic filtering complete:")
        self.logger.info(f"  Input records: {initial_count:,}")
        self.logger.info(f"  Outside Philadelphia: {self.stats['outside_bounds']:,}")
        self.logger.info(f"  Retained: {self.stats['final_records']:,}")
        
        return df_filtered
    
    def check_county_coding(self, df: pd.DataFrame, 
                          county_col: str = 'COUNTY') -> pd.DataFrame:
        """
        Check for county miscoding (York appearing in Philadelphia data).
        
        Known issue from R analysis: Some records coded as York county (67)
        but have Philadelphia coordinates.
        
        Args:
            df: Input dataframe
            county_col: County code column name
            
        Returns:
            DataFrame with county quality flags
        """
        self.logger.info("Checking county coding")
        
        if county_col not in df.columns:
            self.logger.warning(f"County column not found: {county_col}")
            return df
        
        # Philadelphia county code is 51
        # York county code is 67
        county_counts = df[county_col].value_counts()
        
        self.logger.info("County distribution:")
        for county, count in county_counts.head(5).items():
            self.logger.info(f"  County {county}: {count:,} records")
        
        # Flag non-Philadelphia counties
        non_philly = df[county_col] != 51
        if non_philly.any():
            self.stats['county_mismatches'] = non_philly.sum()
            self.logger.warning(f"Found {non_philly.sum()} records with non-Philadelphia county codes")
            df.loc[non_philly, 'COUNTY_QUALITY_FLAG'] = 'NON_PHILLY_COUNTY'
        
        return df
    
    def create_geodataframe(self, df: pd.DataFrame,
                           lat_col: str = 'DEC_LATITUDE',
                           lon_col: str = 'DEC_LONGITUDE',
                           crs: str = 'EPSG:4326') -> gpd.GeoDataFrame:
        """
        Convert DataFrame to GeoDataFrame with Point geometries.
        
        Args:
            df: Input dataframe
            lat_col: Latitude column name
            lon_col: Longitude column name
            crs: Coordinate reference system (default: WGS84)
            
        Returns:
            GeoDataFrame
        """
        self.logger.info(f"Creating GeoDataFrame with CRS: {crs}")
        
        # Create geometry column
        has_coords = df[lat_col].notna() & df[lon_col].notna()
        
        geometry = [
            Point(lon, lat) if has_coord else None
            for lon, lat, has_coord in zip(df[lon_col], df[lat_col], has_coords)
        ]
        
        gdf = gpd.GeoDataFrame(df, geometry=geometry, crs=crs)
        
        self.logger.info(f"Created GeoDataFrame: {len(gdf):,} records")
        self.logger.info(f"  With geometry: {gdf.geometry.notna().sum():,}")
        
        return gdf
    
    def process_category(self, category: str,
                        lat_col: str = 'DEC_LATITUDE',
                        lon_col: str = 'DEC_LONGITUDE') -> Optional[pd.DataFrame]:
        """
        Process a single category through complete geographic workflow.
        
        Args:
            category: PennDOT category name
            lat_col: Latitude column name
            lon_col: Longitude column name
            
        Returns:
            Processed dataframe or None
        """
        self.logger.info("=" * 80)
        self.logger.info(f"Processing category: {category}")
        self.logger.info("=" * 80)
        
        # Load harmonized data
        input_file = PROCESSED_DATA_DIR / f"{category.lower()}_harmonized.parquet"
        
        if not input_file.exists():
            self.logger.error(f"Harmonized data not found: {input_file}")
            self.logger.error("Run harmonization (stage 2) first!")
            return None
        
        self.logger.info(f"Loading {input_file.name}")
        df = pd.read_parquet(input_file)
        
        self.logger.info(f"Loaded {len(df):,} records")
        log_dataframe_info(df, f"Input {category}")
        
        # Reset stats
        self.stats = {k: 0 for k in self.stats}
        
        # Apply geographic processing
        df = self.validate_coordinates(df, lat_col, lon_col)
        df = self.check_county_coding(df)
        df = self.filter_to_philadelphia(df, lat_col, lon_col)
        
        # Save processed data
        output_file = PROCESSED_DATA_DIR / f"{category.lower()}_geographic.parquet"
        
        self.logger.info(f"Saving to {output_file.name}")
        df.to_parquet(output_file, index=False, engine='pyarrow')
        
        file_size = output_file.stat().st_size / (1024 * 1024)
        self.logger.info(f"Saved {len(df):,} records ({file_size:.2f} MB)")
        
        # Save statistics
        stats_file = METADATA_DIR / f"{category.lower()}_geographic_stats.json"
        import json
        # Convert numpy types to Python types for JSON serialization
        stats_json = {k: int(v) if hasattr(v, 'item') else v for k, v in self.stats.items()}
        with open(stats_file, 'w') as f:
            json.dump(stats_json, f, indent=2)
        
        return df


def main():
    """Main execution function."""
    logger.info("=" * 80)
    logger.info("GEOGRAPHIC FILTERING SCRIPT")
    logger.info("=" * 80)
    
    # Only process CRASH category (has geographic data)
    # Other categories (PERSON, VEHICLE, etc.) join to CRASH via CRN
    category = "CRASH"
    
    geo_filter = GeographicFilter()
    df = geo_filter.process_category(category)
    
    if df is not None:
        logger.info("=" * 80)
        logger.info("GEOGRAPHIC FILTERING COMPLETE")
        logger.info("=" * 80)
        logger.info(f"Final dataset: {len(df):,} records")
    else:
        logger.error("Geographic filtering failed!")
        return 1
    
    return 0


if __name__ == "__main__":
    import sys
    sys.exit(main())
