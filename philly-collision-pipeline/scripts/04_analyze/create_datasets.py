"""
Create Analysis-Ready Datasets

Joins the 8 PennDOT categories by CRN (Crash Report Number) and creates
specialized datasets for different analyses:

1. Cyclist-focused dataset: CRASH + CYCLE + PERSON + VEHICLE + Weather
2. Pedestrian-focused dataset: CRASH + PERSON (pedestrians) + VEHICLE + Weather
3. Full integrated dataset: All categories joined with weather

Based on R analysis lessons learned:
- Use CRN as primary key for joining
- Handle missing joins gracefully
- Create analysis-specific subsets
- Add derived features for common analyses

Author: FDC Project
Date: 2025-10-26
"""

import sys
from pathlib import Path
import pandas as pd
from typing import Optional, List, Dict
from datetime import datetime

# Add parent directory to path for imports
sys.path.append(str(Path(__file__).parent.parent))

from config import (
    PROCESSED_DATA_DIR,
    FINAL_DATA_DIR,
    PENNDOT_CATEGORIES
)
from utils.logging_utils import setup_logger, log_dataframe_info

# Initialize logger
logger = setup_logger(__name__)


class DatasetCreator:
    """
    Creates analysis-ready datasets by joining PennDOT categories.
    """
    
    def __init__(self):
        """Initialize dataset creator."""
        self.logger = logger
        self.data = {}  # Will hold loaded dataframes
        
        self.stats = {
            'crash_records': 0,
            'cyclist_crashes': 0,
            'pedestrian_crashes': 0,
            'total_persons': 0,
            'total_vehicles': 0
        }
        
        self.logger.info("DatasetCreator initialized")
    
    def load_weather_integrated_crash(self) -> pd.DataFrame:
        """
        Load the weather-integrated CRASH data.
        
        Returns:
            Weather-integrated crash dataframe
        """
        crash_file = PROCESSED_DATA_DIR / "crash_weather_integrated.parquet"
        
        if not crash_file.exists():
            self.logger.error(f"Weather-integrated crash data not found: {crash_file}")
            self.logger.error("Run stages 1-4 first!")
            raise FileNotFoundError(f"Required file: {crash_file}")
        
        self.logger.info(f"Loading {crash_file.name}")
        df = pd.read_parquet(crash_file)
        
        self.stats['crash_records'] = len(df)
        self.logger.info(f"Loaded {len(df):,} crash records")
        log_dataframe_info(df, "Weather-integrated CRASH")
        
        self.data['crash'] = df
        return df
    
    def load_harmonized_category(self, category: str) -> Optional[pd.DataFrame]:
        """
        Load harmonized data for a category.
        
        Args:
            category: PennDOT category name
            
        Returns:
            Dataframe or None if not found
        """
        file_path = PROCESSED_DATA_DIR / f"{category.lower()}_harmonized.parquet"
        
        if not file_path.exists():
            self.logger.warning(f"Harmonized data not found: {file_path.name}")
            return None
        
        self.logger.info(f"Loading {file_path.name}")
        df = pd.read_parquet(file_path)
        
        self.logger.info(f"Loaded {len(df):,} records from {category}")
        log_dataframe_info(df, f"{category} data")
        
        self.data[category.lower()] = df
        return df
    
    def create_cyclist_dataset(self) -> pd.DataFrame:
        """
        Create cyclist-focused dataset.
        
        Joins: CRASH (weather-integrated) + CYCLE + PERSON + VEHICLE
        
        Returns:
            Cyclist-focused dataframe
        """
        self.logger.info("=" * 80)
        self.logger.info("Creating Cyclist-Focused Dataset")
        self.logger.info("=" * 80)
        
        # Load base crash data with weather
        crash_df = self.load_weather_integrated_crash()
        
        # Load CYCLE data
        cycle_df = self.load_harmonized_category("CYCLE")
        
        if cycle_df is None:
            self.logger.error("CYCLE data required for cyclist dataset")
            return None
        
        # Join CRASH and CYCLE on CRN
        self.logger.info("Joining CRASH with CYCLE on CRN...")
        
        # Ensure CRN column exists and is consistent type
        if 'CRN' not in crash_df.columns or 'CRN' not in cycle_df.columns:
            self.logger.error("CRN column not found in data")
            return None
        
        # Merge
        cyclist_df = crash_df.merge(
            cycle_df,
            on='CRN',
            how='inner',  # Only crashes with bicycle involvement
            suffixes=('', '_cycle')
        )
        
        self.stats['cyclist_crashes'] = len(cyclist_df)
        self.logger.info(f"Cyclist crashes: {len(cyclist_df):,}")
        
        # Optionally add PERSON data for cyclist details
        person_df = self.load_harmonized_category("PERSON")
        if person_df is not None:
            # Filter to cyclist persons only
            if 'PERSON_TYPE' in person_df.columns:
                cyclist_persons = person_df[person_df['PERSON_TYPE'].isin(['PEDALCYCLIST', 'BICYCLIST'])]
                self.logger.info(f"Found {len(cyclist_persons):,} cyclist person records")
                
                # Join
                cyclist_df = cyclist_df.merge(
                    cyclist_persons,
                    on='CRN',
                    how='left',
                    suffixes=('', '_person')
                )
        
        # Add vehicle data
        vehicle_df = self.load_harmonized_category("VEHICLE")
        if vehicle_df is not None:
            # Aggregate vehicle info by CRN
            vehicle_summary = vehicle_df.groupby('CRN').agg({
                'VEH_TYPE': lambda x: ', '.join(x.dropna().astype(str).unique()),
                'VEH_ROLE_CD': lambda x: ', '.join(x.dropna().astype(str).unique())
            }).reset_index()
            
            cyclist_df = cyclist_df.merge(
                vehicle_summary,
                on='CRN',
                how='left',
                suffixes=('', '_veh')
            )
        
        self.logger.info(f"Final cyclist dataset: {len(cyclist_df):,} rows, {len(cyclist_df.columns)} columns")
        
        return cyclist_df
    
    def create_pedestrian_dataset(self) -> pd.DataFrame:
        """
        Create pedestrian-focused dataset.
        
        Joins: CRASH (weather-integrated) + PERSON (pedestrians) + VEHICLE
        
        Returns:
            Pedestrian-focused dataframe
        """
        self.logger.info("=" * 80)
        self.logger.info("Creating Pedestrian-Focused Dataset")
        self.logger.info("=" * 80)
        
        # Load base crash data
        crash_df = self.load_weather_integrated_crash()
        
        # Filter to crashes with pedestrian involvement
        # Check for pedestrian count column
        ped_cols = [col for col in crash_df.columns if 'PED' in col.upper() and 'COUNT' in col.upper()]
        
        if ped_cols:
            ped_col = ped_cols[0]
            self.logger.info(f"Using pedestrian indicator: {ped_col}")
            pedestrian_df = crash_df[crash_df[ped_col] > 0].copy()
        else:
            self.logger.warning("No pedestrian count column found, using all crashes")
            pedestrian_df = crash_df.copy()
        
        self.stats['pedestrian_crashes'] = len(pedestrian_df)
        self.logger.info(f"Pedestrian crashes: {len(pedestrian_df):,}")
        
        # Add PERSON data filtered to pedestrians
        person_df = self.load_harmonized_category("PERSON")
        if person_df is not None:
            # Filter to pedestrians
            if 'PERSON_TYPE' in person_df.columns:
                pedestrians = person_df[person_df['PERSON_TYPE'] == 'PEDESTRIAN']
                self.logger.info(f"Found {len(pedestrians):,} pedestrian person records")
                
                pedestrian_df = pedestrian_df.merge(
                    pedestrians,
                    on='CRN',
                    how='left',
                    suffixes=('', '_person')
                )
        
        # Add vehicle data
        vehicle_df = self.load_harmonized_category("VEHICLE")
        if vehicle_df is not None:
            vehicle_summary = vehicle_df.groupby('CRN').agg({
                'VEH_TYPE': lambda x: ', '.join(x.dropna().astype(str).unique())
            }).reset_index()
            
            pedestrian_df = pedestrian_df.merge(
                vehicle_summary,
                on='CRN',
                how='left',
                suffixes=('', '_veh')
            )
        
        self.logger.info(f"Final pedestrian dataset: {len(pedestrian_df):,} rows, {len(pedestrian_df.columns)} columns")
        
        return pedestrian_df
    
    def create_full_integrated_dataset(self) -> pd.DataFrame:
        """
        Create full integrated dataset with all categories.
        
        Returns:
            Complete integrated dataframe
        """
        self.logger.info("=" * 80)
        self.logger.info("Creating Full Integrated Dataset")
        self.logger.info("=" * 80)
        
        # Start with weather-integrated crash data
        full_df = self.load_weather_integrated_crash()
        
        # Load and join all other categories
        categories_to_join = ['PERSON', 'VEHICLE', 'ROADWAY']
        
        for category in categories_to_join:
            cat_df = self.load_harmonized_category(category)
            
            if cat_df is None:
                self.logger.warning(f"Skipping {category} - not available")
                continue
            
            # For categories with multiple records per CRN, aggregate or keep separate
            if category in ['PERSON', 'VEHICLE']:
                # These have multiple records per crash - keep as separate table
                # For full dataset, we'll save them separately
                self.logger.info(f"Keeping {category} as separate table (multiple records per CRN)")
                continue
            else:
                # Categories like ROADWAY typically have 1 record per CRN
                self.logger.info(f"Joining {category} to main dataset...")
                full_df = full_df.merge(
                    cat_df,
                    on='CRN',
                    how='left',
                    suffixes=('', f'_{category.lower()}')
                )
        
        self.logger.info(f"Full integrated dataset: {len(full_df):,} rows, {len(full_df.columns)} columns")
        
        return full_df
    
    def save_dataset(self, df: pd.DataFrame, name: str, description: str) -> Path:
        """
        Save dataset to final directory.
        
        Args:
            df: Dataframe to save
            name: Dataset name (e.g., 'cyclist_focused')
            description: Dataset description for logging
            
        Returns:
            Path to saved file
        """
        output_file = FINAL_DATA_DIR / f"{name}.parquet"
        
        self.logger.info(f"Saving {description} to {output_file.name}")
        df.to_parquet(output_file, index=False, engine='pyarrow')
        
        file_size = output_file.stat().st_size / (1024 * 1024)  # MB
        self.logger.info(f"Saved {len(df):,} rows ({file_size:.2f} MB)")
        
        # Also save as CSV for wider compatibility
        csv_file = FINAL_DATA_DIR / f"{name}.csv"
        self.logger.info(f"Also saving as CSV: {csv_file.name}")
        df.to_csv(csv_file, index=False)
        
        csv_size = csv_file.stat().st_size / (1024 * 1024)
        self.logger.info(f"CSV: {csv_size:.2f} MB")
        
        return output_file


def main():
    """Main execution function."""
    logger.info("=" * 80)
    logger.info("DATASET CREATION SCRIPT")
    logger.info("=" * 80)
    
    creator = DatasetCreator()
    
    results = {}
    
    # Create cyclist dataset
    try:
        logger.info("\n" + "=" * 80)
        cyclist_df = creator.create_cyclist_dataset()
        if cyclist_df is not None and not cyclist_df.empty:
            creator.save_dataset(cyclist_df, 'cyclist_focused', 'Cyclist-focused dataset')
            results['cyclist'] = {'status': 'success', 'rows': len(cyclist_df)}
        else:
            results['cyclist'] = {'status': 'skipped', 'reason': 'No data'}
    except Exception as e:
        logger.error(f"Failed to create cyclist dataset: {e}", exc_info=True)
        results['cyclist'] = {'status': 'failed', 'error': str(e)}
    
    # Create pedestrian dataset
    try:
        logger.info("\n" + "=" * 80)
        pedestrian_df = creator.create_pedestrian_dataset()
        if pedestrian_df is not None and not pedestrian_df.empty:
            creator.save_dataset(pedestrian_df, 'pedestrian_focused', 'Pedestrian-focused dataset')
            results['pedestrian'] = {'status': 'success', 'rows': len(pedestrian_df)}
        else:
            results['pedestrian'] = {'status': 'skipped', 'reason': 'No data'}
    except Exception as e:
        logger.error(f"Failed to create pedestrian dataset: {e}", exc_info=True)
        results['pedestrian'] = {'status': 'failed', 'error': str(e)}
    
    # Create full integrated dataset
    try:
        logger.info("\n" + "=" * 80)
        full_df = creator.create_full_integrated_dataset()
        if full_df is not None and not full_df.empty:
            creator.save_dataset(full_df, 'full_integrated', 'Full integrated dataset')
            results['full'] = {'status': 'success', 'rows': len(full_df)}
        else:
            results['full'] = {'status': 'skipped', 'reason': 'No data'}
    except Exception as e:
        logger.error(f"Failed to create full dataset: {e}", exc_info=True)
        results['full'] = {'status': 'failed', 'error': str(e)}
    
    # Save individual category tables for reference
    logger.info("\n" + "=" * 80)
    logger.info("Saving individual category tables")
    logger.info("=" * 80)
    
    for category in ['PERSON', 'VEHICLE']:
        if category.lower() in creator.data:
            df = creator.data[category.lower()]
            creator.save_dataset(df, category.lower(), f"{category} table")
    
    # Print summary
    logger.info("\n" + "=" * 80)
    logger.info("DATASET CREATION COMPLETE")
    logger.info("=" * 80)
    
    for dataset_name, result in results.items():
        if result['status'] == 'success':
            logger.info(f"✓ {dataset_name}: {result['rows']:,} rows")
        elif result['status'] == 'skipped':
            logger.warning(f"⊘ {dataset_name}: {result['reason']}")
        else:
            logger.error(f"✗ {dataset_name}: {result.get('error', 'Unknown error')}")
    
    logger.info(f"\nFinal datasets saved to: {FINAL_DATA_DIR}")
    
    return results


if __name__ == "__main__":
    import sys
    sys.exit(0 if main() else 1)
