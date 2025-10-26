"""
Schema Harmonization Script

Handles schema drift across years (2005-2024) by standardizing column names,
data types, and categorical values. Based on the handle_mismatch() function
from the R analysis which addressed year-over-year schema inconsistencies.

Key transformations:
- Column name standardization (handle renaming across years)
- Data type harmonization (convert mismatched types to compatible format)
- Categorical value mapping (standardize codes like helmet Y/N/U)
- Missing column handling (add placeholders for new/removed columns)

Author: FDC Project
Date: 2025-10-26
"""

import sys
from pathlib import Path
import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Optional
from datetime import datetime
import json

# Add parent directory to path for imports
sys.path.append(str(Path(__file__).parent.parent))

from config import (
    RAW_DATA_DIR,
    PROCESSED_DATA_DIR,
    METADATA_DIR,
    PENNDOT_CATEGORIES,
    YEARS
)
from utils.logging_utils import setup_logger, log_dataframe_info

# Initialize logger
logger = setup_logger(__name__)


class SchemaHarmonizer:
    """
    Harmonizes schemas across years for PennDOT crash data.
    
    Implements Python version of R's handle_mismatch() function with
    additional features for comprehensive schema standardization.
    """
    
    def __init__(self, category: str):
        """
        Initialize harmonizer for a specific data category.
        
        Args:
            category: One of PENNDOT_CATEGORIES (CRASH, PERSON, etc.)
        """
        self.category = category
        self.logger = logger
        
        # Master schemas will be loaded from profiling results
        self.master_schema = None
        self.column_mappings = {}
        self.type_mappings = {}
        self.categorical_mappings = {}
        
        self.logger.info(f"SchemaHarmonizer initialized for category: {category}")
    
    def load_master_schema(self) -> Dict:
        """
        Load master schema from profiling results.
        
        Returns:
            Dict with master schema definition
        """
        schema_file = METADATA_DIR / "schema_analysis_report.json"
        
        if not schema_file.exists():
            self.logger.error(f"Schema analysis report not found: {schema_file}")
            self.logger.error("Run profiling (stage 2) first!")
            raise FileNotFoundError(f"Schema report required: {schema_file}")
        
        with open(schema_file, 'r') as f:
            analysis = json.load(f)
        
        # Get most recent year's schema as master
        if self.category in analysis['categories']:
            cat_data = analysis['categories'][self.category]
            # Use the latest year as master schema
            years = sorted(cat_data['years_available'], reverse=True)
            if years:
                latest_year = years[0]
                self.master_schema = cat_data['schema_by_year'].get(str(latest_year), {})
                self.logger.info(f"Loaded master schema from year {latest_year}")
                self.logger.info(f"Master schema has {len(self.master_schema)} columns")
                return self.master_schema
        
        self.logger.warning(f"No master schema found for {self.category}")
        return {}
    
    def handle_mismatch(self, df1: pd.DataFrame, df2: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Python implementation of R's handle_mismatch() function.
        
        Reconciles schema differences between two dataframes by:
        1. Converting mismatched column types to compatible format (usually string)
        2. Adding missing columns with NaN
        3. Ensuring column order consistency
        
        Args:
            df1: First dataframe (usually accumulated data)
            df2: Second dataframe (new data to append)
            
        Returns:
            Tuple of (harmonized_df1, harmonized_df2)
        """
        self.logger.info("Handling schema mismatch between dataframes")
        self.logger.info(f"DF1: {df1.shape[0]} rows, {df1.shape[1]} columns")
        self.logger.info(f"DF2: {df2.shape[0]} rows, {df2.shape[1]} columns")
        
        # Get all unique columns
        all_columns = set(df1.columns) | set(df2.columns)
        
        # Add missing columns to each dataframe
        for col in all_columns:
            if col not in df1.columns:
                df1[col] = np.nan
                self.logger.debug(f"Added missing column to DF1: {col}")
            if col not in df2.columns:
                df2[col] = np.nan
                self.logger.debug(f"Added missing column to DF2: {col}")
        
        # Harmonize data types for common columns
        common_columns = set(df1.columns) & set(df2.columns)
        type_conversions = 0
        
        for col in common_columns:
            dtype1 = df1[col].dtype
            dtype2 = df2[col].dtype
            
            if dtype1 != dtype2:
                # Convert both to string (safest common type)
                df1[col] = df1[col].astype(str)
                df2[col] = df2[col].astype(str)
                type_conversions += 1
                self.logger.debug(f"Converted {col}: {dtype1} + {dtype2} -> object")
        
        if type_conversions > 0:
            self.logger.info(f"Converted {type_conversions} columns to compatible types")
        
        # Ensure consistent column order
        column_order = sorted(all_columns)
        df1 = df1[column_order]
        df2 = df2[column_order]
        
        self.logger.info("Schema harmonization complete")
        return df1, df2
    
    def standardize_column_names(self, df: pd.DataFrame, year: int) -> pd.DataFrame:
        """
        Standardize column names across years.
        
        Handles known column renamings (e.g., HELMET_IND -> HELMET_USED)
        
        Args:
            df: Input dataframe
            year: Data year (for year-specific mappings)
            
        Returns:
            DataFrame with standardized column names
        """
        # Define known column name mappings (old -> new)
        name_mappings = {
            # Add mappings as discovered during profiling
            # Example: 'OLD_NAME': 'NEW_NAME'
        }
        
        if name_mappings:
            df = df.rename(columns=name_mappings)
            self.logger.info(f"Renamed {len(name_mappings)} columns")
        
        return df
    
    def harmonize_data_types(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Harmonize data types to match master schema.
        
        Args:
            df: Input dataframe
            
        Returns:
            DataFrame with harmonized data types
        """
        if not self.master_schema:
            self.logger.warning("No master schema loaded, skipping type harmonization")
            return df
        
        for col, expected_type in self.master_schema.items():
            if col in df.columns:
                current_type = str(df[col].dtype)
                
                # Convert if types don't match
                if current_type != expected_type:
                    try:
                        if expected_type.startswith('int'):
                            df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')
                        elif expected_type.startswith('float'):
                            df[col] = pd.to_numeric(df[col], errors='coerce')
                        elif expected_type == 'datetime64[ns]':
                            df[col] = pd.to_datetime(df[col], errors='coerce')
                        else:
                            df[col] = df[col].astype(str)
                        
                        self.logger.debug(f"Converted {col}: {current_type} -> {expected_type}")
                    except Exception as e:
                        self.logger.warning(f"Failed to convert {col}: {e}")
        
        return df
    
    def standardize_categorical_values(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Standardize categorical values (e.g., helmet usage Y/N/U).
        
        Known issues from R analysis:
        - Helmet indicator: Y, N, U, blank
        - Injury severity: various codes
        
        Args:
            df: Input dataframe
            
        Returns:
            DataFrame with standardized categorical values
        """
        # Helmet usage standardization
        if 'HELMET_IND' in df.columns:
            df['HELMET_IND'] = df['HELMET_IND'].str.upper().str.strip()
            # Map blanks to U (Unknown)
            df.loc[df['HELMET_IND'].isna(), 'HELMET_IND'] = 'U'
            df.loc[df['HELMET_IND'] == '', 'HELMET_IND'] = 'U'
            
            # Validate values
            valid_values = {'Y', 'N', 'U'}
            invalid = ~df['HELMET_IND'].isin(valid_values)
            if invalid.any():
                self.logger.warning(f"Found {invalid.sum()} invalid HELMET_IND values, setting to U")
                df.loc[invalid, 'HELMET_IND'] = 'U'
        
        return df
    
    def harmonize_year_data(self, year: int) -> Optional[pd.DataFrame]:
        """
        Load and harmonize data for a single year.
        
        Args:
            year: Year to process
            
        Returns:
            Harmonized dataframe or None if file not found
        """
        # Find CSV file for this category and year
        # Pattern: CATEGORY_PHILADELPHIA_YEAR.csv (e.g., CRASH_PHILADELPHIA_2023.csv)
        pattern_options = [
            f"{self.category}_PHILADELPHIA_{year}.csv",
            f"{self.category}_{year}.csv",
            f"{self.category}S_PHILADELPHIA_{year}.csv",  # FLAGS has an S
        ]
        
        file_path = None
        for pattern in pattern_options:
            files = list(RAW_DATA_DIR.glob(pattern))
            if files:
                file_path = files[0]
                break
        
        if not file_path:
            self.logger.warning(f"No file found for {self.category} {year}")
            self.logger.warning(f"Tried patterns: {pattern_options}")
            return None
        
        self.logger.info(f"Processing {file_path.name}")
        
        # Load data
        try:
            df = pd.read_csv(file_path, low_memory=False)
            self.logger.info(f"Loaded {len(df)} rows, {len(df.columns)} columns")
        except Exception as e:
            self.logger.error(f"Failed to load {file_path}: {e}")
            return None
        
        # Apply transformations
        df = self.standardize_column_names(df, year)
        # Skip type harmonization if no master schema
        # df = self.harmonize_data_types(df)
        df = self.standardize_categorical_values(df)
        
        # Add metadata columns
        df['DATA_YEAR'] = year
        df['PROCESSING_DATE'] = datetime.now().isoformat()
        
        return df
    
    def harmonize_all_years(self, years: Optional[List[int]] = None) -> pd.DataFrame:
        """
        Harmonize data across all years using handle_mismatch() approach.
        
        Mimics the R code's loop that combines data across years:
        for (i in seq_along(file_path)) {
            if (i == 1) {
                data[[tolower(data_set)]] <- new_data
            } else:
                handled_data <- handle_mismatch(data[[tolower(data_set)]], new_data)
                data[[tolower(data_set)]] <- bind_rows(handled_data[[1]], handled_data[[2]])
            }
        }
        
        Args:
            years: List of years to process (default: all years from config)
            
        Returns:
            Combined harmonized dataframe
        """
        if years is None:
            years = YEARS
        
        self.logger.info(f"Harmonizing {self.category} data for years: {min(years)}-{max(years)}")
        
        # Try to load master schema (optional, won't fail if not available)
        try:
            self.load_master_schema()
        except Exception as e:
            self.logger.warning(f"Could not load master schema: {e}")
            self.logger.info("Continuing without master schema")
        
        combined_df = None
        successful_years = []
        failed_years = []
        
        for i, year in enumerate(sorted(years)):
            self.logger.info(f"Processing year {year} ({i+1}/{len(years)})")
            
            df_year = self.harmonize_year_data(year)
            
            if df_year is None:
                failed_years.append(year)
                continue
            
            if i == 0 or combined_df is None:
                # First year - initialize combined dataframe
                combined_df = df_year
                self.logger.info(f"Initialized with {len(combined_df)} rows")
            else:
                # Subsequent years - use handle_mismatch
                combined_df, df_year = self.handle_mismatch(combined_df, df_year)
                
                # Concatenate
                combined_df = pd.concat([combined_df, df_year], ignore_index=True)
                self.logger.info(f"Combined total: {len(combined_df)} rows")
            
            successful_years.append(year)
        
        self.logger.info("=" * 80)
        self.logger.info("HARMONIZATION SUMMARY")
        self.logger.info("=" * 80)
        self.logger.info(f"Successful years: {len(successful_years)}")
        self.logger.info(f"Failed years: {len(failed_years)}")
        if combined_df is not None:
            self.logger.info(f"Total rows: {len(combined_df):,}")
            self.logger.info(f"Total columns: {len(combined_df.columns)}")
            log_dataframe_info(combined_df, f"Final {self.category} dataset")
        
        return combined_df
    
    def save_harmonized_data(self, df: pd.DataFrame) -> Path:
        """
        Save harmonized data to processed directory.
        
        Args:
            df: Harmonized dataframe
            
        Returns:
            Path to saved file
        """
        output_file = PROCESSED_DATA_DIR / f"{self.category.lower()}_harmonized.parquet"
        
        self.logger.info(f"Saving harmonized data to {output_file}")
        df.to_parquet(output_file, index=False, engine='pyarrow')
        
        file_size = output_file.stat().st_size / (1024 * 1024)  # MB
        self.logger.info(f"Saved {len(df):,} rows to {output_file.name} ({file_size:.2f} MB)")
        
        return output_file


def harmonize_category(category: str, years: Optional[List[int]] = None) -> pd.DataFrame:
    """
    Convenience function to harmonize a single category.
    
    Args:
        category: PennDOT category name
        years: Optional list of years (default: all)
        
    Returns:
        Harmonized dataframe
    """
    harmonizer = SchemaHarmonizer(category)
    df = harmonizer.harmonize_all_years(years)
    
    if df is not None and not df.empty:
        harmonizer.save_harmonized_data(df)
    
    return df


def main(categories: Optional[List[str]] = None, years: Optional[List[int]] = None):
    """
    Main execution function.
    
    Args:
        categories: List of categories to process (default: all)
        years: List of years to process (default: all)
    """
    logger.info("=" * 80)
    logger.info("SCHEMA HARMONIZATION SCRIPT")
    logger.info("=" * 80)
    
    if categories is None:
        categories = PENNDOT_CATEGORIES
    
    if years is None:
        years = YEARS
    
    logger.info(f"Categories to process: {', '.join(categories)}")
    logger.info(f"Years to process: {min(years)}-{max(years)}")
    
    results = {}
    
    for category in categories:
        logger.info("")
        logger.info("=" * 80)
        logger.info(f"PROCESSING CATEGORY: {category}")
        logger.info("=" * 80)
        
        try:
            df = harmonize_category(category, years)
            results[category] = {
                'status': 'success',
                'rows': len(df) if df is not None else 0,
                'columns': len(df.columns) if df is not None else 0
            }
        except Exception as e:
            logger.error(f"Failed to process {category}: {e}", exc_info=True)
            results[category] = {
                'status': 'failed',
                'error': str(e)
            }
    
    # Print summary
    logger.info("")
    logger.info("=" * 80)
    logger.info("HARMONIZATION COMPLETE")
    logger.info("=" * 80)
    
    for category, result in results.items():
        if result['status'] == 'success':
            logger.info(f"✓ {category}: {result['rows']:,} rows, {result['columns']} columns")
        else:
            logger.error(f"✗ {category}: {result['error']}")
    
    return results


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Harmonize PennDOT crash data schemas")
    parser.add_argument(
        '--categories',
        nargs='+',
        choices=PENNDOT_CATEGORIES,
        help='Categories to process (default: all)'
    )
    parser.add_argument(
        '--years',
        nargs='+',
        type=int,
        help='Years to process (default: all)'
    )
    parser.add_argument(
        '--test',
        action='store_true',
        help='Test mode: process single year only'
    )
    
    args = parser.parse_args()
    
    years = args.years
    if args.test and years is None:
        years = [2023]
        logger.info("TEST MODE: Processing 2023 only")
    
    main(categories=args.categories, years=years)
