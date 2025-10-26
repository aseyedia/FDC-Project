"""
Data Quality Assessment Framework

Implements automated quality checks addressing known issues from R analysis:
1. Geographic validity (coordinates within Philadelphia bounds)
2. County miscoding (York county issue)
3. Missing coordinate values
4. Coordinate precision standardization
5. Date/time consistency
6. Categorical variable consistency (helmet usage Y/N/U, injury severity)

Author: Arta Seyedian  
Date: October 2025
"""

import pandas as pd
import numpy as np
from pathlib import Path
from typing import Dict, List, Tuple
import pandera as pa
from pandera import Column, Check, DataFrameSchema

import sys
sys.path.append(str(Path(__file__).parent.parent))

from config import PHILLY_BOUNDS, PENNDOT_CATEGORIES
from utils.logging_utils import setup_logger, log_dataframe_info


logger = setup_logger("quality_checks")


class QualityChecker:
    """Performs data quality checks on crash data."""
    
    def __init__(self):
        """Initialize quality checker."""
        self.quality_report = {
            'checks_performed': [],
            'issues_found': [],
            'summary': {}
        }
    
    def check_geographic_bounds(self, df: pd.DataFrame, lat_col: str = 'DEC_LAT', lon_col: str = 'DEC_LONG') -> Tuple[pd.DataFrame, dict]:
        """
        Check if coordinates fall within Philadelphia geographic bounds.
        
        Args:
            df: DataFrame with coordinates
            lat_col: Latitude column name
            lon_col: Longitude column name
            
        Returns:
            Tuple of (DataFrame with validity flag, report dict)
        """
        logger.info("Checking geographic bounds")
        
        report = {
            'check': 'geographic_bounds',
            'total_records': len(df),
            'issues': []
        }
        
        if lat_col not in df.columns or lon_col not in df.columns:
            logger.warning(f"Coordinate columns not found: {lat_col}, {lon_col}")
            report['issues'].append({'type': 'missing_columns', 'columns': [lat_col, lon_col]})
            return df, report
        
        # Check for missing coordinates
        missing_coords = df[[lat_col, lon_col]].isnull().any(axis=1)
        num_missing = missing_coords.sum()
        
        if num_missing > 0:
            logger.warning(f"Found {num_missing} records with missing coordinates ({num_missing/len(df)*100:.2f}%)")
            report['issues'].append({
                'type': 'missing_coordinates',
                'count': int(num_missing),
                'percentage': float(num_missing/len(df)*100)
            })
        
        # Check bounds (only for non-missing coordinates)
        valid_coords = ~missing_coords
        
        out_of_bounds_lat = (
            (df.loc[valid_coords, lat_col] < PHILLY_BOUNDS['lat_min']) |
            (df.loc[valid_coords, lat_col] > PHILLY_BOUNDS['lat_max'])
        )
        
        out_of_bounds_lon = (
            (df.loc[valid_coords, lon_col] < PHILLY_BOUNDS['lon_min']) |
            (df.loc[valid_coords, lon_col] > PHILLY_BOUNDS['lon_max'])
        )
        
        out_of_bounds = out_of_bounds_lat | out_of_bounds_lon
        num_out_of_bounds = out_of_bounds.sum()
        
        if num_out_of_bounds > 0:
            logger.warning(f"Found {num_out_of_bounds} records with out-of-bounds coordinates ({num_out_of_bounds/len(df)*100:.2f}%)")
            report['issues'].append({
                'type': 'out_of_bounds',
                'count': int(num_out_of_bounds),
                'percentage': float(num_out_of_bounds/len(df)*100)
            })
        
        # Add validity flag
        df['geo_valid'] = valid_coords & ~out_of_bounds
        
        report['valid_records'] = int(df['geo_valid'].sum())
        report['invalid_records'] = int((~df['geo_valid']).sum())
        
        self.quality_report['checks_performed'].append(report)
        
        logger.info(f"Geographic check: {report['valid_records']} valid, {report['invalid_records']} invalid")
        
        return df, report
    
    def check_county_coding(self, df: pd.DataFrame, county_col: str = 'COUNTY') -> Tuple[pd.DataFrame, dict]:
        """
        Check county coding. Known issue: all records marked as York (67).
        
        Args:
            df: DataFrame
            county_col: County column name
            
        Returns:
            Tuple of (DataFrame with flag, report dict)
        """
        logger.info("Checking county coding")
        
        report = {
            'check': 'county_coding',
            'total_records': len(df),
            'issues': []
        }
        
        if county_col not in df.columns:
            logger.warning(f"County column not found: {county_col}")
            report['issues'].append({'type': 'missing_column', 'column': county_col})
            return df, report
        
        # Check county distribution
        county_counts = df[county_col].value_counts()
        
        # Philadelphia county code should be 91, but known issue is 67 (York)
        if 67 in county_counts.index or '67' in county_counts.index:
            if len(county_counts) == 1 and (county_counts.index[0] == 67 or county_counts.index[0] == '67'):
                logger.warning("KNOWN ISSUE: All records coded as York County (67) instead of Philadelphia (91)")
                report['issues'].append({
                    'type': 'york_county_miscoding',
                    'description': 'All records incorrectly coded as York County',
                    'count': int(county_counts.iloc[0])
                })
                
                # Flag for correction
                df['county_miscoded'] = True
        
        report['county_distribution'] = {str(k): int(v) for k, v in county_counts.items()}
        
        self.quality_report['checks_performed'].append(report)
        
        return df, report
    
    def check_coordinate_precision(self, df: pd.DataFrame, lat_col: str = 'DEC_LAT', lon_col: str = 'DEC_LONG') -> Tuple[pd.DataFrame, dict]:
        """
        Check and standardize coordinate precision.
        
        Args:
            df: DataFrame with coordinates
            lat_col: Latitude column name
            lon_col: Longitude column name
            
        Returns:
            Tuple of (DataFrame with standardized precision, report dict)
        """
        logger.info("Checking coordinate precision")
        
        report = {
            'check': 'coordinate_precision',
            'total_records': len(df),
            'issues': []
        }
        
        if lat_col not in df.columns or lon_col not in df.columns:
            return df, report
        
        # Analyze precision (count decimal places)
        def count_decimals(value):
            if pd.isnull(value):
                return None
            s = str(value)
            if '.' in s:
                return len(s.split('.')[1])
            return 0
        
        lat_precision = df[lat_col].apply(count_decimals)
        lon_precision = df[lon_col].apply(count_decimals)
        
        logger.info(f"Latitude precision range: {lat_precision.min()}-{lat_precision.max()} decimal places")
        logger.info(f"Longitude precision range: {lon_precision.min()}-{lon_precision.max()} decimal places")
        
        # Standardize to 6 decimal places (~0.11 meters precision)
        df[f'{lat_col}_standardized'] = df[lat_col].round(6)
        df[f'{lon_col}_standardized'] = df[lon_col].round(6)
        
        report['precision_standardized'] = 6
        
        self.quality_report['checks_performed'].append(report)
        
        return df, report
    
    def check_date_consistency(self, df: pd.DataFrame, date_col: str = 'CRASH_DATE', year_col: str = 'CRASH_YEAR') -> Tuple[pd.DataFrame, dict]:
        """
        Check date/time consistency.
        
        Args:
            df: DataFrame
            date_col: Date column name
            year_col: Year column name
            
        Returns:
            Tuple of (DataFrame with flags, report dict)
        """
        logger.info("Checking date consistency")
        
        report = {
            'check': 'date_consistency',
            'total_records': len(df),
            'issues': []
        }
        
        if date_col not in df.columns:
            logger.warning(f"Date column not found: {date_col}")
            return df, report
        
        # Parse dates
        try:
            df[f'{date_col}_parsed'] = pd.to_datetime(df[date_col], errors='coerce')
            
            # Check for parsing failures
            parse_failures = df[f'{date_col}_parsed'].isnull().sum()
            if parse_failures > 0:
                logger.warning(f"Failed to parse {parse_failures} dates")
                report['issues'].append({
                    'type': 'parse_failure',
                    'count': int(parse_failures)
                })
            
            # If year column exists, check consistency
            if year_col in df.columns:
                df[f'{year_col}_from_date'] = df[f'{date_col}_parsed'].dt.year
                year_mismatch = (df[year_col] != df[f'{year_col}_from_date'])
                num_mismatch = year_mismatch.sum()
                
                if num_mismatch > 0:
                    logger.warning(f"Found {num_mismatch} records with year column mismatch")
                    report['issues'].append({
                        'type': 'year_mismatch',
                        'count': int(num_mismatch)
                    })
        
        except Exception as e:
            logger.error(f"Error checking dates: {e}")
            report['issues'].append({'type': 'error', 'message': str(e)})
        
        self.quality_report['checks_performed'].append(report)
        
        return df, report
    
    def check_categorical_consistency(self, df: pd.DataFrame, category_name: str) -> Tuple[pd.DataFrame, dict]:
        """
        Check consistency of categorical variables.
        
        Args:
            df: DataFrame
            category_name: Data category (e.g., 'CYCLE')
            
        Returns:
            Tuple of (DataFrame with flags, report dict)
        """
        logger.info(f"Checking categorical consistency for {category_name}")
        
        report = {
            'check': 'categorical_consistency',
            'category': category_name,
            'total_records': len(df),
            'issues': []
        }
        
        # Check helmet indicator for CYCLE data
        if category_name == 'CYCLE' and 'PC_HLMT_IND' in df.columns:
            helmet_values = df['PC_HLMT_IND'].value_counts(dropna=False)
            logger.info(f"Helmet indicator values: {helmet_values.to_dict()}")
            
            # Expected values: Y, N, U (or blank for unknown)
            valid_values = {'Y', 'N', 'U', '', np.nan}
            invalid = ~df['PC_HLMT_IND'].isin(valid_values)
            
            if invalid.any():
                logger.warning(f"Found {invalid.sum()} invalid helmet indicator values")
                report['issues'].append({
                    'type': 'invalid_helmet_values',
                    'count': int(invalid.sum()),
                    'unexpected_values': df.loc[invalid, 'PC_HLMT_IND'].unique().tolist()
                })
            
            # Standardize: blank to 'U'
            df['PC_HLMT_IND'] = df['PC_HLMT_IND'].replace('', 'U').fillna('U')
        
        self.quality_report['checks_performed'].append(report)
        
        return df, report
    
    def run_all_checks(self, df: pd.DataFrame, category: str) -> pd.DataFrame:
        """
        Run all quality checks on a DataFrame.
        
        Args:
            df: DataFrame to check
            category: Data category
            
        Returns:
            DataFrame with quality flags added
        """
        logger.info(f"Running all quality checks for {category}")
        
        # Run each check
        df, _ = self.check_geographic_bounds(df)
        df, _ = self.check_county_coding(df)
        df, _ = self.check_coordinate_precision(df)
        df, _ = self.check_date_consistency(df)
        df, _ = self.check_categorical_consistency(df, category)
        
        return df
    
    def generate_report(self) -> dict:
        """Generate comprehensive quality report."""
        logger.info("Generating quality assessment report")
        
        # Summarize issues
        summary = {
            'total_checks': len(self.quality_report['checks_performed']),
            'checks_with_issues': sum(1 for check in self.quality_report['checks_performed'] if check['issues']),
            'total_issues': sum(len(check['issues']) for check in self.quality_report['checks_performed'])
        }
        
        self.quality_report['summary'] = summary
        
        return self.quality_report


def main():
    """Main execution for testing."""
    logger.info("Quality Assessment Framework loaded")
    logger.info("This module is designed to be imported and used by other scripts")
    logger.info("Example usage:")
    logger.info("  from quality_checks import QualityChecker")
    logger.info("  checker = QualityChecker()")
    logger.info("  df_checked = checker.run_all_checks(df, 'CRASH')")


if __name__ == "__main__":
    main()
