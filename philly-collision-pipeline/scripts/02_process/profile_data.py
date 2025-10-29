"""
Data Profiling and Schema Analysis Script

Analyzes PennDOT crash data across all years to identify:
- Schema changes (column additions/removals)
- Data type inconsistencies
- Missing data patterns
- Value range changes

Creates a comprehensive schema comparison matrix to guide harmonization.

This addresses the known issue of schema drift across years that was
identified in the R analysis with the handle_mismatch() function.

Author: Arta Seyedian
Date: October 2025
"""

import pandas as pd
import numpy as np
from pathlib import Path
from typing import Dict, List, Tuple
import json
from collections import defaultdict
import glob

import sys
sys.path.append(str(Path(__file__).parent.parent))

from config import (
    RAW_DATA_DIR,
    PROCESSED_DATA_DIR,
    METADATA_DIR,
    PENNDOT_CATEGORIES,
    YEARS
)
from utils.logging_utils import setup_logger, log_dataframe_info


logger = setup_logger("profile_data")


class DataProfiler:
    """Profiles crash data to understand schema evolution and quality issues."""
    
    def __init__(self):
        """Initialize the data profiler."""
        self.schema_info = defaultdict(lambda: defaultdict(dict))
        self.profiles = {}
        
    def find_csv_files(self, category: str) -> List[Path]:
        """
        Find all CSV files for a given category.
        
        Args:
            category: Data category (e.g., 'CRASH', 'CYCLE')
            
        Returns:
            List of file paths
        """
        pattern = str(RAW_DATA_DIR / f"{category}*.csv")
        files = glob.glob(pattern)
        
        logger.info(f"Found {len(files)} files for category {category}")
        return [Path(f) for f in sorted(files)]
    
    def extract_year_from_filename(self, filepath: Path) -> int:
        """
        Extract year from filename.
        
        Args:
            filepath: Path to CSV file
            
        Returns:
            Year as integer
        """
        # Filenames typically like: CRASH_2020.csv
        try:
            for year in YEARS:
                if str(year) in filepath.name:
                    return year
        except:
            pass
        
        logger.warning(f"Could not extract year from {filepath.name}")
        return None
    
    def profile_file(self, filepath: Path, category: str) -> dict:
        """
        Profile a single CSV file.
        
        Args:
            filepath: Path to CSV file
            category: Data category
            
        Returns:
            Dictionary with profiling information
        """
        year = self.extract_year_from_filename(filepath)
        
        logger.info(f"Profiling {category} for year {year}: {filepath.name}")
        
        try:
            # Read first few rows to get schema
            df = pd.read_csv(filepath, nrows=1000)
            
            profile = {
                'year': year,
                'category': category,
                'filename': filepath.name,
                'file_size_mb': filepath.stat().st_size / (1024 * 1024),
                'columns': list(df.columns),
                'num_columns': len(df.columns),
                'dtypes': {col: str(dtype) for col, dtype in df.dtypes.items()},
                'sample_rows': len(df)
            }
            
            # Add column-level statistics (from sample)
            column_stats = {}
            for col in df.columns:
                stats = {
                    'dtype': str(df[col].dtype),
                    'null_count': int(df[col].isnull().sum()),
                    'null_pct': float(df[col].isnull().sum() / len(df) * 100),
                    'unique_count': int(df[col].nunique()),
                }
                
                # Add sample values for categorical-looking columns
                if df[col].nunique() < 20:
                    stats['sample_values'] = df[col].value_counts().head(10).to_dict()
                
                # Add range for numeric columns
                if pd.api.types.is_numeric_dtype(df[col]):
                    stats['min'] = float(df[col].min()) if not df[col].isnull().all() else None
                    stats['max'] = float(df[col].max()) if not df[col].isnull().all() else None
                
                column_stats[col] = stats
            
            profile['column_stats'] = column_stats
            
            logger.info(f"Profiled {filepath.name}: {profile['num_columns']} columns, {profile['sample_rows']} sample rows")
            
            return profile
            
        except Exception as e:
            logger.error(f"Error profiling {filepath}: {e}")
            return None
    
    def profile_category(self, category: str) -> List[dict]:
        """
        Profile all years for a data category.
        
        Args:
            category: Data category to profile
            
        Returns:
            List of profile dictionaries
        """
        logger.info(f"=" * 60)
        logger.info(f"Profiling category: {category}")
        logger.info(f"=" * 60)
        
        csv_files = self.find_csv_files(category)
        
        if not csv_files:
            logger.warning(f"No CSV files found for category {category}")
            return []
        
        profiles = []
        for filepath in csv_files:
            profile = self.profile_file(filepath, category)
            if profile:
                profiles.append(profile)
        
        self.profiles[category] = profiles
        return profiles
    
    def compare_schemas(self, category: str) -> dict:
        """
        Compare schemas across years for a category.
        
        Args:
            category: Data category
            
        Returns:
            Schema comparison dictionary
        """
        if category not in self.profiles:
            logger.warning(f"No profiles found for {category}")
            return {}
        
        profiles = self.profiles[category]
        
        # Track all columns seen
        all_columns = set()
        year_columns = {}
        year_dtypes = {}
        
        for profile in profiles:
            year = profile['year']
            cols = set(profile['columns'])
            all_columns.update(cols)
            year_columns[year] = cols
            year_dtypes[year] = profile['dtypes']
        
        # Build comparison matrix
        comparison = {
            'category': category,
            'all_columns': sorted(all_columns),
            'num_unique_columns': len(all_columns),
            'year_range': f"{min(year_columns.keys())}-{max(year_columns.keys())}",
            'columns_by_year': {},
            'dtype_changes': {},
            'added_columns': {},
            'removed_columns': {},
        }
        
        # Track column presence by year
        for col in sorted(all_columns):
            comparison['columns_by_year'][col] = {
                year: col in year_columns[year]
                for year in sorted(year_columns.keys())
            }
        
        # Track dtype changes
        for col in sorted(all_columns):
            dtypes_by_year = {}
            for year in sorted(year_columns.keys()):
                if col in year_dtypes[year]:
                    dtypes_by_year[year] = year_dtypes[year][col]
            
            # Check if dtypes changed
            unique_dtypes = set(dtypes_by_year.values())
            if len(unique_dtypes) > 1:
                comparison['dtype_changes'][col] = dtypes_by_year
        
        # Identify added/removed columns
        years_sorted = sorted(year_columns.keys())
        for col in sorted(all_columns):
            first_year = None
            last_year = None
            
            for year in years_sorted:
                if col in year_columns[year]:
                    if first_year is None:
                        first_year = year
                    last_year = year
            
            if first_year != years_sorted[0]:
                comparison['added_columns'][col] = first_year
            
            if last_year != years_sorted[-1]:
                comparison['removed_columns'][col] = last_year
        
        return comparison
    
    def generate_report(self) -> dict:
        """
        Generate comprehensive profiling report.
        
        Returns:
            Report dictionary
        """
        logger.info("Generating comprehensive profiling report")
        
        report = {
            'categories': {},
            'summary': {
                'total_categories': len(self.profiles),
                'total_files': sum(len(profiles) for profiles in self.profiles.values()),
                'schema_issues': []
            }
        }
        
        for category in self.profiles.keys():
            comparison = self.compare_schemas(category)
            report['categories'][category] = comparison
            
            # Add to summary issues
            if comparison['dtype_changes']:
                report['summary']['schema_issues'].append({
                    'category': category,
                    'issue': 'dtype_changes',
                    'affected_columns': list(comparison['dtype_changes'].keys())
                })
            
            if comparison['added_columns']:
                report['summary']['schema_issues'].append({
                    'category': category,
                    'issue': 'added_columns',
                    'columns': list(comparison['added_columns'].keys())
                })
            
            if comparison['removed_columns']:
                report['summary']['schema_issues'].append({
                    'category': category,
                    'issue': 'removed_columns',
                    'columns': list(comparison['removed_columns'].keys())
                })
        
        return report
    
    def save_report(self, report: dict, filename: str = "schema_analysis_report.json"):
        """
        Save profiling report to file.
        
        Args:
            report: Report dictionary
            filename: Output filename
        """
        output_path = METADATA_DIR / filename
        
        logger.info(f"Saving report to {output_path}")
        
        with open(output_path, 'w') as f:
            json.dump(report, f, indent=2)
        
        logger.info(f"Report saved successfully")
        
        # Also create a human-readable summary
        summary_path = METADATA_DIR / "schema_analysis_summary.txt"
        with open(summary_path, 'w') as f:
            f.write("=" * 80 + "\n")
            f.write("PENNDOT CRASH DATA SCHEMA ANALYSIS SUMMARY\n")
            f.write("=" * 80 + "\n\n")
            
            f.write(f"Total Categories Analyzed: {report['summary']['total_categories']}\n")
            f.write(f"Total Files Profiled: {report['summary']['total_files']}\n\n")
            
            f.write("SCHEMA ISSUES FOUND:\n")
            f.write("-" * 80 + "\n")
            
            for issue in report['summary']['schema_issues']:
                f.write(f"\nCategory: {issue['category']}\n")
                f.write(f"Issue Type: {issue['issue']}\n")
                
                if 'affected_columns' in issue:
                    f.write(f"Affected Columns: {', '.join(issue['affected_columns'])}\n")
                elif 'columns' in issue:
                    f.write(f"Columns: {', '.join(issue['columns'])}\n")
        
        logger.info(f"Summary saved to {summary_path}")


def main():
    """Main execution function."""
    logger.info("Data Profiling Script Started")
    
    profiler = DataProfiler()
    
    # Profile each category
    for category in PENNDOT_CATEGORIES:
        profiler.profile_category(category)
    
    # Generate and save report
    report = profiler.generate_report()
    profiler.save_report(report)
    
    # Log summary
    logger.info("=" * 60)
    logger.info("PROFILING SUMMARY")
    logger.info("=" * 60)
    logger.info(f"Categories profiled: {report['summary']['total_categories']}")
    logger.info(f"Total files: {report['summary']['total_files']}")
    logger.info(f"Schema issues found: {len(report['summary']['schema_issues'])}")
    
    if report['summary']['schema_issues']:
        logger.warning("Schema issues detected - harmonization will be required")
    else:
        logger.info("No schema issues detected")
    
    logger.info("Data Profiling Script Completed")
    
    return report


if __name__ == "__main__":
    main()
