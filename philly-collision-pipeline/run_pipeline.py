#!/usr/bin/env python
"""
Master Pipeline Execution Script

Orchestrates the complete Philadelphia Collision Data Curation Pipeline.
Runs all stages in sequence with error handling and progress reporting.

Usage:
    python run_pipeline.py                    # Run all stages
    python run_pipeline.py --stages 1,2       # Run specific stages
    python run_pipeline.py --test             # Test mode (single year)

Stages:
    1. Acquire - Download PennDOT and NOAA data
    2. Profile - Analyze schema changes
    3. Process - Quality checks and harmonization
    4. Integrate - Geographic filtering and weather merge
    5. Analyze - Create final datasets

Author: Arta Seyedian
Date: October 2025
"""

import sys
import argparse
from pathlib import Path
from datetime import datetime
import json

# Add scripts to path
sys.path.append(str(Path(__file__).parent / "scripts"))

from config import LOGS_DIR, METADATA_DIR
from utils.logging_utils import setup_logger

# Initialize logger
logger = setup_logger("run_pipeline")


class PipelineRunner:
    """Orchestrates the complete data curation pipeline."""
    
    def __init__(self, test_mode: bool = False):
        """
        Initialize pipeline runner.
        
        Args:
            test_mode: If True, run with limited data for testing
        """
        self.test_mode = test_mode
        self.start_time = datetime.now()
        self.results = {
            'start_time': self.start_time.isoformat(),
            'test_mode': test_mode,
            'stages': {}
        }
        
        logger.info("=" * 80)
        logger.info("PHILADELPHIA COLLISION DATA CURATION PIPELINE")
        logger.info("=" * 80)
        logger.info(f"Start time: {self.start_time}")
        logger.info(f"Test mode: {test_mode}")
        logger.info("=" * 80)
    
    def run_stage_1_acquire(self) -> bool:
        """
        Stage 1: Data Acquisition
        Download PennDOT crash data and NOAA weather data.
        """
        logger.info("\n" + "=" * 80)
        logger.info("STAGE 1: DATA ACQUISITION")
        logger.info("=" * 80)
        
        stage_start = datetime.now()
        
        try:
            # Import acquisition modules dynamically (folder names start with numbers)
            import importlib
            download_penndot = importlib.import_module('scripts.01_acquire.download_penndot')
            download_noaa = importlib.import_module('scripts.01_acquire.download_noaa')
            
            # Download PennDOT data
            logger.info("Downloading PennDOT crash data...")
            if self.test_mode:
                logger.info("TEST MODE: Downloading single year only")
                penndot_stats = download_penndot.download_all_penndot_data(years=[2023])
            else:
                penndot_stats = download_penndot.download_all_penndot_data()
            
            # Download NOAA data
            logger.info("Downloading NOAA weather data...")
            if self.test_mode:
                weather_df = download_noaa.download_all_weather_data(start_year=2023, end_year=2023)
            else:
                weather_df = download_noaa.download_all_weather_data()
            
            if weather_df is not None and not weather_df.empty:
                download_noaa.save_weather_data(weather_df)
            
            stage_end = datetime.now()
            duration = (stage_end - stage_start).total_seconds()
            
            self.results['stages']['1_acquire'] = {
                'status': 'success',
                'duration_seconds': duration,
                'penndot_stats': penndot_stats,
                'weather_records': len(weather_df) if weather_df is not None else 0
            }
            
            logger.info(f"Stage 1 completed in {duration:.0f} seconds")
            return True
            
        except Exception as e:
            logger.error(f"Stage 1 failed: {e}", exc_info=True)
            self.results['stages']['1_acquire'] = {
                'status': 'failed',
                'error': str(e)
            }
            return False
    
    def run_stage_2_profile(self) -> bool:
        """
        Stage 2: Data Profiling
        Analyze schema changes and data quality issues.
        """
        logger.info("\n" + "=" * 80)
        logger.info("STAGE 2: DATA PROFILING")
        logger.info("=" * 80)
        
        stage_start = datetime.now()
        
        try:
            # Import processing modules dynamically (folder names start with numbers)
            import importlib
            profile_data = importlib.import_module('scripts.02_process.profile_data')
            
            logger.info("Profiling data schemas...")
            report = profile_data.main()
            
            stage_end = datetime.now()
            duration = (stage_end - stage_start).total_seconds()
            
            self.results['stages']['2_profile'] = {
                'status': 'success',
                'duration_seconds': duration,
                'categories_profiled': report['summary']['total_categories'],
                'schema_issues': len(report['summary']['schema_issues'])
            }
            
            logger.info(f"Stage 2 completed in {duration:.0f} seconds")
            return True
            
        except Exception as e:
            logger.error(f"Stage 2 failed: {e}", exc_info=True)
            self.results['stages']['2_profile'] = {
                'status': 'failed',
                'error': str(e)
            }
            return False
    
    def run_stage_3_process(self) -> bool:
        """
        Stage 3: Data Processing
        Schema harmonization across years.
        """
        logger.info("\n" + "=" * 80)
        logger.info("STAGE 3: SCHEMA HARMONIZATION")
        logger.info("=" * 80)
        
        stage_start = datetime.now()
        
        try:
            # Import harmonization module dynamically
            import importlib
            harmonize_schema = importlib.import_module('scripts.02_process.harmonize_schema')
            
            logger.info("Harmonizing schemas across years...")
            if self.test_mode:
                results = harmonize_schema.main(years=[2023])
            else:
                results = harmonize_schema.main()
            
            stage_end = datetime.now()
            duration = (stage_end - stage_start).total_seconds()
            
            # Count successes
            success_count = sum(1 for r in results.values() if r['status'] == 'success')
            
            self.results['stages']['3_harmonize'] = {
                'status': 'success',
                'duration_seconds': duration,
                'categories_processed': len(results),
                'successful': success_count
            }
            
            logger.info(f"Stage 3 completed in {duration:.0f} seconds")
            return True
            
        except Exception as e:
            logger.error(f"Stage 3 failed: {e}", exc_info=True)
            self.results['stages']['3_harmonize'] = {
                'status': 'failed',
                'error': str(e)
            }
            return False
    
    def run_stage_4_integrate(self) -> bool:
        """
        Stage 4: Data Integration
        Geographic filtering and weather merging.
        """
        logger.info("\n" + "=" * 80)
        logger.info("STAGE 4: GEOGRAPHIC FILTERING & WEATHER INTEGRATION")
        logger.info("=" * 80)
        
        stage_start = datetime.now()
        
        try:
            # Import integration modules dynamically
            import importlib
            geographic_filter = importlib.import_module('scripts.03_integrate.geographic_filter')
            merge_weather = importlib.import_module('scripts.03_integrate.merge_weather')
            
            # Step 1: Geographic filtering
            logger.info("Step 1: Applying geographic filters...")
            geo_filter = geographic_filter.GeographicFilter()
            geo_df = geo_filter.process_category("CRASH")
            
            # Step 2: Weather integration  
            logger.info("\nStep 2: Merging weather data...")
            weather_integrator = merge_weather.WeatherCrashIntegrator()
            weather_df = weather_integrator.process_crash_category("CRASH")
            
            stage_end = datetime.now()
            duration = (stage_end - stage_start).total_seconds()
            
            self.results['stages']['4_integrate'] = {
                'status': 'success',
                'duration_seconds': duration,
                'geographic_records': len(geo_df) if geo_df is not None else 0,
                'weather_matched': weather_integrator.stats['crashes_matched']
            }
            
            logger.info(f"Stage 4 completed in {duration:.0f} seconds")
            return True
            
        except Exception as e:
            logger.error(f"Stage 4 failed: {e}", exc_info=True)
            self.results['stages']['4_integrate'] = {
                'status': 'failed',
                'error': str(e)
            }
            return False
    
    def run_stage_5_analyze(self) -> bool:
        """
        Stage 5: Create Analysis Datasets
        Generate final analysis-ready datasets.
        """
        logger.info("\n" + "=" * 80)
        logger.info("STAGE 5: CREATE ANALYSIS DATASETS")
        logger.info("=" * 80)
        
        stage_start = datetime.now()
        
        try:
            # Import dataset creation module dynamically
            import importlib
            create_datasets = importlib.import_module('scripts.04_analyze.create_datasets')
            
            logger.info("Creating specialized datasets...")
            results = create_datasets.main()
            
            stage_end = datetime.now()
            duration = (stage_end - stage_start).total_seconds()
            
            # Check if any datasets were successfully created
            success = any(r.get('status') == 'success' for r in results.values())
            
            self.results['stages']['5_analyze'] = {
                'status': 'success' if success else 'failed',
                'duration_seconds': duration,
                'datasets': results
            }
            
            logger.info(f"Stage 5 completed in {int(duration)} seconds")
            return success
            
        except Exception as e:
            logger.error(f"Stage 5 failed: {e}", exc_info=True)
            self.results['stages']['5_analyze'] = {
                'status': 'failed',
                'error': str(e)
            }
            return False
    
    def run_all(self, stages: list = None):
        """
        Run all or selected pipeline stages.
        
        Args:
            stages: List of stage numbers to run (default: all)
        """
        stage_functions = {
            1: self.run_stage_1_acquire,
            2: self.run_stage_2_profile,
            3: self.run_stage_3_process,
            4: self.run_stage_4_integrate,
            5: self.run_stage_5_analyze
        }
        
        if stages is None:
            stages = list(stage_functions.keys())
        
        for stage_num in stages:
            if stage_num not in stage_functions:
                logger.error(f"Invalid stage number: {stage_num}")
                continue
            
            success = stage_functions[stage_num]()
            
            if not success:
                logger.error(f"Stage {stage_num} failed. Stopping pipeline.")
                break
        
        # Finalize
        self.end_time = datetime.now()
        self.results['end_time'] = self.end_time.isoformat()
        self.results['total_duration_seconds'] = (self.end_time - self.start_time).total_seconds()
        
        # Save results
        self.save_results()
        
        # Print summary
        self.print_summary()
    
    def save_results(self):
        """Save pipeline execution results to JSON."""
        results_path = LOGS_DIR / f"pipeline_run_{self.start_time.strftime('%Y%m%d_%H%M%S')}.json"
        
        # Convert numpy types to Python types for JSON serialization
        def convert_types(obj):
            """Recursively convert numpy types to Python types."""
            if isinstance(obj, dict):
                return {k: convert_types(v) for k, v in obj.items()}
            elif isinstance(obj, list):
                return [convert_types(item) for item in obj]
            elif hasattr(obj, 'item'):  # numpy types have .item() method
                return obj.item()
            else:
                return obj
        
        results_json = convert_types(self.results)
        
        with open(results_path, 'w') as f:
            json.dump(results_json, f, indent=2)
        
        logger.info(f"Pipeline results saved to {results_path}")
    
    def print_summary(self):
        """Print pipeline execution summary."""
        logger.info("\n" + "=" * 80)
        logger.info("PIPELINE EXECUTION SUMMARY")
        logger.info("=" * 80)
        
        total_duration = self.results['total_duration_seconds']
        hours = int(total_duration // 3600)
        minutes = int((total_duration % 3600) // 60)
        seconds = int(total_duration % 60)
        
        logger.info(f"Total duration: {hours}h {minutes}m {seconds}s")
        logger.info("")
        
        for stage_name, stage_info in self.results['stages'].items():
            status = stage_info['status']
            duration = stage_info.get('duration_seconds', 0)
            
            status_emoji = {
                'success': '✓',
                'failed': '✗',
                'pending': '○'
            }.get(status, '?')
            
            logger.info(f"{status_emoji} {stage_name}: {status.upper()} ({duration:.0f}s)")
        
        logger.info("=" * 80)


def main():
    """Main entry point with argument parsing."""
    parser = argparse.ArgumentParser(
        description="Philadelphia Collision Data Curation Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    python run_pipeline.py                    # Run all stages
    python run_pipeline.py --stages 1,2       # Run stages 1 and 2 only
    python run_pipeline.py --test             # Test mode (single year)
    python run_pipeline.py --test --stages 1  # Test acquisition only
        """
    )
    
    parser.add_argument(
        '--stages',
        type=str,
        help='Comma-separated list of stages to run (e.g., "1,2,3")',
        default=None
    )
    
    parser.add_argument(
        '--test',
        action='store_true',
        help='Run in test mode (limited data for faster execution)'
    )
    
    args = parser.parse_args()
    
    # Parse stages
    stages = None
    if args.stages:
        try:
            stages = [int(s.strip()) for s in args.stages.split(',')]
        except ValueError:
            logger.error("Invalid stage numbers. Use comma-separated integers (e.g., '1,2,3')")
            return
    
    # Run pipeline
    runner = PipelineRunner(test_mode=args.test)
    runner.run_all(stages=stages)


if __name__ == "__main__":
    main()
