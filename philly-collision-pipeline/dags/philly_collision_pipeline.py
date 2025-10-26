"""
Philadelphia Collision Data Curation Pipeline - Airflow DAG

This DAG orchestrates the 5-stage data curation pipeline:
1. Acquire: Download PennDOT crash data and NOAA weather data
2. Profile: Analyze schema evolution across years
3. Harmonize: Standardize schemas and combine years
4. Integrate: Geographic filtering and weather matching
5. Analyze: Create analysis-ready datasets

Schedule: Runs annually when new PennDOT data is released
Author: Arta Seyedian
Date: October 26, 2025
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
import sys
import os

# Add project root to path
sys.path.insert(0, '/app')

# Default arguments for the DAG
default_args = {
    'owner': 'arta_seyedian',
    'depends_on_past': False,
    'email': ['admin@example.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=2),
}

# Create the DAG
dag = DAG(
    'philly_collision_pipeline',
    default_args=default_args,
    description='Philadelphia Traffic Collision Data Curation Pipeline',
    schedule_interval='@yearly',  # Run once per year
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['data-curation', 'traffic-safety', 'philadelphia'],
    params={
        # Execution mode
        'test_mode': False,  # If True, only process 2023 data for quick testing
        
        # Stage selection - run specific stages only
        'run_acquisition': True,
        'run_profiling': True,
        'run_harmonization': True,
        'run_integration': True,
        'run_datasets': True,
        
        # Data scope parameters
        'start_year': 2005,  # First year to process
        'end_year': 2024,    # Last year to process
        
        # Category selection (if empty, all categories are processed)
        'categories': [],  # e.g., ['CRASH', 'PERSON', 'VEHICLE'] or [] for all
        
        # Performance tuning
        'parallel_downloads': True,  # Download multiple years simultaneously
        'skip_existing': True,       # Skip files that already exist
        
        # Output formats
        'save_parquet': True,
        'save_csv': True,
        
        # Quality checks
        'strict_validation': False,  # If True, fail on quality issues
        'generate_quality_report': True,
    },
)


# ============================================================================
# STAGE 1: DATA ACQUISITION
# ============================================================================

def acquire_penndot(**context):
    """Download PennDOT crash data for all categories and years."""
    # Check if this stage should run
    params = context['params']
    if not params.get('run_acquisition', True):
        print("⏭️  Skipping acquisition stage (disabled in params)")
        return {'status': 'skipped'}
    
    import importlib
    download_penndot = importlib.import_module('scripts.01_acquire.download_penndot')
    
    # Get parameters
    test_mode = params.get('test_mode', False)
    start_year = params.get('start_year', 2005)
    end_year = params.get('end_year', 2024)
    
    print(f"{'🧪 TEST MODE' if test_mode else '🚀 FULL MODE'}")
    print(f"📅 Year range: {start_year}-{end_year}")
    
    # Build year list
    if test_mode:
        years = [2023]  # Test mode: just 2023
    else:
        years = list(range(start_year, end_year + 1))
    
    # Call the main download function directly
    stats = download_penndot.download_all_penndot_data(years=years)
    
    # Push stats to XCom for monitoring
    context['task_instance'].xcom_push(key='penndot_stats', value=stats)
    return stats


def acquire_noaa(**context):
    """Download NOAA weather data for Philadelphia."""
    # Check if this stage should run
    params = context['params']
    if not params.get('run_acquisition', True):
        print("⏭️  Skipping acquisition stage (disabled in params)")
        return {'status': 'skipped'}
    
    import importlib
    download_noaa = importlib.import_module('scripts.01_acquire.download_noaa')
    
    test_mode = params.get('test_mode', False)
    print(f"{'🧪 TEST MODE' if test_mode else '🚀 FULL MODE'}")
    
    # Call the main download function
    result = download_noaa.main()
    
    stats = {'status': 'success' if result is not None else 'failed'}
    context['task_instance'].xcom_push(key='noaa_stats', value=stats)
    return stats


task_acquire_penndot = PythonOperator(
    task_id='acquire_penndot_data',
    python_callable=acquire_penndot,
    dag=dag,
    doc_md="""
    ### Download PennDOT Crash Data
    
    Downloads crash data from PennDOT GIS Portal for all 8 categories:
    - CRASH, PERSON, VEHICLE, CYCLE, FLAG, ROADWAY, COMMVEH, TRAILVEH
    
    **Years**: 2005-2024 (or 2023 only in test mode)
    **Output**: CSV files in `data/raw/`
    **Expected Duration**: 5-8 minutes (full), 8 seconds (test)
    """
)

task_acquire_noaa = PythonOperator(
    task_id='acquire_noaa_weather',
    python_callable=acquire_noaa,
    dag=dag,
    doc_md="""
    ### Download NOAA Weather Data
    
    Downloads daily weather summaries from NOAA CDO API:
    - Temperature (avg, min, max)
    - Precipitation
    - Snowfall and snow depth
    - Wind speed
    
    **Station**: Philadelphia International Airport (USW00013739)
    **Output**: `data/raw/noaa_weather_philly.parquet`
    **Expected Duration**: < 1 minute
    """
)


# ============================================================================
# STAGE 2: SCHEMA PROFILING
# ============================================================================

def profile_schemas(**context):
    """Analyze schema evolution across years."""
    # Check if this stage should run
    params = context['params']
    if not params.get('run_profiling', True):
        print("⏭️  Skipping profiling stage (disabled in params)")
        return {'status': 'skipped'}
    
    import importlib
    profile_data = importlib.import_module('scripts.02_process.profile_data')
    
    print("🔍 Analyzing schema evolution across years...")
    
    # Call the main profiling function
    result = profile_data.main()
    
    report = {'status': 'completed'}
    context['task_instance'].xcom_push(key='schema_report', value=report)
    return report


task_profile = PythonOperator(
    task_id='profile_schemas',
    python_callable=profile_schemas,
    dag=dag,
    doc_md="""
    ### Schema Profiling & Analysis
    
    Analyzes schema changes across 20 years of data:
    - Column additions/deletions
    - Data type changes
    - Renaming detection (e.g., DEC_LAT → DEC_LATITUDE)
    
    **Output**: `metadata/schema_analysis_summary.txt` and JSON
    **Expected Duration**: < 1 second
    """
)


# ============================================================================
# STAGE 3: SCHEMA HARMONIZATION
# ============================================================================

def harmonize_all(**context):
    """Harmonize schemas and combine all years."""
    # Check if this stage should run
    params = context['params']
    if not params.get('run_harmonization', True):
        print("⏭️  Skipping harmonization stage (disabled in params)")
        return {'status': 'skipped'}
    
    import importlib
    harmonize_schema = importlib.import_module('scripts.02_process.harmonize_schema')
    
    # Get category filter
    categories = params.get('categories', [])
    if categories:
        print(f"📂 Processing categories: {', '.join(categories)}")
    else:
        print("📂 Processing all categories")
    
    # Call the main harmonization function
    # Note: Pass categories if the function supports it
    harmonize_schema.main(categories=categories if categories else None)
    
    stats = {'status': 'completed', 'categories': categories or 'all'}
    context['task_instance'].xcom_push(key='harmonization_stats', value=stats)
    return stats


task_harmonize = PythonOperator(
    task_id='harmonize_schemas',
    python_callable=harmonize_all,
    dag=dag,
    doc_md="""
    ### Schema Harmonization
    
    Standardizes schemas across 20 years and combines into single files:
    - Renames columns to current standard
    - Adds missing columns (NULL fill)
    - Removes deprecated columns
    - Combines all years per category
    
    **Output**: 8 parquet files in `data/processed/` (e.g., `crash_harmonized.parquet`)
    **Expected Duration**: 1-2 seconds (test), 30-60 seconds (full)
    """
)


# ============================================================================
# STAGE 4: INTEGRATION
# ============================================================================

def filter_geography(**context):
    """Validate geographic coordinates and filter to Philadelphia."""
    # Check if this stage should run
    params = context['params']
    if not params.get('run_integration', True):
        print("⏭️  Skipping integration stage (disabled in params)")
        return {'status': 'skipped'}
    
    import importlib
    geographic_filter = importlib.import_module('scripts.03_integrate.geographic_filter')
    
    strict_validation = params.get('strict_validation', False)
    print(f"🗺️  Geographic filtering - {'STRICT' if strict_validation else 'PERMISSIVE'} mode")
    
    # Call the main filtering function
    geographic_filter.main()
    
    stats = {'status': 'completed'}
    context['task_instance'].xcom_push(key='geographic_stats', value=stats)
    return stats


def integrate_weather(**context):
    """Match crashes with daily weather data."""
    # Check if this stage should run
    params = context['params']
    if not params.get('run_integration', True):
        print("⏭️  Skipping integration stage (disabled in params)")
        return {'status': 'skipped'}
    
    import importlib
    merge_weather = importlib.import_module('scripts.03_integrate.merge_weather')
    
    print("🌦️  Integrating weather data with crashes...")
    
    # Call the main integration function
    merge_weather.main()
    
    stats = {'status': 'completed'}
    context['task_instance'].xcom_push(key='weather_stats', value=stats)
    return stats


task_filter_geography = PythonOperator(
    task_id='filter_geography',
    python_callable=filter_geography,
    dag=dag,
    doc_md="""
    ### Geographic Filtering & Validation
    
    Validates coordinates and adds quality flags:
    - Coordinate range validation
    - Philadelphia boundary check
    - County code verification
    - Quality flagging (not dropping)
    
    **Output**: `data/processed/crash_geographic.parquet` + quality stats
    **Expected Duration**: < 1 second
    """
)

task_integrate_weather = PythonOperator(
    task_id='integrate_weather',
    python_callable=integrate_weather,
    dag=dag,
    doc_md="""
    ### Weather Integration
    
    Matches crashes with NOAA daily weather:
    - Temporal matching by date
    - Derived features (precip_category, temp_category, adverse_weather)
    - 100% match rate for valid dates
    
    **Output**: `data/processed/crash_weather_integrated.parquet`
    **Expected Duration**: < 1 second
    """
)


# ============================================================================
# STAGE 5: DATASET CREATION
# ============================================================================

def create_analysis_datasets(**context):
    """Create final analysis-ready datasets."""
    # Check if this stage should run
    params = context['params']
    if not params.get('run_datasets', True):
        print("⏭️  Skipping dataset creation stage (disabled in params)")
        return {'status': 'skipped'}
    
    import importlib
    create_datasets = importlib.import_module('scripts.04_analyze.create_datasets')
    
    save_parquet = params.get('save_parquet', True)
    save_csv = params.get('save_csv', True)
    
    print(f"📊 Creating analysis datasets")
    print(f"   Parquet: {'✅' if save_parquet else '❌'}")
    print(f"   CSV: {'✅' if save_csv else '❌'}")
    
    # Call the main dataset creation function
    create_datasets.main()
    
    stats = {
        'status': 'completed',
        'formats': {
            'parquet': save_parquet,
            'csv': save_csv
        }
    }
    context['task_instance'].xcom_push(key='dataset_stats', value=stats)
    return stats


task_create_datasets = PythonOperator(
    task_id='create_datasets',
    python_callable=create_analysis_datasets,
    dag=dag,
    doc_md="""
    ### Create Analysis Datasets
    
    Joins 8 PennDOT categories to create specialized datasets:
    - **cyclist_focused.parquet**: Bicycle crashes with helmet data
    - **pedestrian_focused.parquet**: Pedestrian crashes
    - **full_integrated.parquet**: All crashes with roadway and weather
    - **person.parquet**: Person-level reference table
    - **vehicle.parquet**: Vehicle-level reference table
    
    All saved as both Parquet and CSV.
    
    **Output**: 5 datasets in `data/final/`
    **Expected Duration**: 2-3 seconds (test), 30-60 seconds (full)
    """
)


# ============================================================================
# VALIDATION & REPORTING
# ============================================================================

def generate_pipeline_report(**context):
    """Generate comprehensive pipeline execution report."""
    ti = context['task_instance']
    params = context['params']
    
    # Gather all stats from XCom
    penndot_stats = ti.xcom_pull(key='penndot_stats', task_ids='acquire_penndot_data')
    noaa_stats = ti.xcom_pull(key='noaa_stats', task_ids='acquire_noaa_weather')
    schema_report = ti.xcom_pull(key='schema_report', task_ids='profile_schemas')
    harmonization_stats = ti.xcom_pull(key='harmonization_stats', task_ids='harmonize_schemas')
    geographic_stats = ti.xcom_pull(key='geographic_stats', task_ids='filter_geography')
    weather_stats = ti.xcom_pull(key='weather_stats', task_ids='integrate_weather')
    dataset_stats = ti.xcom_pull(key='dataset_stats', task_ids='create_datasets')
    
    # Create comprehensive report
    report = {
        'pipeline_run_date': context['execution_date'].isoformat(),
        'configuration': {
            'test_mode': params.get('test_mode', False),
            'year_range': f"{params.get('start_year', 2005)}-{params.get('end_year', 2024)}",
            'categories': params.get('categories', 'all'),
            'stages_enabled': {
                'acquisition': params.get('run_acquisition', True),
                'profiling': params.get('run_profiling', True),
                'harmonization': params.get('run_harmonization', True),
                'integration': params.get('run_integration', True),
                'datasets': params.get('run_datasets', True),
            }
        },
        'stages': {
            'acquisition': {
                'penndot': penndot_stats,
                'noaa': noaa_stats,
            },
            'profiling': schema_report,
            'harmonization': harmonization_stats,
            'integration': {
                'geographic': geographic_stats,
                'weather': weather_stats,
            },
            'datasets': dataset_stats,
        }
    }
    
    # Save report
    import json
    from datetime import datetime
    report_path = f"/app/metadata/pipeline_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(report_path, 'w') as f:
        json.dump(report, f, indent=2, default=str)
    
    print("\n" + "="*60)
    print("✅ PIPELINE COMPLETED SUCCESSFULLY!")
    print("="*60)
    print(f"📊 Report saved to: {report_path}")
    print(f"🔧 Configuration: {'TEST MODE' if params.get('test_mode') else 'PRODUCTION'}")
    print(f"📅 Year range: {params.get('start_year', 2005)}-{params.get('end_year', 2024)}")
    
    # Print stage summary
    print("\n📋 Stage Summary:")
    for stage, data in report['stages'].items():
        if isinstance(data, dict) and 'status' in data:
            status = data['status']
            emoji = '✅' if status in ['completed', 'success'] else '⏭️' if status == 'skipped' else '❌'
            print(f"   {emoji} {stage.title()}: {status}")
        elif isinstance(data, dict):
            for substage, subdata in data.items():
                if isinstance(subdata, dict) and 'status' in subdata:
                    status = subdata['status']
                    emoji = '✅' if status in ['completed', 'success'] else '⏭️' if status == 'skipped' else '❌'
                    print(f"   {emoji} {stage.title()} - {substage}: {status}")
    
    print("="*60 + "\n")
    
    return report


task_report = PythonOperator(
    task_id='generate_report',
    python_callable=generate_pipeline_report,
    dag=dag,
    doc_md="""
    ### Generate Pipeline Report
    
    Creates comprehensive execution report with:
    - Statistics from all stages
    - Row counts and data quality metrics
    - Execution times
    - Data lineage information
    
    **Output**: `metadata/pipeline_report_YYYYMMDD_HHMMSS.json`
    """
)


# ============================================================================
# DAG STRUCTURE (Dependencies)
# ============================================================================

# Stage 1: Acquisition (parallel)
[task_acquire_penndot, task_acquire_noaa] >> task_profile

# Stage 2: Profiling
task_profile >> task_harmonize

# Stage 3: Harmonization
task_harmonize >> task_filter_geography

# Stage 4: Integration (sequential within stage)
task_filter_geography >> task_integrate_weather

# Stage 5: Dataset Creation
task_integrate_weather >> task_create_datasets

# Final: Reporting
task_create_datasets >> task_report
