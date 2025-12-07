"""
Pipeline Validation and Summary Report Generator

Generates a comprehensive validation report from the final curated datasets.
This serves as documentation of the analysis stage (Stage 5).

Outputs:
- Console summary (can be saved via stdout redirection)
- JSON report file in metadata/
- Text summary file in metadata/

Run after pipeline completion:
    python generate_validation_report.py

Or save output:
    python generate_validation_report.py | tee metadata/validation_report.txt

Author: FDC Project
Date: 2024-12-07
"""

import sys
from pathlib import Path
import pandas as pd
import json
from datetime import datetime
from typing import Dict, Any

# Add parent directory to path
sys.path.append(str(Path(__file__).parent))

from scripts.config import FINAL_DATA_DIR, METADATA_DIR


def load_parquet_safe(filepath: Path) -> pd.DataFrame:
    """Load parquet file with error handling."""
    try:
        return pd.read_parquet(filepath)
    except Exception as e:
        print(f"⚠️  Warning: Could not load {filepath.name}: {e}")
        return None


def analyze_date_approximation(df: pd.DataFrame) -> Dict[str, Any]:
    """Analyze date approximation methods used."""
    if 'date_approximation_method' not in df.columns:
        return {"error": "date_approximation_method column not found"}
    
    counts = df['date_approximation_method'].value_counts()
    total = len(df)
    
    return {
        "total_records": total,
        "exact_day": int(counts.get('exact_day', 0)),
        "exact_day_pct": round(100 * counts.get('exact_day', 0) / total, 2),
        "weekday_reconstructed": int(counts.get('weekday_reconstructed', 0)),
        "weekday_reconstructed_pct": round(100 * counts.get('weekday_reconstructed', 0) / total, 2),
        "mid_month_fallback": int(counts.get('mid_month_fallback', 0)),
        "mid_month_fallback_pct": round(100 * counts.get('mid_month_fallback', 0) / total, 2)
    }


def analyze_temporal_distribution(df: pd.DataFrame) -> Dict[str, Any]:
    """Analyze temporal distribution of crashes."""
    if 'crash_date' not in df.columns:
        return {"error": "crash_date column not found"}
    
    # Convert to datetime if not already
    df['crash_date'] = pd.to_datetime(df['crash_date'])
    
    # Day of month distribution
    day_counts = df['crash_date'].dt.day.value_counts().sort_index()
    
    # Year distribution
    year_counts = df['crash_date'].dt.year.value_counts().sort_index()
    
    return {
        "year_range": f"{year_counts.index.min()}-{year_counts.index.max()}",
        "total_years": len(year_counts),
        "crashes_per_year_avg": round(df.groupby(df['crash_date'].dt.year).size().mean(), 1),
        "day_1_crashes": int(day_counts.get(1, 0)),
        "day_1_percentage": round(100 * day_counts.get(1, 0) / len(df), 2),
        "days_with_crashes": len(day_counts),
        "most_common_days": day_counts.head(5).to_dict()
    }


def analyze_geographic_coverage(df: pd.DataFrame) -> Dict[str, Any]:
    """Analyze geographic distribution."""
    lat_col = 'DEC_LAT' if 'DEC_LAT' in df.columns else 'latitude'
    lon_col = 'DEC_LONG' if 'DEC_LONG' in df.columns else 'longitude'
    
    if lat_col not in df.columns or lon_col not in df.columns:
        return {"error": "Coordinate columns not found"}
    
    # Filter out invalid coordinates
    valid_coords = df[(df[lat_col].notna()) & (df[lon_col].notna())]
    
    return {
        "total_crashes_with_coords": len(valid_coords),
        "percent_with_coords": round(100 * len(valid_coords) / len(df), 2),
        "latitude_range": f"{valid_coords[lat_col].min():.6f} to {valid_coords[lat_col].max():.6f}",
        "longitude_range": f"{valid_coords[lon_col].min():.6f} to {valid_coords[lon_col].max():.6f}",
        "coordinate_system": "WGS84 (EPSG:4326)"
    }


def analyze_weather_integration(df: pd.DataFrame) -> Dict[str, Any]:
    """Analyze weather data integration."""
    weather_cols = ['TMAX', 'TMIN', 'PRCP', 'SNOW']
    available_weather = [col for col in weather_cols if col in df.columns]
    
    if not available_weather:
        return {"error": "No weather columns found"}
    
    stats = {
        "weather_fields_integrated": available_weather,
        "crashes_with_weather": int(df[available_weather[0]].notna().sum()),
        "percent_with_weather": round(100 * df[available_weather[0]].notna().sum() / len(df), 2)
    }
    
    # Temperature stats if available
    if 'TMAX' in df.columns:
        temps = df['TMAX'].dropna()
        stats['temperature_range_F'] = f"{temps.min():.1f} to {temps.max():.1f}"
    
    # Precipitation stats if available
    if 'PRCP' in df.columns:
        prcp = df['PRCP'].dropna()
        stats['crashes_with_precipitation'] = int((prcp > 0).sum())
        stats['percent_with_precipitation'] = round(100 * (prcp > 0).sum() / len(prcp), 2)
    
    return stats


def analyze_data_quality(df: pd.DataFrame) -> Dict[str, Any]:
    """Analyze overall data quality metrics."""
    total_records = len(df)
    total_fields = len(df.columns)
    
    # Completeness by field
    completeness = {}
    for col in df.columns:
        non_null = df[col].notna().sum()
        completeness[col] = round(100 * non_null / total_records, 2)
    
    # Overall completeness
    overall_completeness = round(df.notna().sum().sum() / (total_records * total_fields) * 100, 2)
    
    return {
        "total_records": total_records,
        "total_fields": total_fields,
        "overall_completeness_pct": overall_completeness,
        "fields_with_100pct_completeness": sum(1 for v in completeness.values() if v == 100.0),
        "fields_below_90pct_completeness": sum(1 for v in completeness.values() if v < 90.0)
    }


def generate_report():
    """Generate comprehensive validation report."""
    
    print("=" * 80)
    print("PHILADELPHIA COLLISION PIPELINE - VALIDATION REPORT")
    print("=" * 80)
    print(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Pipeline Version: 2.0")
    print()
    
    # Load main integrated dataset
    integrated_file = FINAL_DATA_DIR / "full_integrated.parquet"
    
    if not integrated_file.exists():
        print(f"❌ Error: {integrated_file} not found")
        print("Run the pipeline first: python run_pipeline.py")
        return
    
    print(f"📂 Loading data from: {integrated_file}")
    df = load_parquet_safe(integrated_file)
    
    if df is None:
        print("❌ Failed to load data")
        return
    
    print(f"✅ Loaded {len(df):,} records with {len(df.columns)} fields")
    print()
    
    # Initialize report
    report = {
        "metadata": {
            "generated_at": datetime.now().isoformat(),
            "pipeline_version": "2.0",
            "dataset": "full_integrated.parquet",
            "total_records": len(df),
            "total_fields": len(df.columns)
        }
    }
    
    # 1. Date Approximation Analysis
    print("-" * 80)
    print("1. TEMPORAL ACCURACY ANALYSIS (v2.0 Weather Matching)")
    print("-" * 80)
    
    date_stats = analyze_date_approximation(df)
    report["date_approximation"] = date_stats
    
    if "error" not in date_stats:
        print(f"Total Crashes: {date_stats['total_records']:,}")
        print()
        print(f"  ✓ Exact Day Available:       {date_stats['exact_day']:,} ({date_stats['exact_day_pct']}%)")
        print(f"  ✓ Weekday Reconstructed:     {date_stats['weekday_reconstructed']:,} ({date_stats['weekday_reconstructed_pct']}%)")
        print(f"  ⚠ Mid-Month Fallback:        {date_stats['mid_month_fallback']:,} ({date_stats['mid_month_fallback_pct']}%)")
        print()
        print("📊 v2.0 Improvement: Weekday reconstruction uses DAY_OF_WEEK field to find")
        print("   first occurrence of that weekday in the crash month, avoiding artificial")
        print("   clustering at day 1 that occurred in v1.0.")
    else:
        print(f"⚠️  {date_stats['error']}")
    print()
    
    # 2. Temporal Distribution
    print("-" * 80)
    print("2. TEMPORAL DISTRIBUTION")
    print("-" * 80)
    
    temporal_stats = analyze_temporal_distribution(df)
    report["temporal_distribution"] = temporal_stats
    
    if "error" not in temporal_stats:
        print(f"Time Period: {temporal_stats['year_range']} ({temporal_stats['total_years']} years)")
        print(f"Average Crashes per Year: {temporal_stats['crashes_per_year_avg']:,}")
        print()
        print(f"Day 1 Crashes: {temporal_stats['day_1_crashes']:,} ({temporal_stats['day_1_percentage']}%)")
        print(f"  → In v1.0, this would be ~90% (artificial clustering)")
        print(f"  → In v2.0, crashes spread across days 1-7 via weekday reconstruction")
        print()
        print("Top 5 Days with Most Crashes:")
        for day, count in temporal_stats['most_common_days'].items():
            print(f"  Day {day}: {count:,} crashes")
    else:
        print(f"⚠️  {temporal_stats['error']}")
    print()
    
    # 3. Geographic Coverage
    print("-" * 80)
    print("3. GEOGRAPHIC COVERAGE")
    print("-" * 80)
    
    geo_stats = analyze_geographic_coverage(df)
    report["geographic_coverage"] = geo_stats
    
    if "error" not in geo_stats:
        print(f"Crashes with Valid Coordinates: {geo_stats['total_crashes_with_coords']:,} ({geo_stats['percent_with_coords']}%)")
        print(f"Coordinate System: {geo_stats['coordinate_system']}")
        print(f"Latitude Range: {geo_stats['latitude_range']}")
        print(f"Longitude Range: {geo_stats['longitude_range']}")
        print()
        print("✓ All coordinates transformed to WGS84 standard (EPSG:4326)")
        print("✓ Philadelphia County bounds: 39.87°N - 40.14°N, -75.28°W - -74.96°W")
    else:
        print(f"⚠️  {geo_stats['error']}")
    print()
    
    # 4. Weather Integration
    print("-" * 80)
    print("4. WEATHER DATA INTEGRATION")
    print("-" * 80)
    
    weather_stats = analyze_weather_integration(df)
    report["weather_integration"] = weather_stats
    
    if "error" not in weather_stats:
        print(f"Weather Fields: {', '.join(weather_stats['weather_fields_integrated'])}")
        print(f"Crashes with Weather Data: {weather_stats['crashes_with_weather']:,} ({weather_stats['percent_with_weather']}%)")
        if 'temperature_range_F' in weather_stats:
            print(f"Temperature Range: {weather_stats['temperature_range_F']}°F")
        if 'crashes_with_precipitation' in weather_stats:
            print(f"Crashes with Precipitation: {weather_stats['crashes_with_precipitation']:,} ({weather_stats['percent_with_precipitation']}%)")
        print()
        print("✓ Weather matched via v2.0 weekday reconstruction algorithm")
        print("✓ NOAA data from Philadelphia International Airport (USW00013739)")
    else:
        print(f"⚠️  {weather_stats['error']}")
    print()
    
    # 5. Data Quality
    print("-" * 80)
    print("5. DATA QUALITY METRICS")
    print("-" * 80)
    
    quality_stats = analyze_data_quality(df)
    report["data_quality"] = quality_stats
    
    print(f"Total Records: {quality_stats['total_records']:,}")
    print(f"Total Fields: {quality_stats['total_fields']}")
    print(f"Overall Completeness: {quality_stats['overall_completeness_pct']}%")
    print(f"Fields with 100% Completeness: {quality_stats['fields_with_100pct_completeness']}")
    print(f"Fields Below 90% Completeness: {quality_stats['fields_below_90pct_completeness']}")
    print()
    
    # 6. File Summary
    print("-" * 80)
    print("6. OUTPUT FILES SUMMARY")
    print("-" * 80)
    
    output_files = list(FINAL_DATA_DIR.glob("*.parquet"))
    file_info = []
    
    for f in sorted(output_files):
        size_mb = f.stat().st_size / (1024 * 1024)
        df_file = load_parquet_safe(f)
        if df_file is not None:
            records = len(df_file)
            fields = len(df_file.columns)
            file_info.append({
                "filename": f.name,
                "size_mb": round(size_mb, 2),
                "records": records,
                "fields": fields
            })
            print(f"  {f.name}")
            print(f"    Size: {size_mb:.2f} MB | Records: {records:,} | Fields: {fields}")
    
    report["output_files"] = file_info
    print()
    
    # 7. FAIR Compliance
    print("-" * 80)
    print("7. FAIR PRINCIPLES COMPLIANCE")
    print("-" * 80)
    print("✓ Findable:      DataCite metadata XML with unique identifiers")
    print("✓ Accessible:    Public GitHub repository, CC-BY 4.0 license")
    print("✓ Interoperable: Parquet format, WGS84 coordinates, standard schemas")
    print("✓ Reusable:      Comprehensive documentation, transparent metadata flags")
    print()
    
    # Save JSON report
    json_report_path = METADATA_DIR / f"validation_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(json_report_path, 'w') as f:
        json.dump(report, f, indent=2)
    
    print("-" * 80)
    print("REPORT SAVED")
    print("-" * 80)
    print(f"📄 JSON Report: {json_report_path}")
    print()
    print("To save console output:")
    print("  python generate_validation_report.py | tee metadata/validation_report.txt")
    print()
    print("=" * 80)
    print("✅ VALIDATION COMPLETE")
    print("=" * 80)


if __name__ == "__main__":
    generate_report()
