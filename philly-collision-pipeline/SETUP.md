# Philadelphia Collision Pipeline - Setup Guide

## Complete Installation and Setup Instructions

### System Requirements

- **Operating System**: macOS, Linux, or Windows 10+
- **Python**: 3.9 or higher
- **Memory**: 32GB RAM recommended (minimum 16GB)
- **Storage**: 50GB free disk space
- **Internet**: Required for data downloads

---

## Step-by-Step Setup

### 1. Install Python

**macOS** (using Homebrew):
```bash
brew install python@3.11
```

**Linux** (Ubuntu/Debian):
```bash
sudo apt update
sudo apt install python3.11 python3.11-venv python3-pip
```

**Windows**:
Download from [python.org](https://www.python.org/downloads/) and install.

Verify installation:
```bash
python --version  # Should show 3.9 or higher
```

### 2. Clone Repository

```bash
cd ~/githubProjects  # or your preferred location
git clone https://github.com/aseyedia/FDC-Project.git
cd FDC-Project/philly-collision-pipeline
```

### 3. Create Virtual Environment

**macOS/Linux**:
```bash
python3 -m venv venv
source venv/bin/activate
```

**Windows**:
```cmd
python -m venv venv
.\venv\Scripts\activate
```

Your prompt should now show `(venv)` prefix.

### 4. Install Dependencies

```bash
pip install --upgrade pip
pip install -r requirements.txt
```

This installs:
- Data processing: pandas, numpy, pyarrow
- Geographic operations: geopandas, shapely
- Quality assessment: great-expectations, pandera
- API access: requests, noaa-sdk
- Logging: loguru
- Testing: pytest

Installation may take 5-10 minutes.

### 5. Configure Environment Variables

```bash
cp .env.example .env
```

Edit `.env` file:
```bash
# Get NOAA API token from https://www.ncdc.noaa.gov/cdo-web/token
NOAA_API_TOKEN=YOUR_TOKEN_HERE

# Optional: Adjust year range
START_YEAR=2005
END_YEAR=2024
```

**Getting NOAA API Token**:
1. Visit https://www.ncdc.noaa.gov/cdo-web/token
2. Enter your email address
3. Check email for token (usually arrives in minutes)
4. Copy token to `.env` file

### 6. Create Required Directories

```bash
mkdir -p logs
```

The pipeline will create other directories as needed.

### 7. Verify Setup

Test imports:
```bash
python -c "import pandas; import geopandas; print('Setup successful!')"
```

---

## Running the Pipeline

### Full Pipeline (Recommended for First Run)

Run all stages in sequence:

```bash
# 1. Download PennDOT data (~2-3 hours, ~10GB)
python scripts/01_acquire/download_penndot.py

# 2. Download NOAA weather data (~30 minutes)
python scripts/01_acquire/download_noaa.py

# 3. Profile data to understand schema changes
python scripts/02_process/profile_data.py

# 4. Process and harmonize schemas
python scripts/02_process/harmonize_schema.py

# 5. Apply geographic filtering
python scripts/03_integrate/geographic_filter.py

# 6. Integrate weather data
python scripts/03_integrate/merge_weather.py

# 7. Create final analysis-ready datasets
python scripts/04_analyze/create_datasets.py
```

### Individual Stages

You can run stages individually if you already have data:

**Download only one year** (for testing):
```python
# Edit download_penndot.py, line ~275:
# Change: download_all_penndot_data()
# To: download_all_penndot_data(years=[2023])
```

**Profile existing data**:
```bash
python scripts/02_process/profile_data.py
```

---

## Expected Outputs

### After Acquisition (`01_acquire`)

`data/raw/`:
- `Philadelphia_YYYY.zip` - Downloaded ZIP files (one per year)
- `CRASH_YYYY.csv` - Crash records
- `CYCLE_YYYY.csv` - Bicycle involvement
- `PERSON_YYYY.csv` - Person details
- `VEHICLE_YYYY.csv` - Vehicle details
- `ROADWAY_YYYY.csv` - Road characteristics
- `noaa_weather_philly.parquet` - Weather data

### After Processing (`02_process`)

`data/processed/`:
- Harmonized versions of each category
- Quality flags added

`metadata/`:
- `schema_analysis_report.json` - Detailed schema comparison
- `schema_analysis_summary.txt` - Human-readable summary

### After Integration (`03_integrate`)

`data/processed/`:
- Geographic filtered datasets
- Weather-merged datasets

### Final Datasets (`04_analyze`)

`data/final/`:
- `cyclist_crashes.parquet` - Bicycle crash analysis dataset
- `pedestrian_crashes.parquet` - Pedestrian crash analysis dataset
- `weather_correlated_crashes.parquet` - All crashes with weather

---

## Troubleshooting

### Import Errors

**Problem**: `ModuleNotFoundError: No module named 'X'`

**Solution**:
```bash
# Ensure virtual environment is activated
source venv/bin/activate  # macOS/Linux
.\venv\Scripts\activate   # Windows

# Reinstall requirements
pip install -r requirements.txt
```

### Memory Issues

**Problem**: `MemoryError` during processing

**Solutions**:
1. Process years individually instead of all at once
2. Use CSV instead of Parquet (edit `.env`: `OUTPUT_FORMAT=csv`)
3. Increase swap space (Linux) or virtual memory (Windows)

### Download Failures

**Problem**: PennDOT download fails for certain years

**Solutions**:
1. Check internet connection
2. Some years may not be available - check PennDOT portal manually
3. Retry with: `python scripts/01_acquire/download_penndot.py`

### NOAA API Issues

**Problem**: NOAA download fails or returns empty data

**Solutions**:
1. Verify API token in `.env`
2. Check token hasn't expired (tokens expire after ~1 year)
3. Check NOAA service status: https://www.ncdc.noaa.gov/cdo-web/
4. Respect rate limits (script includes delays)

### Permission Errors

**Problem**: `Permission denied` when writing files

**Solution**:
```bash
# Ensure data directories are writable
chmod -R u+w data/
```

---

## Monitoring Progress

### Log Files

All scripts write detailed logs to `logs/`:
- `logs/download_penndot.log` - PennDOT acquisition
- `logs/download_noaa.log` - NOAA acquisition
- `logs/profile_data.log` - Data profiling
- etc.

View logs in real-time:
```bash
tail -f logs/download_penndot.log
```

### Progress Bars

Scripts use `tqdm` for progress indication during:
- File downloads
- Data processing
- Quality checks

---

## Advanced Configuration

### Change Download Years

Edit `.env`:
```bash
START_YEAR=2020
END_YEAR=2023
```

### Adjust Philadelphia Boundaries

If you need different geographic bounds, edit `.env`:
```bash
PHILLY_LAT_MIN=39.867
PHILLY_LAT_MAX=40.138
PHILLY_LON_MIN=-75.280
PHILLY_LON_MAX=-74.956
```

### Change Output Format

Parquet (recommended):
```bash
OUTPUT_FORMAT=parquet
```

CSV (more compatible, larger files):
```bash
OUTPUT_FORMAT=csv
```

### Logging Level

For debugging:
```bash
LOG_LEVEL=DEBUG
```

For production:
```bash
LOG_LEVEL=INFO
```

---

## Docker Setup (Alternative)

### Prerequisites
- Docker Desktop installed
- Docker Compose installed

### Build and Run

```bash
cd philly-collision-pipeline
docker-compose up --build
```

This will:
1. Build isolated environment
2. Install all dependencies
3. Run complete pipeline
4. Output to `data/final/`

### Access Container

```bash
docker-compose run pipeline bash
```

---

## Testing Your Installation

Run the test suite:

```bash
# Install test dependencies (if not already installed)
pip install pytest pytest-cov

# Run tests
pytest tests/

# With coverage report
pytest --cov=scripts tests/
```

---

## Getting Help

### Check Logs
```bash
ls -lh logs/
cat logs/[script_name].log
```

### Common Issues Document
See `docs/TROUBLESHOOTING.md` (if available)

### GitHub Issues
Open an issue at: https://github.com/aseyedia/FDC-Project/issues

---

## Next Steps After Setup

1. **Review profiling output**: `metadata/schema_analysis_summary.txt`
2. **Check quality reports**: Generated during processing
3. **Explore datasets**: Use pandas or geopandas to load final datasets
4. **Run analysis**: Import datasets into your preferred analysis tool

---

## Cleanup

### Remove Downloaded Data

```bash
# Keep directory structure, remove files
find data/raw -type f -delete
find data/processed -type f -delete
find data/final -type f -delete
```

### Remove Virtual Environment

```bash
deactivate  # Exit venv first
rm -rf venv/
```

### Complete Reset

```bash
# Remove all generated files
rm -rf data/ logs/ metadata/ venv/
git clean -fdx  # WARNING: Removes all untracked files!
```

---

## Performance Tips

1. **Use Parquet format**: 10x faster than CSV for large datasets
2. **Process incrementally**: Run one year at a time for testing
3. **Use SSD storage**: Significantly faster I/O
4. **Increase memory**: 32GB+ recommended for full dataset
5. **Parallel processing**: Some scripts support multi-threading (check individual script docs)

---

## Updating

Pull latest changes:
```bash
git pull origin main
pip install --upgrade -r requirements.txt
```

---

**Last Updated**: October 2025  
**Maintained By**: Arta Seyedian
