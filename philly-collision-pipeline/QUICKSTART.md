# Quick Start Guide

## Get Running in 5 Minutes

### 1. Navigate to Pipeline Directory
```bash
cd /Users/artas/githubProjects/FDC-Project/philly-collision-pipeline
```

### 2. Create Virtual Environment
```bash
python3 -m venv venv
source venv/bin/activate
```

### 3. Install Dependencies
```bash
pip install --upgrade pip
pip install -r requirements.txt
```

### 4. Configure Environment
```bash
cp .env.example .env
# Edit .env and add your NOAA API token
# Get token at: https://www.ncdc.noaa.gov/cdo-web/token
```

### 5. Test the Setup
```bash
# Run in test mode (downloads only 2023 data)
python run_pipeline.py --test --stages 1,2
```

This will:
- Download PennDOT crash data for 2023 (~500MB)
- Download NOAA weather data for 2023
- Profile the data schemas
- Generate reports in `logs/` and `metadata/`

Expected time: ~10-15 minutes

---

## What Was Downloaded?

Check `data/raw/`:
```bash
ls -lh data/raw/
```

You should see:
- `Philadelphia_2023.zip` - Downloaded ZIP
- `CRASH_2023.csv` - Crash records
- `CYCLE_2023.csv` - Bicycle involvement
- `PERSON_2023.csv` - Person details
- `VEHICLE_2023.csv` - Vehicle details
- etc.

---

## View the Results

**Schema analysis**:
```bash
cat metadata/schema_analysis_summary.txt
```

**Execution logs**:
```bash
tail -50 logs/download_penndot.log
tail -50 logs/profile_data.log
```

---

## Run Full Pipeline

When ready to process all years (2005-2024):
```bash
# This will take 2-3 hours and download ~10GB
python run_pipeline.py
```

Or run stages individually:
```bash
python run_pipeline.py --stages 1      # Acquisition only
python run_pipeline.py --stages 2      # Profiling only
python run_pipeline.py --stages 1,2,3  # First 3 stages
```

---

## Next Steps

1. Review `metadata/schema_analysis_summary.txt` to understand data issues
2. Complete remaining pipeline stages (harmonization, integration, analysis)
3. Run quality checks on your specific use cases
4. Generate final analysis-ready datasets

---

## Troubleshooting

**Virtual environment not activating?**
```bash
# macOS/Linux
source venv/bin/activate

# Windows
.\venv\Scripts\activate
```

**Import errors?**
```bash
# Ensure venv is activated (prompt shows "(venv)")
pip install -r requirements.txt
```

**NOAA download fails?**
- Check your API token in `.env`
- Get a new token at https://www.ncdc.noaa.gov/cdo-web/token
- Wait a few minutes after requesting (token sent via email)

**Memory issues?**
- Use test mode: `--test`
- Process one year at a time
- Close other applications

---

## File Structure Overview

```
philly-collision-pipeline/
├── run_pipeline.py          # Main execution script
├── requirements.txt         # Python dependencies
├── .env.example            # Configuration template
├── README.md               # Full documentation
├── SETUP.md                # Detailed setup guide
│
├── data/
│   ├── raw/                # Downloaded data
│   ├── processed/          # Cleaned data
│   └── final/              # Analysis-ready datasets
│
├── scripts/
│   ├── config.py           # Configuration
│   ├── utils/              # Shared utilities
│   ├── 01_acquire/         # Download scripts
│   ├── 02_process/         # Processing scripts
│   ├── 03_integrate/       # Integration scripts
│   └── 04_analyze/         # Analysis scripts
│
├── metadata/               # Data dictionaries and reports
├── logs/                   # Execution logs
└── tests/                  # Unit tests
```

---

## Quick Commands Reference

```bash
# Test mode (fast, single year)
python run_pipeline.py --test

# Full pipeline (all years)
python run_pipeline.py

# Specific stages
python run_pipeline.py --stages 1      # Acquisition
python run_pipeline.py --stages 2      # Profiling
python run_pipeline.py --stages 1,2    # Both

# View logs
tail -f logs/download_penndot.log
ls -lh logs/

# Check data
ls -lh data/raw/
du -sh data/raw/

# View reports
cat metadata/schema_analysis_summary.txt
cat metadata/schema_analysis_report.json | jq
```

---

**Need Help?** See `SETUP.md` for detailed troubleshooting or open an issue on GitHub.
