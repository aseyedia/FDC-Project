# Philadelphia Collision Data Curation Pipeline

**Automated, Reproducible Pipeline for Multi-Source Traffic Safety Data**

[![Python 3.9+](https://img.shields.io/badge/python-3.9+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

---

## Overview

This project implements an **automated data curation pipeline** for Philadelphia traffic collision data (2005-2024) from PennDOT, integrated with NOAA weather data. Following the [USGS Science Data Lifecycle Model](https://www.usgs.gov/products/data-and-tools/data-management/data-lifecycle), this pipeline addresses critical data quality challenges identified through [prior exploratory analysis](https://aseyedia.github.io/philly-crash-stats/code/phillyCrashStats.html).

### Key Features

✅ **Automated Data Acquisition**: Downloads 8 PennDOT crash data categories and NOAA weather data  
✅ **Schema Harmonization**: Handles inconsistent schemas across 20 years of data  
✅ **Quality Assessment**: Automated checks for geographic validity, missing data, and categorical consistency  
✅ **Geographic Filtering**: Precise Philadelphia boundary filtering with coordinate standardization  
✅ **Weather Integration**: Spatial-temporal matching of crash and weather data  
✅ **Reproducible**: Fully containerized with Docker for independent deployment  
✅ **Well-Documented**: DataCite metadata and comprehensive data dictionaries  

---

## Project Structure

```
philly-collision-pipeline/
├── data/
│   ├── raw/              # Downloaded ZIP and CSV files
│   ├── processed/        # Cleaned and harmonized data
│   └── final/            # Analysis-ready datasets
├── scripts/
│   ├── config.py         # Central configuration
│   ├── utils/            # Logging and utility functions
│   ├── 01_acquire/       # Data download scripts
│   │   ├── download_penndot.py
│   │   └── download_noaa.py
│   ├── 02_process/       # Data processing scripts
│   │   ├── profile_data.py
│   │   ├── quality_checks.py
│   │   └── harmonize_schema.py
│   ├── 03_integrate/     # Data integration
│   │   ├── geographic_filter.py
│   │   └── merge_weather.py
│   └── 04_analyze/       # Dataset creation
│       └── create_datasets.py
├── metadata/             # DataCite metadata and data dictionaries
├── tests/                # Unit tests
├── docs/                 # Documentation
├── docker/               # Docker configuration
├── logs/                 # Execution logs
├── requirements.txt      # Python dependencies
└── README.md            # This file
```

---

## Quick Start

### Prerequisites

- **Python 3.9+**
- **Git**
- **32GB RAM recommended** (for processing full dataset)
- **~50GB disk space** (for raw and processed data)

### Installation

1. **Clone the repository**
   ```bash
   git clone https://github.com/aseyedia/FDC-Project.git
   cd FDC-Project/philly-collision-pipeline
   ```

2. **Create and activate virtual environment**
   ```bash
   python -m venv venv
   source venv/bin/activate  # On macOS/Linux
   # or
   .\venv\Scripts\activate  # On Windows
   ```

3. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

4. **Configure environment**
   ```bash
   cp .env.example .env
   # Edit .env and add your NOAA API token (get from https://www.ncdc.noaa.gov/cdo-web/token)
   ```

---

## Usage

### 1. Data Acquisition

**Download PennDOT crash data** (all 8 categories, 2005-2024):
```bash
python scripts/01_acquire/download_penndot.py
```

**Download NOAA weather data**:
```bash
python scripts/01_acquire/download_noaa.py
```

### 2. Data Profiling

**Analyze schema changes across years**:
```bash
python scripts/02_process/profile_data.py
```

This generates:
- `metadata/schema_analysis_report.json` - Detailed schema comparison
- `metadata/schema_analysis_summary.txt` - Human-readable summary

### 3. Data Processing

**Run quality checks and harmonization**:
```bash
python scripts/02_process/harmonize_schema.py
```

### 4. Data Integration

**Apply geographic filtering**:
```bash
python scripts/03_integrate/geographic_filter.py
```

**Merge weather data**:
```bash
python scripts/03_integrate/merge_weather.py
```

### 5. Create Analysis-Ready Datasets

```bash
python scripts/04_analyze/create_datasets.py
```

This creates:
- `cyclist_crashes.parquet` - Bicycle-involved collisions
- `pedestrian_crashes.parquet` - Pedestrian-involved collisions  
- `weather_correlated_crashes.parquet` - All crashes with weather data

---

## Data Sources

### Primary: PennDOT Crash Database

**Source**: [PennDOT GIS Open Data Portal](https://crashinfo.penndot.pa.gov/PCIT/welcome.html)  
**Coverage**: 2005-2024, Philadelphia County  
**Categories**:
- **CRASH**: Main crash records with location, date, severity
- **CYCLE**: Bicycle involvement details
- **PERSON**: Person-level information (injuries, demographics)
- **VEHICLE**: Vehicle details
- **ROADWAY**: Road characteristics
- **FLAG**: Flag person details
- **COMMVEH**: Commercial vehicle involvement
- **TRAILVEH**: Trailer/RV details

### Secondary: NOAA Weather Data

**Source**: [NOAA Climate Data Online](https://www.ncdc.noaa.gov/cdo-web/)  
**Station**: Philadelphia International Airport (GHCND:USW00013739)  
**Variables**:
- Daily temperature (min, max, average)
- Precipitation
- Wind speed
- Snow depth/accumulation

---

## Known Data Quality Issues

Based on [prior R analysis](https://aseyedia.github.io/philly-crash-stats/code/phillyCrashStats.html), this pipeline addresses:

### 1. Schema Drift (2005-2024)
- **Issue**: Column names and data types change across years
- **Solution**: `handle_mismatch()` function adapted from R, comprehensive schema mapping

### 2. County Miscoding
- **Issue**: All records incorrectly coded as York County (67) instead of Philadelphia (91)
- **Solution**: Automated detection and flagging for correction

### 3. Geographic Data Quality
- **Issue**: Missing coordinates, out-of-bounds values, inconsistent precision
- **Solution**: Boundary validation, precision standardization, missing data flagging

### 4. Categorical Inconsistency
- **Issue**: Helmet usage indicators have blanks and inconsistent coding
- **Solution**: Standardization to Y/N/U format with blank → 'U' conversion

---

## Pipeline Architecture

Following the **USGS Science Data Lifecycle Model**:

### Sequential Stages

1. **Plan** ✓ - Requirements defined, workflow designed
2. **Acquire** ✓ - Automated download scripts with error handling
3. **Process** ✓ - Quality assessment, schema harmonization
4. **Analyze** ⏳ - Analysis-ready dataset creation
5. **Preserve** ⏳ - Metadata packaging, format standards
6. **Publish/Share** ⏳ - Docker containerization, documentation

### Cross-Cutting Activities

- **Describe**: DataCite metadata, data dictionaries
- **Manage Quality**: Systematic quality protocols
- **Backup & Secure**: Version control, secure handling

---

## Testing

Run unit tests:
```bash
pytest tests/
```

Run with coverage:
```bash
pytest --cov=scripts tests/
```

---

## Docker Deployment

Build and run the pipeline in Docker:
```bash
docker-compose up --build
```

This will:
1. Create isolated environment with all dependencies
2. Run the complete pipeline
3. Output final datasets to `data/final/`

---

## Configuration

Edit `.env` to customize:

```bash
# Data year range
START_YEAR=2005
END_YEAR=2024

# Geographic bounds (Philadelphia)
PHILLY_LAT_MIN=39.867
PHILLY_LAT_MAX=40.138
PHILLY_LON_MIN=-75.280
PHILLY_LON_MAX=-74.956

# Output format
OUTPUT_FORMAT=parquet  # or 'csv'

# Logging
LOG_LEVEL=INFO  # DEBUG, INFO, WARNING, ERROR
```

---

## Output Datasets

Final datasets in `data/final/`:

| Dataset | Description | Key Fields |
|---------|-------------|------------|
| `cyclist_crashes.parquet` | Bicycle-involved collisions | CRN, date, location, helmet_use, injury_severity, weather |
| `pedestrian_crashes.parquet` | Pedestrian-involved collisions | CRN, date, location, injury_severity, weather |
| `weather_correlated_crashes.parquet` | All crashes with weather | All crash fields + temp, precipitation, wind |

All datasets include:
- Standardized coordinates (WGS84, 6 decimal precision)
- Quality flags (`geo_valid`, `county_miscoded`)
- Harmonized categorical variables
- Joined weather data by date

---

## Metadata

DataCite-compliant metadata in `metadata/datacite.json` includes:
- Title, Creator, Publication Year
- Subject keywords
- Geographic coverage (Philadelphia bounding box)
- Temporal coverage (2005-2024)
- Related identifiers (source URLs)
- Rights/License information

Data dictionary in `metadata/data_dictionary.md` documents:
- All field definitions
- Source systems
- Transformations applied
- Quality flags and their meanings

---

## Project Timeline

**Weeks 1-3** ✓ - Automated download scripts, data profiling  
**Weeks 4-6** ⏳ - Quality assessment, schema harmonization  
**Weeks 7-9** ⏳ - Data integration, geographic filtering  
**Weeks 10-12** ⏳ - Metadata, containerization, documentation  

---

## Related Projects

- **[Exploratory R Analysis](https://aseyedia.github.io/philly-crash-stats/code/phillyCrashStats.html)** - Initial analysis that identified data quality issues
- **[Narrative Visualization](https://aseyedia.github.io/cs416-narrative-viz/)** - Interactive D3.js visualization of crash data

---

## Contributing

This is an academic project for CS 598: Foundations of Data Curation.  
For questions or suggestions, please open an issue.

---

## License

MIT License - See LICENSE file for details

---

## Author

**Arta Seyedian**  
*Fall 2025*  
*CS 598: Foundations of Data Curation*  
University of Illinois Urbana-Champaign

---

## Acknowledgments

- **PennDOT** for open crash data
- **NOAA** for climate data access
- **USGS** for the Science Data Lifecycle Model framework
- Course instructors and peers for feedback

---

## Citation

If you use this pipeline or datasets, please cite:

```bibtex
@software{seyedian2025philly_collision_pipeline,
  author = {Seyedian, Arta},
  title = {Philadelphia Collision Data Curation Pipeline},
  year = {2025},
  url = {https://github.com/aseyedia/FDC-Project}
}
```
