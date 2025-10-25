## Reproducible Multi-Source Traffic Safety Data Curation: Building an Automated Pipeline for Philadelphia Collision Analysis

*Arta Seyedian*
*Fall 2025*
*CS 598: Foundations of Data Curation*

![Map of Philly](<img/Pasted image 20250915124307.png>)

### Overview

The Commonwealth of Pennsylvania makes available to the general public a wealth of open data sets for both professional and citizen researchers to browse and scrutinize. One such open data set is the PennDOT public crash dataset available through PennDOT's GIS Open Data Portal. This vast dataset contains all recorded vehicle collisions between the years 2005 - 2025. However, this rich dataset presents significant data curation challenges that limit its use for systematic analysis. Through multiple prior projects with this dataset, including [previous exploratory work](https://aseyedia.github.io/philly-crash-stats/code/phillyCrashStats.html) and a [narrative visualization project](https://github.com/aseyedia/cs416-narrative-viz.git), I have already identified several critical issues, including inconsistent schema across multiple years, problems with geographic data quality, and integration complexities when combining multiple data sources. The aim is to create a robust and reproducible workflow for curating Philadelphia traffic safety data that addresses these challenges while producing an analysis-ready dataset suitable for municipal traffic safety planning.

### Plan

The project will follow the USGS Science Data Lifecycle Model (Faundeen et al., 2014), which is used for shepherding scientific data through six stages with three cross-cutting activities:

**Sequential Stages:**

- **Plan**: Define data curation requirements and workflow for multi-year PennDOT datasets.
- **Acquire**: Develop automated scripts for downloading PennDOT crash data (2002-2024) and NOAA weather data for the Philadelphia region, including daily temperature, precipitation, and wind speed measurements from nearby weather stations.
- **Process**: Implement systematic quality assessment framework, geographic filtering, schema harmonization, and data integration pipelines.
- **Analyze**: Create analysis-ready datasets to demonstrate workflow effectiveness.
- **Preserve:** Package datasets with appropriate metadata and format standards.
- **Publish/Share:** Deploy the reproducible workflow in a containerized environment (such as Docker) with comprehensive documentation.

**Cross-cutting Activities:**

- **Describe:** Metadata creation according to DataCite standards, including detailed data dictionaries and sourcing.
- **Manage Quality:** Systematic quality assessment protocols addressing known issues (e.g. county miscoding, schema drift, coordinate precision).
- **Backup & Secure:** Version control implementation and secure data handling procedures.

### Data Sources

The primary data source for this project will be the [PennDOT crash datasets](https://crashinfo.penndot.pa.gov/PCIT/welcome.html) from 2005-2024 as made available through the PennDOT's GIS Open Data Portal, which encompasses eight data categories (e.g. CRASH, CYCLE, VEHICLE, ROADWAY) filtered to Philadelphia city limits.

Secondarily, we will integrate NOAA weather station data for Philadelphia International Airport, with a particular focus on daily temperature, precipitation, wind speed and visibility conditions to correlate with collision patterns and discrete collision occurrences.

### Timeline

**Weeks 1-3**: Develop automated download scripts for PennDOT datasets and NOAA API integration. Conduct comprehensive data profiling to identify dataset inconsistencies and quality issues.

**Weeks 4-6**: Develop and implement quality assessment framework addressing known problems (county miscoding, missing coordinates, inconsistent categorization). Create schema harmonization procedures for cross-year compatibility.

**Weeks 7-9**: Build data integration pipelines linking crash and weather data through spatial-temporal matching algorithms. Implement geographic filtering for Philadelphia boundaries and coordinate system standardization.

**Weeks 10-12**: Create comprehensive metadata following DataCite standards, develop data dictionaries, and implement workflow containerization using Docker. Finalize documentation and reproducibility testing to ensure independent deployment capability.