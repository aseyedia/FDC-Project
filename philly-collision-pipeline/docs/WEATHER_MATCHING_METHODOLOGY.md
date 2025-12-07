# Weather-Crash Data Integration Methodology

## Problem Statement

The PennDOT crash data lacks a `CRASH_DAY` field, providing only `CRASH_YEAR` and `CRASH_MONTH`. This presents a challenge for matching crashes to daily weather observations from NOAA, which are organized by specific dates.

## Available Temporal Data

PennDOT crash records include:
- **CRASH_YEAR**: Year of crash occurrence (e.g., 2023)
- **CRASH_MONTH**: Month of crash (1-12)
- **DAY_OF_WEEK**: Day of week when crash occurred (PennDOT encoding: 1=Sunday, 2=Monday, ..., 7=Saturday)
- **HOUR_OF_DAY**: Hour when crash occurred (0-23)

While we don't have the exact day of the month, the `DAY_OF_WEEK` field provides valuable temporal context.

## Methodology: Weekday-Based Date Reconstruction

### Approach

Instead of arbitrarily assigning all crashes to a single day (e.g., the 1st or 15th of the month), we use the `DAY_OF_WEEK` field to reconstruct a **representative date** within the crash month:

1. **For each crash**: Identify the day of week (e.g., "Tuesday")
2. **Find the first occurrence** of that weekday in the crash month
3. **Match to weather data** for that specific date

### Rationale

This approach has several advantages over using a fixed day:

1. **Temporal Distribution**: Crashes are distributed across the month according to their actual weekday occurrence
2. **Weather Variation**: Captures more realistic weather variability within a month (e.g., early-month vs. late-month conditions)
3. **Use of Available Data**: Leverages the `DAY_OF_WEEK` field that was previously unused
4. **Defensible Approximation**: The first occurrence of a weekday is as reasonable as any other occurrence (no bias toward beginning/middle/end of month)

### Implementation Details

#### PennDOT to Python Weekday Mapping

PennDOT uses: `1=Sunday, 2=Monday, 3=Tuesday, ..., 7=Saturday`

Python's `datetime.weekday()` uses: `0=Monday, 1=Tuesday, ..., 6=Sunday`

**Conversion formula**: `python_weekday = (penndot_day - 2) % 7`

#### Algorithm

```python
def find_weekday_in_month(year, month, day_of_week_code):
    """
    Find the first occurrence of a given weekday in a month.
    
    Args:
        year: Crash year (e.g., 2023)
        month: Crash month (1-12)
        day_of_week_code: PennDOT day code (1=Sun, 2=Mon, ..., 7=Sat)
    
    Returns:
        datetime object for first occurrence of that weekday in month
    """
    # Convert PennDOT encoding to Python encoding
    python_weekday = (day_of_week_code - 2) % 7
    
    # Start from first day of month
    first_day = datetime(year, month, 1)
    
    # Calculate days ahead to target weekday
    days_ahead = (python_weekday - first_day.weekday()) % 7
    
    # Return target date
    return first_day + timedelta(days=days_ahead)
```

**Example**:
- Crash occurred in **July 2023** on a **Wednesday** (PennDOT code = 4)
- July 1, 2023 was a Saturday
- First Wednesday in July 2023 was **July 5, 2023**
- Crash is matched to weather data from **July 5, 2023**

### Fallback Strategies

The pipeline implements a **hierarchical fallback** approach:

1. **Best case**: If `CRASH_DAY` exists and is populated → use exact date
   - Result: Precise day-level matching
   - Flag: `date_approximation_method = 'exact_day'`

2. **Good case**: If `DAY_OF_WEEK` is available → reconstruct using first weekday occurrence
   - Result: Representative day within month
   - Flag: `date_approximation_method = 'weekday_reconstructed'`

3. **Fallback**: If neither field available → use 15th of month
   - Result: Mid-month approximation (avoids month-boundary effects)
   - Flag: `date_approximation_method = 'mid_month_fallback'`

### Metadata and Transparency

Every crash record includes a `date_approximation_method` field indicating which approach was used:

| Method | Meaning | Precision |
|--------|---------|-----------|
| `exact_day` | Actual crash day known | Day-level |
| `weekday_reconstructed` | Day-of-week used to estimate date | ~Week-level |
| `mid_month_fallback` | No temporal info, used 15th | Month-level only |

This allows downstream analysts to:
- Filter by precision level if needed
- Understand the temporal uncertainty in weather matching
- Make informed decisions about analysis granularity

## Limitations and Considerations

### What This Method Does **NOT** Do

1. **Does not determine the exact day**: We don't know if a Wednesday crash occurred on the 5th, 12th, 19th, or 26th
2. **Does not account for intra-month weather trends**: Early-month weather may differ from late-month
3. **Cannot match to hourly weather**: `HOUR_OF_DAY` is available but NOAA provides daily summaries only

### What This Method **DOES** Provide

1. **Better than fixed-day assignment**: Distributes crashes temporally rather than clustering all at day 1 or 15
2. **Utilizes all available data**: Leverages `DAY_OF_WEEK` field that was previously ignored
3. **Transparent approximation**: Clearly documents the methodology and limitations
4. **Consistent and reproducible**: Deterministic algorithm with clear logic

### Comparison to Alternatives

| Approach | Pros | Cons |
|----------|------|------|
| **Use 1st of month** | Simple, consistent | All crashes assigned same weather; ignores temporal variation |
| **Use 15th of month** | Avoids month boundaries | Still arbitrary; ignores weekday patterns |
| **Random day in month** | Simulates variation | Non-reproducible; introduces uncertainty without data basis |
| **Weekday reconstruction (chosen)** | Uses available data; temporally distributed; defensible | Still approximate; assumes first occurrence representative |

## Weather Data Characteristics

### NOAA Daily Summaries

The NOAA Climate Data Online (CDO) API provides **daily summaries** from Philadelphia International Airport (USW00013739):

- **Temporal resolution**: One observation per day
- **Time of observation**: Typically midnight-to-midnight UTC
- **Variables**: Temperature (max/min/avg), precipitation, snow, wind speed

### Matching Logic

The pipeline performs a **LEFT JOIN** on `crash_date`:

```sql
SELECT * FROM crash_data
LEFT JOIN weather_data ON crash_data.crash_date = weather_data.date
```

- **Expected match rate**: 100% (NOAA has complete daily coverage 2005-2024)
- **Handling missing weather**: Preserved as NULL (rare, typically due to data quality issues)

### Derived Weather Features

From raw NOAA variables, the pipeline creates interpretable categories:

| Feature | Derivation | Categories |
|---------|------------|------------|
| `precip_category` | Based on daily precipitation (mm) | none (0mm), light (0-2.5mm), moderate (2.5-10mm), heavy (>10mm) |
| `temp_category` | Based on average temperature (°C) | cold (<0), mild (0-15), warm (15-25), hot (>25) |
| `adverse_weather` | Boolean flag | `True` if precip > 0 OR snow > 0 |

## Validation and Quality Assurance

### Automated Checks

The pipeline logs:
1. **Count by approximation method**: How many crashes use each strategy
2. **Match rate**: Percentage of crashes successfully matched to weather
3. **Date range verification**: Ensures reconstructed dates fall within expected bounds

### Example Output

```
Crash dates prepared:
  Total crashes: 8,619
  With valid dates: 8,619
Date approximation methods:
    weekday_reconstructed: 8,547 crashes (99.2%)
    mid_month_fallback: 72 crashes (0.8%)
  Date range: 2023-01-01 to 2023-12-30
  
Weather matching:
  Crashes matched: 8,619/8,619 (100.0%)
```

### Sanity Checks

1. **No crashes on impossible dates**: No Feb 30th, etc.
2. **Uniform weekday distribution**: Each day of week appears roughly equally across year
3. **Temporal clustering**: First occurrence dates should span days 1-7 of each month

## Use Cases and Analyst Guidance

### Appropriate Analyses

This methodology supports:

✅ **Monthly weather trend analysis**: "Do crashes increase in rainy months?"
✅ **Seasonal pattern detection**: "Are winter crashes more severe?"
✅ **Weekday-specific weather effects**: "Are rainy Mondays more dangerous than rainy Fridays?"
✅ **Long-term climate trends**: "Has precipitation-related crash rate changed over 20 years?"

### Inappropriate Analyses

This methodology **does NOT** support:

❌ **Day-specific event correlation**: "Did the July 15th rainstorm cause crashes?"
❌ **Sequential day pattern analysis**: "Crashes the day after rain vs. day of rain"
❌ **Hourly weather matching**: Cannot match `HOUR_OF_DAY` to hourly conditions
❌ **Intra-month temporal trends**: Cannot determine if crash was early/mid/late in month

### Recommended Practices

1. **Filter by `date_approximation_method`** if precision is critical:
   ```python
   # Only use high-confidence matches
   high_precision = df[df['date_approximation_method'] == 'weekday_reconstructed']
   ```

2. **Aggregate to monthly level** for most robust analysis:
   ```python
   # Avoid over-interpreting day-level precision
   monthly_summary = df.groupby(['CRASH_YEAR', 'CRASH_MONTH']).agg({...})
   ```

3. **Include methodology in all publications**:
   - Cite this document
   - Note the weekday reconstruction approach
   - Acknowledge day-of-month uncertainty

## Future Improvements

### Potential Enhancements

1. **Request CRASH_DAY from PennDOT**: If field becomes available, pipeline auto-detects and uses exact dates
2. **Multiple representative days**: Use 1st, 2nd, 3rd, and 4th occurrence of weekday; average their weather
3. **Hourly weather data**: Integrate radar/satellite data for hour-specific conditions (requires different data source)
4. **Bayesian date estimation**: Use crash density patterns to probabilistically assign days

### Data Provider Advocacy

**Recommendation**: Request that PennDOT add `CRASH_DAY` to future data releases. This single field would eliminate all approximation uncertainty.

## References

### Data Sources
- **PennDOT Crash Data**: Pennsylvania Department of Transportation, Crash Information Tool (PCIT)
  - Fields: CRASH_YEAR, CRASH_MONTH, DAY_OF_WEEK, HOUR_OF_DAY
  - URL: https://crashinfo.penndot.pa.gov/

- **NOAA Weather Data**: National Centers for Environmental Information, Climate Data Online (CDO)
  - Station: Philadelphia International Airport (USW00013739)
  - API: https://www.ncdc.noaa.gov/cdo-web/webservices/v2

### Related Documentation
- `merge_weather.py`: Implementation code
- `DATA_DICTIONARY.md`: Field definitions and types
- `PROJECT_ASSESSMENT.md`: Overall project methodology

---

**Document Version**: 2.0  
**Last Updated**: December 7, 2024  
**Author**: Arta Seyedian  
**Course**: CS 598 Foundations of Data Curation  
**Status**: Revised based on instructor feedback (Progress Report)
