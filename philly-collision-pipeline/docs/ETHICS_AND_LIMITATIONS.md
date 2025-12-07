# Ethical Considerations and Data Limitations

## Philadelphia Traffic Collision Data Curation Project

**Author**: Arta Seyedian  
**Course**: CS 598 Foundations of Data Curation  
**Date**: December 7, 2024

---

## Overview

This document addresses the ethical considerations, potential biases, and responsible use guidelines for the Philadelphia traffic collision dataset. As data curators, we have a responsibility to be transparent about data limitations and potential misuse scenarios.

## Privacy and De-identification

### Source Data Privacy

**PennDOT crash data is already de-identified**:
- ✅ No names of individuals involved
- ✅ No license plate numbers
- ✅ No specific street addresses (only intersection/route identifiers)
- ✅ No personal contact information

**However**, the dataset includes:
- ⚠️ Precise GPS coordinates (latitude/longitude to 6 decimal places ≈ 10cm accuracy)
- ⚠️ Crash times (year, month, day-of-week, hour)
- ⚠️ Demographic information (age, sex) of people involved
- ⚠️ Vehicle descriptions (make, model, color)

### Re-identification Risk

**Concern**: Combining temporal, spatial, and demographic data could potentially re-identify individuals involved in crashes, especially for:
- Fatal crashes (often reported in media)
- Crashes involving unusual vehicles or circumstances
- Crashes in low-traffic areas where few events occur

**Mitigation Strategies Implemented**:
1. **No name/contact fields preserved** from source data
2. **Geographic aggregation recommended** for public-facing visualizations (use census tracts or neighborhoods, not exact coordinates)
3. **Temporal aggregation recommended** for public reports (monthly summaries, not hour-specific)
4. **Documentation warns** against publishing maps with individual crash markers

**Recommendation for Public Sharing**:
```python
# DO NOT publish raw coordinates
# Instead, aggregate to spatial grid or census geography
crashes_aggregated = crashes.groupby('census_tract').agg({
    'CRN': 'count',
    'MAX_SEVERITY_LEVEL': 'max',
    # ... other summary stats
})
```

### Sensitive Populations

Certain groups may be disproportionately identifiable:
- **Cyclists and pedestrians**: Smaller population, crashes more notable
- **Commercial vehicle operators**: Fleet data may be known
- **Fatal crash victims**: Often reported in media with names

**Guideline**: Extra caution when analyzing these populations; avoid case studies that could reveal identities.

## Bias and Representation Issues

### Reporting Bias

**Not all crashes are reported equally**:

1. **Minor crashes underreported**: Fender-benders without injuries often unreported
   - **Impact**: Dataset overrepresents severe crashes
   - **Implication**: Cannot use this data to estimate true crash rates, only reported crash patterns

2. **Private property exclusion**: Crashes in parking lots, driveways not included
   - **Impact**: Urban residential crashes may be undercounted
   
3. **Hit-and-run underreporting**: Victims may not report if no injuries/witnesses
   - **Impact**: Cyclist/pedestrian crashes may be undercounted

**Transparency Note**: This dataset reflects **reported crashes to PennDOT**, not all crashes that occurred.

### Geographic Bias

**PennDOT data quality varies by jurisdiction**:
- **Philadelphia Police Department**: Primary reporting agency
- **Pennsylvania State Police**: Highways and some suburban areas
- **Other agencies**: Vary in reporting thoroughness

**Observed Issues**:
- ✅ 99.4% of crashes have valid coordinates (good coverage)
- ❌ 100% show incorrect county code (data quality issue)
- ⚠️ Coordinate precision varies (2-6 decimal places)

**Spatial Bias Concerns**:
- High-traffic areas (downtown) likely have better reporting/documentation
- Low-traffic areas may have less precise coordinates (e.g., rural roads)
- Tourist areas may have more thorough investigation than residential

### Demographic Bias

**Age and sex** are recorded for involved persons, but:
- ⚠️ Missing data for ~10-15% of person records (based on 2023 sample)
- ⚠️ Sex is binary (Male/Female) with no option for non-binary/other
- ⚠️ Race/ethnicity **not consistently recorded** in older years

**Implication**: Cannot reliably analyze racial disparities in traffic enforcement or crash outcomes with this dataset.

### Temporal Bias

**Crash reporting practices changed over 20 years (2005-2024)**:
- Field additions (e.g., autonomous vehicle levels added in 2020)
- Coding changes (e.g., micro-mobility categories in 2022)
- Technology improvements (GPS accuracy improved over time)

**Recommendation**: 
- Trend analysis should acknowledge schema evolution
- Comparisons across decades should use only universally-present fields
- Year-over-year percent changes more reliable than absolute counts

## Weather Data Limitations

### Spatial Approximation

**Single weather station** (Philadelphia International Airport) used for entire city:
- Distance from airport to furthest Philadelphia point: ~15 miles
- **Concern**: Hyperlocal weather (e.g., afternoon thunderstorms) not captured
- **Impact**: Weather-crash correlations are **approximate**, not precise

**Example scenario**:
- Rainstorm hits North Philadelphia at 3 PM
- Airport (South Philadelphia) remains dry
- Crashes in North Philly would be coded as "no precipitation"

**Mitigation**: Use categorical variables (`adverse_weather`) rather than precise precipitation amounts for correlation analysis.

### Temporal Approximation (v2.0 Improved)

**Day-of-month uncertainty** (see `WEATHER_MATCHING_METHODOLOGY.md`):
- `DAY_OF_WEEK` used to reconstruct approximate date
- First occurrence of weekday in month used
- **Still approximate**: Don't know if Wednesday crash was 1st, 2nd, 3rd, or 4th Wednesday

**Recommendation**: 
- Monthly or weekly aggregations more robust than daily
- Don't over-interpret day-specific weather correlations
- Always disclose approximation method in analysis

## Data Quality Issues

### Known Data Defects

1. **County miscoding** (100% incorrect):
   - All records show `COUNTY=67` (York) instead of `51` (Philadelphia)
   - **Workaround**: Use geographic coordinates for spatial filtering
   - **Impact**: Cannot rely on county field for filtering

2. **Missing temporal precision**:
   - No `CRASH_DAY` field in source data
   - Reconstructed using `DAY_OF_WEEK` (v2.0 improvement)
   - **Impact**: Weather matching is approximate

3. **PERSON_TYPE inconsistency**:
   - Cyclist/pedestrian flags unreliable in PERSON table
   - **Workaround**: Use CYCLE table and `PED_COUNT` from CRASH table
   - **Impact**: May miss some crashes involving vulnerable users

**Transparency Approach**: All quality issues **flagged, not filtered**. Analysts can make own decisions about data inclusion.

## Potential Misuse Scenarios

### Inappropriate Uses (Do Not Do)

❌ **Individual blame/liability determination**:
- "This person was at fault because..."
- Legal proceedings require original crash reports, not curated dataset

❌ **Predictive policing of locations**:
- Over-policing high-crash areas may create feedback loop
- Enforcement disparities may reflect bias, not just crash risk

❌ **Insurance discrimination**:
- Aggregating by neighborhood for insurance pricing
- Could perpetuate redlining and inequitable access

❌ **Sensationalist mapping**:
- Publishing maps of fatal crashes with victim details
- Voyeuristic "crash tourism" or re-traumatizing families

❌ **Pseudoscientific causal claims**:
- "Crashes increase 47.3% on rainy days" (ignoring uncertainty)
- Weather approximation doesn't support this level of precision

### Responsible Uses (Encouraged)

✅ **Vision Zero planning**:
- Identify high-crash corridors for infrastructure improvements
- Prioritize safety investments in underserved areas

✅ **Policy evaluation**:
- Assess impact of traffic calming measures (before/after analysis)
- Evaluate effectiveness of safety campaigns

✅ **Temporal pattern analysis**:
- Seasonal trends in crash severity
- Time-of-day patterns for pedestrian/cyclist crashes

✅ **Infrastructure advocacy**:
- Evidence-based arguments for bike lanes, crosswalks, traffic signals
- Community organizing around traffic safety

✅ **Academic research**:
- Weather-crash correlations (acknowledging limitations)
- Long-term trend analysis
- Comparative studies across cities

## Data Sharing and Access

### Licensing

**Source data**: PennDOT and NOAA data are **public domain** (U.S. government works)

**Curated dataset**: Released under **CC0 (Public Domain Dedication)** 
- Anyone can use for any purpose
- No attribution required (though appreciated)
- **Why CC0?**: Maximizes reusability; source data already public

### Publication Recommendations

**If depositing in repository** (e.g., Illinois Data Bank, Zenodo):

1. **Include comprehensive documentation**:
   - This ethics document
   - Data dictionary with field limitations
   - Methodology documents
   - Known issues log

2. **Embargo precise coordinates** (optional):
   - Consider publishing only aggregated geographic data
   - Provide exact coordinates on request for legitimate research

3. **Add use case examples**:
   - Show appropriate analyses
   - Demonstrate proper citation
   - Include code snippets for common tasks

4. **Version clearly**:
   - v2.0 includes weekday-based weather matching
   - Future updates will add subsequent years
   - Deprecation policy for old methodologies

### Citation Requirements

**Required attributions**:
```
Primary data source: Pennsylvania Department of Transportation (PennDOT),
  Crash Information Tool (PCIT), 2005-2024.
  https://crashinfo.penndot.pa.gov/

Weather data: NOAA National Centers for Environmental Information,
  Climate Data Online (CDO), Station USW00013739.
  https://www.ncdc.noaa.gov/cdo-web/

Curated by: Arta Seyedian (2024), Philadelphia Traffic Collision Dataset
  with Weather Integration. University of Illinois.
  [DOI when available]
```

## Ethical Decision-Making Framework

### When in Doubt, Ask:

1. **Privacy**: Could this analysis re-identify individuals?
   - If yes → aggregate further or omit

2. **Harm**: Could this use cause harm to communities?
   - If yes → consult stakeholders or reconsider

3. **Transparency**: Am I being honest about limitations?
   - If no → revise documentation

4. **Equity**: Does this perpetuate existing biases?
   - If yes → add context or warning

5. **Necessity**: Is precise data needed for this question?
   - If no → use aggregated/anonymized version

## Researcher Responsibilities

If you use this dataset, you should:

1. **Read all documentation**: Understand limitations before analyzing
2. **Cite sources properly**: Acknowledge PennDOT, NOAA, and curation work
3. **Disclose uncertainties**: Weather matching is approximate; state this in findings
4. **Consider context**: Crashes are human tragedies, not just data points
5. **Engage stakeholders**: Consult with Vision Zero Philadelphia, Bicycle Coalition, etc.
6. **Share responsibly**: Don't publish individual-level data publicly

## Updates and Feedback

### Reporting Issues

If you identify additional ethical concerns or data quality issues:

**Contact**: aseyedia@illinois.edu  
**GitHub Issues**: [Repository URL]

### Living Document

This ethics document will be **updated** as:
- New use cases emerge
- Privacy concerns are identified
- Stakeholder feedback is received
- Best practices evolve

**Version History**:
- v1.0 (Dec 7, 2024): Initial ethical considerations
- [Future versions will be listed here]

## Acknowledgments

### Stakeholder Consultation

While this project is primarily academic, it was informed by:
- Vision Zero Philadelphia goals and priorities
- Bicycle Coalition of Greater Philadelphia advocacy work
- USDOT traffic safety research best practices
- Federal Highway Administration crash data guidelines

### Inspiration

This ethics framework draws from:
- **FAIR Principles**: Findable, Accessible, Interoperable, Reusable
- **CARE Principles**: Collective benefit, Authority to control, Responsibility, Ethics
- **APA Ethics Code**: Beneficence, justice, fidelity, integrity
- **Data Feminism**: Acknowledge power, challenge binaries, elevate emotion & embodiment

## Conclusion

**Data curation is an ethical practice.** It's not enough to build a technically correct pipeline—we must consider:

- Who is represented (and who isn't)
- Who might be harmed by analysis
- Who benefits from data access
- Who controls the narrative

This dataset exists to **improve traffic safety** and support **Vision Zero** goals. Any use that doesn't serve those ends should be reconsidered.

**Remember**: Behind every crash record is a person, a family, a community. Treat this data with the respect and care it deserves.

---

**Questions or Concerns?**

If you're unsure whether a particular use is ethical, **ask**. Contact the curator, consult your IRB, engage with affected communities. When in doubt, err on the side of caution and transparency.

**Safe streets for all.**
