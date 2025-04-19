# Data Pipeline
This document describes data pipeline for HESA student data.


## Multiple 'Deliveries'
The pipeline handles deliveries through consistent patterns in each phase:
- **Extraction**: Processing each delivery's files separately
- **Loading**: Creating delivery-specific load tables
- **Integration**: Using UNION operations in DBT staging models
- **Modeling**: Embedding delivery codes in surrogate keys


## Pipeline Phases
### Extraction
Python scripts validate and cleanse the 'delivery' CSV files, performing:
- Data cleansing and validation
- Column renaming and basic reformatting
- Routing of invalid records to 'bad data' directory with reasons indicated
- Output of valid records to 'transformed' directory ready for loading

### Loading
Python scripts copy each 'transformed' CSV file and lookup file to a load table. 
No significant logic is applied during this phase.

### Integration
DBT staging models integrate data from multiple deliveries through:
- JOIN operations to connect related entities
- UNION operations to combine multiple HESA deliveries
- DISTINCT filtering to get program codes from student/program data

### Modeling
DBT dimension and fact models create the final star schema structure:
- Dimension tables with delivery-aware surrogate keys
- Fact tables with measures and dimension references