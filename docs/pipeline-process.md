# Data Pipeline
This document details the ETL implementation approach and processing flow used to transform denormalised student data into a dimensional model. It covers the implementation of each pipeline phase from extraction through dimensional modelling.

<div style="margin: 1em 0; min-height: 20px;"></div>

## Multiple Deliveries
HESA data arrives periodically in annual submissions (e.g., "22056_20240331"). The pipeline is designed to handle these multiple deliveries while maintaining data lineage throughout the process:

- **Directory Structure**: Each delivery code has its own directory containing CSV files
- **Extraction**: Extract scripts process CSV files by delivery code parameter
- **Loading**: Load tables are created with delivery-specific naming
- **Integration**: DBT staging models combine data across deliveries using SQL UNION operations
- **Modelling**: Delivery codes are embedded in surrogate keys for tracking lineage


<div style="margin: 1em 0; min-height: 20px;"></div>

## Pipeline Phases
### Extraction Phase
Python scripts validate and cleanse the 'delivery' CSV files, performing:
- Data cleansing and validation (e.g. date columns must be 'YYYY-MM-DD')
- Column renaming and basic reformatting
- Routing of invalid records to 'bad data' directory with reasons indicated
- Output of valid records to 'transformed' directory ready for loading

<div style="margin: 1em 0; min-height: 20px;"></div>

### Load Phase
Python scripts copy each 'transformed' CSV file and lookup file to a load table.
No significant logic is applied during this phase.
MySQL casts date strings as DATE (previously validated for 'YYYY-MM-DD')

<div style="margin: 1em 0; min-height: 20px;"></div>

### Integration Phase
DBT staging integrates (combines) data from multiple deliveries through:
- JOIN operations to connect related entities
- UNION operations to combine multiple HESA deliveries
- DISTINCT filtering to get program codes from student/program data
#### Example UNION to combine deliveries during staging
```sql
SELECT 
    student_guid, 
    first_names,
    delivery_code
FROM {{ ref('load_hesa_22056_20240331_students') }}

UNION ALL

SELECT 
    student_guid, 
    first_names,
    delivery_code
FROM {{ ref('load_hesa_23056_20250331_students') }}
```

#### Example DISTINCT to get program details during staging
```sql
SELECT DISTINCT 
    t1.program_guid, 
    t1.program_code, 
    t1.program_name, 
    t1.source_file, 
    t1.hesa_delivery
FROM 
    {{ source('hesa', 'load_hesa_22056_20240331_student_programs') }} t1
```

<div style="margin: 1em 0; min-height: 20px;"></div>

### Dimensional Modelling Phase
DBT dimension and fact models create the final star schema structure:
- Dimension tables with delivery-aware surrogate keys
- Fact tables with measures and dimension references


<div style="margin: 3em 0 1em 0; border-top: 1px solid #ccc; padding-top: 1em;">
  <strong>Navigation:</strong>
  <a href="README.md">Home</a> |
  <a href="architecture.md">Architecture</a> |
  <a href="container-first.md">Container First</a> |
  <a href="data-deliveries.md">HESA Deliveries</a> |
  <a href="data-model.md">Data Model</a> |
  <a href="getting-started.md">Getting Started</a> |
  <a href="hesa-data-info.md">HESA Data Info</a> |
  <a href="pipeline-process.md">Pipeline Process</a> |
  <a href="scripts.md">Scripts</a>
</div>