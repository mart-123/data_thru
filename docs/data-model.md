# Data Warehouse: Star Schema Design
This document outlines the star schema (dimensional data model) used for the HESA data warehouse. For information about load/stage tables please refer to <a href="pipeline-process.md">Pipeline Process</a>.

It should be noted that, whilst the dataset is a small subset of a typical university data warehouse, it is designed to demonstrate good practice in:
- ETL development
- handling time-based datasets
- dimensional modeling
- data wrangling for canonical keys

<div style="margin: 1em 0; min-height: 20px;"></div>

## Final Dimensional model
The final dimensional model consists of:
- **Main dimension tables** main business entities (students, programs)
- **Look-up dimensions** descriptive labels (e.g. demographic categories) including canonical keys for historical trend analysis
- **Fact tables** metrics (student program enrollments) to be analysis by dimensions

For detailed column descriptions and business rules, see the DBT YAML definitions in [`dbt/models/schema.yml`](../dbt/models/schema.yml).

<div style="margin: 1em 0; min-height: 20px;"></div>

## Compound dimension keys
Each dimension has a surrogate key with a human-readable pattern: `<dim_code>_<business_key>_<delivery>` (e.g., `PGM_<guid>_22056_20240331`)

This supports troubleshooting and also enables dimension rebuilds without breaking foreign key references.

<div style="margin: 1em 0; min-height: 20px;"></div>

## Delivery-Aware Dimensions
- Delivery Code uniquely identifies each tranche of CSV files received from HESA.
- It is a composite value incorporating the receipt date.
- It is stored in warehouse tables as column `hesa_delivery`
- Each student/program/ethnicity code/etc has discrete data per delivery.

<div style="margin: 1em 0; min-height: 20px;"></div>

## Inter-Delivery Canonical Mappings
- Look-up codes such as religion may vary between HESA deliveries due to wording changes.
- For example, old ethnicity code '101 (white british)' could be replaced with new '601 (british white)'.
- Report writers need to report on historical trends even when data is categorised with new/old values.
- This is handled using 'canonical keys' which define a common key (e.g. 101/601 may each have canonical key 601).
- Data analysts supply canonical mappings in a CSV file, some wrangling required by data engineer.
- The mappings are loaded into the required dimension as a 'canonical key'.

<div style="margin: 1em 0; min-height: 20px;"></div>

## Star Schema ERD

```mermaid
erDiagram
    DIM_HESA_STUDENT ||--o{ FACT_HESA_STUDENT_PROGRAMS : "student details"
    DIM_HESA_PROGRAM ||--o{ FACT_HESA_STUDENT_PROGRAMS : "program details"
    FACT_HESA_STUDENT_PROGRAMS }o--|| DIM_HESA_DELIVERY : "delivery context"
    FACT_HESA_STUDENT_PROGRAMS }o--|| DIM_DATE : "enrolment date details"

    DIM_HESA_STUDENT {
        varchar dim_hesa_student_key PK
        char student_guid
        varchar first_names
        varchar last_name
        date dob
        varchar ethnicity FK
        varchar religion FK
        varchar sexid FK
    }

    DIM_HESA_PROGRAM {
        varchar dim_hesa_program_key PK
        char program_guid
        varchar program_code
        varchar program_name
    }

    DIM_HESA_DELIVERY {
        varchar dim_hesa_delivery_key PK
        varchar delivery_code
        date delivery_received
        varchar delivery_version
        boolean delivery_current
        varchar collection_ref
        varchar collection_sent
        varchar delivery_description
    }
    
    DIM_DATE {
        varchar dim_date_key PK
        date calendar_date
        int year
        int month
        varchar month_name
        int weekend
    }
    
    FACT_HESA_STUDENT_PROGRAMS {
        varchar dim_hesa_student_key FK
        varchar dim_hesa_program_key FK
        varchar dim_hesa_delivery_key FK
        varchar dim_enrol_date_key FK
        boolean fees_paid_bool
    }
```

<div style="margin: 1em 0; min-height: 20px;"></div>

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