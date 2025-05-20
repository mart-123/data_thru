# Data Warehouse: Star Schema
The star schema design includes dimension and fact tables as visualised below. For detailed column descriptions and business rules, see the DBT YAML definitions in [`dbt/models/schema.yml`](../dbt/models/schema.yml).

## Compound dimension keys
Each dimension has a surrogate key with a human-readable pattern: `<dim_code>_<business_key>_<delivery>` (e.g., `PGM_<guid>_22056_20240331`)

This supports troubleshooting and also enables dimension rebuilds without breaking foreign key references.

## Delivery-Aware Dimensions
- Delivery Code uniquely identifies each tranche of CSV files received from HESA.
- It is a composite value incorporating the receipt date.
- It is stored in warehouse tables as column `hesa_delivery`
- Each student/program/ethnicity code/etc has discrete data per delivery.
- Dimensions contain delivery code in their surrogate key to support readability.


## Inter-Delivery Mappings
- Dimensions have delivery code as part of their PK.
- Delivery Code uniquely identifies each tranche of CSV files received from HESA.
- It is a composite value incorporating the receipt date.
- It is stored in warehouse tables as column `hesa_delivery`
- This allows each student/program/ethnicity code/etc to have different data per delivery.
- Dimensions also contain the delivery code in their surrogate key


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
        boolean is_weekend
    }
    
    FACT_HESA_STUDENT_PROGRAMS {
        varchar dim_hesa_student_key FK
        varchar dim_hesa_program_key FK
        varchar dim_hesa_delivery_key FK
        varchar dim_enrol_date_key FK
        boolean fees_paid_bool
    }
```

<div style="margin: 3em 0 1em 0; border-top: 1px solid #ccc; padding-top: 1em;">
  <strong>Navigation:</strong>
  <a href="README.md">Home</a> 
  <a href="architecture.md">Architecture</a> |
  <a href="data-deliveries.md">HESA Deliveries</a> |
  <a href="data-model.md">Data Model</a> |
  <a href="pipeline-process.md">Pipeline Process</a> |
  <a href="hesa-data-info.md">HESA Data Info</a> |
  <a href="scripts.md">Scripts</a>
</div>