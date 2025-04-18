# Data Warehouse: Star Schema

The star schema design includes:

- **Dimension Tables**: Student, Program, Religion, Ethnicity, etc.
- **Fact Tables**: Student-Program relationships

Each dimension includes surrogate keys built with human-readable patterns:
`DIM_<type>_<code>_<delivery>` (e.g., `STU_12345_22056_20240331`)


## Delivery-Aware Surrogate Keys

All dimension tables use surrogate keys that embed the HESA delivery code:

- `DIM_HESA_STUDENT_KEY`: `STU_<student_guid>_<hesa_delivery>`
- `DIM_HESA_RELIGION_KEY`: `REL_<religion_code>_<hesa_delivery>`

This design allows the same entity (e.g., a student) to have different dimension records across multiple deliveries, capturing changes over time.


## Star Schema Design

```mermaid
erDiagram
    FACT_HESA_STUDENT_PROGRAMS ||--o{ DIM_HESA_STUDENT : "student dimension"
    FACT_HESA_STUDENT_PROGRAMS ||--o{ DIM_HESA_PROGRAM : "program dimension"

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

    FACT_HESA_STUDENT_PROGRAMS {
        varchar dim_hesa_student_key FK
        varchar dim_hesa_program_key FK
        date enrol_date
        boolean fees_paid_bool
        varchar hesa_delivery
    }