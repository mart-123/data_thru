# HESA Data Pipeline (project 'data_thru')

This project contains an ETL pipeline that ingests a source dataset and transforms it into a dimensional model.

## Data
The fictional dataset is based loosely on the real-world HESA (Higher Education Statistics Agency) student dataset. Lookup tables were obtained from HESA but student data was generated using a test data generator website.

## Key components
- Extract: Python scripts for data extraction, basic transformations and data quality filters
- Load: MySQL table creation and data loading
- Transform: DBT models for staging, dimensions and facts
- Orchestration: Prefect flows for pipeline execution

## Documentation Sections
* [Architecture Overview](architecture.md)
* [Data Model](data-model.md)
* [ETL Process](etl-process.md)
* [HESA Data Dictionary](hesa-data-dictionary.md)
* [Development Guide](development-guide.md)