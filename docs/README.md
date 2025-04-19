# HESA Data Pipeline (project 'data_thru')

This portfolio project contains an ETL pipeline that ingests student data and transforms it into a dimensional model for analysis/reporting. It demonstrates data engineering practices including ETL pipelines, data quality filters, historical data versioning, dimensional modelling and automated testing.

The dataset loosely resembles student data from HESA (Higher Education Statistics Agency). The architecture resembles what could be found in a university data warehouse.

## Platform
Some companies are moving their data warehouses to the cloud. This can be a convenient option but cost management and platform tie-in should be considered. This project is containerised so that it can be deployed locally or on a cloud provider with minimal changes.

## Data
Tha student/program data is fictional, generated using a test data generator. Look-up tables were downloaded from HESA (Higher Education Statistics Agency) and modified for test scenarios.

## Automated testing
Load tables are compared with their originating CSV files.
Stage tables are each compared with an 'expected results' CSV file.
Details of failed test cases support bug fixing.

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