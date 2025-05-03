# HESA Data Pipeline (project 'data_thru')

This portfolio project demonstrates a data warehouse solution for higher education student data. It comprises:

- Python ETL pipeline for data extraction, cleansing and simple transformations
- DBT models for creating a dimensional star schema in MySQL
- Docker containerisation for simple local/cloud deployment
- Automated testing

The architecture incorporates data engineering practices including data quality filters, historical versioning (SCD Type 2), dimensional modelling. This reflects a typical university solution.

## Data
Student data was created using a test data generator. Look-up tables were downloaded from HESA (Higher Education Statistics Agency) and adapted for testing purposes.

## Automated testing
- Load tables are tested by row/column level comparison with their originating CSV files.
- Stage tables are each tested by comparison with a manually created 'expected results' CSV file.

## Key components
- Extract: Python scripts for data extraction, field-level transformations and data quality filters
- Load: Data ingestion into MySQL tables closely resembling data sources
- Transform: DBT models for data integration and dimensional modelling (facts and dimensions)
- Orchestration: Python script handles pipeline execution and dependencies

## Documentation Sections
* [Architecture](architecture.md)
* [Data Model](data-model.md)
* [ETL Process](etl-process.md)
* [HESA Data Dictionary](hesa-data-dictionary.md)
* [Development Guide](development-guide.md)


<div style="margin: 3em 0 1em 0; border-top: 1px solid #ccc; padding-top: 1em;">
  <strong>Navigation:</strong>
  <a href="architecture.md">Architecture</a> |
  <a href="data-deliveries.md">HESA Deliveries</a> |
  <a href="data-model.md">Data Model</a> |
  <a href="pipeline-process.md">Pipeline Process</a> |
  <a href="scripts.md">Scripts</a> |
  <a href="daily.md">Development Journal</a>
</div>