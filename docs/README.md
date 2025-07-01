# HESA Data Pipeline Project
This portfolio project demonstrates a production-quality data warehouse solution for higher education student data. Industry-standard patterns and best practices are used to implement an ETL pipeline using Python, DBT, MySQL and Docker Compose. A subset of the HESA student dataset is used to demonstrate data cleansing, transformation, canonical mapping and dimensional modelling.

The following documentation describes the data, ETL flow, scripts, architectural decisions and step-by-step instructions for setup and execution.

<div style="margin: 1em 0; min-height: 20px;"></div>


## On this Page
- [Project Documentation](#project-documentation)
- [Key Features](#key-features)
- [Architecture Overview](#architecture-overview)
- [Installing and Running](#installing-and-running)
- [Checking Results](#checking-results)
- [Automated Testing](#automated-testing)


<div style="margin: 1em 0; min-height: 20px;"></div>


## Project Documentation
This README provides a high-level overview. For detailed information, please see these **separate wiki pages**:
* [Architecture](architecture.md) - Detailed design and component descriptions
* [Data Deliveries](data-deliveries.md) - Format and structure of input data
* [Data Model](data-model.md) - Star schema and dimension details
* [Pipeline Process](pipeline-process.md) - ETL workflow and processing steps
* [HESA Data Info](hesa-data-info.md) - Background on the HESA higher education dataset
* [Development Guide](development-guide.md) - Downloading, configuring and running the project


<div style="margin: 1em 0; min-height: 20px;"></div>


## Key Features
- **Multi-delivery data pipeline**: Handles multiple yearly HESA data deliveries with version tracking
- **Data quality measures**: Data is cleansed, validated and filtered with detailed error information
- **Dimensional data model**: Star schema for reporting
- **Canonical key mapping**: Supports trend analysis across datasets with different look-up categories
- **Automated testing**: Detailed component test cases with comparison against expected results
- **Containerised deployment**: Docker and Docker Compose for consistent execution environments


<div style="margin: 1em 0; min-height: 20px;"></div>


## Installation
This project uses a container-first approach for a consistent environment. 

For detailed setup instructions, see [Getting Started](getting-started.md).


<div style="margin: 1em 0; min-height: 20px;"></div>


## Architecture Overview
The solution implements a complete data pipeline with these components:

- **Python extraction layer**: Validates raw data, applies field-level transformations, isolates bad data
- **MySQL database**: Stores load tables, stage tables and dimensional model
- **DBT transformation layer**: Handles staging, integration and dimensional modelling
- **Docker infrastructure**: Containerises database and application components, orchestrated with Docker Compose

It reflects real-world practices as could be found in a university data warehouse.


<div style="margin: 1em 0; min-height: 20px;"></div>

## Installing and Running
Please refer to separate page 'Getting Started'.

<div style="margin: 1em 0; min-height: 20px;"></div>


## Automated Testing
Component test suite and expected results are in `/tests/component/` and `/_mounts/data/expected` 

To run an individual test script (with detailed console output):
```bash
# --run-etl : if present, test script first runs the script it is testing
./run_container_py.sh tests/component/test_stage_hesa_nn056_students.py
```

To run all component test scripts (and run ETL scripts in each case):
```bash
# --run-etl : if present, each test scripts first run the script it is testing
./run_container_py.sh tests/component/run_component_tests --run-etl
```


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