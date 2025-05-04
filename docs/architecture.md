# Architecture Overview
This document describes the architecture of the HESA data warehouse, including its ETL pipeline, database structure, code organisation and config management. It also explains design decisions that shaped the implementation.


<div style="margin: 2em 0; min-height: 20px;"></div>


## Contents
- [Pipeline Diagram](#pipeline-diagram)
- [Scripts](#scripts)
- [Orchestration](#orchestration)
- [Data Quality](#data-quality)
- [Config Management](#config-management)
- [Docker Containerisation](#docker-containerisation)
- [Error Handling](#error-handling)
- [Logging](#logging)
- [Design Decisions](#design-decisions)


<div style="margin: 2em 0; min-height: 20px;"></div>


## Pipeline Diagram
```mermaid
graph TD
    G[HESA Delivery Received] -->|Raw CSV Files| A[Extract Scripts]
    A -->|Cleansed Data| B[Load Scripts]
    A -->|Bad Data| F[(Data Quality Exceptions)]
    B -->|Load Tables| C[DBT Integrations]
    C -->|Stage Tables| D[DBT Dimensional Modelling]
    D -->|Dimensional Data| E[(Star Schema)]
```
<style> .mermaid { width: 50%; margin: 0 auto; } </style>


<div style="margin: 2em 0; min-height: 20px;"></div>


## Script Architecture
The codebase incorporates Python scripts and DBT models.

- **Core Classes**:
  - `TableCopier`: Copies data from one table to another
  - `CsvTableCopier`: Copies data from a CSV file to a table
  - Both implement batching/chunk-based processing for memory efficiency
  - Note: `TableCopier.py` currently unused as staging onwards now handled by DBT

- **Python Scripts**:
  - Extract Scripts: Process CSV files into cleansed format with data validation
  - Load Scripts: Move cleansed data into database tables using copier classes
  - Delivery Code: Scripts take 'delivery code' argument to enable multiple deliveries to be ingested
  - Look-up name: Generic look-up table loader takes 'look-up name' argument

- **Parameterised Execution**:
  - Orchestration: `hesa_nn056_pipeline.py` manages execution order and dependencies
  - Delivery Codes: Hard-coded in orchestration script, for each delivery to be ingested
  - Look-up table names: Hard-coded in orchestration script for each look-up table to be ingested 
  - Phase dependencies: Extract/load/stage/dim/fact phases are each dependent on preceding phase

<div style="margin: 2em 0; min-height: 30px;"></div>


## Orchestration
- Script `hesa_nn056_pipeline.py` handles pipelines execution order and dependencies.
- Pipeline has several phases: extract, load, stage, dimensions and facts
- Each phase is dependent on prior phase success

<div style="margin: 2em 0; min-height: 30px;"></div>


## Data Quality
Data quality filtering occurs in the extract scripts. Records are validated and output to either a 'transformed' or 'bad data' file.

- **Validation During Extraction**:
  - Format checks (e.g., email format, dates)
  - Missing or incomplete checks (mandatory values)
  - Format verification

- **Data Quality Filtering**:
  - Invalid records are diverted to "bad data" files
  - Reasons for "bad data" are appended in the bad data files

- **Resolving Data Issues**
  - Bad data may be re-issued by HESA or fixed by Warehouse team if expedient
  - Data corrected by HESA should be provided in completely new 'Delivery'
  - Simple data corrections done locally can be reloaded by re-running pipeline

This approach provides detailed data quality information for remediation, and improves stability by quarantining the bad data.


<div style="margin: 2em 0; min-height: 30px;"></div>


## Config Management

- **Environment Variables**
  - Hold DB credentials and filepath for main config file
  - Stored in `.env` file for host execution
  - Stored in `docker-compose.yml` for containerised execution

- **Main Config File**:
  - Typically named `config.json`, `test_config.json` or similar
  - Holds nested directory structure for CSV data, logs, DBT models, etc

- **Loading Config**:
  - Function `get_config()` in `data_platform_core.py` handles config loading
  - It attempts to load `.env` into environment variables (if file not found, env variables should have been set up directly in `docker-compose.yml`)
  - Main config file, which is loaded into dictionary `config`

This approach enables:
  - Standard config across the codebase
  - Different MySQL DB and CSV directories per environment (dev, test, live)
  - Containerised or direct execution in host system.


<div style="margin: 2em 0; min-height: 30px;"></div>


## Docker Containerisation
The warehouse is containerised using Docker (local execution also supported during development):

- **Multi-Container Structure**:
  - MySQL container for database storage
  - Application container for pipeline execution

- **Volume Mapping**:
  - Data directories mapped to host for persistence
  - Log directories mounted to facilitate easier debugging
  - DBT profiles directory mounted to provide database connection information

- **Configuration**:
  - Database connection parameters passed via variables (for containerised execution)
  - Database connection parameters stored in `.env` file (for local execution)
  - Application config stored in `config.json` file
  - Data and log directories mounted via Docker Compose script

This approach enables consistent deployment while maintaining flexibility for local development.


<div style="margin: 2em 0; min-height: 30px;"></div>


## Error Handling
Errors are mitigated, isolated and reported as follows:

- **Transaction Management**:
  - Python scripts use commit/rollback logic on success/error

- **Retry Logic**:
  - Database connections include retry logic, mainly to handle container spin-up for MySQL
  - Configurable retry parameters in `connect_to_db()` function

- **Error Isolation**:
  - Data quality issues are quarantined rather than stopping the pipeline
  - Major pipeline phases execute sequentially with dependency checks
  - Scripts inside each pipeline phase can run independently of each other
  - Enables consistent deployments across environments

- **Pipeline Logging**:
  - Pipeline progress (script names, row counts) logged to persistent logfile
  - Pipeline errors (exceptions and unrecoverable errors) are logged to a separate error log

- **Restartable**:
  - Processes are restartable without having to re-run prior dependencies
  - Scripts contain no special 'restart from' logic, always processing entire dataset


<div style="margin: 2em 0; min-height: 30px;"></div>


## Logging
The system uses a structured logging approach implemented through `data_platform_core.py`:

- **Log Configuration**:
  - `set_up_logging()` function configures logging for each script
  - Log levels are configurable through configuration files

- **Log Destinations**:
  - File logging for permanent record
  - Console output for interactive monitoring
  - Container logs captured by Docker logging driver

- **Log Content**:
  - Execution progress and milestones
  - Performance metrics (execution time, row counts)
  - Error details with context
  - Data quality statistics

This logging strategy supports both operational monitoring and post-execution analysis.


<div style="margin: 2em 0; min-height: 30px;"></div>


## Design Decisions
### Technology Choices
- **Python for extraction**: Provides flexibility for complex field-level transformations and validation
- **MySQL for database**: Chosen for balance of simplicity and functionality in a demonstration project
- **DBT for transformations**: Enables SQL-based transformation with version control and testing
- **Docker for deployment**: Ensures consistent environment across development and deployment

### Architectural Patterns
- **ETL vs ELT**: In practice not always possible or desirable to separate extract and transform logic, some transformation occurs in extract scripts
- **Field-level transformations in extract phase**: Name parsing happens during extraction for efficiency
- **Parameterised scripts**: All extract and load scripts take a 'delivery code' argument, so that multiple datasets can be processed without new scripts
- **Separation of concerns**: Distinct phases of orchestration make codebase/problem resolution easier to reason about

### Data Quality Management
- **Format validation**: Data validated during initial extract phase
- **Data quarantine**: Bad data removed and reportable, preventing pipeline execution errors
- **Bad data handling**: Invalid records are captured with reason codes for analysis
- **Testing strategy**: Automated testing at both load and stage levels

### Error Handling & Resilience
- **Component isolation**: Each pipeline phase operates independently, allowing for partial runs
- **Explicit dependency management**: Orchestration script enforces proper sequencing



<div style="margin: 3em 0 1em 0; border-top: 1px solid #ccc; padding-top: 1em;">
  <strong>Navigation:</strong>
  <a href="architecture.md">Architecture</a> |
  <a href="data-deliveries.md">HESA Deliveries</a> |
  <a href="data-model.md">Data Model</a> |
  <a href="pipeline-process.md">Pipeline Process</a> |
  <a href="scripts.md">Scripts</a> |
  <a href="daily.md">Development Journal</a>
</div>