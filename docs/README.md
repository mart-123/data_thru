# HESA Data Pipeline Project
This portfolio project demonstrates a data warehouse solution for higher education student data. It showcases a complete ETL pipeline from raw CSV files to dimensional data model, using industry-standard patterns and practices.


<div style="margin: 1em 0; min-height: 20px;"></div>


## On this Page
- [Project Documentation](#project-documentation)
- [Key Features](#key-features)
- [Architecture Overview](#architecture-overview)
- [Getting Started](#getting-started)
- [Automated Testing](#automated-testing)


<div style="margin: 1em 0; min-height: 20px;"></div>


## Project Documentation
This README provides a high-level overview. For detailed information, please see these **separate wiki pages**:
* [Architecture](architecture.md) - Detailed design and component descriptions
* [Data Deliveries](data-deliveries.md) - Format and structure of input data
* [Data Model](data-model.md) - Star schema and dimension details
* [Pipeline Process](pipeline-process.md) - ETL workflow and processing steps
* [HESA Data Info](hesa-data-info.md) - Background on the higher education data
* [Development Guide](development-guide.md) - Contributing to the project


<div style="margin: 1em 0; min-height: 20px;"></div>


## Key Features
- **Multi-delivery data pipeline**: Handles multiple yearly HESA data deliveries with version tracking
- **Quality-assured data flow**: Validates, cleanses and transforms student demographics and enrolment data
- **Dimensional data model**: Star schema with surrogate keys that maintain data lineage
- **Automated testing**: Component tests for data transformations with comparison against expected results
- **Containerised deployment**: Docker and Docker Compose for consistent execution environments


<div style="margin: 1em 0; min-height: 20px;"></div>


## Architecture Overview
The solution implements a complete data pipeline with these components:

- **Python extraction layer**: Validates raw data, applies field-level transformations, isolates bad data
- **MySQL database**: Stores load tables, stage tables, and dimensional model
- **DBT transformation layer**: Handles staging, integration, and dimensional modelling
- **Docker infrastructure**: Containerises both database and application components

It reflects real-world practices as could be found in a university data warehouse.


<div style="margin: 1em 0; min-height: 20px;"></div>


## Getting Started

### Prerequisites
1. Clone the repository
```bash
   git clone https://github.com/mart-123/data_thru.git
   cd data_thru
```

2. Create log and data directory structures described in `architecture.md`

3. Create delivery directory (e.g. `data/deliveries/22056_20240331/`) and copy HESA CSV files there


### Option 1: Containerised Execution (Recommended)

1. Configure volume mappings in docker-compose.yml to match your local paths:
```bash
# Update these paths to match your environment:
#  [your-data-path]:/data
#  [your-log-path]:/log
nano docker-compose.yml
```

2. Customise MySQL credentials in `docker-compose.yml` (for both services/containers).

3. Start MySQL container
```bash
docker-compose up -d
```

4. Create dim_date and load tables (for 22056, 23056, etc):
```bash
# IF NECESSARY, create and populate dim_date
docker-compose run --rm app python3 utils/create_dim_date.py

# Repeat for 22056, 23056, etc
docker-compose run --rm app python3 utils/create_hesa_22056_load_tables.py
```

5. Run containerised ETL pipeline: 
```bash
# Run ETL pipeline (also starts MySQL if necessary)
docker compose --profile etl up -d

# To re-run pipeline if container already running
docker-compose run --rm app python3 flows/hesa_nn056_pipeline.py
```


### Option 2: Local Development Setup

1. Install MySQL and create a database for the project.

2. Create `.env` file with these variables:
```bash
BASE_DIR=/path/to/project-root
DATA_DIR=/path/to/data
LOG_DIR=/path/to/log
CONFIG_FILE=/path/to/project/app_config/test_config.json
DB_HOST=localhost
DB_PORT=3306
DB_USER=dev_user
DB_PWD=dev_user_pwd
DB_NAME=college_dev
```

3. Install dependencies: `pip install -r requirements.txt`

4. Create dim_date and load tables (for 22056, 23056, etc):
```bash
# IF NECESSARY, create and populate dim_date
python3 utils/create_dim_date.py

# Repeat for 22056, 23056, etc
python3 utils/create_hesa_22056_load_tables.py
```

5. Run pipeline: `python3 flows/hesa_nn056_pipeline.py`


### Checking Results
1. Monitor execution and check table/row count details via log file:
```bash
# Check detailed log
cat [your-log-path]/test/etl_info.log

# Check error log
cat [your-log-path]/test/etl_error.log
```

2. Check DBT logs
```bash
# View DBT logs if containerised
docker-compose logs -f app

# (they otherwise appear in stdout in the shell)
```

3. Check for data quality exceptions (e.g. `data/bad_data/*.csv/`)

4. Run queries against the dimensional model

<div style="margin: 1em 0; min-height: 20px;"></div>

## Automated Testing
A suite of component test scripts and expected results has been build in `/tests/component/`

to run an individual test script (with detailed console output):
```bash
# --run-etl : if present, test script first runs the script it is testing
python3 -m tests.component.run_component_tests
```

To run all component test scripts:
```bash
# --run-etl : if present, each test scripts first run the script it is testing
python3 -m tests.component.run_component_tests --run-etl
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