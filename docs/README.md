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


## Architecture Overview
The solution implements a complete data pipeline with these components:

- **Python extraction layer**: Validates raw data, applies field-level transformations, isolates bad data
- **MySQL database**: Stores load tables, stage tables and dimensional model
- **DBT transformation layer**: Handles staging, integration and dimensional modelling
- **Docker infrastructure**: Containerises database and application components, orchestrated with Docker Compose

It reflects real-world practices as could be found in a university data warehouse.


<div style="margin: 1em 0; min-height: 20px;"></div>


## Getting Started

### Note on Python Commands
This documentation uses `python` to refer to the Python interpreter command. Depending on your system:
- On Windows: Use `python` 
- On some Linux/Mac systems: You might need to use `python3` if both Python 2 and 3 are installed

Example:
```bash
# Windows
python utils/create_dim_date.py

# Some Linux/Mac systems with both Python 2 & 3
python3 utils/create_dim_date.py
```

### Prerequisites
1. Clone the repository
```bash
   git clone https://github.com/mart-123/data_thru.git
   cd data_thru
```

2. Create `.env` file based on `.env.example`


### Option 1: Containerised Execution (Recommended)

1. If required, set data and log mappings in `docker-compose.yml` to match local directories (for quick test run, no changes needed):
```bash
# Update these paths to match your environment:
#  [your_data_location]:/app/_mounts/data
#  [your_logs_location]:/app/_mounts/logs
nano docker-compose.yml
```

2. Customise MySQL credentials in `.env`.

3. Start MySQL container
```bash
docker compose up -d
```

4. Create dim_date and load tables (for 22056, 23056, etc):
```bash
# If necessary, create and populate dim_date
docker compose run --rm app python utils/create_dim_date.py

# Repeat for 22056, 23056, etc
docker compose run --rm app python utils/create_hesa_22056_load_tables.py
```

5. Run containerised ETL pipeline: 
```bash
# Run ETL pipeline (--build if running for first time)
docker compose --profile etl up --build -d

# To manually re-run pipeline in temporary container (without restarting everything)
docker compose run --rm app python flows/hesa_nn056_pipeline.py
```

### Option 2: Local Development Setup

1. Install MySQL and create a database for the project.

2. Customise MySQL credentials in `.env`.

3. Set up Python environment:
```bash
# Create virtual environment
python -m venv venv_dev

# Activate virtual environment
# On Linux/Mac:
source venv_dev/bin/activate
# On Windows:
# venv_dev\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

4. Create dim_date and load tables (for 22056, 23056, etc):
```bash
# If necessary, create and populate dim_date
python utils/create_dim_date.py

# Repeat for 22056, 23056, etc
python utils/create_hesa_22056_load_tables.py
```

5. Run pipeline: `python flows/hesa_nn056_pipeline.py`


### Checking Results
1. Check extract and load logs:
```bash
#  (default location <local repo>/data_thru/_mounts/logs)
cat [your_mounted_path]/logs/etl_info.log
cat [your_mounted_path]/logs/etl_error.log
```

2. Check DBT transformation logs:
For local execution these are written to the console.
```bash
# View container output including DBT stages/dimensions/facts
docker logs data-thru-etl-app

# For live monitoring (follow mode)
docker logs -f data-thru-etl-app
```

3. Check for data quality exceptions (e.g. `[your_mounted_path]/data/bad_data/*.csv/`)

4. Run queries against the dimensional model to validate results.


<div style="margin: 1em 0; min-height: 20px;"></div>


## Automated Testing
A suite of component test scripts and expected results has been build in `/tests/component/`

to run an individual test script (with detailed console output):
```bash
# --run-etl : if present, test script first runs the script it is testing
python -m tests.component.run_component_tests
```

To run all component test scripts:
```bash
# --run-etl : if present, each test scripts first run the script it is testing
python -m tests.component.run_component_tests --run-etl
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