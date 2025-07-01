# Getting Started
This page describes how to download, config, run and check results of the ETL pipeline.

## Pre-requisites
This project is designed for Linux-based environments. The following are recommended:
- Python 3.8+ (includes pip)
- Linux/macOS system or WSL2 on Windows
- Docker and Docker Compose for containerised execution
- Git

## Download project
1. Clone the repository
```bash
    git clone https://github.com/mart-123/data_thru.git
    cd data_thru
```

2. Create `.env` file based on `.env.example`, customising the MySQL credentials.


## Containerised Config and Execution
Hosts a MySQL database in Docker (persisting in local Docker volume). Builds the ETL pipeline into a Docker image, and orchestrates both using Docker Compose.

1. Set data and log directories in `docker-compose.yml` to local directories (this step is not essential, test data and logging is pre-configured):
```bash
# Update these paths to match your environment:
#  [your_data_location]:/app/_mounts/data
#  [your_logs_location]:/app/_mounts/logs
nano docker-compose.yml
```

2. Set up Python virtual environment and project requirements:
(This step is for VS Code intelligence only; execution dependencies are installed during Docker build)
```bash
# Create virtual environment (only for VS Code intelligence - dependencies are installed by Docker)
sudo apt install python3-venv
python3 -m venv venv_dev
source venv_dev/bin/activate
pip install -r requirements.txt
```

3. Start MySQL container, create ETL user, create non-DBT managed tables:
```bash
./setup.sh
```

4. (A) Running ETL pipeline in temporary container (recommended): 
```bash
# Run ETL pipeline (DBT logs written only to console)
./run_container_py.sh flows/hesa_nn056_pipeline.py

# Run ETL pipeline (DBT logs copied to log file)
./run_container_py.sh flows/hesa_nn056_pipeline.py | tee -a _mounts/logs/dbt_output_$(date +%Y%m%d_%H%M%S).log
```

4. (B) Running ETL pipeline in persistent container (for development/debugging): 
```bash
# Start persistent ETL container
docker compose --profile etl up -d

# Run a specific script in the persistent container
docker compose exec app python flows/hesa_nn056_pipeline.py

# Check console output
docker logs data-thru-etl-app

# Follow logs in real-time
docker logs -f data-thru-etl-app

# Stop the persistent container when done
docker compose --profile etl down
```

5. Executing commands with container wrappers: 
```bash
# Running a Python script in the temporary ETL container
./run_container_py.sh tests/component/run_component_tests.py

# Running DBT commands
./run_container_dbt.sh debug   # test connection
./run_container_dbt.sh run     # run all models
./run_container_dbt.sh run --select stage_hesa_nn056_students # specific model
```

# Checking Results
## Database Queries
MySQL runs in a container. To use a host-side GUI tool, connect using localhost:3307.

For simple command-line queries:
```bash
# Run a specific query
export $(grep -v '^#' .env | xargs)

docker compose exec -e MYSQL_PWD=${MYSQL_PASSWORD} mysql mysql -u ${MYSQL_USER} ${MYSQL_DATABASE} -e "SELECT COUNT(*) FROM dim_date;"
```

## Extract/Load Logs (Python scripts)
Extract/load progress is written to two persisting, cumulative log files, mounted from your host machine.
```bash
#  (default location <local repo>/data_thru/_mounts/logs)
cat [your_mounted_path]/logs/etl_info.log
cat [your_mounted_path]/logs/etl_error.log
```

## DBT Transformation Logs (for DBT stage/dimension/fact)
DBT progress is written to the console. If redirected as demonstrated above, it will be found in  `_mounts/logs/dbt_pipeline_$(date +%Y%m%d_%H%M%S).log`.


## Data Quality Exceptions
These are mapped to your host machine (e.g. `./_mounts/data/bad_data/*.csv/`)
```bash
#  (default location <local repo>/data_thru/_mounts/data/bad_data/)
cat [your_mounted_path]/logs/etl_info.log
cat [your_mounted_path]/logs/etl_error.log
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