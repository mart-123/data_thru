# HESA Data Pipeline Project

This portfolio project demonstrates a data warehouse solution for higher education student data. It showcases a complete ETL pipeline from raw CSV files to dimensional data model, using industry-standard patterns and practices.

## Key Features

- **Multi-delivery data pipeline**: Handles multiple yearly HESA data deliveries with version tracking
- **Quality-assured data flow**: Validates, cleanses and transforms student demographics and enrolment data
- **Dimensional data model**: Star schema with surrogate keys that maintain data lineage
- **Automated testing**: Component tests for data transformations with comparison against expected results
- **Containerised deployment**: Docker and Docker Compose for consistent execution environments

## Architecture Overview

The solution implements a complete data pipeline with these components:

- **Python extraction layer**: Validates raw data, applies field-level transformations, isolates bad data
- **MySQL database**: Stores load tables, stage tables, and dimensional model
- **DBT transformation layer**: Handles staging, integration, and dimensional modeling
- **Docker infrastructure**: Containerises both database and application components

It reflects real-world practices as could be found in a university data warehouse.

## Getting Started
To run the containerised DB and pipeline: 
```bash
# Clone the repository
git clone https://github.com/mart-123/data_thru.git
cd data_thru

# Start the containers
docker-compose up -d

# Run the pipeline
docker-compose run --rm app python [hesa_nn056_pipeline.py](http://_vscodecontentref_/0)

# Monitor logs
docker-compose logs -f app
```

For local development without Docker:
1. Set up MySQL database
2. Configure `.env` file with database connection details
3. Install requirements: `pip install -r requirements.txt`
4. Run the pipeline: `python3 flows/hesa_nn056_pipeline.py`


## Documentation
For more detailed information, please see the documentation sections below:
* [Architecture](architecture.md)
* [Data Deliveries](data-deliveries.md)
* [Data Model](data-model.md)
* [Pipeline Process](pipeline-process.md)
* [HESA Data Info](hesa-data-info.md)
* [Development Guide](development-guide.md)

