FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && \
    apt-get upgrade -y && \
    apt-get install --no-install-recommends -y default-mysql-client && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Install Python dependencies and DBT
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt dbt-core dbt-mysql prefect

# Copy application code
COPY . .

# Command to run
CMD ["python", "flows/hesa_nn056_pipeline.py"]
