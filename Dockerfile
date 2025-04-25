FROM python:3.11-slim

# Assign name to application root directory
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

# Set Python path to include app directory
ENV PYTHONPATH=/app

# Command to run
CMD ["python", "-u", "flows/hesa_nn056_pipeline.py"]
#CMD ["python", "-u", "test_import.py"]
