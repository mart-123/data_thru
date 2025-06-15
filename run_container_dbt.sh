#!/bin/bash
# Runs DBT command inside ETL container
docker compose run --rm app dbt "$@" --profiles-dir=/app/dbt/profiles --project-dir=/app/dbt
