#!/bin/bash
# Runs Python script inside ETL container
docker compose run --rm app python "$@"
