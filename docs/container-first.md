# Container-First Development Approach

This project adopts a "container-first" approach where all execution happens inside containers rather than on your local machine. This provides several benefits:

1. **Consistent Environment**: Everyone works with identical dependencies and configurations
2. **Simplified Onboarding**: New developers can start without complex local setup
3. **Reliable Database Connectivity**: Container networking eliminates connection/permission issues
4. **Identical Dev/Test/Prod**: Local development matches production environment

## Dependency Management

Dependencies are handled differently for execution vs. development:

**Execution Dependencies** (in container):
- Defined in `requirements.txt`
- Installed during Docker image build
- To add a new package:
  ```bash
  # 1. Add to requirements.txt
  echo "new-package==1.0.0" >> requirements.txt
  
  # 2. Rebuild the container
  docker compose build app
  ```

**Development Dependencies** (local machine):
- The `venv_dev` environment is for IDE intelligence only
- It should mirror the container dependencies
- After adding packages to requirements.txt:
  ```bash
  # Update local dev environment for code completion
  source venv_dev/bin/activate
  pip install -r requirements.txt
  ```

## Common Development Commands

```bash
# Run Python scripts in container
./run_container_py.sh flows/hesa_nn056_pipeline.py

# Run DBT commands in container
./run_container_dbt.sh run

# Start an interactive bash session in the container
docker compose exec app bash

# View logs in real-time
docker logs -f data-thru-etl-app
```

<div style="margin: 1em 0; min-height: 20px;"></div>

This approach eliminates "works on my machine" problems and ensures consistent behavior across all environments.


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