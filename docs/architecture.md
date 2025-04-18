# Architecture Overview

## Pipeline Components

```mermaid
graph TD
    A[Extract Scripts] -->|CSV Files| B[Load Scripts]
    B -->|MySQL Tables| C[DBT Staging]
    C -->|Stage Models| D[DBT Dimensions]
    D -->|Dimension Tables| E[DBT Facts]
    F[Prefect Flow] -->|Orchestrates| A
    F -->|Orchestrates| B
    F -->|Orchestrates| C
    F -->|Orchestrates| D
    F -->|Orchestrates| E
mermaid```

