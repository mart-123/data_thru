# Data Engineering Portfolio Project Journey

## Technical Evolution
### January 2025: Foundation & Core ETL Development
- Developed initial ETL pipeline with Python and Pandas
- Implemented data validation and good/bad results files
- Initial implementation of multi-delivery processing
- Transitioned from SQLite to MySQL for WSL compatibility and overall feature set

### February 2025: Performance & Integration
- Improved CSV validation using batching and parallel processing
- Built pipeline orchestration script
- Stage tables integrating multi-entity data (students, demographics, programs)
- Implemented structured error management with detailed reason tracking
- Refined pipeline for consistent handling of multiple deliveries

### March 2025: Architecture & Testing
- Implemented OOP principles for improved code organization
- Developed component-based testing strategy using "expected results" files
- Containerised database with Docker for consistent environment
- Improved database connection management with retry and consistent transaction handling

### April 2025: DBT, Modelling, Containerise, Deployment
- Re-implemented stage tables with DBT
- Implemented dimensional modelling with DBT
- Created full Docker containerization with volume mapping 
- Developed documentation framework

### May 2025: 
- Wiki content and 'Getting Started/Install Guide'
- Refactored code to simplify config
- Canonical keys to enable cross-delivery reporting

<div style="margin: 2em 0; min-height: 30px;"></div>


# Key Learnings

## Python Data Engineering
- **Pandas Optimization**: Learned to handle chunked processing and type inference challenges
- **Parallelization**: Implemented multi-processing for transforms but discovered it wasn't beneficial for this I/O-bound workload

## Database Technologies
- **MySQL vs SQLite**: Transitioned from SQLite to MySQL to resolve WSL compatibility issues
- **Connection Management**: Implemented retry logic and proper transaction handling

## Containerization
- **Docker Compose**: Configured multi-container setup with database and application containers
- **Volume Mapping**: Configured persistent storage for containerised pipeline and MySQL database

## Testing & Quality
- **Component Testing**: Developed automated tests for extract, load and staging processes
- **Data Quality Management**: Implemented comprehensive validation and detailed error tracking

## Dimensional Modelling
- **DBT Implementation**: Applied DBT for staging, dimensional modelling, data integration
- **Dual-Key Pattern**: Developed delivery-based surrogate keys and canonical keys, supporting lineage and cross-delivery analysis
- **Code Mapping**: Implemented canonical code mapping for lookup dimensions to handle changing codes/meanings between deliveries
- **Dimensional Design**: Built star schema with dimension/fact separation and human-readable surrogate keys

<div style="margin: 2em 0; min-height: 30px;"></div>


# Data Warehouse Project Journey

## Project Timeline

```mermaid
gantt
    title Pipeline Development Timeline
    dateFormat  YYYY-MM-DD
    axisFormat %b %d
    
    section 1. Foundation
    Initial Setup & Source Data Creation   :2025-01-20
    Data Quality Framework Implementation  :2025-01-25
    MySQL Database Architecture           :2025-01-28
    
    section 2. Data Processing
    Parallel Processing Implementation    :2025-02-06
    Structured Error Handling System      :2025-02-10
    Multi-Delivery Support Pattern        :2025-02-15
    
    section 3. Advanced ETL
    Dynamic Field-Level Transformations   :2025-03-01
    Automated Data Validation Framework   :2025-03-08
    Component-Based Testing Strategy      :2025-03-17
    
    section 4. Dimensional Model & Deployment
    DBT Star Schema Implementation        :2025-04-01
    Docker Multi-Container Architecture   :2025-04-10

    section 5. Documentation, Code Improvements
    Markdown Wiki                         :2025-04-20
    Installation/Developer Guide          :2025-04-24
    Code Refactor                         :2025-05-06

    section 6. Canonical Keys
    Canonical Keys and Code Mappings      :2025-05-08
```


<div style="margin: 2em 0; min-height: 30px;"></div>


## Key Technical Implementations

### ETL Pipeline
- **Multi-Phase Processing**: Built an ELT pipeline with extract, load, and transform phases
- **Parallel Processing**: Used batched validation and multi-processing to improve resource usage
- **Error Handling**: Implemented validation with detailed error tracking for failed records
- **Multi-Delivery Support**: Created parameterised execution for multiple data deliveries

### Data Quality Management
- **Validation**: Added field-level checks for formats, data types, and relationships
- **Error Tracking**: Captured specific reasons for validation failures
- **Data Lineage**: Maintained delivery codes throughout the pipeline

### Database & Transformation
- **Dimensional Modelling**: Created star schema
- **DBT Layers**: Built transformation layers from raw to dimensional models
- **SQL Patterns**: Used UNION, JOIN, and DISTINCT operations for multiple deliveries
- **Canonical Keys**: Dual-key approach with delivery-specific surrogate keys and canonical business keys for cross-delivery analytics
- **Code Mappings**: Standardised canonical codes for lookup dimensions to handle changing codes/labels between deliveries

### System Architecture
- **Docker Containers**: Set up multi-container solution with volume mapping
- **Configuration**: Created centralised configuration system for different environments
- **Error Management**: Added connection retry logic and transaction handling


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