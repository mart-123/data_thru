# HESA Deliveries
This document explains the concept of 'HESA Deliveries' and how it is implemented in the HESA Student data pipeline.


## Contents
- [Background](#background)
- [Concepts](#concepts)
- [HESA Delivery Identifier](#hesa-delivery-identifier)
- [Delivery Data Flow Diagram](#delivery-data-flow-diagram)
- [Implementation in the Data Model](#implementation-in-the-data-model)
- [Receiving a new Delivery from HESA](#receiving-a-new-delivery-from-hesa)
- [Multi-Delivery Advantages](#multi-delivery-advantages)


<div style="margin: 2em 0; min-height: 20px;"></div>


## Background
Universities send an annual student data 'collection' to HESA. It contains academic activity, outcomes and personal information.

HESA enriches the data with statistical data and 'delivers' an enriched dataset back to each institution for data analysis. HESA may issue revisions of the same 'delivery'.


<div style="margin: 2em 0; min-height: 30px;"></div>


## Concepts
### Data Collections
- Each year, HESA 'collects' student data from all UK universities
- These can be identified by the academic year of the data (e.g. '22' means 202223 data)
- These are currently annual but may in future become intra-year

### CSV Deliveries
- HESA enriches the collected data and delivers it back to each university
- It is returned as a set of CSV files
- HESA may deliver revised versions of a dataset if errors occurred
- Every initial/revised data delivery should be preserved in the warehouse

### HESA Schemas
- 'HESA Schema' refers to HESA's internal 3NF model for student data
- Its structure can affect the shape of the CSV data delivery
- Prior to 2022, the HESA schema evolved each year
- In 2022 HESA published a stable schema called 'Data Futures', to remain stable for several years
- Stable schema '056' is for 2022 onwards, schema '057' will be used from 2028



<div style="margin: 2em 0; min-height: 30px;"></div>


## HESA Delivery Identifier
Each delivery is identified by a unique code in the format `YYXXX_YYYYMMDD`:
- `YY` : academic year of original data ('22' for 2022/23)
- `XXX` : HESA schema used ('056' for student collections 2022-2026)
- `YYYYMMDD` : date the enriched data was delivered

<br/>

For example:
- `22056_20240331`: March 2024 delivery for 2022-23 collection
- `23056_20250331`: March 2025 delivery for the 2023-24 collection


<div style="margin: 2em 0; min-height: 30px;"></div>


## CSV Directory Structure
Each data delivery is copied into its own sub-directory within a 'deliveries' directory. The ETL pipeline moves validated data into a 'transformed' directory for loading into the warehouse. Bad data flows into a 'bad data' directory for exception reporting. Please see `architecture.md` for directory structure.


<div style="margin: 2em 0; min-height: 30px;"></div>


## Delivery Data Flow Diagram

```mermaid
graph TD
    A[University] -->|Annual Data Collection| B[HESA]
    B -->|Enriched Data| C[Initial Delivery YYXXX]
    C -->|Delivery Renamed YYXXX_YYYYMMDD| D[University Directory Structure]
    B -->|Enriched Data Re-issued| E[Re-Issued Delivery YYXXX]
    E -->|Delivery Renamed YYXXX_YYYYMMDD| D
    B -->|Manual Download| Y[Look-up CSV Files]
    Y -->|Look-up Files Renamed YYXXX_YYYYMMDD| D
    D -->|Extract Phase| F[Load Tables per Delivery]
    F -->|Stage Phase| G[Stage Tables Combine Deliveries]
    G -->|Dimension Phase| H[Star Schema with Delivery-Aware Keys]
```


<div style="margin: 2em 0; min-height: 30px;"></div>


## Delivery Code in the Data Model
The HESA delivery concept impacts various aspects of the data architecture and pipeline.

### Delivery Datasets
- Each year HESA collects data and delivers an enriched version
- Each delivery is linked to its original collection year (`collection_code` '22056', '23056', '24056', etc)
- The warehouse team manually assigns a `delivery_code` by adding date of receipt to the collection code (e.g. '22056_20240331')
- Each delivery has a separate sub-directory in `<host>/data/deliveries`

### Look-up Tables
- HESA CSV files contain coded values such as ethnicity, religion codes
- Look-up lists are available on the HESA website as CSV files
- Warehouse team download look-up lists and copy to same sub-directory as main CSV files
- If canonical mappings are required, these can be wrangled using `lookup_mappings_ethnicity` as an example

### How Delivery Code Affects Database Structure
- A set of load tables is created for every HESA delivery
- Load tables for multiple deliveries are merged into one stage table by UNION
- Load and stage tables have `hesa_delivery` column indicating the delivery id
- Each dimension table has a compound natural key including `hesa_delivery`

### Pipeline Processing
- Orchestration script `hesa_nn056_pipelines.py` handles pipeline execution per HESA delivery
- Extract/load is via Python scripts parameterised per delivery code
- DBT staging combine deliveries using UNION operations
- Dimensional model maintains delivery context in surrogate keys

### Data Access
- Queries can filter on specific deliveries or span multiple deliveries
- Reports can compare data across different deliveries
- Changing look-up codes can be compared across deliveries via canonical keys

<div style="margin: 2em 0; min-height: 30px;"></div>


## Receiving a new Delivery from HESA
When new delivery is received:
- Create load tables via scripts like `create_hesa_23056_load_tables.py`
- Update staging models to UNION the new load tables
- Add extract/load steps to orchestration script (`hesa_nn056_pipeline.py` or new script if new HESA schema)


<div style="margin: 2em 0; min-height: 30px;"></div>


## Multi-Delivery Advantages
Embedding the delivery id in natural keys of the dimensional model provides:

1. **Historical Data Preservation**: All deliveries and revisions are retained
2. **Trend/Change Tracking**: Differences between deliveries can be analysed
3. **Version-Specific Reporting**: Reports can target specific deliveries
4. **Data Lineage**: Each record maintains its delivery context throughout the pipeline


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