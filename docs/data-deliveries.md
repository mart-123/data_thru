# HESA Deliveries
This document explains the concept of 'HESA Deliveries' and how it is implemented in the HESA Student data pipeline.

## Background
Universities send an annual student data 'collection' to HESA. It contains academic activity, outcomes and personal information.

HESA enriches the data with statistical data and 'delivers' an enriched dataset back to each institution for data analysis. HESA may issue revisions of the same 'delivery'.


## 'HESA Delivery' Identifier
Each delivery is identified by a unique code in the format `YYXXX_YYYYMMDD`:
- `YY` : academic year of original data ('22' for 2022/23)
- `XXX` : HESA schema used ('056' for student collections 2022-2026)
- `YYYYMMDD` : date the enriched data was delivered

For example:
- `22056_20240331`: March 2024 delivery for 2022-23 collection
- `23056_20250331`: March 2025 delivery for the 2023-24 collection


## Implementation in the Data Model

The HESA delivery concept impacts various aspects of the data architecture and pipeline.

### Delivery Datasets
- Each delivery has a separate sub-directory in `data/deliveries`
- Main CSV data files are moved into the subdirectory
- A set of lookup files should also be copied to the subdirectory

### Database Structure
- Each load table has `hesa_delivery` column indicating the delivery id
- Each dimension table has compound natural key including `hesa_delivery`
- This means one student/program/religion code/etc appears multiple times in each dimension (once per delivery they were included in)

### Pipeline Processing
- Orchestration scripts ensure pipeline is executed for each HESA delivery
- Staging models combine deliveries using UNION operations
- Dimension models maintain delivery context in surrogate keys

### Data Access
- Queries can filter on specific deliveries or span multiple deliveries
- Reports can compare data across different deliveries


## Receiving a new Delivery from HESA
When new delivery is received:
- Create load tables via scripts like `create_hesa_23056_load_tables.py`
- Update staging models to UNION the new load tables
- Add extract/load steps to Prefect orchestrator


## Multi-Delivery Advantages
Embedding the delivery id in natural keys of the dimensional model provides:

1. **Historical Data Preservation**: All deliveries and revisions are retained
2. **Trend/Change Tracking**: Differences between deliveries can be analysed
3. **Version-Specific Reporting**: Reports can target specific deliveries
4. **Data Lineage**: Each record maintains its delivery context throughout the pipeline



