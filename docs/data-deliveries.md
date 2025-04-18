# HESA Deliveries
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
- CSV files per delivery are stored in separate directories

### Database Structure
- Each load table has a `hesa_delivery` column denoting what CSV file collection it is from
- Each dimension table has compound natural key including `hesa_delivery`
- This means one student/program/religion code/etc appears multiple times in each dimension (once per delivery they were included in)

### ETL Process
- Orchestration scripts ensure pipeline is executed for each HESA delivery
- Staging models combine deliveries using UNION operations
- Dimension models maintain delivery context in surrogate keys

### Data Access
- Queries can filter on specific deliveries or span multiple deliveries
- Reports can compare data across different deliveries


## Multi-Delivery Advantages
Embedding the delivery id in natural keys of the dimensional model provides:

1. **Historical Data Preservation**: All deliveries and revisions are retained
2. **Trend/Change Tracking**: Differences between deliveries can be analysed
3. **Version-Specific Reporting**: Reports can target specific deliveries
4. **Data Lineage**: Each record maintains its delivery context throughout the pipeline



