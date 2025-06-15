# HESA Data Reference
This page describes the HESA data that is the subject of this data warehouse project. Note that, whilst HESA has a 3NF internal data store, some of the CSV files they provide are denormalised in order to reduce the number of files.


<div style="margin: 2em 0; min-height: 30px;"></div>


## HESA Documentation
HESA provides extensive documentation about their student data schema and the CSV files that they provide.

| Documentation | Notes | 
|--------|---------|
| [HESA's official documentation](https://www.hesa.ac.uk/collection/c22056) | Information about the 22056 student data collection |


<div style="margin: 2em 0; min-height: 30px;"></div>


## Entities

| Entity | Purpose | Source File | Key Transformations |
|--------|---------|------------|---------------------|
| Student | Core student details | hesa_<delivery_code>_data_students.csv | Name parsing, email normalization |
| Demographics | Per-student demographic information | hesa_<delivery_code>_data_demographics.csv | None |
| Student Program | Per-year enrolment status of students on their programs. Also includes program details in a denormalised manner. | hesa_<delivery_code>_data_student_programs.csv | None |
| Lookup tables | Coded data e.g. ethnicity, religion codes | hesa_<delivery_code>_lookup_*.csv | None |


<div style="margin: 2em 0; min-height: 30px;"></div>


## Look-up Tables
| Name | Description |
|------|-------------|
| DISABILITY | Disability classifications |
| ETHNICITY | Ethnic background |
| GENDERID | Gender identity |
| RELIGION | Religious affiliation |
| SEXID | Sexual identity |
| SEXORT | Sexual orientation |
| TRANS | Denotes whether gender id matches birth certificate |
| Z_ETHNICGRP1 | Ethnic grouping (most granular) |
| Z_ETHNICGRP2 | Ethnic grouping (medium granularity) |
| Z_ETHNICGRP3 | Ethnic grouping (most general) |


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