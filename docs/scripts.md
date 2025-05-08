# Scripts
## Extract Scripts
### /flows/hesa_nn056_pipeline.py
Main pipeline orchestration script.
Manages execution order and dependencies.
Hard-coded parameters for multiple deliveries and look-up tables.

### /ingest/extract/extract_hesa_nn056_students.py
Reads and transforms HESA-supplied CSV file of student data conforming to HESA '056' schema.
Parameters: delivery_code (e.g. `22056_20240331`).
Input: `hesa_<delivery_code>_data_students.csv`
Main output: `hesa_<delivery_code>_students_transformed.csv`
Bad data: `hesa_<delivery_code>_students_bad_data.csv`
Transformations: convert nulls to empty strings, space trim, name split, email lowercasing, column renaming to match load table.
Data quality filters: rows with missing values, incomplete emails, bad dates are written to 'bad data' file.

### /ingest/extract/extract_hesa_nn056_demographics.py
Reads and transforms HESA-supplied CSV file of student demographic details conforming to HESA '056' schema.
Parameters: delivery_code (e.g. `22056_20240331`).
Input: `hesa_<delivery_code>_data_demographics.csv`
Main output: `hesa_<delivery_code>_demographics_transformed.csv`
Bad data: `hesa_<delivery_code>_demographics_bad_data.csv`
Transformations: convert nulls to empty strings, column renaming to match load table.
Data quality filters: rows with missing values are written to 'bad data' file.

### /ingest/extract/extract_hesa_nn056_student_programs.py
Reads and transforms HESA-supplied CSV file of student program enrolments conforming to HESA '056' schema.
Parameters: delivery_code (e.g. `22056_20240331`).
Input: `hesa_<delivery_code>_student_programs.csv`
Main output: `hesa_<delivery_code>_student_programs_transformed.csv`
Bad data: `hesa_<delivery_code>_student_programs_bad_data.csv`
Transformations: convert nulls to empty strings, convert fees flag to uppercase, column renaming to match load table.
Data quality filters: rows with missing values, unexpected values, bad dates are written to 'bad data' file.


<div style="margin: 2em 0; min-height: 30px;"></div>


## Load Scripts
### /ingest/load/load_hesa_nn056_students.py
Copies transformed student data to load table (HESA '056' schema).
Parameters: delivery_code (e.g. `22056_20240331`).
Input: CSV file `extracted_hesa_<delivery_code>_students.csv`
Output: Table `load_hesa_<delivery_code>_students.csv`

### /ingest/load/load_hesa_nn056_demographics.py
Copies student demographic data to load table (HESA '056' schema).
Parameters: delivery_code (e.g. `22056_20240331`).
Input: CSV file `extracted_hesa_<delivery_code>_demographics.csv`
Output: Table `load_hesa_<delivery_code>_demographics.csv`

### /ingest/load/load_hesa_nn056_student_programs.py
Copies student-program enrolments to load table (HESA '056' schema).
Parameters: delivery_code (e.g. `22056_20240331`).
Input: CSV file `extracted_hesa_<delivery_code>_student_programs.csv`
Output: Table `load_hesa_<delivery_code>_student_programs.csv`

### /ingest/load/load_hesa_nn056_lookup_table.py
Copies a HESA look-up table to its load table.
Parameters: delivery_code (e.g. `22056_20240331`), look-up name (e.g. `ETHNICITY`)
Input: CSV file `hesa_<delivery_code>_lookup_<lookup_name>.csv`
Output: Table `load_hesa_<delivery_code>_lookup_<lookup_name>.csv`


<div style="margin: 2em 0; min-height: 30px;"></div>


## DBT Models
### Staging Models
Staging models serve mainly to combine data from multiple deliveries, but some normalisation (student/programs) also occurs

#### /dbt/models/staging/stage_hesa_nn056_students.sql
Combines student and demographic data. Also combines data from multiple deliveries.
Input: `load_hesa_<delivery_code>_students`, `load_hesa_<delivery_code>_demographics`  
Output: `stage_hesa_nn056_student`
Key operations: INNER JOIN (combine student/demographic), UNION (combine deliveries)

#### /dbt/models/staging/stage_hesa_nn056_programs.sql
Gets unique program codes from student-program relationships. Also combines data from multiple deliveries.
Input: `load_hesa_<delivery_code>_student_programs`  
Output: `stage_hesa_nn056_programs`
Key operations: SELECT DISTINCT (unique students), UNION (combine deliveries)

#### /dbt/models/staging/stage_hesa_nn056_student_programs.sql
Gets student-program relationships across deliveries. Also combines data from multiple deliveries.
Input: `load_hesa_<delivery_code>_student_programs`  
Output: `stage_hesa_nn056_student_programs`
Key operations: JOIN (validate students/programs), UNION (combine deliveries)

#### /dbt/models/staging/stage_hesa_nn056_lookup_ethnicity.sql (and other lookups)
Combines lookup data from multiple deliveries.
Input: `load_hesa_<delivery_code>_lookup_<name>`  
Output: Combined lookup codes with delivery context
Key operations: UNION
Note: Similar models exist for RELIGION, GENDER, SEXID, SEXORT, TRANS, etc.


### Dimension Models
Dimension models have human-readable surrogate keys to support data inspection.

#### /dbt/models/dimensions/dim_hesa_student.sql
Creates the student dimension with delivery-aware surrogate keys.
Dimension key pattern: `STU_<student_guid>_<delivery_code>`  
Input: `stage_hesa_nn056_students`  
Output: Student dimension with demographic attributes
Attributes: student name, contact details, demographic codes

#### /dbt/models/dimensions/dim_hesa_program.sql
Creates the program dimension with delivery-aware surrogate keys.
Dimension key pattern: `PGM_<program_guid>_<delivery_code>`  
Input: `stage_hesa_nn056_programs`  
Output: Program dimension with program details
Attributes: program code, program name

#### /dbt/models/dimensions/dim_hesa_ethnicity.sql (and other dimensions)
Creates lookup dimensions from lookup tables.
Dimension key pattern: `ETH_<code>_<delivery_code>`  
Input: `stage_hesa_nn056_lookup_ethnicity`  
Output: Ethnicity dimension with codes and labels
Note: Similar models exist for RELIGION, GENDER, SEXID, SEXORT, TRANS, etc.

### Fact Models
Fact tables connect dimensions and store measures.

#### /dbt/models/facts/fact_hesa_student_programs.sql
Creates the student program enrollment fact table.
Input: `stage_hesa_nn056_student_programs`, `dim_hesa_student`, `dim_hesa_program`  
Output: Student program enrollments with surrogate keys  
Measures: `fees_paid_bool` (0/1)  
Join operations: Inner joins to both student and program dimensions  
Grain: One row per student-program combination per delivery




<div style="margin: 3em 0 1em 0; border-top: 1px solid #ccc; padding-top: 1em;">
  <strong>Navigation:</strong>
  <a href="architecture.md">Architecture</a> |
  <a href="data-deliveries.md">HESA Deliveries</a> |
  <a href="data-model.md">Data Model</a> |
  <a href="pipeline-process.md">Pipeline Process</a> |
  <a href="hesa-data-info.md">HESA Data Info</a> |
  <a href="scripts.md">Scripts</a>
</div>