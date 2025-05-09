# Scripts
## Extract Scripts
### /flows/hesa_nn056_pipeline.py
Main pipeline orchestration script.
Manages execution order and dependencies.
Hard-coded parameters for multiple deliveries and look-up tables.

### /ingest/extract/extract_hesa_nn056_students.py

| **Component** | **Details** |
|-----------|---------|
| **Parameters** | delivery_code (e.g. `22056_20240331`) |
| **Input** | `hesa_<delivery_code>_data_students.csv` |
| **Main output** | `hesa_<delivery_code>_students_transformed.csv` |
| **Bad data** | `hesa_<delivery_code>_students_bad_data.csv` |
| **Transformations** | Convert nulls to empty strings, space trim, name split, email lowercasing, column renaming to match load table |
| **Data quality filters** | Rows with missing values, incomplete emails, bad dates are written to 'bad data' file |

### /ingest/extract/extract_hesa_nn056_demographics.py

| **Component** | **Details** |
|-----------|---------|
| **Parameters** | delivery_code (e.g. `22056_20240331`) |
| **Input** | `hesa_<delivery_code>_data_demographics.csv` |
| **Main output** | `hesa_<delivery_code>_demographics_transformed.csv` |
| **Bad data** | `hesa_<delivery_code>_demographics_bad_data.csv` |
| **Transformations** | Convert nulls to empty strings, column renaming to match load table |
| **Data quality filters** | Rows with missing values are written to 'bad data' file |

### /ingest/extract/extract_hesa_nn056_student_programs.py

| **Component** | **Details** |
|-----------|---------|
| **Parameters** | delivery_code (e.g. `22056_20240331`) |
| **Input** | `hesa_<delivery_code>_student_programs.csv` |
| **Main output** | `hesa_<delivery_code>_student_programs_transformed.csv` |
| **Bad data** | `hesa_<delivery_code>_student_programs_bad_data.csv` |
| **Transformations** | Convert nulls to empty strings, convert fees flag to uppercase, column renaming to match load table |
| **Data quality filters** | Rows with missing values, unexpected values, bad dates are written to 'bad data' file |


<div style="margin: 2em 0; min-height: 30px;"></div>


## Load Scripts

| **Script** | **/ingest/load/load_hesa_nn056_students.py** |
|-----------|---------|
| Parameters | delivery_code (e.g. `22056_20240331`) |
| Input | CSV file `extracted_hesa_<delivery_code>_students.csv` |
| Output | Table `load_hesa_<delivery_code>_students.csv` |

<br>

| **Script** | **/ingest/load/load_hesa_nn056_students.py** |
|-----------|---------|
| Parameters | delivery_code (e.g. `22056_20240331`) |
| Input | CSV file `extracted_hesa_<delivery_code>_students.csv` |
| Output | Table `load_hesa_<delivery_code>_students.csv` |

<br>

| **Script** | **/ingest/load/load_hesa_nn056_demographics.py** |
|-----------|---------|
| Parameters | delivery_code (e.g. `22056_20240331`) |
| Input | CSV file `extracted_hesa_<delivery_code>_demographics.csv` |
| Output | Table `load_hesa_<delivery_code>_demographics.csv` |

<br>

| **Script** | **/ingest/load/load_hesa_nn056_student_programs.py** |
|-----------|---------|
| Parameters | delivery_code (e.g. `22056_20240331`) |
| Input | CSV file `extracted_hesa_<delivery_code>_student_programs.csv` |
| Output | Table `load_hesa_<delivery_code>_student_programs.csv` |

<br>

| **Script** | **/ingest/load/load_hesa_nn056_lookup_table.py** |
|-----------|---------|
| Parameters | delivery_code (e.g. `22056_20240331`), look-up name (e.g. `ETHNICITY`) |
| Input | CSV file `hesa_<delivery_code>_lookup_<lookup_name>.csv` |
| Output | Table `load_hesa_<delivery_code>_lookup_<lookup_name>.csv` |


<div style="margin: 2em 0; min-height: 30px;"></div>


## DBT Models
### Staging Models
Staging models serve mainly to combine data from multiple deliveries, but some normalisation (student/programs) also occurs

| **Script** | **/dbt/models/staging/stage_hesa_nn056_students.sql** |
|-----------|---------|
| Input | `load_hesa_<delivery_code>_students`, `load_hesa_<delivery_code>_demographics` |
| Output | `stage_hesa_nn056_student` |
| Operations | INNER JOIN (combine student/demographic), UNION (combine deliveries) |

<br>

| **Script** | **/dbt/models/staging/stage_hesa_nn056_programs.sql** |
|-----------|---------|
| Input | `load_hesa_<delivery_code>_student_programs` |
| Output | `stage_hesa_nn056_programs` |
| Operations | SELECT DISTINCT (unique students), UNION (combine deliveries) |

<br>

| **Script** | **/dbt/models/staging/stage_hesa_nn056_student_programs.sql** |
|-----------|---------|
| Input | `load_hesa_<delivery_code>_student_programs` |
| Output | `stage_hesa_nn056_student_programs` |
| Operations | JOIN (validate students/programs), UNION (combine deliveries) |

<br>

| **Script** | **/dbt/models/staging/stage_hesa_nn056_lookup_ethnicity.sql** |
|-----------|---------|
| Input | `load_hesa_<delivery_code>_lookup_<name>` |
| Output | Combined lookup codes with delivery context |
| Operations | UNION |
| Note | Similar models exist for RELIGION, GENDER, SEXID, SEXORT, TRANS, etc. |


### Dimension Models
Dimension models have human-readable surrogate keys to support data inspection.

| **Script** | **/dbt/models/dimensions/dim_hesa_student.sql** |
|-----------|---------|
| Dimension key pattern | `STU_<student_guid>_<delivery_code>` |
| Input | `stage_hesa_nn056_students` |
| Output | Student dimension with demographic attributes |
| Attributes | Student name, contact details, demographic codes |

<br>

| **Script** | **/dbt/models/dimensions/dim_hesa_program.sql** |
|-----------|---------|
| Dimension key pattern | `PGM_<program_guid>_<delivery_code>` |
| Input | `stage_hesa_nn056_programs` |
| Output | Program dimension with program details |
| Attributes | Program code, program name |

<br>

| **Script** | **/dbt/models/dimensions/dim_hesa_ethnicity.sql** |
|-----------|---------|
| Dimension key pattern | `ETH_<code>_<delivery_code>` |
| Input | `stage_hesa_nn056_lookup_ethnicity` |
| Output | Ethnicity dimension with codes and labels |
| Note | Similar models exist for RELIGION, GENDER, SEXID, SEXORT, TRANS, etc. |

<br>

### Fact Models
Fact tables connect dimensions and store measures.

| **Script** | **/dbt/models/facts/fact_hesa_student_programs.sql** |
|-----------|---------|
| Input | `stage_hesa_nn056_student_programs`, `dim_hesa_student`, `dim_hesa_program` |
| Output | Student program enrollments with surrogate keys |
| Measures | `fees_paid_bool` (0/1) |
| Joins | Inner joins to both student and program dimensions |
| Grain | One row per student-program combination per delivery |

<br>

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