## Scripts
### hesa_nn056_pipeline.py
Main orchestration script. Manages execution order and dependencies within main pipeline.
Parameterised execution of ELT scripts for multiple deliveries.
List of lookup tables also is parameterised in this script.

### extract_hesa_nn056_students
Performs simple, column-level transformations including null values, space trimming and email casing. Numerous data quality filters. Script is parameterised by delivery code.

### extract_hesa_nn056_demographics
Processes demographic data for student. Validates for expected column names and missing values.

### extract_hesa_nn056_student_programs
Processes student program enrolments, handling validation, data cleansing, and field-level transformations. Parameterized by delivery code.


<div style="margin: 2em 0; min-height: 30px;"></div>


