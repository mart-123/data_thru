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


<div style="margin: 3em 0 1em 0; border-top: 1px solid #ccc; padding-top: 1em;">
  <strong>Navigation:</strong>
  <a href="architecture.md">Architecture</a> |
  <a href="data-deliveries.md">HESA Deliveries</a> |
  <a href="data-model.md">Data Model</a> |
  <a href="pipeline-process.md">Pipeline Process</a> |
  <a href="hesa-data-info.md">HESA Data Info</a> |
  <a href="scripts.md">Scripts</a>
</div>