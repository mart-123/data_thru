{{ config(
    materialized='table',
    unique_key='dim_hesa_student_key',
    tags=['dimension', 'hesa', 'student'])
}}

/*
  This dimension uses a dual-key pattern for cross-delivery analysis:
  
  1. Surrogate key (dim_hesa_student_key): STU_<guid>_<delivery>
     - Includes delivery code to maintain delivery-specific context
     - Serves as the primary key for this dimension
     - Used in fact table joins to maintain data lineage
  
  2. Canonical key (canonical_student_key): <student_guid>
     - Delivery-independent identifier for the same student
     - Enables cross-delivery analysis and reporting
     - Allows tracking the same student across different HESA deliveries
*/

WITH main_data AS (
    SELECT student_guid,
        first_names, 
        last_name, 
        phone, 
        email, 
        dob,
        home_addr, 
        home_postcode, 
        home_country, 
        term_addr, 
        term_postcode, 
        term_country,
        source_file, 
        hesa_delivery,
        ethnicity, 
        gender, 
        religion, 
        sexid, 
        sexort, 
        trans,
        ethnicity_grp1, 
        ethnicity_grp2, 
        ethnicity_grp3
    FROM {{ ref('stage_hesa_nn056_students')}}
)

SELECT
    CONCAT('STU_', student_guid, '_', hesa_delivery) as dim_hesa_student_key,
    student_guid,
    student_guid as canonical_student_key,
    first_names, 
    last_name, 
    phone, 
    email, 
    dob,
    home_addr, 
    home_postcode, 
    home_country, 
    term_addr, 
    term_postcode, 
    term_country,
    source_file, 
    hesa_delivery,
    ethnicity, 
    gender, 
    religion, 
    sexid, 
    sexort, 
    trans,
    ethnicity_grp1, 
    ethnicity_grp2, 
    ethnicity_grp3
FROM main_data