{{ config(
    materialized='table',
    unique_key='dim_hesa_student_key',
    tags=['dimension', 'hesa', 'student'])
}}

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