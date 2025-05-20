{{ config(
    materialized='table',
    unique_key=['dim_hesa_student_key', 'dim_hesa_program_key'],
    tags=['fact', 'hesa', 'enrolment'])
}}

/*
  Note on uniqueness constraints:
  The unique key of this fact table consists of dim_hesa_student_key and dim_hesa_program_key.
  Those dimensions each have a guid as their business key, with their surrogate key consisting of
  that guid PLUS the delivery id. Hesa_delivery is therefore an IMPLICIT part of the uniqueness constraint.
  
  The same student in two deliveries will have two different dimension keys (STU_<guid>_<delivery1> vs 
  STU_<guid>_<delivery2>) but will share the same student_guid. This student_guid is exposed as 
  canonical_student_key to make its purpose clear - enabling cross-delivery student analysis.
*/

WITH main_data AS (
    SELECT student_guid,
            program_guid,
            enrol_date,
            fees_paid,
            source_file,
            hesa_delivery
    FROM {{ ref('stage_hesa_nn056_student_programs') }}
),
students AS (
    SELECT dim_hesa_student_key,
            canonical_student_key,
            student_guid,
            hesa_delivery
    FROM {{ ref('dim_hesa_student') }}
),
programs AS (
    SELECT dim_hesa_program_key,
            canonical_program_key,
            program_guid,
            hesa_delivery
    FROM {{ ref('dim_hesa_program') }}
),
deliveries AS (
    SELECT dim_hesa_delivery_key,
            delivery_code
    FROM {{ ref('dim_hesa_delivery') }}
),
enrol_dates AS (
    SELECT dim_date_key,
            calendar_date
    FROM {{ source('hesa', 'dim_date') }}
)

SELECT
    -- Main dimension keys (for unique constraint)
    students.dim_hesa_student_key,
    programs.dim_hesa_program_key,

    -- Canonical dimension keys (for cross-delivery reporting)
    students.canonical_student_key,
    programs.canonical_program_key,

    -- HESA delivery code (main context)
    main_data.hesa_delivery,
    deliveries.dim_hesa_delivery_key,

    -- Fact attributes
    main_data.enrol_date,
    enrol_dates.dim_date_key as dim_enrol_date_key,
    main_data.fees_paid,

    -- Convert fees_paid from Y/N to 1/0 for ease of reporting
    CASE WHEN main_data.fees_paid = 'Y'
        THEN 1 ELSE 0 END as fees_paid_bool,
    
    main_data.source_file

FROM main_data
INNER JOIN students
    ON students.student_guid = main_data.student_guid
    AND students.hesa_delivery = main_data.hesa_delivery
INNER JOIN programs
    ON programs.program_guid = main_data.program_guid
    AND programs.hesa_delivery = main_data.hesa_delivery
INNER JOIN deliveries
    ON deliveries.delivery_code = main_data.hesa_delivery
INNER JOIN enrol_dates
    ON enrol_dates.calendar_date = main_data.enrol_date
