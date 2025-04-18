{{ config(
    materialized='table',
    unique_key=['dim_hesa_student_key', 'dim_hesa_program_key'],
    tags=['fact', 'hesa', 'enrolment'])
}}

/*
  Note on uniqueness constraints:
  The unique key of this fact table consists of dim_hesa_student_key and dim_hesa_program_key.
  Those dimensions each have a guid as their business key, but their natural key consists of
  that plus the delivery id. Hesa_delivery is therefore an implicit part of the uniqueness constraint.
  The same student in two deliveries will have one GUID but two rows in dim_hesa_student.
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
            student_guid,
            hesa_delivery
    FROM {{ ref('dim_hesa_student') }}
),
programs AS (
    SELECT dim_hesa_program_key,
            program_guid,
            hesa_delivery
    FROM {{ ref('dim_hesa_program') }}
)

SELECT
    -- Main dimension keys (for unique constraint)
    students.dim_hesa_student_key,
    programs.dim_hesa_program_key,

    -- HESA delivery code (for context only)
    main_data.hesa_delivery,

    -- Fact attributes
    main_data.enrol_date,
    main_data.fees_paid,
    -- Convert fees_paid from Y/N to 1/0 for ease of reporting
    CASE WHEN main_data.fees_paid = 'Y' THEN 1 ELSE 0 END as fees_paid_bool,
    main_data.source_file
FROM main_data
INNER JOIN students
    ON students.student_guid = main_data.student_guid
    AND students.hesa_delivery = main_data.hesa_delivery
INNER JOIN programs
    ON programs.program_guid = main_data.program_guid
    AND programs.hesa_delivery = main_data.hesa_delivery
