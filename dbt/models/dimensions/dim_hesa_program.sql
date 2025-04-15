{{ config(materialized='table',
    unique_key='dim_hesa_program_key',
    tags=['dimension', 'hesa', 'program'])
    }}

WITH main_data AS (
    SELECT program_guid, 
            program_code, 
            program_name, 
            source_file, 
            hesa_delivery
    FROM {{ref('stage_hesa_nn056_programs')}}
)

SELECT CONCAT('PGM_', program_guid, '_', hesa_delivery) as dim_hesa_program_key,
        program_guid, 
        program_code, 
        program_name, 
        source_file, 
        hesa_delivery
FROM main_data
