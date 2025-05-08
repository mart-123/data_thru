{{ config(materialized='table',
    unique_key='dim_hesa_program_key',
    tags=['dimension', 'hesa', 'program'])
    }}

/*
  This dimension uses a dual-key pattern for cross-delivery analysis:
  
  1. Surrogate key (dim_hesa_program_key): PGM_<guid>_<delivery>
     - Includes delivery code to maintain delivery-specific context
     - Serves as the primary key for this dimension
     - Used in fact table joins to maintain data lineage
  
  2. Canonical key (canonical_program_key): <program_guid>
     - Delivery-independent identifier for the same program
     - Enables cross-delivery analysis and reporting
     - Allows tracking the same program across different HESA deliveries
*/

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
        program_guid as canonical_program_key,
        program_code, 
        program_name, 
        source_file, 
        hesa_delivery
FROM main_data
