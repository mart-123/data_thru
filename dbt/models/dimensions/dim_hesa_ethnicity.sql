-- models/dimensions/dim_ethnicity.sql
-- Create dimension of ethnicity codes
{{ config(
    materialized='table',
    unique_key='ethnicity_id',
    tags=['dimension', 'hesa', 'lookup'])
}}

WITH source_data AS (
    SELECT
        code,
        label,
        hesa_delivery,
        source_file
    FROM {{ ref('stage_hesa_nn056_lookup_ethnicity') }}
)

SELECT
    CONCAT('ETH_', code, '_', hesa_delivery) as ethnicity_id,
    code as ethnicity_code, -- Business key of HESA look-up
    label as ethnicity_label, -- Human-readable description
    hesa_delivery, -- HESA delivery version
    source_file -- Original CSV filename
FROM source_data
