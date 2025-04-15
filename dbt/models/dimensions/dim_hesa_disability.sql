-- models/dimensions/dim_disability.sql
-- Create dimension of disability codes
{{ config(
    materialized='table',
    unique_key='dim_hesa_disability_key',
    tags=['dimension', 'hesa', 'lookup'])
}}

WITH source_data AS (
    SELECT
        code,
        label,
        hesa_delivery,
        source_file
    FROM {{ ref('stage_hesa_nn056_lookup_disability') }}
)

SELECT
    CONCAT('DIS_', code, '_', hesa_delivery) as dim_hesa_disability_key,
    code as disability_code, -- Business key of HESA look-up
    label as disability_label, -- Human-readable description
    hesa_delivery, -- HESA delivery version
    source_file -- Original CSV filename
FROM source_data
