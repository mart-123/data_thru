-- models/dimensions/dim_religion.sql
-- Create dimension of religion codes
{{ config(
    materialized='table',
    unique_key='religion_id',
    tags=['dimension', 'hesa', 'lookup'])
}}

WITH source_data AS (
    SELECT
        code,
        label,
        hesa_delivery,
        source_file
    FROM {{ ref('stage_hesa_nn056_lookup_religion') }}
)

SELECT
    CONCAT('REL_', code, '_', hesa_delivery) as religion_id,
    code as religion_code, -- Business key of HESA look-up
    label as religion_label, -- Human-readable description
    hesa_delivery, -- HESA delivery version
    source_file -- Original CSV filename
FROM source_data
