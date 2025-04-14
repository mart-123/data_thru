-- models/dimensions/dim_sexort.sql
-- Create dimension of sexort codes
{{ config(
    materialized='table',
    unique_key='sexort_id',
    tags=['dimension', 'hesa', 'lookup'])
}}

WITH source_data AS (
    SELECT
        code,
        label,
        hesa_delivery,
        source_file
    FROM {{ ref('stage_hesa_nn056_lookup_sexort') }}
)

SELECT
    CONCAT('SXO_', code, '_', hesa_delivery) as sexort_id,
    code as sexort_code, -- Business key of HESA look-up
    label as sexort_label, -- Human-readable description
    hesa_delivery, -- HESA delivery version
    source_file -- Original CSV filename
FROM source_data
