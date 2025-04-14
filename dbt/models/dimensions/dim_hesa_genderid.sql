-- models/dimensions/dim_genderid.sql
-- Create dimension of genderid codes
{{ config(
    materialized='table',
    unique_key='genderid_id',
    tags=['dimension', 'hesa', 'lookup'])
}}

WITH source_data AS (
    SELECT
        code,
        label,
        hesa_delivery,
        source_file
    FROM {{ ref('stage_hesa_nn056_lookup_genderid') }}
)

SELECT
    CONCAT('GID_', code, '_', hesa_delivery) as genderid_id,
    code as genderid_code, -- Business key of HESA look-up
    label as genderid_label, -- Human-readable description
    hesa_delivery, -- HESA delivery version
    source_file -- Original CSV filename
FROM source_data
