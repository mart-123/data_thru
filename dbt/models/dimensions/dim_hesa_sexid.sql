-- models/dimensions/dim_sexid.sql
-- Create dimension of sexid codes
{{ config(
    materialized='table',
    unique_key='dim_hesa_sexid_key',
    tags=['dimension', 'hesa', 'lookup'])
}}

WITH source_data AS (
    SELECT
        code,
        label,
        hesa_delivery,
        source_file
    FROM {{ ref('stage_hesa_nn056_lookup_sexid') }}
)

SELECT
    CONCAT('SXI_', code, '_', hesa_delivery) as dim_hesa_sexid_key,
    code as sexid_code, -- Business key of HESA look-up
    label as sexid_label, -- Human-readable description
    hesa_delivery, -- HESA delivery version
    source_file -- Original CSV filename
FROM source_data
