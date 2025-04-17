-- models/dimensions/dim_trans.sql
-- Create dimension of trans codes
{{ config(
    materialized='table',
    unique_key='dim_hesa_trans_key',
    tags=['dimension', 'hesa', 'lookup'])
}}

WITH source_data AS (
    SELECT
        code,
        label,
        hesa_delivery,
        source_file
    FROM {{ ref('stage_hesa_nn056_lookup_trans') }}
)

SELECT
    CONCAT('TRA_', code, '_', hesa_delivery) as dim_hesa_trans_key,
    code as trans_code, -- Business key of HESA look-up
    label as trans_label, -- Human-readable description
    hesa_delivery, -- HESA delivery version
    source_file -- Original CSV filename
FROM source_data
