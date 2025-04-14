-- models/dimensions/dim_trans.sql
-- Create dimension of trans codes
{{ config(
    materialized='table',
    unique_key='trans_id',
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
    CONCAT('TRN_', code, '_', hesa_delivery) as trans_id,
    code as trans_code, -- Business key of HESA look-up
    label as trans_label, -- Human-readable description
    hesa_delivery, -- HESA delivery version
    source_file -- Original CSV filename
FROM source_data
