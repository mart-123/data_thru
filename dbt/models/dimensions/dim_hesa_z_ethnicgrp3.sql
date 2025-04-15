-- models/dimensions/dim_z_ethnicgrp3.sql
-- Create dimension of z_ethnicgrp3 codes
{{ config(
    materialized='table',
    unique_key='dim_hesa_z_ethnicgrp3_key',
    tags=['dimension', 'hesa', 'lookup'])
}}

WITH source_data AS (
    SELECT
        code,
        label,
        hesa_delivery,
        source_file
    FROM {{ ref('stage_hesa_nn056_lookup_z_ethnicgrp3') }}
)

SELECT
    CONCAT('EG3_', code, '_', hesa_delivery) as dim_hesa_z_ethnicgrp3_key,
    code as z_ethnicgrp3_code, -- Business key of HESA look-up
    label as z_ethnicgrp3_label, -- Human-readable description
    hesa_delivery, -- HESA delivery version
    source_file -- Original CSV filename
FROM source_data
