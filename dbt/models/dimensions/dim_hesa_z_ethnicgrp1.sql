-- models/dimensions/dim_z_ethnicgrp1.sql
-- Create dimension of z_ethnicgrp1 codes
{{ config(
    materialized='table',
    unique_key='z_ethnicgrp1_id',
    tags=['dimension', 'hesa', 'lookup'])
}}

WITH source_data AS (
    SELECT
        code,
        label,
        hesa_delivery,
        source_file
    FROM {{ ref('stage_hesa_nn056_lookup_z_ethnicgrp1') }}
)

SELECT
    CONCAT('EG1_', code, '_', hesa_delivery) as z_ethnicgrp1_id,
    code as z_ethnicgrp1_code, -- Business key of HESA look-up
    label as z_ethnicgrp1_label, -- Human-readable description
    hesa_delivery, -- HESA delivery version
    source_file -- Original CSV filename
FROM source_data
