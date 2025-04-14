-- models/dimensions/dim_z_ethnicgrp2.sql
-- Create dimension of z_ethnicgrp2 codes
{{ config(
    materialized='table',
    unique_key='z_ethnicgrp2_id',
    tags=['dimension', 'hesa', 'lookup'])
}}

WITH source_data AS (
    SELECT
        code,
        label,
        hesa_delivery,
        source_file
    FROM {{ ref('stage_hesa_nn056_lookup_z_ethnicgrp2') }}
)

SELECT
    CONCAT('EG2_', code, '_', hesa_delivery) as z_ethnicgrp2_id,
    code as z_ethnicgrp2_code, -- Business key of HESA look-up
    label as z_ethnicgrp2_label, -- Human-readable description
    hesa_delivery, -- HESA delivery version
    source_file -- Original CSV filename
FROM source_data
