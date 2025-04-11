-- models/dimensions/dim_religion.sql
-- Create dimension of religion codes
{{ config(materialized='table') }}

WITH source_data AS (
    SELECT
        code,
        label,
        hesa_delivery,
        source_file
    FROM {{ ref('stage_hesa_nn056_lookup_religion') }}
)

SELECT
    CONCAT(code, '_', hesa_delivery) as religion_id,
    code as religion_code,
    label as religion_label,
    hesa_delivery,
    source_file
FROM source_data
