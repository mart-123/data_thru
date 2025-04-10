-- models/staging/stg_religion.sql
-- Stage religion codes from source tables
{{ config(materialized='view') }}

SELECT
  code,
  label,
  hesa_delivery,
  source_file
FROM {{ source('hesa', 'stage_hesa_nn056_lookup_religion') }}
