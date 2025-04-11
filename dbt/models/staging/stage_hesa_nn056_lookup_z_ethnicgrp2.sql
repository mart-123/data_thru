SELECT
  code,
  label,
  hesa_delivery,
  source_file
FROM {{ source('hesa', 'load_hesa_22056_20240331_lookup_z_ethnicgrp2') }}

UNION

SELECT
  code,
  label,
  hesa_delivery,
  source_file
FROM {{ source('hesa', 'load_hesa_23056_20250331_lookup_z_ethnicgrp2') }}
