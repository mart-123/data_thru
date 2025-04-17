{% set dimension_models = [
  'dim_hesa_disability',
  'dim_hesa_ethnicity',
  'dim_hesa_genderid',
  'dim_hesa_program',
  'dim_hesa_religion',
  'dim_hesa_sexid',
  'dim_hesa_sexort',
  'dim_hesa_student', 
  'dim_hesa_trans',
  'dim_hesa_z_ethnicgrp1',
  'dim_hesa_z_ethnicgrp2',
  'dim_hesa_z_ethnicgrp3'
] %}

{{ codegen.generate_model_yaml(model_names = dimension_models) }}