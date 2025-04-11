SELECT DISTINCT 
    t1.program_guid, 
    t1.program_code, 
    t1.program_name, 
    t1.source_file, 
    t1.hesa_delivery
FROM 
    {{ source('hesa', 'load_hesa_22056_20240331_student_programs') }} t1

UNION

SELECT DISTINCT 
    t2.program_guid, 
    t2.program_code, 
    t2.program_name, 
    t2.source_file, 
    t2.hesa_delivery
FROM 
    {{ source('hesa', 'load_hesa_23056_20250331_student_programs') }} t2;
