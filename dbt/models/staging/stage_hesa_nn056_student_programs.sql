SELECT 
    t1.student_guid, 
    t1.program_guid, 
    t1.enrol_date,         
    t1.fees_paid, 
    t1.source_file, 
    t1.hesa_delivery
FROM 
    {{ source('hesa', 'load_hesa_22056_20240331_student_programs') }} t1
JOIN 
    {{ ref('stage_hesa_nn056_students') }} s1
    ON s1.student_guid = t1.student_guid
    AND s1.hesa_delivery = t1.hesa_delivery
JOIN 
    {{ ref('stage_hesa_nn056_programs') }} p1
    ON p1.program_guid = t1.program_guid
    AND p1.hesa_delivery = t1.hesa_delivery 

UNION

SELECT 
    t2.student_guid, 
    t2.program_guid, 
    t2.enrol_date,
    t2.fees_paid, 
    t2.source_file, 
    t2.hesa_delivery
FROM 
    {{ source('hesa', 'load_hesa_23056_20250331_student_programs') }} t2
JOIN 
    {{ ref('stage_hesa_nn056_students') }} s2
    ON s2.student_guid = t2.student_guid
    AND s2.hesa_delivery = t2.hesa_delivery
JOIN 
    {{ ref('stage_hesa_nn056_programs') }} p2
    ON p2.program_guid = t2.program_guid
    AND p2.hesa_delivery = t2.hesa_delivery
