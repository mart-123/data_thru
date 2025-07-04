SELECT 
    t1.student_guid, 
    t1.first_names, 
    t1.last_name, 
    t1.phone, 
    t1.email, 
    t1.dob,
    t1.home_addr, 
    t1.home_postcode, 
    t1.home_country, 
    t1.term_addr, 
    t1.term_postcode, 
    t1.term_country,
    t1.source_file, 
    t1.hesa_delivery,
    t2.ethnicity, 
    t2.gender, 
    t2.religion, 
    t2.sexid, 
    t2.sexort, 
    t2.trans,
    t2.ethnicity_grp1, 
    t2.ethnicity_grp2, 
    t2.ethnicity_grp3
FROM 
    {{ source('hesa', 'load_hesa_22056_20240331_students') }} t1
INNER JOIN 
    {{ source('hesa', 'load_hesa_22056_20240331_demographics') }} t2
    ON t2.student_guid = t1.student_guid
    AND t2.hesa_delivery = t1.hesa_delivery

UNION

SELECT 
    t3.student_guid, 
    t3.first_names, 
    t3.last_name, 
    t3.phone, 
    t3.email, 
    t3.dob,
    t3.home_addr, 
    t3.home_postcode, 
    t3.home_country, 
    t3.term_addr, 
    t3.term_postcode, 
    t3.term_country,
    t3.source_file, 
    t3.hesa_delivery,
    t4.ethnicity, 
    t4.gender, 
    t4.religion, 
    t4.sexid, 
    t4.sexort, 
    t4.trans,
    t4.ethnicity_grp1, 
    t4.ethnicity_grp2, 
    t4.ethnicity_grp3
FROM 
    {{ source('hesa', 'load_hesa_23056_20250331_students') }} t3
INNER JOIN 
    {{ source('hesa', 'load_hesa_23056_20250331_demographics') }} t4
    ON t4.student_guid = t3.student_guid
    AND t4.hesa_delivery = t3.hesa_delivery
