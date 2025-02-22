CREATE TABLE hesa_3nf_course (
        courseid NVARCHAR(50),
        coursetitle NVARCHAR(255),
        PRIMARY KEY (COURSEID)
        );


CREATE TABLE hesa_3nf_student (
        birthdte DATE,
        ethnic NVARCHAR(3),
        fnames NVARCHAR(100),
        genderid NVARCHAR(2),
        nation NVARCHAR(2),
        ownstu NVARCHAR(50),
        religion NVARCHAR(2),
        scn NVARCHAR(9),
        sexid NVARCHAR(2),
        sexort NVARCHAR(2),
        sid NVARCHAR(17),
        sname16 NVARCHAR(100),
        surname NVARCHAR(100),
        trans NVARCHAR(2),
        ucasperid NVARCHAR(10),
        z_nationgrp1 NVARCHAR(2),
        PRIMARY KEY (SID)
        );

