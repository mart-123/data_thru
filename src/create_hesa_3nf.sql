CREATE TABLE hesa_3nf_course (
        bittm NVARCHAR(2),
        clsdcrs NVARCHAR(2),
        courseid NVARCHAR(50),
        coursetitle NVARCHAR(255),
        fullyflex NVARCHAR(2),
        prerequisite NVARCHAR(2),
        qualid NVARCHAR(50),
        sandwich NVARCHAR(2),
        ttcid NVARCHAR(2),
        PRIMARY KEY (COURSEID)
        );


CREATE TABLE hesa_3nf_student (
        birthdte DATE,
        carer NVARCHAR(2),
        ethnic NVARCHAR(3),
        fnames NVARCHAR(100),
        genderid NVARCHAR(2),
        langpref NVARCHAR(2),
        nation NVARCHAR(2),
        ownstu NVARCHAR(50),
        religion NVARCHAR(2),
        scn NVARCHAR(9),
        serleave NVARCHAR(2),
        serstu NVARCHAR(2),
        sexid NVARCHAR(2),
        sexort NVARCHAR(2),
        sid NVARCHAR(17),
        sname16 NVARCHAR(100),
        ssn NVARCHAR(13),
        studep NVARCHAR(2),
        surname NVARCHAR(100),
        trans NVARCHAR(2),
        ttaccom NVARCHAR(2),
        ttpcode VARCHAR(8),
        ucasperid NVARCHAR(10),
        uln NVARCHAR(10),
        z_nationgrp1 NVARCHAR(2),
        PRIMARY KEY (SID)
        );

