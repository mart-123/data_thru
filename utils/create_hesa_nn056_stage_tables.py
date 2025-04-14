# mysql is running in windows
"""
This module creates HESA stage tables. Note the nn056 designation, these tables
take a merge of multiple load tables each from 22056 onwards under the nn056 schema.
"""
import mysql.connector
import traceback
import mysql.connector.cursor
from utils.data_platform_core import get_config, set_up_logging, connect_to_db

def init():
    config = get_config()
    set_up_logging(config)

    return config


def generate_create_statements():
    create_statements = {}
    
    create_statements["stage_hesa_nn056_students"] = """
    CREATE TABLE stage_hesa_nn056_students (
        student_guid CHAR(36),
        first_names VARCHAR(250),
        last_name VARCHAR(250),
        phone VARCHAR(250),
        email VARCHAR(250),
        home_addr VARCHAR(250),
        home_postcode VARCHAR(50),
        home_country VARCHAR(100),
        term_addr VARCHAR(250),
        term_postcode VARCHAR(50),
        term_country VARCHAR(100),
        dob DATE,
        ethnicity VARCHAR(3),
        gender VARCHAR(3),
        religion VARCHAR(3),
        sexid VARCHAR(3),
        sexort VARCHAR(3),
        trans VARCHAR(3),
        ethnicity_grp1 VARCHAR(3),
        ethnicity_grp2 VARCHAR(3),
        ethnicity_grp3 VARCHAR(3),
        insert_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT 'Timestamp of insert',
        source_file VARCHAR(250) COMMENT 'File from which data was loaded',
        hesa_delivery VARCHAR(20) COMMENT 'Originating HESA delivery',
        PRIMARY KEY (hesa_delivery, student_guid)
        )
        COMMENT='Student records delivered from HESA nn056 schema';
    """

    create_statements["stage_hesa_nn056_student_programs"] = """
    CREATE TABLE stage_hesa_nn056_student_programs (
        student_guid CHAR(36) COMMENT 'HESA db-internal PK student id',
        program_guid CHAR(36) COMMENT 'HESA db-internal PK for program',
        enrol_date DATE COMMENT 'Date on which student enrolled for academic session',
        fees_paid CHAR(1) COMMENT 'Indicates whether fees have been paid for the academic session',
        insert_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT 'Timestamp of insert',
        source_file VARCHAR(250) COMMENT 'File from which data was loaded',
        hesa_delivery VARCHAR(20) COMMENT 'Originating HESA delivery',
        PRIMARY KEY (hesa_delivery, student_guid, program_guid)
        )
        COMMENT='Student-program links delivered from HESA nn056 schema';
    """

    create_statements["stage_hesa_nn056_programs"] = """
    CREATE TABLE stage_hesa_nn056_programs (
        program_guid CHAR(36) COMMENT 'HESA db-internal PK for program',
        program_code VARCHAR(10) COMMENT 'Human-readable, unique code for the program of study',
        program_name VARCHAR(100) COMMENT 'The program name that will appear on award certificate',
        insert_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT 'Timestamp of insert',
        source_file VARCHAR(250) COMMENT 'File from which data was loaded',
        hesa_delivery VARCHAR(20) COMMENT 'Originating HESA delivery',
        PRIMARY KEY (hesa_delivery, program_guid)
        )
        COMMENT='Student-program links, re-normalised (CSV from hesa was denormalised)';
    """

    create_statements["stage_hesa_nn056_lookup_disability"] = """
    CREATE TABLE stage_hesa_nn056_lookup_disability (
        code VARCHAR(5) COMMENT 'HESA-internal code',
        label VARCHAR(400) COMMENT 'Human-readable description relating to the lookup code',
        insert_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT 'Timestamp of insert',
        source_file VARCHAR(250) COMMENT 'File from which data was loaded',
        hesa_delivery VARCHAR(20) COMMENT 'Originating HESA delivery',
        PRIMARY KEY (hesa_delivery, code)
        )
        COMMENT='HESA-provided lookup for DISABILTIY codes';
    """

    create_statements["stage_hesa_nn056_lookup_ethnicity"] = """
    CREATE TABLE stage_hesa_nn056_lookup_ethnicity (
        code VARCHAR(5) COMMENT 'HESA-internal lookup code',
        label VARCHAR(400) COMMENT 'Description for lookup code',
        insert_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT 'Timestamp of insert',
        source_file VARCHAR(250) COMMENT 'File from which data was loaded',
        hesa_delivery VARCHAR(20) COMMENT 'Originating HESA delivery',
        PRIMARY KEY (hesa_delivery, code)
        )
        COMMENT='HESA-provided lookup for ETHNICITY code';
    """

    create_statements["stage_hesa_nn056_lookup_genderid"] = """
    CREATE TABLE stage_hesa_nn056_lookup_genderid (
        code VARCHAR(5) COMMENT 'HESA-internal lookup code',
        label VARCHAR(400) COMMENT 'Description for lookup code',
        insert_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT 'Timestamp of insert',
        source_file VARCHAR(250) COMMENT 'File from which data was loaded',
        hesa_delivery VARCHAR(20) COMMENT 'Originating HESA delivery',
        PRIMARY KEY (hesa_delivery, code)
        )
        COMMENT='HESA-provided lookup for GENDERID code, denoting whether student identifies with birth-registered sex';
    """

    create_statements["stage_hesa_nn056_lookup_religion"] = """
    CREATE TABLE stage_hesa_nn056_lookup_religion (
        code VARCHAR(5) COMMENT 'HESA-internal lookup code',
        label VARCHAR(400) COMMENT 'Description for lookup code',
        insert_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT 'Timestamp of insert',
        source_file VARCHAR(250) COMMENT 'File from which data was loaded',
        hesa_delivery VARCHAR(20) COMMENT 'Originating HESA delivery',
        PRIMARY KEY (hesa_delivery, code)
        )
        COMMENT='HESA-provided lookup for RELIGION code';
    """

    create_statements["stage_hesa_nn056_lookup_sexid"] = """
    CREATE TABLE stage_hesa_nn056_lookup_sexid (
        code VARCHAR(5) COMMENT 'HESA-internal lookup code',
        label VARCHAR(400) COMMENT 'Description for lookup code',
        insert_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT 'Timestamp of insert',
        source_file VARCHAR(250) COMMENT 'File from which data was loaded',
        hesa_delivery VARCHAR(20) COMMENT 'Originating HESA delivery',
        PRIMARY KEY (hesa_delivery, code)
        )
        COMMENT='HESA-provided lookup for SEXID code';
    """

    create_statements["stage_hesa_nn056_lookup_sexort"] = """
    CREATE TABLE stage_hesa_nn056_lookup_sexort (
        code VARCHAR(5) COMMENT 'HESA-internal lookup code',
        label VARCHAR(400) COMMENT 'Description for lookup code',
        insert_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT 'Timestamp of insert',
        source_file VARCHAR(250) COMMENT 'File from which data was loaded',
        hesa_delivery VARCHAR(20) COMMENT 'Originating HESA delivery',
        PRIMARY KEY (hesa_delivery, code)
        )
        COMMENT='HESA-provided lookup for SEXORT code';
    """

    create_statements["stage_hesa_nn056_lookup_trans"] = """
    CREATE TABLE stage_hesa_nn056_lookup_trans (
        code VARCHAR(5) COMMENT 'HESA-internal lookup code',
        label VARCHAR(400) COMMENT 'Description for lookup code',
        insert_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT 'Timestamp of insert',
        source_file VARCHAR(250) COMMENT 'File from which data was loaded',
        hesa_delivery VARCHAR(20) COMMENT 'Originating HESA delivery',
        PRIMARY KEY (hesa_delivery, code)
        )
        COMMENT='HESA-provided lookup for TRANS code';
    """

    create_statements["stage_hesa_nn056_lookup_z_ethnicgrp1"] = """
    CREATE TABLE stage_hesa_nn056_lookup_z_ethnicgrp1 (
        code VARCHAR(5) COMMENT 'HESA-internal lookup code',
        label VARCHAR(400) COMMENT 'Description for lookup code',
        insert_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT 'Timestamp of insert',
        source_file VARCHAR(250) COMMENT 'File from which data was loaded',
        hesa_delivery VARCHAR(20) COMMENT 'Originating HESA delivery',
        PRIMARY KEY (hesa_delivery, code)
        )
        COMMENT='HESA-provided lookup for Z_ETHNICGRP1';
    """

    create_statements["stage_hesa_nn056_lookup_z_ethnicgrp2"] = """
    CREATE TABLE stage_hesa_nn056_lookup_z_ethnicgrp2 (
        code VARCHAR(5) COMMENT 'HESA-internal lookup code',
        label VARCHAR(400) COMMENT 'Description for lookup code',
        insert_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT 'Timestamp of insert',
        source_file VARCHAR(250) COMMENT 'File from which data was loaded',
        hesa_delivery VARCHAR(20) COMMENT 'Originating HESA delivery',
        PRIMARY KEY (hesa_delivery, code)
        )
        COMMENT='HESA-provided lookup for Z_ETHNICGRP2';
    """

    create_statements["stage_hesa_nn056_lookup_z_ethnicgrp3"] = """
    CREATE TABLE stage_hesa_nn056_lookup_z_ethnicgrp3 (
        code VARCHAR(5) COMMENT 'HESA-internal lookup code',
        label VARCHAR(400) COMMENT 'Description for lookup code',
        insert_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT 'Timestamp of insert',
        source_file VARCHAR(250) COMMENT 'File from which data was loaded',
        hesa_delivery VARCHAR(20) COMMENT 'Originating HESA delivery',
        PRIMARY KEY (hesa_delivery, code)
        )
        COMMENT='HESA-provided lookup for Z_ETHNICGRP3';
    """

    return create_statements



def create_table(cursor: mysql.connector.cursor.MySQLCursor, table_name, create_statement):
    try:
        cursor.execute(f"""
            SELECT COUNT(*)
            FROM information_schema.tables
            WHERE table_name = '{table_name}'        
            """)
        
        if cursor.fetchone()[0] == 1:
            print(f"Table {table_name} : already exists")
        else:
            cursor.execute(create_statement)            
            print(f"Table {table_name} : created")

    except mysql.connector.Error as err:
        print(f"Exception during creation of {table_name}: {err}")
        raise




def main():
    # Declare here to ensure except/finally work if connection fails
    conn = None

    try:
        config = init()
        conn = connect_to_db(config)
        cursor = conn.cursor()
        create_statements = generate_create_statements()

        for table_name, create_statement in create_statements.items():
            create_table(cursor, table_name, create_statement)

        conn.commit()
        print("Table creation complete")

    except Exception as e:
#        print(f"Error during stage table creation: {e}")
        traceback.print_exc()
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()


if __name__ == '__main__':
    main()
