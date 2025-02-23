# mysql is running in windows. Basic server info:
# Server instance: MySQL92
"""
This module creates college database and tables.
"""
import mysql.connector
import mysql.connector.cursor
from utils.etl_utils import get_config, set_up_logging, connect_to_db

def init():
    config = get_config()
    set_up_logging(config)

    return config


def generate_create_statements():
    create_statements = {
        'load_22056_students':
            """
            CREATE TABLE load_22056_students (
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
                hesa_delivery VARCHAR(10) DEFAULT '22056' COMMENT 'Originating HESA delivery'
                )
                COMMENT='Student records';
            """,
        'load_22056_student_programs':
            """
            CREATE TABLE load_22056_student_programs (
                student_guid CHAR(36) COMMENT 'Source system student id',
                email VARCHAR(250) COMMENT 'Student email, included here only for data verification',
                dob DATE COMMENT 'Student DoB, included here only for data verification',
                program_guid CHAR(36) COMMENT 'Vendor-provided unique id for program of study',
                program_code VARCHAR(10) COMMENT 'Human-readable, unique code for the program of study',
                program_name VARCHAR(100) COMMENT 'The program name that will appear on award certificate',
                enrol_date DATE COMMENT 'Date on which student enrolled for the academic session',
                fees_paid CHAR(1) COMMENT 'Indicates whether fees have been paid for the academic session',
                hesa_delivery VARCHAR(10) DEFAULT '22056' COMMENT 'Originating HESA delivery'
                )
                COMMENT='Student-program links combined with program details (i.e. denormalised)';
            """,
        'load_22056_student_demographics':
            """
            CREATE TABLE load_22056_student_demographics (
                student_guid CHAR(36),
                ethnicity VARCHAR(3),
                gender VARCHAR(3),
                religion VARCHAR(3),
                sexid VARCHAR(3),
                sexort VARCHAR(3),
                trans VARCHAR(3),
                ethnicity_grp1 VARCHAR(3),
                ethnicity_grp2 VARCHAR(3),
                ethnicity_grp3 VARCHAR(3),
                hesa_delivery VARCHAR(10) DEFAULT '22056' COMMENT 'Originating HESA delivery'
                );
            """,
        'load_static_22056_disability':
            """
            CREATE TABLE load_static_22056_disability (
                code VARCHAR(5) COMMENT 'HESA-internal code',
                label VARCHAR(400) COMMENT 'Human-readable description relating to the LOV code',
                hesa_delivery VARCHAR(10) DEFAULT '22056' COMMENT 'Originating HESA delivery'
                )
                COMMENT='HESA-provided LOV for DISABILTIY codes';
            """,
        'load_static_22056_ethnicity':
            """
            CREATE TABLE load_static_22056_ethnicity (
                code VARCHAR(5) COMMENT 'HESA-internal LOV code',
                label VARCHAR(400) COMMENT 'Description for LOV code',
                hesa_delivery VARCHAR(10) DEFAULT '22056' COMMENT 'Originating HESA delivery'
                )
                COMMENT='HESA-provided LOV for ETHNICITY code';
            """,
        'load_static_22056_genderid':
            """
            CREATE TABLE load_static_22056_genderid (
                code VARCHAR(5) COMMENT 'HESA-internal LOV code',
                label VARCHAR(400) COMMENT 'Description for LOV code',
                hesa_delivery VARCHAR(10) DEFAULT '22056' COMMENT 'Originating HESA delivery'
                )
                COMMENT='HESA-provided LOV for GENDERID code';
            """,
        'load_static_22056_religion':
            """
            CREATE TABLE load_static_22056_religion (
                code VARCHAR(5) COMMENT 'HESA-internal LOV code',
                label VARCHAR(400) COMMENT 'Description for LOV code',
                hesa_delivery VARCHAR(10) DEFAULT '22056' COMMENT 'Originating HESA delivery'
                )
                COMMENT='HESA-provided LOV for RELIGION code';
            """,
        'load_static_22056_sexid':
            """
            CREATE TABLE load_static_22056_sexid (
                code VARCHAR(5) COMMENT 'HESA-internal LOV code',
                label VARCHAR(400) COMMENT 'Description for LOV code',
                hesa_delivery VARCHAR(10) DEFAULT '22056' COMMENT 'Originating HESA delivery'
                )
                COMMENT='HESA-provided LOV for SEXID code';
            """,
        'load_static_22056_sexort':
            """
            CREATE TABLE load_static_22056_sexort (
                code VARCHAR(5) COMMENT 'HESA-internal LOV code',
                label VARCHAR(400) COMMENT 'Description for LOV code',
                hesa_delivery VARCHAR(10) DEFAULT '22056' COMMENT 'Originating HESA delivery'
                )
                COMMENT='HESA-provided LOV for SEXORT code';
            """,
        'load_static_22056_trans':
            """
            CREATE TABLE load_static_22056_trans (
                code VARCHAR(5) COMMENT 'HESA-internal LOV code',
                label VARCHAR(400) COMMENT 'Description for LOV code'
                hesa_delivery VARCHAR(10) DEFAULT '22056' COMMENT 'Originating HESA delivery'
                )
                COMMENT='HESA-provided LOV for TRANS code';
            """,
        'load_hesa_static_22056_z_ethnicgrp1':
            """
            CREATE TABLE load_static_22056_z_ethnicgrp1 (
                code VARCHAR(5) COMMENT 'HESA-internal LOV code',
                label VARCHAR(400) COMMENT 'Description for LOV code',
                hesa_delivery VARCHAR(10) DEFAULT '22056' COMMENT 'Originating HESA delivery'
                )
                COMMENT='HESA-provided LOV for Z_ETHNICGRP1';
            """,
        'load_static_22056_z_ethnicgrp2':
            """
            CREATE TABLE load_static_22056_z_ethnicgrp2 (
                code VARCHAR(5) COMMENT 'HESA-internal LOV code',
                label VARCHAR(400) COMMENT 'Description for LOV code',
                hesa_delivery VARCHAR(10) DEFAULT '22056' COMMENT 'Originating HESA delivery'
                )
                COMMENT='HESA-provided LOV for Z_ETHNICGRP2';
            """,
        'load_static_22056_z_ethnicgrp3':
            """
            CREATE TABLE load_static_22056_z_ethnicgrp3 (
                code VARCHAR(5) COMMENT 'HESA-internal LOV code',
                label VARCHAR(400) COMMENT 'Description for LOV code'.
                hesa_delivery VARCHAR(10) DEFAULT '22056' COMMENT 'Originating HESA delivery'
                )
                COMMENT='HESA-provided LOV for Z_ETHNICGRP3';
            """,
    }

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
        print(f"Error during creation of {table_name}: {err}")
        raise




def main():
    """
    Creates tables for the 'college' sample database.
    """
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
        print(f"Error during table creation: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()


if __name__ == '__main__':
    main()
