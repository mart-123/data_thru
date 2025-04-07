# mysql is running in windows. Basic server info:
# Server instance: MySQL92
"""
This module creates HESA load tables for the 22056 delivery. For subsequent deliveries
(e.g. 23056, 24056) clone this script with new table names. These subsequent sets of
load tables will need to be added to merge logic for loading the nn056 stage tables.
"""
import mysql.connector
import traceback
import mysql.connector.cursor
from src.etl.core.etl_utils import get_config, set_up_logging, connect_to_db

def init():
    config = get_config()
    set_up_logging(config)

    return config


def generate_create_statements():
    create_statements = {
        'load_hesa_22056_20240331_students':
            """
            CREATE TABLE load_hesa_22056_20240331_students (
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
                load_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT 'Timestamp of insert',
                source_file VARCHAR(250) COMMENT 'File from which data was loaded',
                hesa_delivery VARCHAR(20) DEFAULT '22056_20240331' COMMENT 'Originating HESA delivery'
                )
                COMMENT='Student records';
            """,
        'load_hesa_22056_20240331_student_programs':
            """
            CREATE TABLE load_hesa_22056_20240331_student_programs (
                student_guid CHAR(36) COMMENT 'Source system student id',
                email VARCHAR(250) COMMENT 'Student email, included here only for data verification',
                program_guid CHAR(36) COMMENT 'Vendor-provided unique id for program of study',
                program_code VARCHAR(10) COMMENT 'Human-readable, unique code for the program of study',
                program_name VARCHAR(100) COMMENT 'The program name that will appear on award certificate',
                enrol_date DATE COMMENT 'Date on which student enrolled for the academic session',
                fees_paid CHAR(1) COMMENT 'Indicates whether fees have been paid for the academic session',
                load_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT 'Timestamp of insert',
                source_file VARCHAR(250) COMMENT 'File from which data was loaded',
                hesa_delivery VARCHAR(20) DEFAULT '22056_20240331' COMMENT 'Originating HESA delivery'
                )
                COMMENT='Student-program links combined with program details (i.e. denormalised)';
            """,
        'load_hesa_22056_20240331_student_demographics':
            """
            CREATE TABLE load_hesa_22056_20240331_student_demographics (
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
                load_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT 'Timestamp of insert',
                source_file VARCHAR(250) COMMENT 'File from which data was loaded',
                hesa_delivery VARCHAR(20) DEFAULT '22056_20240331' COMMENT 'Originating HESA delivery'
                );
            """,
        'load_hesa_22056_20240331_lookup_disability':
            """
            CREATE TABLE load_hesa_22056_20240331_lookup_disability (
                code VARCHAR(5) COMMENT 'HESA-internal code',
                label VARCHAR(400) COMMENT 'Human-readable description relating to the lookup code',
                load_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT 'Timestamp of insert',
                source_file VARCHAR(250) COMMENT 'File from which data was loaded',
                hesa_delivery VARCHAR(20) DEFAULT '22056_20240331' COMMENT 'Originating HESA delivery'
                )
                COMMENT='HESA-provided lookup for DISABILTIY codes';
            """,
        'load_hesa_22056_20240331_lookup_ethnicity':
            """
            CREATE TABLE load_hesa_22056_20240331_lookup_ethnicity (
                code VARCHAR(5) COMMENT 'HESA-internal lookup code',
                label VARCHAR(400) COMMENT 'Description for lookup code',
                load_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT 'Timestamp of insert',
                source_file VARCHAR(250) COMMENT 'File from which data was loaded',
                hesa_delivery VARCHAR(20) DEFAULT '22056_20240331' COMMENT 'Originating HESA delivery'
                )
                COMMENT='HESA-provided lookup for ETHNICITY code';
            """,
        'load_hesa_22056_20240331_lookup_genderid':
            """
            CREATE TABLE load_hesa_22056_20240331_lookup_genderid (
                code VARCHAR(5) COMMENT 'HESA-internal lookup code',
                label VARCHAR(400) COMMENT 'Description for lookup code',
                load_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT 'Timestamp of insert',
                source_file VARCHAR(250) COMMENT 'File from which data was loaded',
                hesa_delivery VARCHAR(20) DEFAULT '22056_20240331' COMMENT 'Originating HESA delivery'
                )
                COMMENT='HESA-provided lookup for GENDERID code';
            """,
        'load_hesa_22056_20240331_lookup_religion':
            """
            CREATE TABLE load_hesa_22056_20240331_lookup_religion (
                code VARCHAR(5) COMMENT 'HESA-internal lookup code',
                label VARCHAR(400) COMMENT 'Description for lookup code',
                load_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT 'Timestamp of insert',
                source_file VARCHAR(250) COMMENT 'File from which data was loaded',
                hesa_delivery VARCHAR(20) DEFAULT '22056_20240331' COMMENT 'Originating HESA delivery'
                )
                COMMENT='HESA-provided lookup for RELIGION code';
            """,
        'load_hesa_22056_20240331_lookup_sexid':
            """
            CREATE TABLE load_hesa_22056_20240331_lookup_sexid (
                code VARCHAR(5) COMMENT 'HESA-internal lookup code',
                label VARCHAR(400) COMMENT 'Description for lookup code',
                load_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT 'Timestamp of insert',
                source_file VARCHAR(250) COMMENT 'File from which data was loaded',
                hesa_delivery VARCHAR(20) DEFAULT '22056_20240331' COMMENT 'Originating HESA delivery'
                )
                COMMENT='HESA-provided lookup for SEXID code';
            """,
        'load_hesa_22056_20240331_lookup_sexort':
            """
            CREATE TABLE load_hesa_22056_20240331_lookup_sexort (
                code VARCHAR(5) COMMENT 'HESA-internal lookup code',
                label VARCHAR(400) COMMENT 'Description for lookup code',
                load_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT 'Timestamp of insert',
                source_file VARCHAR(250) COMMENT 'File from which data was loaded',
                hesa_delivery VARCHAR(20) DEFAULT '22056_20240331' COMMENT 'Originating HESA delivery'
                )
                COMMENT='HESA-provided lookup for SEXORT code';
            """,
        'load_hesa_22056_20240331_lookup_trans':
            """
            CREATE TABLE load_hesa_22056_20240331_lookup_trans (
                code VARCHAR(5) COMMENT 'HESA-internal lookup code',
                label VARCHAR(400) COMMENT 'Description for lookup code',
                load_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT 'Timestamp of insert',
                source_file VARCHAR(250) COMMENT 'File from which data was loaded',
                hesa_delivery VARCHAR(20) DEFAULT '22056_20240331' COMMENT 'Originating HESA delivery'
                )
                COMMENT='HESA-provided lookup for TRANS code';
            """,
        'load_hesa_22056_20240331_lookup_z_ethnicgrp1':
            """
            CREATE TABLE load_hesa_22056_20240331_lookup_z_ethnicgrp1 (
                code VARCHAR(5) COMMENT 'HESA-internal lookup code',
                label VARCHAR(400) COMMENT 'Description for lookup code',
                load_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT 'Timestamp of insert',
                source_file VARCHAR(250) COMMENT 'File from which data was loaded',
                hesa_delivery VARCHAR(20) DEFAULT '22056_20240331' COMMENT 'Originating HESA delivery'
                )
                COMMENT='HESA-provided lookup for Z_ETHNICGRP1';
            """,
        'load_hesa_22056_20240331_lookup_z_ethnicgrp2':
            """
            CREATE TABLE load_hesa_22056_20240331_lookup_z_ethnicgrp2 (
                code VARCHAR(5) COMMENT 'HESA-internal lookup code',
                label VARCHAR(400) COMMENT 'Description for lookup code',
                load_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT 'Timestamp of insert',
                source_file VARCHAR(250) COMMENT 'File from which data was loaded',
                hesa_delivery VARCHAR(20) DEFAULT '22056_20240331' COMMENT 'Originating HESA delivery'
                )
                COMMENT='HESA-provided lookup for Z_ETHNICGRP2';
            """,
        'load_hesa_22056_20240331_lookup_z_ethnicgrp3':
            """
            CREATE TABLE load_hesa_22056_20240331_lookup_z_ethnicgrp3 (
                code VARCHAR(5) COMMENT 'HESA-internal lookup code',
                label VARCHAR(400) COMMENT 'Description for lookup code',
                load_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT 'Timestamp of insert',
                source_file VARCHAR(250) COMMENT 'File from which data was loaded',
                hesa_delivery VARCHAR(20) DEFAULT '22056_20240331' COMMENT 'Originating HESA delivery'
                )
                COMMENT='HESA-provided lookup for Z_ETHNICGRP3';
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
#        print(f"Error during table creation: {e}")
        traceback.print_exc()
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()


if __name__ == '__main__':
    main()
