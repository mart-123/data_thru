# mysql is running in windows. Basic server info:
# Server instance: MySQL92
"""
This module creates college database and tables.
"""
import mysql.connector
import mysql.connector.cursor
from mysql.connector import errorcode
import subprocess
from etl_utils import get_config, set_up_logging

def init():
    config = get_config()
    set_up_logging(config)

    return config


def get_windows_host_ip():
    """Retrieves Windows host IP address (WSL2 loopback address)."""
    try:
        result = subprocess.run(['grep', 'nameserver', '/etc/resolv.conf'], capture_output=True, text=True)
        ip_address = result.stdout.split()[1]
        print(f"Got windows host IP address: {ip_address}")
        return ip_address
    except Exception as e:
        print(f"Error retrieving Windows host IP address: {e}")
        raise


def connect_to_db(config, ip_addr):
    """Connects to MySQL database and returns connection object"""
    print("Connecting to MySQL database...")
    try:
        conn = mysql.connector.connect(
            host=ip_addr,
            port=config['db_port'],
            user=config['db_user'],
            password=config['db_pwd'],
            database=config['db_name']
            )

        print("Successfully connected to db")
        return conn

    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("MySQL error: access denied, check credentials")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("MySQL error: database not found")
        else:
            print(f"MySQL error: {err}")
        
        # The only place we issue an 'exit' - nothing has yet happened
        exit(1)


def create_load_students(cursor: mysql.connector.cursor.MySQLCursor):
    table_name = 'load_students'

    try:
        cursor.execute(f"""
            SELECT COUNT(*)
            FROM information_schema.tables
            WHERE table_name = '{table_name}'        
            """)
        
        if cursor.fetchone()[0] == 1:
            print(f"Table {table_name} already exists")
        else:
            cursor.execute(f"""
                CREATE TABLE {table_name} (
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
                    dob DATE
                    );
                """)
            
            print(f"Created table: {table_name}")

    except mysql.connector.Error as err:
        print(f"Error during creation of {table_name}: {err}")
        raise


def create_load_student_programs(cursor: mysql.connector.cursor.MySQLCursor):
    table_name = 'load_student_programs'

    try:
        cursor.execute(f"""
            SELECT COUNT(*)
            FROM information_schema.tables
            WHERE table_name = '{table_name}'        
            """)
        
        if cursor.fetchone()[0] == 1:
            print(f"Table {table_name} already exists")
        else:
            cursor.execute(f"""
                CREATE TABLE {table_name} (
                    student_guid CHAR(36) COMMENT 'Source system student id',
                    email VARCHAR(250) COMMENT 'Student email, included here only for data verification',
                    dob DATE COMMENT 'Student DoB, included here only for data verification',
                    program_guid CHAR(36) COMMENT 'Vendor-provided unique id for program of study',
                    program_code VARCHAR(10) COMMENT 'Human-readable, unique code for the program of study',
                    program_name VARCHAR(100) COMMENT 'The program name that will appear on award certificate',
                    enrol_date DATE COMMENT 'Date on which student enrolled for the academic session',
                    fees_paid CHAR(1) COMMENT 'Indicates whether fees have been paid for the academic session'
                    )
                    COMMENT='Vendor program data, re-normalised';
                """)
            
            print(f"Created table: {table_name}")

    except mysql.connector.Error as err:
        print(f"Error during creation of {table_name}: {err}")
        raise


def create_load_demographics(cursor: mysql.connector.cursor.MySQLCursor):
    table_name = 'load_demographics'

    try:
        cursor.execute(f"""
            SELECT COUNT(*)
            FROM information_schema.tables
            WHERE table_name = '{table_name}'        
            """)
        
        if cursor.fetchone()[0] == 1:
            print(f"Table {table_name} already exists")
        else:
            cursor.execute(f"""
                CREATE TABLE {table_name} (
                    student_guid CHAR(36),
                    ethnicity VARCHAR(3),
                    gender VARCHAR(3),
                    religion VARCHAR(3),
                    sexid VARCHAR(3),
                    sexort VARCHAR(3),
                    trans VARCHAR(3),
                    ethnicity_grp1 VARCHAR(3),
                    ethnicity_grp2 VARCHAR(3),
                    ethnicity_grp3 VARCHAR(3)
                    );
                """)
            
            print(f"Created table: {table_name}")

    except mysql.connector.Error as err:
        print(f"Error during creation of {table_name}: {err}")
        raise


def create_stage_students(cursor: mysql.connector.cursor.MySQLCursor):
    table_name = 'stage_students'

    try:
        cursor.execute(f"""
            SELECT COUNT(*)
            FROM information_schema.tables
            WHERE table_name = '{table_name}'        
            """)
        
        if cursor.fetchone()[0] == 1:
            print(f"Table {table_name} already exists")
        else:
            cursor.execute(f"""
                CREATE TABLE {table_name} (
--                    student_id INT AUTO_INCREMENT PRIMARY KEY,
                    student_guid CHAR(36), -- not PK, only for linking to source systems
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
                    dob DATE
                    );
                """)
            
            print(f"Created table: {table_name}")

    except mysql.connector.Error as err:
        print(f"Error during creation of {table_name}: {err}")
        raise


def create_stage_programs(cursor: mysql.connector.cursor.MySQLCursor):
    table_name = 'stage_programs'

    try:
        cursor.execute(f"""
            SELECT COUNT(*)
            FROM information_schema.tables
            WHERE table_name = '{table_name}'        
            """)
        
        if cursor.fetchone()[0] == 1:
            print(f"Table {table_name} already exists")
        else:
            cursor.execute(f"""
                CREATE TABLE {table_name} (
                    program_guid CHAR(36) COMMENT 'Vendor-provided unique id for program of study',
                    program_code VARCHAR(10) COMMENT 'Human-readable, unique code for the program of study',
                    program_name VARCHAR(100) COMMENT 'The program name that will appear on award certificate'
                    )
                    COMMENT='Vendor program data, re-normalised';
                """)
            
            print(f"Created table: {table_name}")

    except mysql.connector.Error as err:
        print(f"Error during creation of {table_name}: {err}")
        raise


def create_stage_student_programs(cursor: mysql.connector.cursor.MySQLCursor):
    table_name = 'stage_student_programs'

    try:
        cursor.execute(f"""
            SELECT COUNT(*)
            FROM information_schema.tables
            WHERE table_name = '{table_name}'        
            """)
        
        if cursor.fetchone()[0] == 1:
            print(f"Table {table_name} already exists")
        else:
            cursor.execute(f"""
                CREATE TABLE {table_name} (
                    program_guid CHAR(36),
                    student_guid CHAR(36),
                    enrol_date DATE,
                    fees_paid CHAR(1)
                    )
                    COMMENT='Xref table relating students to programs; many-to-many cardinality with each table.';
                """)
            
            print(f"Created table: {table_name}")

    except mysql.connector.Error as err:
        print(f"Error during creation of {table_name}: {err}")
        raise


def create_load_hesa_static_22056_disability(cursor: mysql.connector.cursor.MySQLCursor):
    table_name = 'load_hesa_static_22056_disability'

    try:
        cursor.execute(f"""
            SELECT COUNT(*)
            FROM information_schema.tables
            WHERE table_name = '{table_name}'        
            """)
        
        if cursor.fetchone()[0] == 1:
            print(f"Table {table_name} already exists")
        else:
            cursor.execute(f"""
                CREATE TABLE {table_name} (
                    code VARCHAR(5) COMMENT 'HESA-internal code',
                    label VARCHAR(400) COMMENT 'Human-readable description relating to the LOV code'
                    )
                    COMMENT='HESA-provided LOV for disability codes';
                """)
            
            print(f"Created table: {table_name}")

    except mysql.connector.Error as err:
        print(f"Error during creation of {table_name}: {err}")
        raise


def create_load_hesa_static_22056_ethnicity(cursor: mysql.connector.cursor.MySQLCursor):
    table_name = 'load_hesa_static_22056_ethnicity'

    try:
        cursor.execute(f"""
            SELECT COUNT(*)
            FROM information_schema.tables
            WHERE table_name = '{table_name}'        
            """)
        
        if cursor.fetchone()[0] == 1:
            print(f"Table {table_name} already exists")
        else:
            cursor.execute(f"""
                CREATE TABLE {table_name} (
                    code VARCHAR(5) COMMENT 'HESA-internal LOV code',
                    label VARCHAR(400) COMMENT 'Description for LOV code'
                    )
                    COMMENT='HESA-provided LOV for ethnicity codes';
                """)
            
            print(f"Created table: {table_name}")

    except mysql.connector.Error as err:
        print(f"Error during creation of {table_name}: {err}")
        raise


def create_load_hesa_static_22056_genderid(cursor: mysql.connector.cursor.MySQLCursor):
    table_name = 'load_hesa_static_22056_genderid'

    try:
        cursor.execute(f"""
            SELECT COUNT(*)
            FROM information_schema.tables
            WHERE table_name = '{table_name}'        
            """)
        
        if cursor.fetchone()[0] == 1:
            print(f"Table {table_name} already exists")
        else:
            cursor.execute(f"""
                CREATE TABLE {table_name} (
                    code VARCHAR(5) COMMENT 'HESA-internal LOV code',
                    label VARCHAR(400) COMMENT 'Description for LOV code'
                    )
                    COMMENT='HESA-provided LOV for GENDERID codes';
                """)
            
            print(f"Created table: {table_name}")

    except mysql.connector.Error as err:
        print(f"Error during creation of {table_name}: {err}")
        raise


def create_load_hesa_static_22056_religion(cursor: mysql.connector.cursor.MySQLCursor):
    table_name = 'load_hesa_static_22056_religion'

    try:
        cursor.execute(f"""
            SELECT COUNT(*)
            FROM information_schema.tables
            WHERE table_name = '{table_name}'        
            """)
        
        if cursor.fetchone()[0] == 1:
            print(f"Table {table_name} already exists")
        else:
            cursor.execute(f"""
                CREATE TABLE {table_name} (
                    code VARCHAR(5) COMMENT 'HESA-internal LOV code',
                    label VARCHAR(400) COMMENT 'Description for LOV code'
                    )
                    COMMENT='HESA-provided LOV for RELIGION codes';
                """)
            
            print(f"Created table: {table_name}")

    except mysql.connector.Error as err:
        print(f"Error during creation of {table_name}: {err}")
        raise


def create_load_hesa_static_22056_sexid(cursor: mysql.connector.cursor.MySQLCursor):
    table_name = 'load_hesa_static_22056_sexid'

    try:
        cursor.execute(f"""
            SELECT COUNT(*)
            FROM information_schema.tables
            WHERE table_name = '{table_name}'        
            """)
        
        if cursor.fetchone()[0] == 1:
            print(f"Table {table_name} already exists")
        else:
            cursor.execute(f"""
                CREATE TABLE {table_name} (
                    code VARCHAR(5) COMMENT 'HESA-internal LOV code',
                    label VARCHAR(400) COMMENT 'Description for LOV code'
                    )
                    COMMENT='HESA-provided LOV for SEXID codes';
                """)
            
            print(f"Created table: {table_name}")

    except mysql.connector.Error as err:
        print(f"Error during creation of {table_name}: {err}")
        raise


def create_load_hesa_static_22056_sexort(cursor: mysql.connector.cursor.MySQLCursor):
    table_name = 'load_hesa_static_22056_sexort'

    try:
        cursor.execute(f"""
            SELECT COUNT(*)
            FROM information_schema.tables
            WHERE table_name = '{table_name}'        
            """)
        
        if cursor.fetchone()[0] == 1:
            print(f"Table {table_name} already exists")
        else:
            cursor.execute(f"""
                CREATE TABLE {table_name} (
                    code VARCHAR(5) COMMENT 'HESA-internal LOV code',
                    label VARCHAR(400) COMMENT 'Description for LOV code'
                    )
                    COMMENT='HESA-provided LOV for SEXORT codes';
                """)
            
            print(f"Created table: {table_name}")

    except mysql.connector.Error as err:
        print(f"Error during creation of {table_name}: {err}")
        raise


def create_load_hesa_static_22056_trans(cursor: mysql.connector.cursor.MySQLCursor):
    table_name = 'load_hesa_static_22056_trans'

    try:
        cursor.execute(f"""
            SELECT COUNT(*)
            FROM information_schema.tables
            WHERE table_name = '{table_name}'        
            """)
        
        if cursor.fetchone()[0] == 1:
            print(f"Table {table_name} already exists")
        else:
            cursor.execute(f"""
                CREATE TABLE {table_name} (
                    code VARCHAR(5) COMMENT 'HESA-internal LOV code',
                    label VARCHAR(400) COMMENT 'Description for LOV code'
                    )
                    COMMENT='HESA-provided LOV for TRANS codes';
                """)
            
            print(f"Created table: {table_name}")

    except mysql.connector.Error as err:
        print(f"Error during creation of {table_name}: {err}")
        raise


def create_load_hesa_static_22056_z_ethnicgrp1(cursor: mysql.connector.cursor.MySQLCursor):
    table_name = 'load_hesa_static_22056_z_ethnicgrp1'

    try:
        cursor.execute(f"""
            SELECT COUNT(*)
            FROM information_schema.tables
            WHERE table_name = '{table_name}'        
            """)
        
        if cursor.fetchone()[0] == 1:
            print(f"Table {table_name} already exists")
        else:
            cursor.execute(f"""
                CREATE TABLE {table_name} (
                    code VARCHAR(5) COMMENT 'HESA-internal LOV code',
                    label VARCHAR(400) COMMENT 'Description for LOV code'
                    )
                    COMMENT='HESA-provided LOV for Z_ETHNICGRP1';
                """)
            
            print(f"Created table: {table_name}")

    except mysql.connector.Error as err:
        print(f"Error during creation of {table_name}: {err}")
        raise


def create_load_hesa_static_22056_z_ethnicgrp2(cursor: mysql.connector.cursor.MySQLCursor):
    table_name = 'load_hesa_static_22056_z_ethnicgrp2'

    try:
        cursor.execute(f"""
            SELECT COUNT(*)
            FROM information_schema.tables
            WHERE table_name = '{table_name}'        
            """)
        
        if cursor.fetchone()[0] == 1:
            print(f"Table {table_name} already exists")
        else:
            cursor.execute(f"""
                CREATE TABLE {table_name} (
                    code VARCHAR(5) COMMENT 'HESA-internal LOV code',
                    label VARCHAR(400) COMMENT 'Description for LOV code'
                    )
                    COMMENT='HESA-provided LOV for Z_ETHNICGRP2';
                """)
            
            print(f"Created table: {table_name}")

    except mysql.connector.Error as err:
        print(f"Error during creation of {table_name}: {err}")
        raise


def create_load_hesa_static_22056_z_ethnicgrp3(cursor: mysql.connector.cursor.MySQLCursor):
    table_name = 'load_hesa_static_22056_z_ethnicgrp3'

    try:
        cursor.execute(f"""
            SELECT COUNT(*)
            FROM information_schema.tables
            WHERE table_name = '{table_name}'        
            """)
        
        if cursor.fetchone()[0] == 1:
            print(f"Table {table_name} already exists")
        else:
            cursor.execute(f"""
                CREATE TABLE {table_name} (
                    code VARCHAR(5) COMMENT 'HESA-internal LOV code',
                    label VARCHAR(400) COMMENT 'Description for LOV code'
                    )
                    COMMENT='HESA-provided LOV for Z_ETHNICGRP3';
                """)
            
            print(f"Created table: {table_name}")

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
        ip_addr = get_windows_host_ip()
        conn = connect_to_db(config, ip_addr)
        cursor = conn.cursor()
        create_load_students(cursor)
        create_load_student_programs(cursor)
        create_load_demographics(cursor)
        create_stage_students(cursor)
        create_stage_programs(cursor)
        create_stage_student_programs(cursor)
        create_load_hesa_static_22056_disability(cursor)
        create_load_hesa_static_22056_ethnicity(cursor)
        create_load_hesa_static_22056_genderid(cursor)
        create_load_hesa_static_22056_religion(cursor)
        create_load_hesa_static_22056_sexid(cursor)
        create_load_hesa_static_22056_sexort(cursor)
        create_load_hesa_static_22056_trans(cursor)
        create_load_hesa_static_22056_z_ethnicgrp1(cursor)
        create_load_hesa_static_22056_z_ethnicgrp2(cursor)
        create_load_hesa_static_22056_z_ethnicgrp3(cursor)
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
