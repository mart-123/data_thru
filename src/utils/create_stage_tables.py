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
        create_stage_students(cursor)
        create_stage_programs(cursor)
        create_stage_student_programs(cursor)
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
