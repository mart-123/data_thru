"""
This module creates college database and tables.
"""
import sqlite3
import os

def create_load_students(conn: sqlite3.Connection):
    """ Creates student load table if not found"""

    table_name = 'load_students'
    cursor = conn.cursor()

    try:
        # Check whether table already exists
        cursor.execute('''
            SELECT name FROM sqlite_master
            WHERE type='table' AND name=?
            ''', (table_name,))
    except Exception as e:
        print(f"Error checking whether table load_students is defined: {e}")
        raise

    try:
        # Create table if it doesn't exist
        if cursor.fetchone():
            print(f"Table {table_name} already exists")
        else:
            print(f"Creating table: {table_name}")

            create_student_table = '''
                CREATE TABLE IF NOT EXISTS load_students (
                    student_id INTEGER PRIMARY KEY AUTOINCREMENT,
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
                    )
                '''

            cursor.execute(create_student_table)
            conn.commit()
            
    except Exception as e:
        print(f"Error during creation of load_students: {e}")
        raise


def main():
    """
    Creates tables for the 'college' sample database.
    """
    try:
        # Connect to database (create if necessary)
        script_dir = os.path.dirname(os.path.abspath(__file__))
        db_path = os.path.join(script_dir, '../data/college_dev.db')
        conn = sqlite3.connect(db_path)

        # Create students table
        create_load_students(conn)

    except Exception as e:
        print(f"Error during database creation: {e}")
        conn.rollback()
    finally:
        conn.close()


if __name__ == '__main__':
    print("Database creation started")
    main()
    print("Database creation finished")
