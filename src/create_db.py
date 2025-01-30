"""
This module creates college database and tables.
"""
import sqlite3
import os


def connect_or_create_db():
    # Connect to database (create if necessary)
    script_dir = os.path.dirname(os.path.abspath(__file__))
    db_path = os.path.join(script_dir, '../data/college_dev.db')
    conn = sqlite3.connect(db_path, timeout=30)
    return conn


def set_journaling_mode(conn: sqlite3.Connection):
    cursor = conn.cursor()
    cursor.execute("PRAGMA journal_mode=WAL;") # WAL or DELETE
    cursor.close()


def show_journaling_mode(conn: sqlite3.Connection):
    cursor = conn.cursor()
    cursor.execute("PRAGMA journal_mode;") # WAL or delete
    journal_mode = cursor.fetchone()[0]
    print(f"Current journal mode: {journal_mode}")
    cursor.close()


def create_load_students(conn: sqlite3.Connection):
    """ Creates student load table if not found"""

    table_name = 'load_students'
    # Declare cursor var here so always available in except/finally scopes
    cursor = None

    try:
        cursor = conn.cursor()
        create_student_table = f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
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
                );
            """

        cursor.execute(create_student_table)
        conn.commit()
        print(f"Checked or created table: {table_name}")

    except Exception as e:
        conn.rollback()
        print(f"Error during creation of load_students: {e}")
        raise

    finally:
        # Attempt to close only if cursor was instantiated
        if cursor:
            cursor.close()



def main():
    """
    Creates tables for the 'college' sample database.
    """
    # Declare here to ensure except/finally work if connection fails
    conn = None

    try:
        # Create/connect to DB
        conn = connect_or_create_db()
        set_journaling_mode(conn)   # experimental to see if fixes DBeaver lock error
        show_journaling_mode(conn)

        # Create students table
        create_load_students(conn)

    except Exception as e:
        print(f"Error during database creation: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()


if __name__ == '__main__':
    print("Database creation started")
    main()
    print("Database creation finished")
