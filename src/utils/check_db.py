"""
Displays table columns in the college database
"""
import sqlite3
import os
from utils.etl_utils import connect_to_db


def display_table_info(conn: sqlite3.Connection, table_name: str):
    cursor = conn.cursor()

    # Check table exists
    cursor.execute('''
        SELECT name from sqlite_master
        WHERE type='table' AND name=?
                   ''', (table_name,))

    if cursor.fetchone():
        print(f"Columns for table {table_name}:")
    else:
        print(f"Table {table_name} not found")
        return None

    # Display columns in table
    cursor.execute(f"PRAGMA table_info({table_name})")
    results = cursor.fetchall()

    for col in results:
        cid, name, type_, notnull, dflt_value, pk = col
        print(f"   cid: {cid} \tname: {name} \ttype: {type_} \tnotnull: {notnull} \tdefault: {dflt_value} \tpk: {pk}")



def check_database_state(conn: sqlite3.Connection):
    try:
        cursor = conn.cursor()

        cursor.execute("PRAGMA journal_mode;")
        journal_mode = cursor.fetchone()[0]
        print(f"Current journal mode: {journal_mode}")

        cursor.execute("PRAGMA busy_timeout;")
        busy_timeout = cursor.fetchone()[0]
        print(f"Busy timeout: {busy_timeout}")

        cursor.close()
        conn.close()
    except Exception as e:
        print(f"Error checking db stateL {e}")



def main():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    db_path = os.path.join(script_dir, '../data/college_dev.db')

    print(f"Checking SQLite db: {db_path}")
    conn = connect_to_db(db_path)
    display_table_info(conn, 'load_students')
    check_database_state(conn)


if __name__ == '__main__':
    main()
