import sqlite3
import pandas as pd
import os

def get_sql_connection():
    try:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        db_path = os.path.join(script_dir, '../data/college_dev.db')
        print(f"Opening SQL connection to {db_path}")
        conn = sqlite3.connect(db_path, timeout=10)
        return conn

    except Exception as e:
        print(f"Error opening SQL db connection: {e}")
        raise



def read_student_csv_file():
    """
    Reads CSV file of student records, returns the resulting DataFrame.
    """
    try:
        # Build path string for csv file
        script_path = os.path.dirname(os.path.abspath(__file__))
        csv_path = os.path.join(script_path, '../data/students_transformed.csv')

        # Read CSV file and return the resulting DataFrame
        df = pd.read_csv(csv_path)
        print(f"Reading {len(df)} rows from {csv_path}")
        return df

    except Exception as e:
        print(f"Error loading CSV file into DataFrame: {e}")
        raise



def cleardown_sql_table(cursor: sqlite3.Cursor):
    """
    Delete all rows from the load_students table (as a load table
    its contents do not persist over time).
    """
    try:
        # Get row count to be displayed after delete is finished
        cursor.execute("SELECT COUNT(*) FROM load_students")
        row_count = cursor.fetchone()[0]
        
        # Delete all rows from the table (commit logic is in 'main')
        cursor.execute("DELETE FROM load_students")

        print(f"Deleted {row_count} rows from load_students")

    except Exception as e:
        print(f"Error clearing down SQL table load_students: {e}")
        raise



def write_to_db_row_by_row(csv_df: pd.DataFrame, cursor: sqlite3.Cursor):
    """
    Iterates over given DataFrame, writes each row to SQL table load_students.
    This version executes an insert per row. Least efficient.
    """
    try:
        # Setup insert command (with value placeholders)
        insert_cmd = """
            INSERT  INTO load_students
                        (student_guid, first_names, last_name, dob, phone, email, home_addr, home_postcode, home_country,
                        term_addr, term_postcode, term_country)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
        
        # Iterate through CSV file, executing insert command for each record
        # (note: commit logic is in 'main')
        for _, row in csv_df.iterrows():
            insert_vals = (row['student_guid'], row['first_names'], row['last_name'], row['dob'], row['phone'], row['email'],
                            row['home_address'], row['home_postcode'], row['home_country'],
                            row['term_address'], row['term_postcode'], row['term_country'])

            cursor.execute(insert_cmd, insert_vals)

        print(f"Wrote {len(csv_df)} rows to load_students")

    except Exception as e:
        print(f"Error writing CSV data to load_students: {e}")
        raise



def write_to_db_execute_many(csv_df: pd.DataFrame, cursor: sqlite3.Cursor):
    """
    Iterates over given DataFrame, writes each row to SQL table load_students.
    This version executes all inserts in a single operation.
    Faster than row_by_row but holds a lot of RAM.
    """
    try:
        # Declare which csv columns to use as insert values
        csv_cols = ['student_guid', 'first_names', 'last_name', 'dob', 'phone', 'email',
                    'home_address', 'home_postcode', 'home_country',
                    'term_address', 'term_postcode', 'term_country']

        # Build array of tuples as values for db mass-insert
        data_for_insert = csv_df[csv_cols].to_records(index=False)

        # Setup insert command (with value placeholders)
        insert_cmd = """
            INSERT  INTO load_students
                        (student_guid, first_names, last_name, dob, phone, email, home_addr, home_postcode, home_country,
                        term_addr, term_postcode, term_country)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """

        # bulk insert CSV data to load_students
        cursor.executemany(insert_cmd, data_for_insert)

        print(f"Wrote {len(csv_df)} rows to load_students")

    except Exception as e:
        print(f"Error writing CSV data to load_students: {e}")
        raise



def main():
    """
    Main function to load CSV of cleansed/transformed student data into SQL db. Steps are:
        - opens CSV file
        - sets up SQL cursor
        - cleardown the destination SQL table
        - copy CSV data to SQL table
    """
    print("Started process to load student CSV to SQL.")

    # Declare here so guaranteed available in except/finally blocks
    conn = None
    cursor = None

    try:
        # Connect to database
        conn = get_sql_connection()
        cursor = conn.cursor()

        # Read data from CSV file
        csv_df = read_student_csv_file()

        # Delete existing data from load_students table
        cleardown_sql_table(cursor)

        # Write CSV data to load_students table
#        write_to_db_row_by_row(csv_df, cursor)
        write_to_db_execute_many(csv_df, cursor)
        conn.commit()

        print("Student load complete")
    except Exception as e:
        # In case of error, rollback DB transaction and display error
        if conn:
            conn.rollback()
        print(f"DB transaction failed and rolled back: {e}")
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

if __name__ == '__main__':
    main()
