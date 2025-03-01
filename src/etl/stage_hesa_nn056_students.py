"""
Merge all 22056/23056/etc student and demographic load tables into one nn056 stage table. 
"""
import mysql.connector.cursor
import pandas as pd
import logging
from mysql.connector.cursor import MySQLCursor
from utils.etl_utils import get_config, set_up_logging, connect_to_db


def init():
    """Set generic config and process-specific additional (filenames, etc)"""
    config = get_config()
    set_up_logging(config)

    # Process-specific config (typically filenames)
    config['output_table'] = 'stage_hesa_nn056_students'

    return config


def read_in_chunks(conn, chunk_size=200):
    """Generator function, executes main query, returns in chunks."""
    try:
        cursor = conn.cursor(buffered=True)

        sql_select = """
                SELECT  t1.student_guid, t1.first_names, t1.last_name, t1.phone, t1.email, t1.dob,
                        t1.home_addr, t1.home_postcode, t1.home_country, t1.term_addr, t1.term_postcode, t1.term_country,
                        t1.source_file stu_src_file,
                        t2.ethnicity, t2.gender, t2.religion, t2.sexid, t2.sexort, t2.trans,
                        t2.ethnicity_grp1, t2.ethnicity_grp2, t2.ethnicity_grp3
                FROM load_hesa_22056_students t1
                INNER JOIN load_hesa_22056_student_demographics t2
                        ON t2.student_guid = t1.student_guid;
                """

        cursor.execute(sql_select)
        total_read = 0

        while True:
            chunk = cursor.fetchmany(chunk_size)
            if not chunk:
                break

            # Convert chunk to dataframe (using cursor)
            df_chunk = pd.DataFrame(chunk, columns=[desc[0] for desc in cursor.description])
            total_read += len(chunk)
            yield df_chunk

        logging.info(f"Read {total_read} rows from main join")

    except Exception as e:
        logging.info(f"Error loading CSV file into DataFrame: {e}")
        raise


def cleardown_sql_table(conn, config):
    """
    Delete all rows from the load_students table (as a load table
    its contents do not persist over time).
    """
    try:
        cursor = conn.cursor(buffered=True)

        # Get row count to be displayed after delete is finished
        cursor.execute(f"SELECT COUNT(*) FROM {config['output_table']}")
        row_count = cursor.fetchone()[0]
        
        # Delete all rows from the table (commit logic is in 'main')
        cursor.execute(f"DELETE FROM {config['output_table']}")

        logging.info(f"Deleted {row_count} rows from {config['output_table']}")

    except Exception as e:
        logging.critical(f"Error clearing down SQL table {config['output_table']}: {e}")
        raise


def write_to_db_execute_many(input_df: pd.DataFrame, conn, config):
    """Writes CSV rows to SQL table load_students."""
    try:
        cursor = conn.cursor(buffered=True)

        # Declare which retrieve columns to use as insert values
        input_cols = ['student_guid', 'first_names', 'last_name', 'dob', 'phone', 'email',
                    'home_addr', 'home_postcode', 'home_country',
                    'term_addr', 'term_postcode', 'term_country',
                    'ethnicity', 'gender', 'religion', 'sexid', 'sexort', 'trans',
                    'ethnicity_grp1', 'ethnicity_grp2', 'ethnicity_grp3',
                    'stu_src_file']

        # Build array of tuples as values for db mass-insert
        data_for_insert = input_df[input_cols].values.tolist()

        # Setup insert command (with value placeholders)
        insert_cmd = f"""
            INSERT  INTO {config['output_table']}
                        (student_guid, first_names, last_name, dob, phone, email,
                        home_addr, home_postcode, home_country,
                        term_addr, term_postcode, term_country,
                        ethnicity, gender, religion, sexid, sexort, trans,
                        ethnicity_grp1, ethnicity_grp2, ethnicity_grp3, source_file)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s)
            """

        # bulk insert CSV data to load_students
        cursor.executemany(insert_cmd, data_for_insert)

    except Exception as e:
        logging.critical(f"Error writing to {config['output_table']}: {e}")
        raise


def main():
    """Main logic, loads cleansed student data into SQL db."""
    config = init()

    # Declare here so guaranteed available in except/finally blocks
    conn = None
    cursor = None

    try:
        # Connect to database
        conn = connect_to_db(config)

        # Delete existing data from load_students table
        cleardown_sql_table(conn, config)

        # Read data from CSV file
        total_written = 0
        for chunk in(read_in_chunks(conn)):
            write_to_db_execute_many(chunk, conn, config)
            conn.commit()
            total_written += len(chunk)

        logging.info(f"Wrote {total_written} rows to SQL table")

    except Exception as e:
        # In case of error, rollback DB transaction and display error
        logging.critical(f"DB transaction failed and rolling back: {e}")
        if conn:    conn.rollback()
        raise

    finally:
        if cursor:  cursor.close()
        if conn:    conn.close()

if __name__ == '__main__':
    main()
