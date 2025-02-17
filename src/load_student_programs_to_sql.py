import mysql.connector
from mysql.connector import errorcode
import mysql.connector.cursor
import pandas as pd
import os
import logging
import subprocess
from etl_utils import get_config, set_up_logging


def init():
    """Set generic config and process-specific additional (filenames, etc)"""
    config = get_config()
    set_up_logging(config)

    # Process-specific config (filenames)
    config['input_path'] = os.path.join(config['transformed_dir'], 'student_programs_transformed.csv')

    return config


def get_windows_host_ip():
    """Retrieves Windows host IP address (WSL2 loopback address)."""
    try:
        result = subprocess.run(['grep', 'nameserver', '/etc/resolv.conf'], capture_output=True, text=True)
        ip_address = result.stdout.split()[1]
        return ip_address
    except Exception as e:
        logging.critical(f"Error retrieving Windows host IP address: {e}")
        raise


def connect_to_db(config, ip_addr: str):
    """Connects to MySQL database and returns connection object"""
    try:
        conn = mysql.connector.connect(
            host=ip_addr,
            port=config['db_port'],
            user=config['db_user'],
            password=config['db_pwd'],
            database=config['db_name']
            )

        logging.info(f"Connected to db: {config['db_name']} host: {ip_addr} port: {config['db_port']}")
        return conn

    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            logging.critical(f"MySQL access denied, check credentials (config). Host: {ip_addr} port: {config['db_port']}, db: {config['db_name']}")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            logging.critical(f"MySQL database not found. Host: {ip_addr}, port: {config['db_port']}, db: {config['db_name']}")
        else:
            logging.critical(f"MySQL error: {err}")
        
        # raise RuntimeError(f"Failed db connection. Host: {ip_addr}, port: {config['db_port']}, db: {config['db_name']}")
        raise


def read_csv_in_chunks(config, chunk_size=200):
    """Generator function, reads students CSV, returns in chunks."""
    try:
        csv_path = config['input_path']
        total_read = 0

        for chunk in pd.read_csv(csv_path, chunksize=chunk_size):
            # add data type transformations here
            # chunk['dob'] = pd.to_datetime(chunk['dob'], format='%Y-%m-%d')    # convert DoB file to date type
            total_read += len(chunk)
            yield chunk

        logging.info(f"Read {total_read} rows from {csv_path}")

    except Exception as e:
        logging.info(f"Error loading CSV file into DataFrame: {e}")
        raise


def cleardown_sql_table(cursor):
    """
    Delete all rows from the load_students table (as a load table
    its contents do not persist over time).
    """
    try:
        table_name = "load_student_programs"

        # Get row count to be displayed after delete is finished
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        row_count = cursor.fetchone()[0]
        
        # Delete all rows from the table (commit logic is in 'main')
        cursor.execute(f"DELETE FROM {table_name}")

        logging.info(f"Deleted {row_count} rows from {table_name}")

    except Exception as e:
        logging.critical(f"Error clearing down SQL table {table_name}: {e}")
        raise


def write_to_db_execute_many(csv_df: pd.DataFrame, cursor):
    """Writes CSV rows to SQL table load_students."""
    try:
        table_name = 'load_student_programs'

        # Declare which csv columns to use as insert values
        csv_cols = ['student_guid', 'email', 'program_guid', 'program_code',
                    'program_name', 'enrol_date', 'fees_paid']

        # Build array of tuples as values for db mass-insert
        data_for_insert = csv_df[csv_cols].values.tolist()

        # Setup insert command (with value placeholders)
        insert_cmd = f"""
            INSERT  INTO {table_name}
                        (student_guid, email, program_guid, program_code, program_name, enrol_date, fees_paid)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
            """

        # bulk insert CSV data to load_students
        cursor.executemany(insert_cmd, data_for_insert)

    except Exception as e:
        logging.critical(f"Error writing CSV data to {table_name}: {e}")
        raise


def main():
    """Main logic, loads cleansed student data into SQL db."""
    config = init()

    # Declare here so guaranteed available in except/finally blocks
    conn = None
    cursor = None

    try:
        # Connect to database
        ip_addr = get_windows_host_ip()
        conn = connect_to_db(config, ip_addr)
        cursor = conn.cursor()

        # Delete existing data from load_students table
        cleardown_sql_table(cursor)

        # Read data from CSV file
        total_written = 0
        for chunk in(read_csv_in_chunks(config)):
            write_to_db_execute_many(chunk, cursor)
            conn.commit()
            total_written += len(chunk)

        logging.info(f"Wrote {total_written} rows to SQL table")

    except Exception as e:
        # In case of error, rollback DB transaction and display error
        logging.critical(f"DB transaction failed and rolling back: {e}")
        if conn:    conn.rollback()

    finally:
        if cursor:  cursor.close()
        if conn:    conn.close()

if __name__ == '__main__':
    main()
