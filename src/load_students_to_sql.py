import mysql.connector
from mysql.connector import errorcode
import mysql.connector.cursor
import pandas as pd
import json
import os
import logging
import subprocess


def set_up_logging(env='dev'):
    """
    Set up logging and create dir/file as necessary.
    Optional parameter 'dev'/'test'/'live'
    """
    try:
        log_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), f'../logs/{env}')
        os.makedirs(log_dir, exist_ok=True)
        log_file = os.path.join(log_dir, 'application.log')

        logging.basicConfig(
            filename=log_file,
            filemode="a",    # append mode
            format="%(asctime)s - %(levelname)s - %(filename)s - %(funcName)s - %(message)s", # log format
            level=logging.INFO # logging threshold
        )
    except Exception as e:
        print(f"Error setting up logging: {e}")
        raise



def get_config():
    """Reads json config file, returns contents as a dictionary"""
    try:
        script_path = os.path.dirname(os.path.abspath(__file__))
        config_path = os.path.join(script_path, '../data/config.json')
        config_file = open(config_path, 'r')
        config = json.load(config_file)
        return config
    
    except Exception as e:
        logging.critical(f"Error opening config {config_path}: {e}")
        raise



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



def read_student_csv_file():
    """
    Reads CSV file of student records, returns the resulting DataFrame.
    """
    try:
        # Build path string for csv file
        script_path = os.path.dirname(os.path.abspath(__file__))
        csv_path = os.path.join(script_path, '../data/students_transformed.csv')

        # Define date parser to convert dob (yyyy-mm-dd) to date type
        date_parser = lambda x: pd.to_datetime(x, format='%Y-%m-%d')

        # Read CSV file and return the resulting DataFrame
        df = pd.read_csv(csv_path)
        df['dob'] = pd.to_datetime(df['dob'], format='%Y-%m-%d')
        logging.info(f"Read {len(df)} rows from {csv_path}")
        return df

    except Exception as e:
        logging.info(f"Error loading CSV file into DataFrame: {e}")
        raise



def cleardown_sql_table(cursor):
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

        logging.info(f"Deleted {row_count} rows from load_students")

    except Exception as e:
        logging.critical(f"Error clearing down SQL table load_students: {e}")
        raise



def write_to_db_row_by_row(csv_df: pd.DataFrame, cursor):
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
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
        
        # Iterate through CSV file, executing insert command for each record
        # (note: commit logic is in 'main')
        for _, row in csv_df.iterrows():
            insert_vals = (row['student_guid'], row['first_names'], row['last_name'], row['dob'], row['phone'], row['email'],
                            row['home_address'], row['home_postcode'], row['home_country'],
                            row['term_address'], row['term_postcode'], row['term_country'])

            cursor.execute(insert_cmd, insert_vals)

        logging.info(f"Wrote {len(csv_df)} rows to load_students")

    except Exception as e:
        logging.critical(f"Error writing CSV data to load_students: {e}")
        raise



def write_to_db_execute_many(csv_df: pd.DataFrame, cursor):
    """
    Writes CSV rows to SQL table load_students.
    This version executes all inserts in a single operation.
    Faster than row_by_row but holds a lot of RAM.
    """
    try:
        # Declare which csv columns to use as insert values
        csv_cols = ['student_guid', 'first_names', 'last_name', 'dob', 'phone', 'email',
                    'home_address', 'home_postcode', 'home_country',
                    'term_address', 'term_postcode', 'term_country']

        # Build array of tuples as values for db mass-insert
        data_for_insert = csv_df[csv_cols].values.tolist()

        # Setup insert command (with value placeholders)
        insert_cmd = """
            INSERT  INTO load_students
                        (student_guid, first_names, last_name, dob, phone, email, home_addr, home_postcode, home_country,
                        term_addr, term_postcode, term_country)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """

        # bulk insert CSV data to load_students
        cursor.executemany(insert_cmd, data_for_insert)

        logging.info(f"Wrote {len(csv_df)} rows to load_students")

    except Exception as e:
        logging.critical(f"Error writing CSV data to load_students: {e}")
        raise



def main():
    """
    Main function to load CSV of cleansed/transformed student data into SQL db. Steps are:
        - opens CSV file
        - sets up SQL cursor
        - cleardown the destination SQL table
        - copy CSV data to SQL table
    """
    set_up_logging()

    # Declare here so guaranteed available in except/finally blocks
    conn = None
    cursor = None

    try:
        # Connect to database
        config = get_config()
        ip_addr = get_windows_host_ip()
        conn = connect_to_db(config, ip_addr)
        cursor = conn.cursor()

        # Read data from CSV file
        csv_df = read_student_csv_file()

        # Delete existing data from load_students table
        cleardown_sql_table(cursor)

        # Write CSV data to load_students table
#        write_to_db_row_by_row(csv_df, cursor)
        write_to_db_execute_many(csv_df, cursor)
        conn.commit()

        logging.info("Student load complete")
    except Exception as e:
        # In case of error, rollback DB transaction and display error
        logging.critical(f"DB transaction failed and rolling back: {e}")
        if conn:
            conn.rollback()
        raise
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

if __name__ == '__main__':
    main()
