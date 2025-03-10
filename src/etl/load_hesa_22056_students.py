import pandas as pd
import os
import logging
from utils.etl_utils import get_config, set_up_logging, connect_to_db


def init():
    """Set generic config and process-specific additional (filenames, etc)"""
    config = get_config()
    set_up_logging(config)

    # Process-specific config (typically filenames)
    config['output_table'] = 'load_hesa_22056_students'
    config["input_file"] = "students_transformed.csv"
    config['input_path'] = os.path.join(config['transformed_dir'], 'students_transformed.csv')
    config['column_mappings'] = {
            'student_guid': 'student_guid',
            'first_names': 'first_names',
            'last_name': 'last_name',
            'dob': 'dob',
            'phone': 'phone',
            'email': 'email',
            'home_address': 'home_addr',
            'home_postcode': 'home_postcode',
            'home_country': 'home_country',
            'term_address': 'term_addr',
            'term_postcode': 'term_postcode',
            'term_country': 'term_country'
    }

    return config


def read_students_in_chunks(config, chunk_size=200):
    """Generator function, reads students CSV, returns in chunks."""
    try:
        csv_path = config['input_path']
        total_read = 0

        for chunk in pd.read_csv(csv_path, chunksize=chunk_size, dtype=str):
            chunk['dob'] = pd.to_datetime(chunk['dob'], format='%Y-%m-%d')    # convert DoB file to date type to match db col
            total_read += len(chunk)
            yield chunk

        logging.info(f"Read {total_read} rows from {csv_path}")

    except Exception as e:
        logging.info(f"Error loading CSV file into DataFrame: {e}")
        raise


def cleardown_sql_table(cursor, config):
    """
    Delete all rows from the load_students table (as a load table
    its contents do not persist over time).
    """
    try:
        # Get row count to be displayed after delete is finished
        cursor.execute(f"SELECT COUNT(*) FROM {config['output_table']}")
        row_count = cursor.fetchone()[0]
        
        # Delete all rows from the table (commit logic is in 'main')
        cursor.execute(f"DELETE FROM {config['output_table']}")

        logging.info(f"Deleted {row_count} rows from {config['output_table']}")

    except Exception as e:
        logging.critical(f"Error clearing down SQL table {config['output_table']}: {e}")
        raise


def write_to_db_execute_many(csv_df: pd.DataFrame, cursor, config):
    """Writes CSV rows to SQL table load_students."""
    try:
        # Set up lists of source (csv) and destination (sql table) columns
        column_mappings: dict = config['column_mappings']
        source_cols = list(column_mappings.keys())
        target_cols = list(column_mappings.values())

        # Build list of tuples as values for db mass-insert
        data_for_insert = csv_df[source_cols].values.tolist()

        # Add 'source file' column to target columns list and
        # add corresponding source filename to value row.
        target_cols.append('source_file')
        for row in data_for_insert:
            row.append(config["input_file"])

        # Setup insert command (with value placeholders)
        columns = ', '.join(target_cols)
        placeholders = ', '.join(['%s'] * len(target_cols))
        insert_cmd = f"""
            INSERT INTO {config['output_table']}
                        ({columns})
                    VALUES ({placeholders})
            """

        # bulk insert CSV data to load_students
        cursor.executemany(insert_cmd, data_for_insert)

    except Exception as e:
        logging.critical(f"Error loading CSV data to {config['output_table']}: {e}")
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
        cursor = conn.cursor()

        # Delete existing data from load_students table
        cleardown_sql_table(cursor, config)

        # Read data from CSV file
        total_written = 0
        for chunk in(read_students_in_chunks(config)):
            write_to_db_execute_many(chunk, cursor, config)
            conn.commit()
            total_written += len(chunk)

        logging.info(f"Wrote {total_written} rows to SQL table")

    except Exception as e:
        # In case of error, rollback DB transaction and display error
        logging.critical(f"Error in ETL process: {e}")
        if conn:    conn.rollback()
        raise

    finally:
        if cursor:  cursor.close()
        if conn:    conn.close()

if __name__ == '__main__':
    main()
