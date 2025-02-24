import pandas as pd
import os
import logging
from utils.etl_utils import get_config, set_up_logging, connect_to_db


def init():
    """Set generic config and process-specific additional (filenames, etc)"""
    config = get_config()
    set_up_logging(config)

    # Process-specific config (typically filenames)
    config['output_table'] = "load_hesa_22056_lookup_z_ethnicgrp1"
    config["input_file"] = "hesa_22056_Z_ETHNICGRP1.csv"
    config['input_path'] = os.path.join(config['lookups_dir'], config['input_file'])

    return config


def read_in_chunks(config, chunk_size=200):
    """Generator function, reads CSV file, returns in chunks of records."""
    try:
        csv_path = config['input_path']
        total_read = 0

        for chunk in pd.read_csv(csv_path, chunksize=chunk_size, dtype=str):
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
    """Writes CSV rows to SQL table"""
    try:
        # Declare which csv columns to use as insert values
        csv_cols = ['Code', 'Label']

        # Build array of tuples as values for db mass-insert
        data_for_insert = csv_df[csv_cols].values.tolist()

        # Append source filename to each values row for insert
        for row in data_for_insert:
            row.append(config["input_file"])

        # Setup insert command (with value placeholders)
        insert_cmd = f"""
            INSERT  INTO {config['output_table']}
                        (code, label, source_file)
                    VALUES (%s, %s, %s)
            """

        # bulk insert to table
        cursor.executemany(insert_cmd, data_for_insert)

    except Exception as e:
        logging.critical(f"Error loading CSV data: {e}")
        raise


def main():
    """Main logic, loads demographic data into SQL db."""
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
        for chunk in(read_in_chunks(config)):
            write_to_db_execute_many(chunk, cursor, config)
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
