"""
Get distinct programme details from un-normalised load table, store in nn056 stage table. 
"""
import pandas as pd
import logging
from utils.etl_utils import get_config, set_up_logging, connect_to_db


def init():
    """Set generic config and process-specific additional (filenames, etc)"""
    config = get_config()
    set_up_logging(config)

    # Process-specific config (typically filenames)
    config['output_table'] = 'stage_hesa_nn056_programs'

    return config


def read_in_chunks(conn, chunk_size=200):
    """Generator function, executes main query, returns in chunks."""
    try:
        cursor = conn.cursor(buffered=True)

        sql_select = """
                    SELECT DISTINCT t1.program_guid, t1.program_code, t1.program_name, t1.source_file, t1.hesa_delivery
                    FROM load_hesa_22056_student_programs t1
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

        logging.info(f"Read {total_read} rows from main query")

    except Exception as e:
        logging.info(f"Error loading SQL results set into DataFrame: {e}")
        raise


def cleardown_sql_table(conn, config):
    """
    Delete all rows from stage table (as a stage table it is not a persistent data store).
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
    """Write input rows to output table."""
    try:
        cursor = conn.cursor(buffered=True)

        # Declare which retrieved columns to use as insert values
        input_cols = ['program_guid', 'program_code', 'program_name',
                    'source_file', 'hesa_delivery']

        # Build array of tuples as values for db mass-insert
        data_for_insert = input_df[input_cols].values.tolist()

        # Setup insert command (with value placeholders)
        insert_cmd = f"""
            INSERT  INTO {config['output_table']}
                        (program_guid, program_code, program_name,
                        source_file, hesa_delivery)
                    VALUES (%s, %s, %s, %s, %s)
            """

        # bulk insert CSV data to load_students
        cursor.executemany(insert_cmd, data_for_insert)

    except Exception as e:
        logging.critical(f"Error writing to {config['output_table']}: {e}")
        raise


def main():
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
