"""
Merge all 22056/23056/etc student and demographic load tables into one nn056 stage table. 
"""
import pandas as pd
import logging
from utils.etl_utils import get_config, set_up_logging, connect_to_db


def init():
    """Set generic config and process-specific additional (filenames, etc)"""
    config = get_config()
    set_up_logging(config)

    # Process-specific config (typically filenames)
    config['output_table'] = 'stage_hesa_nn056_students'
    config['output_cols'] = ['student_guid', 'first_names', 'last_name', 'phone', 'email', 'dob',
                            'home_addr', 'home_postcode', 'home_country', 'term_addr', 'term_postcode', 'term_country',
                            'source_file', 'hesa_delivery', 'ethnicity', 'gender', 'religion', 'sexid', 'sexort', 'trans',
                            'ethnicity_grp1', 'ethnicity_grp2', 'ethnicity_grp3']

    config['source_query'] = {'sql': """
                                    SELECT t1.student_guid, t1.first_names, t1.last_name, t1.phone, t1.email, t1.dob,
                                        t1.home_addr, t1.home_postcode, t1.home_country, t1.term_addr, t1.term_postcode, t1.term_country,
                                        t1.source_file, t1.hesa_delivery,
                                        t2.ethnicity, t2.gender, t2.religion, t2.sexid, t2.sexort, t2.trans,
                                        t2.ethnicity_grp1, t2.ethnicity_grp2, t2.ethnicity_grp3
                                    FROM load_hesa_22056_students t1
                                    INNER JOIN load_hesa_22056_student_demographics t2
                                            ON t2.student_guid = t1.student_guid;
                                    """,
                             'cols': ['student_guid', 'first_names', 'last_name', 'phone', 'email', 'dob',
                                      'home_addr', 'home_postcode', 'home_country', 'term_addr', 'term_postcode', 'term_country',
                                      'source_file', 'hesa_delivery', 'ethnicity', 'gender', 'religion', 'sexid', 'sexort', 'trans',
                                      'ethnicity_grp1', 'ethnicity_grp2', 'ethnicity_grp3']}


    return config


def read_in_chunks(conn, config, chunk_size=200):
    """Generator function, executes main query, returns in chunks."""
    cursor = conn.cursor(buffered=True)

    cursor.execute(config['source_query']['sql'])
    total_read = 0

    while True:
        chunk = cursor.fetchmany(chunk_size)
        if not chunk:
            break

        # Convert chunk to dataframe (using cursor)
        df_chunk = pd.DataFrame(chunk, columns=config['source_query']['cols'])
        total_read += len(chunk)
        yield df_chunk

    logging.info(f"Read {total_read} rows from main join")


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
    cursor = conn.cursor(buffered=True)

    input_cols = config['source_query']['cols']
    output_cols = config['output_cols']

    # Build array of tuples as values for db mass-insert
    data_for_insert = input_df[input_cols].values.tolist()

    # Dynamically generate insert statement
    columns = ', '.join(output_cols)
    placeholders = ', '.join(['%s'] * len(output_cols))

    insert_cmd = f"""
        INSERT  INTO {config['output_table']}
                    ({columns})
                VALUES ({placeholders})
        """

    cursor.executemany(insert_cmd, data_for_insert)


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
        for chunk in(read_in_chunks(conn, config)):
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
