"""
Load nn056 look-up codes in nn056 stage table. 
"""
import pandas as pd
import logging
from utils.etl_utils import get_config, set_up_logging, connect_to_db


def init():
    """Set generic config and process-specific additional (filenames, etc)"""
    config = get_config()
    set_up_logging(config)

    # Process-specific config (typically filenames)
    config['output_table'] = 'stage_hesa_nn056_lookup_z_ethnicgrp2'
    config['output_cols'] = ['code', 'label', 'source_file', 'hesa_delivery']

    config['source_query'] = {
        "sql": """SELECT t1.code, t1.label, t1.source_file, t1.hesa_delivery
                  FROM load_hesa_22056_lookup_z_ethnicgrp2 t1""",
        "cols": ['code', 'label', 'source_file', 'hesa_delivery']
    }

    return config


def read_in_chunks(conn, config, chunk_size=200):
    """Generator function, executes main query, returns in chunks."""
    cursor = conn.cursor(buffered=True)

    cursor.execute(config["source_query"]["sql"])
    total_read = 0

    while True:
        chunk = cursor.fetchmany(chunk_size)
        if not chunk:
            break

        total_read += len(chunk)

        df_chunk = pd.DataFrame(chunk, columns=config['source_query']['cols']) 
        yield df_chunk

    logging.info(f"Read {total_read} rows from main query")


def cleardown_sql_table(conn, config):
    """Delete all rows from stage table about to be written."""

    cursor = conn.cursor(buffered=True)
    cursor.execute(f"SELECT COUNT(*) FROM {config['output_table']}")
    row_count = cursor.fetchone()[0]
    
    cursor.execute(f"DELETE FROM {config['output_table']}")

    logging.info(f"Deleted {row_count} rows from {config['output_table']}")


def write_to_db_execute_many(input_df: pd.DataFrame, conn, config):
    """Write input rows to output table."""
    cursor = conn.cursor(buffered=True)

    input_cols = config['source_query']['cols']
    output_cols = config['output_cols']

    # Build list of tuples (insert values) using cols from input select
    data_for_insert = input_df[input_cols].values.tolist()

    # Dynamically build insert statement
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
    conn = None

    try:
        conn = connect_to_db(config)
        cleardown_sql_table(conn, config)

        total_written = 0
        for chunk in(read_in_chunks(conn, config)):
            write_to_db_execute_many(chunk, conn, config)
            conn.commit()
            total_written += len(chunk)

        logging.info(f"Wrote {total_written} rows to SQL table")

    except Exception as e:
        logging.critical(f"DB transaction failed and rolling back: {e}")
        if conn:
            conn.rollback()
        raise

    finally:
        if conn:
            conn.close()


if __name__ == '__main__':
    main()
