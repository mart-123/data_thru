import pandas as pd
import logging
from ingest.core.etl_utils import get_config, set_up_logging, connect_to_db

class TableCopier():
    """
    Helper class to bulk copy from one table to another, using given SQL query as source.

    Usage: instantiate and then call transfer_data.
    """
    
    def __init__(self, source_sql: str, source_cols: list[str],
                 target_table: str, target_cols: list[str], caller_name: str = None):
        """Initialises TableCopier, fetches config (file paths, db info), sets up logging.
            Parameters:
                source_sql : select statement to get data from source table
                source_cols : column names (in results DF) to use as insert values
                target_table : table referenced by insert statement
                target_cols : target table columns to use in insert statement
                caller_name : name of the calling script/module (for logging)

            Note: 'source_cols' and 'target_cols' should correspond by position and type
        """
        self.config = get_config()
        script_name = caller_name or self.__class__.__name__
        set_up_logging(self.config, script_name)

        self.config["source_sql"] = source_sql
        self.config["source_cols"] = source_cols
        self.config["target_table"] = target_table
        self.config["target_cols"] = target_cols
        

    def _read_in_chunks(self, conn, chunk_size=200):
        """Generator function, executes main query and returns results in chunks."""
        cursor = conn.cursor(buffered=True)

        cursor.execute(self.config["source_sql"])
        total_read = 0

        while True:
            chunk = cursor.fetchmany(chunk_size)
            if not chunk:
                break

            total_read += len(chunk)

            df_chunk = pd.DataFrame(chunk, columns=self.config["source_cols"]) 
            yield df_chunk

        logging.info(f"Read {total_read} rows from main query")


    def _cleardown_target(self, conn):
        """Delete all rows from stage table about to be written."""
        cursor = conn.cursor(buffered=True)
        target_table = self.config["target_table"]

        cursor.execute(f"SELECT COUNT(*) FROM {target_table}")
        row_count = cursor.fetchone()[0]
        cursor.execute(f"DELETE FROM {target_table}")

        logging.info(f"Deleted {row_count} rows from {target_table}")


    def _write_to_target(self, input_df: pd.DataFrame, conn):
        """Writes given dataframe to target table."""
        cursor = conn.cursor(buffered=True)

        input_cols = self.config["source_cols"]
        target_cols = self.config["target_cols"]
        target_table = self.config["target_table"]

        # Build list of tuples (insert values) using cols from input select
        data_for_insert = input_df[input_cols].values.tolist()

        # Build and execute insert statement
        columns = ", ".join(target_cols)
        placeholders = ", ".join(["%s"] * len(target_cols))

        insert_cmd = f"""
            INSERT  INTO {target_table}
                        ({columns})
                    VALUES ({placeholders})
            """

        cursor.executemany(insert_cmd, data_for_insert)


    def transfer_data(self):
        """
        Runs entire table transfer:
            - Connects to db
            - Clears target table
            - Reads data using source query
            - Writes results to target table
        """
        conn = None

        try:
            conn = connect_to_db(self.config)
            self._cleardown_target(conn)

            total_written = 0
            for chunk in(self._read_in_chunks(conn)):
                self._write_to_target(chunk, conn)
                conn.commit()
                total_written += len(chunk)

            logging.info(f"Wrote {total_written} rows to SQL table")

        except Exception as e:
            logging.critical(f"Error in ETL process: {e}")
            if conn:
                conn.rollback()
            raise

        finally:
            if conn:
                conn.close()

