import pandas as pd
import os
import logging
from src.etl.core.etl_utils import get_config, set_up_logging, connect_to_db

class CsvTableCopier():
    """
    Helper class to bulk copy from a CSV file to a SQL table.

    Usage: instantiate and then call transfer_data.
    """
    def __init__(self, source_path: str, target_table: str,
                 column_mappings: dict, caller_name: str = None):
        """Constructor for CsvTableCopier object. Parameters:
            - source_path : fully qualified path of source CSV file
            - target_table : table to which data is written
            - column_mappings : dictionary of column name pairs (csv col: table col)
            - caller_name : name of the calling script/module (for logging)
        """
        self.config = get_config()
        script_name = caller_name or self.__class__.__name__
        set_up_logging(self.config, script_name)

        self.config["source_path"] = source_path
        self.config["target_table"] = target_table
        self.config["column_mappings"] = column_mappings


    def _read_in_chunks(self, chunk_size=200):
        """Generator function, reads CSV file, returns in chunks of records."""
        try:
            csv_path = self.config["source_path"]
            total_read = 0

            for chunk in pd.read_csv(csv_path, chunksize=chunk_size, dtype=str):
                total_read += len(chunk)
                yield chunk

            logging.info(f"Read {total_read} rows from {csv_path}")

        except Exception as e:
            logging.info(f"Error loading CSV file into DataFrame: {e}")
            raise


    def _cleardown_target(self, cursor):
        """
        Delete all rows from the load_students table (as a load table
        its contents do not persist over time).
        """
        try:
            # Get row count to be displayed after delete is finished
            cursor.execute(f"SELECT COUNT(*) FROM {self.config['target_table']}")
            row_count = cursor.fetchone()[0]
            
            # Delete all rows from the table (commit logic is in 'main')
            cursor.execute(f"DELETE FROM {self.config['target_table']}")

            logging.info(f"Deleted {row_count} rows from {self.config['target_table']}")

        except Exception as e:
            logging.critical(f"Error clearing down table {self.config['target_table']}: {e}")
            raise


    def _write_to_target(self, csv_df: pd.DataFrame, cursor):
        """Writes CSV rows to SQL table"""
        try:
            # Declare which csv columns to use as insert values
            column_mappings: dict = self.config["column_mappings"]
            source_cols = list(column_mappings.keys())
            target_cols = list(column_mappings.values())

            # Build array of tuples as values for db mass-insert
            data_for_insert = csv_df[source_cols].values.tolist()

            # Add "source file" column to target columns list and
            # add corresponding source filename to value row.
            target_cols.append("source_file")
            source_file = os.path.basename(self.config["source_path"])
            for row in data_for_insert:
                row.append(source_file)

            # Setup insert command (with value placeholders)
            columns = ", ".join(target_cols)
            placeholders = ", ".join(["%s"] * len(target_cols))
            insert_cmd = f"""
                INSERT INTO {self.config['target_table']}
                            ({columns})
                        VALUES ({placeholders})
                """

            # bulk insert CSV data to load_students
            cursor.executemany(insert_cmd, data_for_insert)

        except Exception as e:
            logging.critical(f"Error loading CSV data: {e}")
            raise


    def transfer_data(self):
        """Main method: gets config, clears down target, copies data."""
        # Declare here so guaranteed available in except/finally blocks
        conn = None
        cursor = None

        try:
            # Connect to database
            conn = connect_to_db(self.config)
            cursor = conn.cursor()

            # Delete existing data from load_students table
            self._cleardown_target(cursor)

            # Read data from CSV file
            total_written = 0
            for chunk in(self._read_in_chunks()):
                self._write_to_target(chunk, cursor)
                conn.commit()
                total_written += len(chunk)

            logging.info(f"Wrote {total_written} rows to table {self.config['target_table']}")

        except Exception as e:
            # In case of error, rollback DB transaction and display error
            logging.critical(f"Error in ETL process: {e}")
            if conn:    conn.rollback()
            raise

        finally:
            if cursor:  cursor.close()
            if conn:    conn.close()
