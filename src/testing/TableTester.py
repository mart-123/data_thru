import os
import pandas as pd
import logging
import datetime
from src.etl.core.etl_utils import get_config, set_up_logging, connect_to_db

class TableTester():
    """
    Helper class for testing SQL table contents vs another table/CSV file.
    """
    
    def __init__(self, target_table: str, column_mappings: dict,
                 source_csv: str = "", source_csv_type: str = "", source_table: str = "",
                 caller_name: str = None):
        """Constructor with source and target details provided as arguments.

            Fetches config (database info, file location paths) and sets up logging.

            Args:
                target_table: Table whose contents are being verified.
                column_mappings: Dictionary mapping source column names to target column names.
                source_csv: CSV filename if target was loaded from transformed CSV.
                source_csv_type: Indicates from which directory to read CSV file.
                source_table: Source table if target was loaded from another table.
                caller_name: Name of calling script/module for logging, defaults to 'UnspecifiedCaller'.
            
            Note: column mappings and key column are converted to lowercase to support inconsistently-cased CSV files.
        """
        self.config = get_config()

        # Set up logging
        script_name = caller_name or __class__.__name__
        set_up_logging(self.config, script_name)

        # Initialise test results collection
        self.test_results = []

        # Store source/target details
        self.source_table = source_table
        self.source_csv = source_csv
        self.target_table = target_table

        # Store column mappings (converted to lowercase to support
        # vendor-supplied CSV files containing capitalised column names).
        self.column_mappings = {k.lower(): v.lower() for k, v in column_mappings.items()}

        # Build filepath for source CSV file if provided.
        if source_csv:
            if source_csv_type == 'lookup':
                self.source_csv_path = os.path.join(self.config["lookups_dir"], source_csv)
            elif source_csv_type == 'transformed':
                self.source_csv_path = os.path.join(self.config["transformed_dir"], source_csv)
            elif source_csv_type == 'expected':
                self.source_csv_path = os.path.join(self.config["expected_dir"], source_csv)
            else:
                self.source_csv_path = os.path.join(self.config["transformed_dir"], source_csv)
        else:
            self.source_csv_path = ""


    def _read_table(self, conn, table_name: str, column_names: list):
        """Retrieves data from the given table."

        Sets up and runs SQL query using table name and column names.
        Results are converted to DataFrame, dates cast as strings ('YYYY-MM-DD'). 

        Args:
            conn: Established database connection.
            table_name: Name of table to be queried.
            column_names: List of column names to be retrieved from table.
        
        Returns:
            df: Query results converted to a DataFrame.
        """
        # Build and run select statement
        cursor = conn.cursor(buffered=True)
        query_columns = ','.join(column_names)
        query_sql = f"""SELECT {query_columns}
                        FROM {table_name}"""

        try:
            cursor.execute(query_sql)
            results = cursor.fetchall()
        except Exception as e:
            logging.info(f"Error running query: {e}")
            raise


        # Log results
        total_read = len(results)
        logging.info(f"Read {total_read} rows from {table_name}")

        df = pd.DataFrame(results, columns=column_names) 

        # Convert date columns to string 'YYYY-MM-DD'. This avoids
        # implicit formatting during date->string casting.
        for col in df.columns:
            # Get non-null values (in order to check their type)
            non_null_values = df[col].dropna()

            # Skip columns that have no values
            if len(non_null_values) == 0:
                continue

            # If first value of column is a date, convert entire column to string (yyyy-mm-dd).
            first_value = non_null_values.iloc[0]
            if isinstance(first_value, datetime.date):
                df[col] = df[col].apply(lambda x: x.strftime('%Y-%m-%d') if x is not None else None)

        return df


    def _read_csv(self, csv_path):
        """Retrieves data from the given CSV file and returns a DataFrame.
        Note: column names are converted to lowercase to facilitate key-based matching.
        
        Args:
            csv_path: Fully qualified filename of CSV file.
        
        Returns:
            df: Query results converted to a DataFrame.
        """
        df = pd.read_csv(csv_path, dtype=str)

        # Convert column names to lowercase (to support key-based row matching)
        df.columns = [col.lower() for col in df.columns]

        logging.info(f"Read {len(df)} rows from {self.source_csv_path}")
        return df


    def _tcA_row_count_test(self, source_df: pd.DataFrame, target_df: pd.DataFrame):
        """
        Compares row count between source and target dataframes.
        
        Note: test is expected to fail when ETL process involved UNION/DISTINCT.

        Returns:
            Success: True for success, False for failure
            Results: On success, test description. On failure, test description and failure details
        """
        if len(source_df) == len(target_df):
            return True, f"Source and target both have {len(source_df)} rows"
        else:
            return False, f"Source has {len(source_df)} rows, target has {len(target_df)} rows"


    def _tcB_column_values_test(self, source_df: pd.DataFrame, target_df: pd.DataFrame):
        """
        Compares column values after sorting both dataframes by all mapped columns.
        
        Returns:
            Success: True for success, False for failure
            Results: On success, test description. On failure, test description and failure details
        """
        # Check row counts match
        if len(source_df) != len(target_df):
            return False, f"ERROR: row count mismatch"

        # Sort both dataframes (on all columns rather than a 'key column')
        source_sort_cols = list(self.column_mappings.keys())
        target_sort_cols = list(self.column_mappings.values())
        source_df = source_df.sort_values(by=source_sort_cols).reset_index(drop=True).copy()
        target_df = target_df.sort_values(by=target_sort_cols).reset_index(drop=True).copy()

        # Iterate through source/target row-pairs and build 'row mismatches' list
        mismatched_row_indices = []
        mismatched_row_details = []

        for i in range(0, len(source_df)):
            source_row = source_df.iloc[i]
            target_row = target_df.iloc[i]

            # Iterate through columns to be compared for the row
            column_mismatches = []

            for source_column, target_column in self.column_mappings.items():
                source_val = source_row[source_column]
                target_val = target_row[target_column]

                # Check for source/target mismatch. Match conditions are NaN-NaN or value-value.
                if ((pd.isna(source_val) and not pd.isna(target_val)) or
                    (pd.isna(target_val) and not pd.isna(source_val)) or
                    (source_val != target_val)):
                    column_mismatches.append(f"{target_column}: {target_row[target_column]}, expected: {source_row[source_column]}")

            # If mismatches found in the row pair, append details to list
            if len(column_mismatches) > 0:
                mismatched_row_indices.append(i)

                if len(mismatched_row_details) < 3:
                    first_col = target_sort_cols[0]
                    second_col = target_sort_cols[1]
                    row_id = f"{first_col}: {target_row[first_col]}, {second_col}: {target_row[second_col]}"
                    mismatched_row_details.append(f"            Row {i} ({row_id}): {','.join(column_mismatches)}")

        if len(mismatched_row_indices) == 0:
            return True, f"All {len(source_df)} rows match"
        else:
            error_msg = f"{len(mismatched_row_indices)} mismatched rows out of {len(source_df)} rows. First few:\n"
            error_msg += "\n".join(mismatched_row_details)
            return False, error_msg


    def print_results(self):
        # build pass/fail result lists (result[1] is boolean returned by each test func)
        tests_passed = [result for result in self.test_results if result[1]]
        tests_failed = [result for result in self.test_results if not result[1]]

        print(f"{len(tests_failed)} tests failed:")
        for test in tests_failed:
            print(f"    {test[0]}: FAILED")
            print(f"      {test[2]}")
            print()

        print(f"{len(tests_passed)} tests passed:")
        for test in tests_passed:
            print(f"    {test[0]}: PASSED")
            print(f"      {test[2]}")
            print()

        logging.info(f"tests passed: {len(tests_passed)} failed: {len(tests_failed)}")


    def run_tests(self):
        conn = None

        try:
            conn = connect_to_db(self.config)

            # Get source data
            source_df = None
            if self.source_csv:
                source_df = self._read_csv(self.source_csv_path)
            else:
                source_columns = list(self.column_mappings.keys())
                source_df = self._read_table(conn, self.source_table, source_columns)

            # Get target data
            target_columns = list(self.column_mappings.values())
            target_df = self._read_table(conn, self.target_table, target_columns)

            test_cases = {
                "tcA_row_count_test": self._tcA_row_count_test,
                "tcB_column_values_test": self._tcB_column_values_test,
            }

            # Run all tests and print results
            for test_name, test_method in test_cases.items():
                passed, result = test_method(source_df, target_df)
                self.test_results.append((test_name, passed, result))

            self.print_results()

        finally:
            if conn:
                conn.close()
