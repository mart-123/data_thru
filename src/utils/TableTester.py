import os
import pandas as pd
import logging
import datetime
from src.utils.etl_utils import get_config, set_up_logging, connect_to_db

class TableTester():
    """
    Helper class for testing SQL table contents vs another table/CSV file.
    """
    
    def __init__(self, target_table: str, column_mappings: dict, key_column: str,
                 source_csv: str = "", source_csv_type: str = "", source_table: str = "",
                 caller_name: str = None):
        """Constructor with source and target details provided as arguments.

            Fetches config (database info, file location paths) and sets up logging.

            Args:
                target_table: Table whose contents are being verified.
                column_mappings: Dictionary mapping source column names to target column names.
                key_column: Name of column used to match rows between source/target datasets.
                source_csv: CSV filename if target was loaded from transformed CSV.
                source_csv_type: Indicates from which directory to read CSV file.
                source_table: Source table if target was loaded from another table.
                caller_name: Name of calling script/module for logging, defaults to 'UnspecifiedCaller'.
        """
        self.config = get_config()
        set_up_logging(self.config)
        self.test_results = []

        # Store parameters in instance variables
        self.caller_name = caller_name or "UnspecifiedCaller"
        self.source_table = source_table
        self.source_csv = source_csv
        self.target_table = target_table
        self.column_mappings = column_mappings
        self.key_column = key_column
        
        # Build filepath for source CSV file if provided.
        if source_csv:
            if source_csv_type == 'lookup':
                self.source_csv_path = os.path.join(self.config["lookups_dir"], source_csv)
            elif source_csv_type == 'transformed':
                self.source_csv_path = os.path.join(self.config["transformed_dir"], source_csv)
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
            logging.info(f"[{self.caller_name}] Error running query: {e}")
            raise


        # Log results
        total_read = len(results)
        logging.info(f"[{self.caller_name}] Read {total_read} rows from {table_name}")

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
        """Retrieves data from the given CSV file.

        Results are converted to DataFrame. No need to cast dates as strings (as in table method)
        as they are read from CSV using dtype=str.

        Args:
            conn: Established database connection.
            csv_path: Fully qualified filename of CSV file.
        
        Returns:
            df: Query results converted to a DataFrame.
        """
        df = pd.read_csv(csv_path, dtype=str)
        total_read = len(df)
        logging.info(f"[{self.caller_name}] Read {total_read} rows from {self.source_csv_path}")
        return df


    def _tcA_row_count_matches(self, source_df: pd.DataFrame, target_df: pd.DataFrame):
        """
        Verifies matching row count between the source and target dataframes.
        
        Note: failure is expected when ETL process being tested involves UNION/DISTINCT logic.
        """
        test_desc = "Row count matches"

        if len(source_df) == len(target_df):
            return True, f"{test_desc}"
        else:
            return False, f"{test_desc} - source: {len(source_df)} target: {len(target_df)}"


    def _tcB_simple_column_values_match(self, source_df: pd.DataFrame, target_df: pd.DataFrame):
        """
        Tests for matching column values in simple 1:1 row compare (sorted on key).
        
        This test case is abandoned on row count mismatch or key mismatch between rows.
        """
        test_desc = "Simple 1:1 row comparison"
        key_column = self.key_column

        # Ensure row counts match
        if len(source_df) != len(target_df):
            return False, f"ERROR: Simple comparison abandoned, row count mismatch"

        # Sort both dataframes on the given key column.
        source_df = source_df.sort_values(by=key_column).reset_index(drop=True).copy()
        target_df = target_df.sort_values(by=key_column).reset_index(drop=True).copy()

        row_mismatches = []

        # Iterate through source/target row-pairs.
        for i in range(0, len(source_df)):
            source_row = source_df.iloc[i]
            target_row = target_df.iloc[i]

            if source_row[key_column] != target_row[key_column]:
                return False, f"ERROR: Simple comparison abandoned, key mismatch: {source_row[key_column]}/{target_row[key_column]}"

            # Compare source/target values for each column.
            column_mismatches = []
            for source_column, target_column in self.column_mappings.items():
                source_val = source_row[source_column]
                target_val = target_row[target_column]

                # Check for source-target mismatch. Match conditions are NaN-NaN or value-value.
                if pd.isna(source_val) and not pd.isna(target_val):
                    column_mismatches.append(f"{target_column}: {target_row[target_column]}, expected: {source_row[source_column]}")
                elif pd.isna(target_val) and not pd.isna(source_val):
                    column_mismatches.append(f"{target_column}: {target_row[target_column]}, expected: {source_row[source_column]}")
                elif source_val != target_val:
                    column_mismatches.append(f"{target_column}: {target_row[target_column]}, expected: {source_row[source_column]}")

            # If row contained any column mismatches, append details to list.
            if len(column_mismatches) > 0:
                row_mismatches.append(f"Mismatches for key {target_row[key_column]}: " + ','.join(column_mismatches))

        if len(row_mismatches) == 0:
            return True, f"{test_desc}"
        else:
            return False, f"{test_desc} - " + '\n'.join(row_mismatches)


    def _tcC_key_based_column_values_match(self, source_df: pd.DataFrame, target_df: pd.DataFrame):
        """
        Tests for matching column values in a key-based row comparison (tests only
        rows where there is a matching key).

        Intended for testing ETL scripts where target is populated using UNION/DISTINCT logic
        (tcB does not work in these cases).
        """
        test_desc = "Key-based column comparison"
        key_column = self.key_column

        # Build dictionary of key/row pairs for source and target DF's
        source_dict = {str(row[key_column]): row for _, row in source_df.iterrows()}
        target_dict = {str(row[key_column]): row for _, row in target_df.iterrows()}

        # Get keys that are common to source/target datasets
        common_keys = set(source_dict.keys()).intersection(set(target_dict.keys()))

        if len(common_keys) == 0:
            return False, f"ERROR: Key-based column matching abandoned, no matching keys"

        # Iterate through source/target row-pairs.
        row_mismatches = []
        for key in common_keys:
            source_row = source_dict[key]
            target_row = target_dict[key]

            # Compare source/target values for each column.
            column_mismatches = []
            for source_column, target_column in self.column_mappings.items():
                source_val = source_row[source_column]
                target_val = target_row[target_column]

                # Check for source-target mismatch. Match conditions are NaN-NaN or value-value.
                if pd.isna(source_val) and not pd.isna(target_val):
                    column_mismatches.append(f"{target_column}: {target_row[target_column]}, expected: {source_row[source_column]}")
                elif pd.isna(target_val) and not pd.isna(source_val):
                    column_mismatches.append(f"{target_column}: {target_row[target_column]}, expected: {source_row[source_column]}")
                elif source_val != target_val:
                    column_mismatches.append(f"{target_column}: {target_row[target_column]}, expected: {source_row[source_column]}")

            # If row contained any column mismatches, append details to list.
            if len(column_mismatches) > 0:
                row_mismatches.append(f"Mismatches for key {key}: " + ','.join(column_mismatches))

        if len(row_mismatches) == 0:
            return True, f"{test_desc} - {len(common_keys)} rows compared, all matched"
        else:
            return False, f"{test_desc} - " + '\n'.join(row_mismatches)


    def print_results(self):
        # build pass/fail result lists (result[1] is boolean returned by each test func)
        tests_passed = [result for result in self.test_results if result[1]]
        tests_failed = [result for result in self.test_results if not result[1]]

        print(f"{len(tests_failed)} tests failed:")
        for test in tests_failed:
            print(f"    {test[0]} ({test[2]}) : FAILED")

        print(f"{len(tests_passed)} tests passed:")
        for test in tests_passed:
            print(f"    {test[0]} ({test[2]}) : PASSED")

        logging.info(f"[{self.caller_name}] tests passed: {len(tests_passed)} failed: {len(tests_failed)}")


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
                "tcA_row_count_matches": self._tcA_row_count_matches,
                "tcB_simple_column_values_match": self._tcB_simple_column_values_match,
                "tcC_key_based_column_values_match": self._tcC_key_based_column_values_match
            }

            # Run all tests and print results
            for test_name, test_method in test_cases.items():
                passed, result = test_method(source_df, target_df)
                self.test_results.append((test_name, passed, result))

            self.print_results()

        finally:
            if conn:
                conn.close()
