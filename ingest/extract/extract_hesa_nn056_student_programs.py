import os
import re
import sys
import time
import traceback
import logging
import pandas as pd
from multiprocessing import Pool
from utils.data_platform_core import get_config, set_up_logging, is_valid_date


def init(delivery_code):
    """Set generic config and process-specific additional (filenames, etc)"""
    config = get_config()
    script_name = os.path.basename(__file__)
    set_up_logging(config, script_name)

    # Process-specific config (typically filenames)
    config["delivery_code"] = delivery_code
    config["input_path"] = os.path.join(config["deliveries_dir"], f"{delivery_code}/hesa_{delivery_code}_data_student_programs.csv")
    config["transformed_path"] = os.path.join(config["transformed_dir"], f"{delivery_code}/hesa_{delivery_code}_student_programs_transformed.csv")
    config["bad_data_path"] = os.path.join(config["bad_data_dir"], f"{delivery_code}/hesa_{delivery_code}_student_programs_bad_data.csv")

    return config


def init_output_files(config):
    """
    Remove output files if they exist. This supports append mode and
    header-row logic during batched file writes.
    """
    try:
        if os.path.exists(config["transformed_path"]):
            os.remove(config["transformed_path"])
        if os.path.exists(config["bad_data_path"]):
            os.remove(config["bad_data_path"])
    
    except Exception as e:
        logging.critical(f"{type(e).__name__} whilst initialising output files: {e}")
        raise e


def read_data_chunks(config, chunk_size=500):
    """
    Generator function - load student CSV and returns as chunks.
    """
    try:
        logging.info(f"Reading extract CSV: {config['input_path']}")

        for chunk in pd.read_csv(config["input_path"], chunksize=chunk_size, dtype=str):
            yield chunk

    except Exception as e:
        logging.critical(f"{type(e).__name__} whilst reading CSV: {e}")
        raise e


def check_columns(df: pd.DataFrame):
    """
    Checks the student programs csv file contains all, and only, expected columns.
    """
    expected_columns = ["stu_id", "email", "program_id", "program_code", "program_name", "enrol_date", "fees_paid"]
    missing_columns = []

    for col in expected_columns:
        if col not in df.columns:
            missing_columns.append(col)

    if len(missing_columns) > 0:
        raise ValueError(f"Columns missing from csv file: {missing_columns}")
    
    if len(expected_columns) != len(df.columns):
        raise ValueError(f"CSV has {len(df.columns)} columns but should have {len(expected_columns)}")


def cleanse_data(df: pd.DataFrame, config):
    """
    Checks for missing/invalid values, writing bad rows to "bad_data" CSV file.
    """
    # Fill NA columns with empty string (simplifies subsequent validation logic)
    columns_to_fill = ["stu_id", "program_id", "program_code", "program_name", "enrol_date", "fees_paid"]
    df[columns_to_fill] = df[columns_to_fill].fillna("")

    # Check for mandatory columns
    mandatory_cols_missing = ((df["stu_id"] == "") | (df["enrol_date"] == "") | (df["fees_paid"] == "") |
                          (df["program_id"] == "") | (df["program_code"] == "") | (df["program_name"] == ""))

    # Ensure valid "fees paid" flag (y/n)
    bad_fees_flag = ~(df["fees_paid"].isin(["y", "Y", "n", "N"]))

    # Check enrol date for yyyy-mm-dd and also that it's an actual date
    date_pattern = re.compile(r"^\d{4}-\d{2}-\d{2}$")
    bad_format_enrol_dates = ~(df["enrol_date"].apply(lambda x: bool(date_pattern.match(x)) if x else False))
    bad_enrol_dates = ~(df["enrol_date"].apply(lambda x: is_valid_date(x) if x else False))

    # Combine error series
    bad_indexes = (mandatory_cols_missing | bad_format_enrol_dates | bad_enrol_dates | bad_fees_flag)
    bad_rows = df[bad_indexes].copy()

    # Add "failure reasons" column to bad data dataframe
    bad_rows["failure_reasons"] = ""

    if any(mandatory_cols_missing):
        bad_rows.loc[mandatory_cols_missing, "failure_reasons"] += "mandatory data missing; "
    
    if any(bad_format_enrol_dates):
        bad_rows.loc[bad_format_enrol_dates, "failure_reasons"] += "bad format enrol date; "

    if any(bad_enrol_dates):
        bad_rows.loc[bad_enrol_dates, "failure_reasons"] += "invalid enrol date; "

    if any(bad_fees_flag):
        bad_rows.loc[bad_fees_flag, "failure_reasons"] += "bad fees flag; "

    bad_rows["failure_reasons"] = bad_rows["failure_reasons"].str.rstrip("; ")

    # write rejected rows to bad data csv
    write_header = not(os.path.exists(config["bad_data_path"]))
    bad_rows.to_csv(config["bad_data_path"], mode="a", header=write_header, index=False)

    # return good rows
    good_rows = df[~bad_indexes]
    return good_rows


def transform_batch(batch: pd.DataFrame):
    """
    Transforms rows that passed validation:
        stu_id : rename to student_guid
        fees_paid : convert to lowercase
    """
    df = batch.copy()
    # Convert fees_paid to lowercase
    df["fees_paid"] = df["fees_paid"].str.upper().str.strip()

    df.rename(columns={"stu_id": "student_guid"}, inplace=True)
    df.rename(columns={"program_id": "program_guid"}, inplace=True)

    return df


def transform_parallel(df, batch_size=50):
    """
    Breaks an input DataFrame into batches and invokes parallelised
    transformation on them all.
    """
    batches = []
    for i in range(0, len(df), batch_size):
        batch = df.iloc[i:i+batch_size]
        batches.append(batch)

    with Pool() as pool:
        transformed_batches = pool.map(transform_batch, batches)
    
    return pd.concat(transformed_batches)


def write_transformed_data(transformed_df: pd.DataFrame, config):
    """Writes dataframe to CSV file, in append mode due to streaming/chunk processing.
    Header row generated for first batch (i.e. at beginning of file)."""    
    try:
        write_header = not(os.path.exists(config["transformed_path"]))
        transformed_df.to_csv(config["transformed_path"], mode="a", header=write_header, index=False)

    except Exception as e:
        logging.critical(f"{type(e).__name__} whilst writing transformed data: {e}")
        raise e


def main():
    try:
        start_time = time.time()

        # General set-up
        delivery_code = sys.argv[1]
        config = init(delivery_code)
        init_output_files(config)
        count_read = 0
        count_transformed = 0

        # Streams/chunks input file and for each chunk:
        #   - check for correct columns
        #   - cleanse data (exceptions go to "bad_data" file)
        #   - transform and write good data ("transformed" file)
        for chunk in read_data_chunks(config, 200):
            count_read += len(chunk)
            chunk_copy = chunk.copy()
            check_columns(chunk_copy)
            chunk_copy = cleanse_data(chunk_copy, config)
            chunk_copy = transform_parallel(chunk_copy)
            count_transformed += len(chunk_copy)
            write_transformed_data(chunk_copy, config)

        #  Final tidy up
        logging.info(f"CSV rows extracted: {count_read}")
        logging.info(f"CSV rows failed validation: {count_read - count_transformed}")
        logging.info(f"CSV rows transformed: {count_transformed}")

        end_time = time.time()
        elapsed_time = end_time - start_time
        logging.info(f"Student program extract complete. Elapsed time: {elapsed_time:.4f} seconds")

    except Exception as e:
        # In case of error, rollback DB transaction and display error
        logging.critical(f"{type(e).__name__} during extract : {e}")
        logging.critical(traceback.format_exc())

if __name__ == "__main__":
    main()
