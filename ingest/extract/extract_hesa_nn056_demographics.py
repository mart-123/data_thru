import pandas as pd
import logging
import os
import sys
from multiprocessing import Pool
import time
import traceback
from utils.data_platform_core import get_config, set_up_logging


def init(delivery_code):
    """Set generic config and process-specific additional (filenames, etc)"""
    config = get_config()
    script_name = os.path.basename(__file__)
    set_up_logging(config, script_name)

    # Process-specific config (typically filenames)
    config["delivery_code"] = delivery_code
    config["input_path"] = os.path.join(config["deliveries_dir"], f"{delivery_code}/hesa_{delivery_code}_data_demographics.csv")
    config["transformed_path"] = os.path.join(config["transformed_dir"], f"{delivery_code}/hesa_{delivery_code}_demographics_transformed.csv")
    config["bad_data_path"] = os.path.join(config["bad_data_dir"], f"{delivery_code}/hesa_{delivery_code}_demographics_bad_data.csv")
    return config


def init_output_files(config):
    """
    Remove output files if they exist. This supports append mode and
    header-row logic during batched file writes.
    """
    if os.path.exists(config["transformed_path"]):
        os.remove(config["transformed_path"])
    if os.path.exists(config["bad_data_path"]):
        os.remove(config["bad_data_path"])


def read_data_chunks(config, chunk_size=500):
    """
    Generator function - load input CSV and returns as chunks.
    """
    logging.info(f"Reading extract file: {config['input_path']}")

    for chunk in pd.read_csv(config["input_path"], chunksize=chunk_size, dtype=str):
        yield chunk


def check_columns(df: pd.DataFrame):
    """
    Checks the csv file contains all, and only, expected columns.
    """
    expected_columns = ["stu_id", "ethnicity", "gender", "religion", "sexid", "sexort", "trans", "ethnicity_grp1", "ethnicity_grp1", "ethnicity_grp1"]
    missing_columns = []

    for col in expected_columns:
        if col not in df.columns:
            missing_columns.append(col)

    if len(missing_columns) > 0:
        raise ValueError(f"Columns missing from csv file: {missing_columns}")
    
    if len(expected_columns) != len(df.columns):
        raise ValueError(f"Demographics CSV has {len(df.columns)} but should have {len(expected_columns)}")


def cleanse_data(df: pd.DataFrame, config):
    """
    Checks for missing/invalid values, writing bad rows to 'bad_data' CSV file.
    """
    # Fill NA columns with empty string (simplifies subsequent validation logic)
    columns_to_fill = ["stu_id", "ethnicity", "gender", "religion", "sexid", "sexort", "trans", "ethnicity_grp1", "ethnicity_grp2", "ethnicity_grp3"]
    df[columns_to_fill] = df[columns_to_fill].fillna("")

    cols_missing = ((df["stu_id"] == "") | (df["ethnicity"] == "") | (df["gender"] == "") | (df["religion"] == "") |
                    (df["sexid"] == "") | (df["sexort"] == "") | (df["trans"] == "") |
                    (df["ethnicity_grp1"] == "") | (df["ethnicity_grp2"] == "") | (df["ethnicity_grp3"] == ""))

    # Combine error series and write bad rows to separate csv file
    bad_indexes = cols_missing
    bad_rows = df[bad_indexes]
    write_header = not(os.path.exists(config["bad_data_path"]))
    bad_rows.to_csv(config["bad_data_path"], mode="a", header=write_header, index=False)

    # return good rows
    good_rows = df[~bad_indexes]
    return good_rows


def transform_batch(batch: pd.DataFrame):
    """
    Transforms rows that have passed validation:
        - stu_id : rename to student_guid    

    Call on entire file, each chunk, or during parallelisation.
    """
    df = batch.copy()
    df.rename(columns={"stu_id": "student_guid"}, inplace=True)

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
    write_header = not(os.path.exists(config["transformed_path"]))
    transformed_df.to_csv(config["transformed_path"], mode="a", header=write_header, index=False)


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
        #   - cleanse data (exceptions go to 'bad_data' file)
        #   - transform and write good data ('transformed' file)
        for chunk in read_data_chunks(config, 200):
            count_read += len(chunk)
            chunk_copy = chunk.copy()
            check_columns(chunk_copy)
            chunk_copy = cleanse_data(chunk_copy, config)
            chunk_copy = transform_parallel(chunk_copy)
            count_transformed += len(chunk_copy)
            write_transformed_data(chunk_copy, config)

        #  Final tidy up
        logging.info(f"Rows extracted: {count_read}")
        logging.info(f"Rows failed validation: {count_read - count_transformed}")
        logging.info(f"Rows transformed: {count_transformed}")

        end_time = time.time()
        elapsed_time = end_time - start_time
        logging.info(f"Demographics transform complete. Elapsed time: {elapsed_time:.4f} seconds")

    except Exception as e:
        logging.critical(f"{type(e).__name__} during extract: {e}")
        logging.critical(traceback.format_exc())


if __name__ == '__main__':
    main()
