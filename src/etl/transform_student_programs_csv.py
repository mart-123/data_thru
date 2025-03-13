import pandas as pd
import logging
import os
import re
from multiprocessing import Pool
import time
import traceback
from src.utils.etl_utils import get_config, set_up_logging, is_valid_date


def init():
    """Set generic config and process-specific additional (filenames, etc)"""
    config = get_config()
    set_up_logging(config)

    # Process-specific config (typically filenames)
    config['input_path'] = os.path.join(config['extracts_dir'], 'student_programs_extract.csv')
    config['transformed_path'] = os.path.join(config['transformed_dir'], 'student_programs_transformed.csv')
    config['bad_data_path'] = os.path.join(config['bad_data_dir'], 'student_programs_bad_data.csv')

    return config


def init_output_files(config):
    """
    Remove output files if they exist. This supports append mode and
    header-row logic during batched file writes.
    """
    try:
        if os.path.exists(config['transformed_path']):
            os.remove(config['transformed_path'])
        if os.path.exists(config['bad_data_path']):
            os.remove(config['bad_data_path'])
    
    except Exception as e:
        logging.critical(f"{type(e).__name__} whilst initialising output files: {e}")
        raise e


def read_data_chunks(config, chunk_size=500):
    """
    Generator function - load student CSV and returns as chunks.
    """
    try:
        logging.info(f"Reading extract CSV: {config['input_path']}")

        for chunk in pd.read_csv(config['input_path'], chunksize=chunk_size, dtype=str):
            yield chunk

    except Exception as e:
        logging.critical(f"{type(e).__name__} whilst reading extract CSV: {e}")
        raise e


def check_columns(df: pd.DataFrame):
    """
    Checks the student programs csv file contains all, and only, expected columns.
    """
    expected_columns = ['stu_id','email','program_id','program_code','program_name','enrol_date','fees_paid']
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
    Checks for missing/invalid values, writing bad rows to 'bad_data' CSV file.
    """
    # Fill NA columns with empty string (simplifies subsequent validation logic)
    columns_to_fill = ['stu_id', 'email', 'program_id', 'program_code', 'program_name','enrol_date','fees_paid']
    df[columns_to_fill] = df[columns_to_fill].fillna('')

    # Convert email and fees_paid to lowercase
    df['email'] = df['email'].str.lower().str.strip()   # email to lowercase, remove spaces
    df['fees_paid'] = df['fees_paid'].str.lower().str.strip()

    # Check for mandatory columns
    other_cols_missing = ((df['stu_id'] == '') | (df['email'] == '') | (df['enrol_date'] == '') | (df['fees_paid'] == '') |
                        (df['program_id'] == '') | (df['program_code'] == '') | (df['program_name'] == ''))

    # Validate email format (contains '@')
    bad_emails = ~(df['email'].str.contains('@', na=False))

    # Check enrol date for yyyy-mm-dd and also that it's an actual date
    date_pattern = re.compile(r'^\d{4}-\d{2}-\d{2}$')
    bad_format_enrol_dates = ~(df['enrol_date'].apply(lambda x: bool(date_pattern.match(x)) if x else False))
    bad_enrol_dates = ~(df['enrol_date'].apply(lambda x: is_valid_date(x) if x else False))

    # Combine error series and write bad rows to separate csv file
    bad_indexes = other_cols_missing | bad_emails | bad_format_enrol_dates | bad_enrol_dates
    bad_rows = df[bad_indexes]
    write_header = not(os.path.exists(config['bad_data_path']))
    bad_rows.to_csv(config['bad_data_path'], mode='a', header=write_header, index=False)

    # return good rows
    good_rows = df[~bad_indexes]
    return good_rows


def extract_names(row):
    """ Helper function to split name into first name(s) and last name"""
    parts = row['name'].split(' ')
    if len(parts) > 1:
        return pd.Series({'first_names': ' '.join(parts[:-1]), 'last_name': parts[-1]})
    else:
        return pd.Series({'first_names': parts[0], 'last_name': ''})


def transform_batch(batch: pd.DataFrame):
    """
    Transforms rows that have passed validation:
        - stu_id : rename to student_guid    

    Call on entire file, each chunk, or during parallelisation.
    """
    df = batch.copy()
    df.rename(columns={'stu_id': 'student_guid'}, inplace=True)
    df.rename(columns={'program_id': 'program_guid'}, inplace=True)

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
        write_header = not(os.path.exists(config['transformed_path']))
        transformed_df.to_csv(config['transformed_path'], mode='a', header=write_header, index=False)

    except Exception as e:
        logging.critical(f"{type(e).__name__} whilst writing transformed student data: {e}")
        raise e


def main():
    try:
        start_time = time.time()

        # General set-up
        config = init()
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
        logging.info(f"CSV rows read: {count_read}")
        logging.info(f"CSV rows failed validation: {count_read - count_transformed}")
        logging.info(f"CSV rows transformed: {count_transformed}")

        end_time = time.time()
        elapsed_time = end_time - start_time
        logging.info(f"Student program transform complete. Elapsed time: {elapsed_time:.4f} seconds")

    except Exception as e:
        # In case of error, rollback DB transaction and display error
        logging.critical(f"{type(e).__name__} whilst processing student_programs_extract.csv : {e}")
        logging.critical(traceback.format_exc())

if __name__ == '__main__':
    main()
