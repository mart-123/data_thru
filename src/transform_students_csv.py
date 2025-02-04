import pandas as pd
import logging
import os
import re


def get_config(env='dev'):
    script_dir = os.path.dirname(os.path.abspath(__file__))
    log_dir = os.path.join(script_dir, f'../logs/{env}')
    data_dir = os.path.join(script_dir, f'../data')

    config = {
        'env' : env,
        'log_dir' : log_dir,
        'log_file_path' : os.path.join(log_dir, 'application.log'),
        'input_path' : os.path.join(data_dir, 'students_extract.csv'),
        'transformed_path' : os.path.join(data_dir, 'students_transformed.csv'),
        'bad_data_path' : os.path.join(data_dir, 'students_bad_data.csv')
    }

    return config



def set_up_logging(config):
    """
    Set up logging and create dir/file as necessary.
    """
    try:
        os.makedirs(config['log_dir'], exist_ok=True)

        logging.basicConfig(
            filename=config['log_file_path'],
            filemode="a",    # append mode
            format="%(asctime)s - %(levelname)s - %(filename)s - %(message)s", # log format
            level=logging.INFO # logging threshold
        )

    except Exception as e:
        print(f"Error setting up logging: {e}")
        raise



def init_output_files(config):
    """Remove output files if they exist. Enables header-row logic during file writes."""
    try:
        if os.path.exists(config['transformed_path']):
            os.remove(config['transformed_path'])
        if os.path.exists(config['bad_data_path']):
            os.remove(config['bad_data_path'])
    
    except Exception as e:
        logging.critical(f"Error initialising output files: {e}")
        raise e




def read_student_data_chunks(config, chunk_size=500):
    """
    Generator function - load student data and returns as chunks.
    """
    try:
        logging.info(f"Reading student extract: {config['input_path']}")

        for chunk in pd.read_csv(config['input_path'], chunksize=chunk_size):
            yield chunk

        # df = pd.read_csv(config['input_path']) # for non-chunk approach, replace above for with this
        # return df

    except Exception as e:
        logging.critical(f"Error reading student extract: {e}")
        raise e



def check_student_cols(df: pd.DataFrame):
    ### Checks the student csv file contains all, and only, expected columns.
    expected_columns = ['stu_id','phone','email','home_address','home_postcode','home_country','term_address','term_postcode','term_country','name','dob']
    missing_columns = []

    for col in expected_columns:
        if col not in df.columns:
            missing_columns.append(col)

    if len(missing_columns) > 0:
        raise ValueError(f"Columns missing from csv file: {missing_columns}")
    
    if len(expected_columns) != len(df.columns):
        raise ValueError(f"Student CSV has {len(df.columns)} but should have {len(expected_columns)}")



def cleanse_student_data(df: pd.DataFrame, config):
    """
    Checks for missing/invalid values, writing bad rows to 'students_bad' CSV file.
    """
    # Fill NA columns with empty string (simplifies subsequent validation logic)
    columns_to_fill = ['stu_id', 'phone', 'email', 'home_address', 'home_postcode', 'home_country', 'term_address', 'term_postcode', 'term_country', 'name', 'dob']
    df[columns_to_fill] = df[columns_to_fill].fillna('')

    # Convert email addresses to lowercase
    df['email'] = df['email'].str.lower().str.strip()   # email to lowercase, remove spaces

    home_addr_incomplete = (
        (df['home_address'] == '') | (df['home_postcode'] == '') | (df['home_country'] == '')
    )

    term_addr_incomplete = (
        (df['term_address'] == '') | (df['term_postcode'] == '') | (df['term_country'] == '')
    )

    other_cols_missing = (df['stu_id'] == '') | (df['phone'] == '') | (df['email'] == '') | (df['name'] == '') | (df['dob'] == '')

    # Check for malformed emails
    bad_emails = ~(df['email'].str.contains('@', na=False))

    # Check DoB for yyyy-mm-dd
    date_pattern = re.compile(r'^\d{4}-\d{2}-\d{2}$')
    bad_dates = ~(df['dob'].apply(lambda x: bool(date_pattern.match(x)) if x else False))

    # Combine error series and write bad rows to separate csv file
    bad_indexes = home_addr_incomplete | term_addr_incomplete | other_cols_missing | bad_emails | bad_dates
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


def transform_student_data(df: pd.DataFrame):
    """Transform remaining (good) rows:
          - phone : remove parenthesese
          - name : rename to 'full_name' and split into first name(s) and last name
          - stu_id : rename to student_guid
    """
    df['phone'] = df['phone'].str.replace('(', '').str.replace(')', '')
    df.rename(columns={'stu_id': 'student_guid'}, inplace=True)

    # split name into first name(s) and last name cols
    names = df.apply(extract_names, axis=1)
    df = pd.concat([df, names], axis=1)
    df.drop(columns=['name'], inplace=True)
    return df



def write_transformed_student_data(transformed_df: pd.DataFrame, config):
    try:
        write_header = not(os.path.exists(config['transformed_path']))
        transformed_df.to_csv(config['transformed_path'], mode='a', header=write_header, index=False)

    except Exception as e:
        logging.critical(f"Error writing transformed student data: {e}")
        raise e



def main():
    config = get_config()
    set_up_logging(config)
    init_output_files(config)
    count_read = 0
    count_transformed = 0

    for chunk in read_student_data_chunks(config, 200):
        count_read += len(chunk)
        chunk_copy = chunk.copy()
        check_student_cols(chunk_copy)
        chunk_copy = cleanse_student_data(chunk_copy, config)
        chunk_copy = transform_student_data(chunk_copy)
        count_transformed += len(chunk_copy)
        write_transformed_student_data(chunk_copy, config)

    logging.info(f"Students read: {count_read}")
    logging.info(f"Students failed validation: {count_read - count_transformed}")
    logging.info(f"Students transformed: {count_transformed}")



if __name__ == '__main__':
    main()
