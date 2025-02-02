import pandas as pd
import logging
import os
import re


def set_up_logging(env='dev'):
    """
    Set up logging and create dir/file as necessary.
    Optional parameter 'dev'/'test'/'live'
    """
    try:
        log_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), f'../logs/{env}')
        os.makedirs(log_dir, exist_ok=True)
        log_file = os.path.join(log_dir, 'application.log')

        logging.basicConfig(
            filename=log_file,
            filemode="a",    # append mode
            format="%(asctime)s - %(levelname)s - %(filename)s - %(funcName)s - %(message)s", # log format
            level=logging.INFO # logging threshold
        )

    except Exception as e:
        print(f"Error setting up logging: {e}")
        raise



def read_student_data(script_dir: str):
    """Load student data into dataframe and check for missing columns"""
    try:
        csv_file_path = os.path.join(script_dir, '../data/students_extract.csv')
        logging.info(f"Reading student extract: {csv_file_path}")
        df = pd.read_csv(csv_file_path)
        return df
    
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



def cleanse_student_data(df: pd.DataFrame, script_dir: str):
    """
    Checks for missing/invalid values, writing bad rows to 'students_bad' CSV file.
    """
    # Fill NA columns with empty string (simplifies subsequent validation logic)
    columns_to_fill = ['stu_id', 'phone', 'email', 'home_address', 'home_postcode', 'home_country', 'term_address', 'term_postcode', 'term_country', 'name', 'dob']
    df[columns_to_fill] = df[columns_to_fill].fillna('')

    # Convert email addresses to lowercase
    df['email'] = df['email'].str.lower().str.strip()   # email to lowercase, remove spaces

    home_addr_incomplete = (
        (df['home_address'] == '') |
        (df['home_postcode'] == '') |
        (df['home_country'] == '')
    )

    term_addr_incomplete = (
        (df['term_address'] == '') |
        (df['term_postcode'] == '') |
        (df['term_country'] == ''))

    other_cols_missing = (df['stu_id'] == '') | (df['phone'] == '') | (df['email'] == '') | (df['name'] == '') | (df['dob'] == '')

    # Check for malformed emails
    bad_emails = ~(df['email'].str.contains('@', na=False))

    # Check DoB for yyyy-mm-dd
    date_pattern = re.compile(r'^\d{4}-\d{2}-\d{2}$')
    bad_dates = ~(df['dob'].apply(lambda x: bool(date_pattern.match(x)) if x else False))

    # Combine error series and write bad rows to separate csv file
    bad_indexes = home_addr_incomplete | term_addr_incomplete | other_cols_missing | bad_emails | bad_dates
    bad_rows = df[bad_indexes]
    new_file_path = os.path.join(script_dir, '../data/students_bad_data.csv')
    bad_rows.to_csv(new_file_path, index=False)
    logging.info(f"CSV rows failed validation: {len(bad_rows)}")

    # return good rows
    good_rows = df[~bad_indexes]
    return good_rows



def extract_names(row):
    parts = row['name'].split(' ')
    if len(parts) > 1:
        return pd.Series({'first_names': ' '.join(parts[:-1]), 'last_name': parts[-1]})
    else:
        return pd.Series({'first_names': parts[0], 'last_name': ''})

def transform_student_data(df: pd.DataFrame):
    """
        Several transformations on remaining good rows:
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



def write_transformed_student_data(transformed_df: pd.DataFrame, script_dir: str):
    try:
        file_path = os.path.join(script_dir, '../data/students_transformed.csv')
        transformed_df.to_csv(file_path, index=False)
        logging.info(f"CSV rows transformed: {len(transformed_df)}")

    except Exception as e:
        logging.critical(f"Error writing transformed student data: {e}")
        raise e



def main():
    set_up_logging()
    script_dir = os.path.dirname(os.path.abspath(__file__))
    df = read_student_data(script_dir)
    check_student_cols(df)
    df = cleanse_student_data(df, script_dir)
    df = transform_student_data(df)
    write_transformed_student_data(df, script_dir)



if __name__ == '__main__':
    main()
