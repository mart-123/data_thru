import subprocess
import os
from src.utils.etl_utils import get_config
import pandas as pd

config = get_config()
test_results = []


def run_etl_process(script_name: str):
    result = subprocess.run(["python3", f"{config['etl_script_dir']}/{script_name}"],
                        capture_output=True, text=True)

    if result.returncode != 0:
        print(f"Error running {script_name}: {result.stderr}")
    else:
        print(f"Script {script_name} completed successfully")


def get_transformed_csv(file_name: str):
    file_path = os.path.join(config['transformed_dir'], file_name)
    csv_df = pd.read_csv(file_path)
    return csv_df


def get_bad_data_csv(file_name: str):
    file_path = os.path.join(config['bad_data_dir'], file_name)
    csv_df = pd.read_csv(file_path)
    return csv_df


def tc001_transformed_row_count(csv_df):
    test_desc = "Transformed file contains 9 rows"

    if len(csv_df) == 9:
        return True, test_desc
    else:
        return False, test_desc


def tc002_transformed_address_commas(csv_df):
    test_desc = "Address containing commas (quote-enclosed) is handled ok"
    test_key = "09D3A997-4763-B7B5-1D31-B48D1B8225A9"

    # Get test record
    bool_series = (csv_df["student_guid"] == test_key)
    matching_rows = csv_df[bool_series]
    if len(matching_rows) == 0:
        return False, f"{test_desc} - ERROR, GUID {test_key} not in file"

    # Evaluate test condition
    student_row = matching_rows.iloc[0]
    if student_row["term_address"] == "P.O. Box 938, 7124 Eu St.":
        return True, test_desc
    else:
        return False, test_desc


def tc003_transformed_triple_name(csv_df):
    test_desc = "Three part name is split to last name and two first names"
    test_key = "A65B14B1-095D-094C-1F30-11986C5CB3B5"

    # Get test record
    bool_series = (csv_df["student_guid"] == test_key)
    matching_rows = csv_df[bool_series]
    if len(matching_rows) == 0:
        return False, f"{test_desc} - ERROR, GUID {test_key} not in file"
    
    # Evaluate test condition
    student_row = matching_rows.iloc[0]
    if ((student_row["last_name"] == "Richardson") and
        (student_row["first_names"] == "Byron David")):
        return True, test_desc
    else:
        return False, test_desc


def tc004_transformed_email(csv_df):
    test_desc = "mixed case email address converted to lowercase"

    # Get test record
    test_desc = "mixed case email address converted to lowercase"
    test_key = "C6471B39-4299-7E9B-5727-9D89AEA2CAD2"

    # Get test record
    bool_series = (csv_df["student_guid"] == test_key)
    matching_rows = csv_df[bool_series]
    if len(matching_rows) == 0:
        return False, f"{test_desc} - ERROR, GUID {test_key} not in file"
    
    # Evaluate test condition
    student_row = matching_rows.iloc[0]
    if student_row["email"] == "sagittiscapitalised@protonmail.edu":
        return True, test_desc
    else:
        return False, test_desc


def tc005_transformed_quoted_name(csv_df):
    test_desc = "single quote in first and last names is preserved"
    test_key = "8C8CE9EE-09BD-AACA-E5C4-47B229CDBBE9"

    # Get test record
    bool_series = (csv_df["student_guid"] == test_key)
    matching_rows = csv_df[bool_series]
    if len(matching_rows) == 0:
        return False, f"{test_desc} - ERROR, student GUID {test_key} not in file"

    # Evaluate test condition
    student_row = matching_rows.iloc[0]
    if ((student_row["first_names"] == "Yoshi Joh'n") and
        (student_row["last_name"] == "D'Wagner")):
        return True, test_desc
    else:
        return False, test_desc


def tc006_transformed_all_cols(csv_df):
    test_desc = "all columns correctly written to transformed file"
    test_key = "9A5E845D-27A9-C9E7-EA82-F64CBC089BDA"

    # Get test record
    bool_series = (csv_df["student_guid"] == test_key)
    matching_rows = csv_df[bool_series]
    if len(matching_rows) == 0:
        return False, f"{test_desc} - ERROR, student GUID {test_key} not in file"

    # Evaluate test condition
    student_row = matching_rows.iloc[0]
    if ((student_row["first_names"] == "Ignatius") and
        (student_row["last_name"] == "Duran") and
        (student_row["dob"] == "1943-07-20") and
        (student_row["email"] == "goodconsequat@record.edu") and
        (student_row["phone"] == "0161 798 2467") and
        (student_row["home_address"] == "421-2317 Eget Avenue") and
        (student_row["home_postcode"] == "33912") and
        (student_row["home_country"] == "Pakistan") and
        (student_row["term_address"] == "651-1322 Metus. Rd.") and
        (student_row["term_postcode"] == "44573") and
        (student_row["term_country"] == "Germany")
        ):
        return True, test_desc
    else:
        return False, test_desc


def run_transformed_file_tests(transformed_csv_df):
    """Check main output for successfully cleansed/transformed records"""
    test_cases = {
        "tc001_transformed_row_count": tc001_transformed_row_count,
        "tc002_transformed_address_commas": tc002_transformed_address_commas,
        "tc003_transformed_triple_name": tc003_transformed_triple_name,
        "tc004_transformed_email": tc004_transformed_email,
        "tc005_transformed_quoted_name": tc005_transformed_quoted_name,
        "tc006_transformed_all_cols": tc006_transformed_all_cols
    }

    for test_name, test_func in test_cases.items():
        passed, test_desc = test_func(transformed_csv_df)
        test_results.append((test_name, passed, test_desc))


def run_bad_file_tests(file_name: str):
    """Check records rejected for data quality issues"""
    print("not implemented")


def print_results():
    # build pass/fail result lists (result[1] is boolean returned by each test func)
    tests_passed = [result for result in test_results if result[1]]
    tests_failed = [result for result in test_results if not result[1]]

    print(f"{len(tests_failed)} tests failed")
    for test in tests_failed:
        print(f"    {test[0]} ({test[2]}) : FAILED")

    print(f"{len(tests_passed)} tests passed")
    for test in tests_passed:
        print(f"    {test[0]} ({test[2]}) : PASSED")


def main():
    run_etl_process("transform_students_csv.py")
    transformed_csv = get_transformed_csv("students_transformed.csv")
    run_transformed_file_tests(transformed_csv)
    print_results()


if __name__ == '__main__':
    main()
