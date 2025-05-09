import pandas as pd
import subprocess
import os
import sys
from utils.data_platform_core import get_config

config = get_config()
test_results = []


def run_etl_process(script_name: str, delivery_code: str):
    result = subprocess.run(["python3", f"{config['extract_script_dir']}/{script_name}", delivery_code],
                        capture_output=True, text=True)

    if result.returncode != 0:
        print(f"error running {script_name}: {result.stderr}")
    else:
        print(f"script {script_name} completed successfully")


def get_transformed_csv(file_name: str, delivery_code: str):
    """Returns a DataFrame of the transformed CSV file"""
    file_path = os.path.join(config['transformed_dir'], delivery_code, file_name)
    csv_df = pd.read_csv(file_path, dtype=str)
    return csv_df


def get_bad_data_csv(file_name: str, delivery_code: str):
    """Returns a DataFrame of the bad data CSV file"""
    file_path = os.path.join(config['bad_data_dir'], delivery_code, file_name)
    csv_df = pd.read_csv(file_path, dtype=str)
    return csv_df


def tc001_transformed_row_count(csv_df):
    test_desc = "Transformed file contains 6 rows"

    if len(csv_df) == 6:
        return True, test_desc
    else:
        return False, test_desc


def tc002_program_name_commas_preserved(csv_df):
    test_desc = "Program name with commas handled ok"
    test_key = "C8DB7DB8-0284-C52F-53C0-842809799A83"

    # Get test record
    bool_series = (csv_df["student_guid"] == test_key)
    matching_rows = csv_df[bool_series]
    if len(matching_rows) == 0:
        return False, f"{test_desc} - ERROR, student_guid {test_key} not in file"

    # Evaluate test condition
    student_row = matching_rows.iloc[0]
    if student_row["program_name"] == "BSc, Medicine":
        return True, test_desc
    else:
        return False, test_desc


def tc003_program_name_quotes_preserved(csv_df):
    test_desc = "single quote in program name preserved"
    test_key = "E8396B31-2DE0-D274-2C68-9CEBA2755721"

    # Get test record
    bool_series = (csv_df["student_guid"] == test_key)
    matching_rows = csv_df[bool_series]
    if len(matching_rows) == 0:
        return False, f"{test_desc} - ERROR - student GUID {test_key} not in file"

    # Evaluate test condition
    student_row = matching_rows.iloc[0]
    if (student_row["program_name"] == "BSc, 'quoted' Medicine"):
        return True, test_desc
    else:
        return False, test_desc


def tc004_good_row_all_cols_copied(csv_df):
    test_desc = "all columns correctly written to transformed file"
    test_key = "7ED0CA1F-B4CB-27E3-39DA-71F96D949814"

    # Get test record
    bool_series = (csv_df["student_guid"] == test_key)
    matching_rows = csv_df[bool_series]
    if len(matching_rows) == 0:
        return False, f"{test_desc} - ERROR - student GUID {test_key} not in file"

    good_row = matching_rows.iloc[0]

    # Declare expected values
    expected_values = {
        "student_guid": "7ED0CA1F-B4CB-27E3-39DA-71F96D949814",
        "email": "sed.sapien@aol.edu",
        "program_guid": "0A769638-3799-86C6-BBBB-38BEBE2487D1",
        "program_code": "ENG10001",
        "program_name": "BA English Lit",
        "enrol_date": "2022-10-04",
        "fees_paid": "N"
    }

    # Evaluate test condition (all columns as expected)
    for col_name, expected_value in expected_values.items():
        if (good_row[col_name] != expected_value):
            return False, f"{test_desc} - ERROR - row {test_key} has unexpected value in {col_name}"

    return True, test_desc


def tc501_bad_data_row_count(bad_df):
    test_desc = "Bad data file contains 5 rows"

    if len(bad_df) == 8:
        return True, test_desc
    else:
        return False, test_desc


def tc502_missing_stu_id_rejected(bad_df):
    test_desc = "Missing stu_id filtered as bad data"

    # Get test record (it has no stu_id, hence 'isna')
    bool_series = (bad_df["stu_id"].isna())
    matching_rows = bad_df[bool_series]
    if len(matching_rows) == 0:
        return False, f"{test_desc} - ERROR - test row not found"

    # Evaluate test condition
    student_row = matching_rows.iloc[0]
    if "mandatory data missing" in student_row["failure_reasons"]:
        return True, test_desc
    else:
        return False, test_desc


def tc503_missing_program_code_rejected(bad_df):
    test_desc = "Missing program code filtered as bad data"
    test_key = "946DDBBE-3BB6-48AF-8B5E-17998281403E"

    # Get test record
    bool_series = (bad_df["stu_id"] == test_key)
    matching_rows = bad_df[bool_series]
    if len(matching_rows) == 0:
        return False, f"{test_desc} - ERROR - student GUID {test_key} not in file"

    # Evaluate test condition
    student_row = matching_rows.iloc[0]
    if ((pd.isna(student_row["program_code"])) and
        ("mandatory data missing" in student_row["failure_reasons"])):
        return True, test_desc
    else:
        return False, test_desc


def tc504_missing_fees_flag_rejected(bad_df):
    test_desc = "Missing fees paid flag filtered as bad data"
    test_key = "0A769638-3799-86C6-4135-38BEBE2487D1"

    # Get test record
    bool_series = (bad_df["stu_id"] == test_key)
    matching_rows = bad_df[bool_series]
    if len(matching_rows) == 0:
        return False, f"{test_desc} - ERROR - student GUID {test_key} not in file"

    # Evaluate test condition
    student_row = matching_rows.iloc[0]
    if ((pd.isna(student_row["fees_paid"])) and
        ('bad fees flag' in student_row["failure_reasons"])):
        return True, test_desc
    else:
        return False, test_desc


def tc505_missing_enrol_date_rejected(bad_df):
    test_desc = "Missing enrolment date filtered as bad data"
    test_key = "FAF961D5-C2EA-A189-93DB-8AA63F5378AA"

    # Get test record
    bool_series = (bad_df["stu_id"] == test_key)
    matching_rows = bad_df[bool_series]
    if len(matching_rows) == 0:
        return False, f"{test_desc} - ERROR - student GUID {test_key} not in file"

    # Evaluate test condition
    student_row = matching_rows.iloc[0]
    if ((pd.isna(student_row["enrol_date"])) and
        ("mandatory data missing" in student_row["failure_reasons"])):
        return True, test_desc
    else:
        return False, test_desc


def tc506_bad_format_enrol_date_rejected(bad_df):
    test_desc = "Enrolment date not in yyyy-mm-dd format filtered as bad data"
    test_key = "DEF961D5-C2EA-A189-93DB-8AA63F5378BB"

    # Get test record
    bool_series = (bad_df["stu_id"] == test_key)
    matching_rows = bad_df[bool_series]
    if len(matching_rows) == 0:
        return False, f"{test_desc} - ERROR - student GUID {test_key} not in file"

    # Evaluate test condition
    student_row = matching_rows.iloc[0]
    if "bad format enrol date" in student_row["failure_reasons"]:
        return True, test_desc
    else:
        return False, test_desc


def tc507_invalid_enrol_date_rejected(bad_df):
    test_desc = "Invalid enrolment date filtered as bad data"
    test_key = "ABC961D5-C2EA-A189-93DB-8AA63F5378BB"

    # Get test record
    bool_series = (bad_df["stu_id"] == test_key)
    matching_rows = bad_df[bool_series]
    if len(matching_rows) == 0:
        return False, f"{test_desc} - ERROR - student GUID {test_key} not in file"

    # Evaluate test condition
    student_row = matching_rows.iloc[0]
    if "invalid enrol date" in student_row["failure_reasons"]:
        return True, test_desc
    else:
        return False, test_desc


def run_transformed_file_tests(transformed_csv_df):
    """Check main output for successfully cleansed/transformed records"""
    test_cases = {
        "tc001_transformed_row_count": tc001_transformed_row_count,
        "tc002_program_name_commas_preserved": tc002_program_name_commas_preserved,
        "tc003_program_name_quotes_preserved": tc003_program_name_quotes_preserved,
        "tc004_good_row_all_cols_copied": tc004_good_row_all_cols_copied
    }

    for test_name, test_func in test_cases.items():
        passed, test_desc = test_func(transformed_csv_df)
        test_results.append((test_name, passed, test_desc))


def run_bad_data_tests(bad_data_csv_df):
    """Check records rejected for data quality issues"""
    test_cases = {
        "tc501_bad_data_row_count": tc501_bad_data_row_count,
        "tc502_missing_stu_id_rejected": tc502_missing_stu_id_rejected,
        "tc503_missing_program_code_rejected": tc503_missing_program_code_rejected,
        "tc504_missing_fees_flag_rejected": tc504_missing_fees_flag_rejected,
        "tc505_missing_enrol_date_rejected": tc505_missing_enrol_date_rejected,
        "tc506_bad_format_enrol_date_rejected": tc506_bad_format_enrol_date_rejected,
        "tc507_invalid_enrol_date_rejected": tc507_invalid_enrol_date_rejected
    }

    for test_name, test_func in test_cases.items():
        passed, test_desc = test_func(bad_data_csv_df)
        test_results.append((test_name, passed, test_desc))


def print_results():
    # build pass/fail result lists (result[1] is boolean returned by each test func)
    tests_passed = [result for result in test_results if result[1]]
    tests_failed = [result for result in test_results if not result[1]]

    print(f"{len(tests_failed)} tests failed:")
    for test in tests_failed:
        print(f"    {test[0]} ({test[2]}) : FAILED")

    print(f"{len(tests_passed)} tests passed:")
    for test in tests_passed:
        print(f"    {test[0]} ({test[2]}) : PASSED")


def main():
    # Set up basic test parameters
    delivery_code = "22056_20240331"
    transformed_filename = f"hesa_{delivery_code}_student_programs_transformed.csv"
    bad_data_filename = f"hesa_{delivery_code}_student_programs_bad_data.csv"

    # Run ETL process if required
    if "--run-etl" in sys.argv:
        run_etl_process("extract_hesa_nn056_student_programs.py", delivery_code)
    
    # Read output files (from script being tested) into DataFrames
    transformed_df = get_transformed_csv(transformed_filename, delivery_code)
    bad_data_df = get_bad_data_csv(bad_data_filename, delivery_code)

    # Run test cases against the two DataFrames
    run_transformed_file_tests(transformed_df)
    run_bad_data_tests(bad_data_df)
    print_results()


if __name__ == '__main__':
    main()
