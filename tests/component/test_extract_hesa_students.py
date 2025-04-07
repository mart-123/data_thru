import pandas as pd
import subprocess
import os
from src.etl.core.etl_utils import get_config

config = get_config()
test_results = []


def run_etl_process(script_name: str, delivery_code: str):
    delivery_code = "22056_20240331"
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
    test_desc = "Transformed file contains 9 rows"

    if len(csv_df) == 9:
        return True, test_desc
    else:
        return False, test_desc


def tc002_address_with_commas_preserved(csv_df):
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


def tc003_triple_name_parsed(csv_df):
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


def tc004_email_converted_lowercase(csv_df):
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


def tc005_quotes_in_name_preserved(csv_df):
    test_desc = "single quote in first and last names is preserved"
    test_key = "8C8CE9EE-09BD-AACA-E5C4-47B229CDBBE9"

    # Get test record
    bool_series = (csv_df["student_guid"] == test_key)
    matching_rows = csv_df[bool_series]
    if len(matching_rows) == 0:
        return False, f"{test_desc} - ERROR - student GUID {test_key} not in file"

    # Evaluate test condition
    student_row = matching_rows.iloc[0]
    if ((student_row["first_names"] == "Yoshi Joh'n") and
        (student_row["last_name"] == "D'Wagner")):
        return True, test_desc
    else:
        return False, test_desc


def tc006_good_row_all_cols_copied(csv_df):
    test_desc = "all columns correctly written to transformed file"
    test_key = "9A5E845D-27A9-C9E7-EA82-F64CBC089BDA"

    # Get test record
    bool_series = (csv_df["student_guid"] == test_key)
    matching_rows = csv_df[bool_series]
    if len(matching_rows) == 0:
        return False, f"{test_desc} - ERROR - student GUID {test_key} not in file"

    # Declare expected values
    expected_values = {
        "student_guid": "9A5E845D-27A9-C9E7-EA82-F64CBC089BDA",
        "first_names": "Ignatius",
        "last_name": "Duran",
        "dob": "1943-07-20",
        "email": "goodconsequat@record.edu",
        "phone": "0161 798 2467",
        "home_address": "421-2317 Eget Avenue",
        "home_postcode": "33912",
        "home_country": "Pakistan",
        "term_address": "651-1322 Metus. Rd.",
        "term_postcode": "44573",
        "term_country": "Germany"
    }

    # Evaluate test condition (all columns as expected)
    student_row = matching_rows.iloc[0]
    for col_name, expected_value in expected_values.items():
        if (student_row[col_name] != expected_value):
            return False, f"{test_desc} - ERROR - row {test_key} has unexpected value in {col_name}"

    return True, test_desc


def tc501_bad_data_row_count(bad_df):
    test_desc = "Bad data file contains 14 rows"

    if len(bad_df) == 14:
        return True, test_desc
    else:
        return False, test_desc


def tc502_missing_stu_guid_rejected(bad_df):
    test_desc = "Missing stu_id filtered as bad data"

    # Get test record (it has no stu_id, hence 'isna')
    bool_series = (bad_df["stu_id"].isna())
    matching_rows = bad_df[bool_series]
    if len(matching_rows) == 0:
        return False, f"{test_desc} - ERROR - test row not found"

    # Evaluate test condition
    student_row = matching_rows.iloc[0]
    if "missing id" in student_row["failure_reasons"]:
        return True, test_desc
    else:
        return False, test_desc


def tc503_missing_phone_rejected(bad_df):
    test_desc = "Missing phone filtered as bad data"
    test_key = "A5514A52-5491-333D-D1C9-7CB8673ACDE6"

    # Get test record
    bool_series = (bad_df["stu_id"] == test_key)
    matching_rows = bad_df[bool_series]
    if len(matching_rows) == 0:
        return False, f"{test_desc} - ERROR - student GUID {test_key} not in file"

    # Evaluate test condition
    student_row = matching_rows.iloc[0]
    if ((pd.isna(student_row["phone"])) and
        ("missing id/phone/email/name/dob" in student_row["failure_reasons"])):
        return True, test_desc
    else:
        return False, test_desc


def tc504_missing_email_rejected(bad_df):
    test_desc = "Missing email filtered as bad data"
    test_key = "994A111B-6C6E-9998-E502-55A9E522E42B"

    # Get test record
    bool_series = (bad_df["stu_id"] == test_key)
    matching_rows = bad_df[bool_series]
    if len(matching_rows) == 0:
        return False, f"{test_desc} - ERROR - student GUID {test_key} not in file"

    # Evaluate test condition
    student_row = matching_rows.iloc[0]
    if ((pd.isna(student_row["email"])) and
        ("missing id/phone/email/name/dob" in student_row["failure_reasons"])):
        return True, test_desc
    else:
        return False, test_desc


def tc505_missing_home_address_rejected(bad_df):
    test_desc = "Missing home address filtered as bad data"
    test_key = "286D124A-87C5-3C7D-7218-687E18397EA2"

    # Get test record
    bool_series = (bad_df["stu_id"] == test_key)
    matching_rows = bad_df[bool_series]
    if len(matching_rows) == 0:
        return False, f"{test_desc} - ERROR - student GUID {test_key} not in file"

    # Evaluate test condition
    student_row = matching_rows.iloc[0]
    if ((pd.isna(student_row["home_address"])) and
        ("missing home addr data" in student_row["failure_reasons"])):
        return True, test_desc
    else:
        return False, test_desc


def tc506_missing_home_postcode_rejected(bad_df):
    test_desc = "Missing home postcode filtered as bad data"
    test_key = "1A2DDEED-79DE-C5CA-BC86-8C113C0255D8"

    # Get test record
    bool_series = (bad_df["stu_id"] == test_key)
    matching_rows = bad_df[bool_series]
    if len(matching_rows) == 0:
        return False, f"{test_desc} - ERROR - student GUID {test_key} not in file"

    # Evaluate test condition
    student_row = matching_rows.iloc[0]
    if ((pd.isna(student_row["home_postcode"])) and
        ("missing home addr data" in student_row["failure_reasons"])):
        return True, test_desc
    else:
        return False, test_desc


def tc507_missing_home_country_rejected(bad_df):
    test_desc = "Missing home country filtered as bad data"
    test_key = "714DA36F-84E2-C90D-E5D0-3551B19B09B3"

    # Get test record
    bool_series = (bad_df["stu_id"] == test_key)
    matching_rows = bad_df[bool_series]
    if len(matching_rows) == 0:
        return False, f"{test_desc} - ERROR - student GUID {test_key} not in file"

    # Evaluate test condition
    student_row = matching_rows.iloc[0]
    if ((pd.isna(student_row["home_country"])) and
        ("missing home addr data" in student_row["failure_reasons"])):
        return True, test_desc
    else:
        return False, test_desc


def tc508_missing_term_address_rejected(bad_df):
    test_desc = "Missing term address filtered as bad data"
    test_key = "5D01E2A9-EEA7-3D2A-156B-93357B5A80CC"

    # Get test record
    bool_series = (bad_df["stu_id"] == test_key)
    matching_rows = bad_df[bool_series]
    if len(matching_rows) == 0:
        return False, f"{test_desc} - ERROR - student GUID {test_key} not in file"

    # Evaluate test condition
    student_row = matching_rows.iloc[0]
    if ((pd.isna(student_row["term_address"])) and
        ("missing term addr data" in student_row["failure_reasons"])):
        return True, test_desc
    else:
        return False, test_desc


def tc509_missing_term_postcode_rejected(bad_df):
    test_desc = "Missing term postcode filtered as bad data"
    test_key = "A7E31E3E-FF53-79C9-F1F6-6ACE79B8C3C8"

    # Get test record
    bool_series = (bad_df["stu_id"] == test_key)
    matching_rows = bad_df[bool_series]
    if len(matching_rows) == 0:
        return False, f"{test_desc} - ERROR - student GUID {test_key} not in file"

    # Evaluate test condition
    student_row = matching_rows.iloc[0]
    if ((pd.isna(student_row["term_postcode"])) and
        ("missing term addr data" in student_row["failure_reasons"])):
        return True, test_desc
    else:
        return False, test_desc


def tc510_missing_term_country_rejected(bad_df):
    test_desc = "Missing term country filtered as bad data"
    test_key = "5B1DF9AF-BA98-2645-6869-D24F32D44DA2"

    # Get test record
    bool_series = (bad_df["stu_id"] == test_key)
    matching_rows = bad_df[bool_series]
    if len(matching_rows) == 0:
        return False, f"{test_desc} - ERROR - student GUID {test_key} not in file"

    # Evaluate test condition
    student_row = matching_rows.iloc[0]
    if ((pd.isna(student_row["term_country"])) and
        ("missing term addr data" in student_row["failure_reasons"])):
        return True, test_desc
    else:
        return False, test_desc


def tc511_missing_name_rejected(bad_df):
    test_desc = "Missing name filtered as bad data"
    test_key = "DA383EB2-28EA-8596-EA62-0D562A427423"

    # Get test record
    bool_series = (bad_df["stu_id"] == test_key)
    matching_rows = bad_df[bool_series]
    if len(matching_rows) == 0:
        return False, f"{test_desc} - ERROR - student GUID {test_key} not in file"

    # Evaluate test condition
    student_row = matching_rows.iloc[0]
    if ((pd.isna(student_row["name"])) and
        ("missing id/phone/email/name/dob" in student_row["failure_reasons"])):
        return True, test_desc
    else:
        return False, test_desc


def tc512_missing_dob_rejected(bad_df):
    test_desc = "Missing DOB filtered as bad data"
    test_key = "A8AFE87E-B50E-8A21-E4DB-A53D7298FB92"

    # Get test record
    bool_series = (bad_df["stu_id"] == test_key)
    matching_rows = bad_df[bool_series]
    if len(matching_rows) == 0:
        return False, f"{test_desc} - ERROR - student GUID {test_key} not in file"

    # Evaluate test condition
    student_row = matching_rows.iloc[0]
    if ((pd.isna(student_row["dob"])) and
        ("missing id/phone/email/name/dob" in student_row["failure_reasons"])):
        return True, test_desc
    else:
        return False, test_desc


def tc513_bad_format_email_rejected(bad_df):
    test_desc = "Email not in format a@b.c filtered as bad data"
    test_key = "47912D6D-15A8-256E-CD9D-91C6CA7C3571"

    # Get test record
    bool_series = (bad_df["stu_id"] == test_key)
    matching_rows = bad_df[bool_series]
    if len(matching_rows) == 0:
        return False, f"{test_desc} - ERROR - student GUID {test_key} not in file"

    # Evaluate test condition
    student_row = matching_rows.iloc[0]
    if "badly formatted email address" in student_row["failure_reasons"]:
        return True, test_desc
    else:
        return False, test_desc


def tc514_bad_format_email_rejected(bad_df):
    test_desc = "Email not in format a@b.c filtered as bad data"
    test_key = "47912D6D-15A8-256E-CD9D-91C6CA7C3571"

    # Get test record
    bool_series = (bad_df["stu_id"] == test_key)
    matching_rows = bad_df[bool_series]
    if len(matching_rows) == 0:
        return False, f"{test_desc} - ERROR - student GUID {test_key} not in file"

    # Evaluate test condition
    student_row = matching_rows.iloc[0]
    if "badly formatted email address" in student_row["failure_reasons"]:
        return True, test_desc
    else:
        return False, test_desc


def tc515_bad_format_dob_rejected(bad_df):
    test_desc = "DoB not in yyyy-mm-dd format filtered as bad data"
    test_key = "5E809209-A56F-9293-A9EB-EF67A9DF35AA"

    # Get test record
    bool_series = (bad_df["stu_id"] == test_key)
    matching_rows = bad_df[bool_series]
    if len(matching_rows) == 0:
        return False, f"{test_desc} - ERROR - student GUID {test_key} not in file"

    # Evaluate test condition
    student_row = matching_rows.iloc[0]
    if "bad format dob" in student_row["failure_reasons"]:
        return True, test_desc
    else:
        return False, test_desc


def tc516_invalid_dob_rejected(bad_df):
    test_desc = "DoB containing invalid date filtered as bad data"
    test_key = "10203040-5060-7080-A9EB-ABABA9DFADCF"

    # Get test record
    bool_series = (bad_df["stu_id"] == test_key)
    matching_rows = bad_df[bool_series]
    if len(matching_rows) == 0:
        return False, f"{test_desc} - ERROR - student GUID {test_key} not in file"

    # Evaluate test condition
    student_row = matching_rows.iloc[0]
    if "invalid dob" in student_row["failure_reasons"]:
        return True, test_desc
    else:
        return False, test_desc


def run_transformed_file_tests(transformed_csv_df):
    """Check main output for successfully cleansed/transformed records"""
    test_cases = {
        "tc001_transformed_row_count": tc001_transformed_row_count,
        "tc002_transformed_address_commas": tc002_address_with_commas_preserved,
        "tc003_triple_name_parsed": tc003_triple_name_parsed,
        "tc004_email_converted_lowercase": tc004_email_converted_lowercase,
        "tc005_quotes_in_name_preserved": tc005_quotes_in_name_preserved,
        "tc006_good_row_all_cols_copied": tc006_good_row_all_cols_copied
    }

    for test_name, test_func in test_cases.items():
        passed, test_desc = test_func(transformed_csv_df)
        test_results.append((test_name, passed, test_desc))


def run_bad_data_tests(bad_data_csv_df):
    """Check records rejected for data quality issues"""
    test_cases = {
        "tc501_bad_data_row_count": tc501_bad_data_row_count,
        "tc502_missing_stu_guid_rejected": tc502_missing_stu_guid_rejected,
        "tc503_missing_phone_rejected": tc503_missing_phone_rejected,
        "tc504_missing_email_rejected": tc504_missing_email_rejected,
        "tc505_missing_home_address_rejected": tc505_missing_home_address_rejected,
        "tc506_missing_home_postcode_rejected": tc506_missing_home_postcode_rejected,
        "tc507_missing_home_country_rejected": tc507_missing_home_country_rejected,
        "tc508_missing_term_address_rejected": tc508_missing_term_address_rejected,
        "tc509_missing_term_postcode_rejected": tc509_missing_term_postcode_rejected,
        "tc510_missing_term_country_rejected": tc510_missing_term_country_rejected,
        "tc511_missing_name_rejected": tc511_missing_name_rejected,
        "tc512_missing_dob_rejected": tc512_missing_dob_rejected,
        "tc513_bad_format_email_rejected": tc513_bad_format_email_rejected,
        "tc514_bad_format_email_rejected": tc514_bad_format_email_rejected,
        "tc515_bad_format_dob_rejected": tc515_bad_format_dob_rejected,
        "tc516_invalid_dob_rejected": tc516_invalid_dob_rejected
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
    delivery_code = "22056_20240331"
    transformed_filename = f"hesa_{delivery_code}_students_transformed.csv"
    bad_data_filename = f"hesa_{delivery_code}_students_bad_data.csv"
    run_etl_process("extract_hesa_nn056_students.py", delivery_code=delivery_code)
    transformed_df = get_transformed_csv(transformed_filename, delivery_code=delivery_code)
    bad_data_df = get_bad_data_csv(bad_data_filename, delivery_code=delivery_code)
    run_transformed_file_tests(transformed_df)
    run_bad_data_tests(bad_data_df)
    print_results()


if __name__ == '__main__':
    main()
