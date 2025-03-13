import subprocess
import os
from src.utils.etl_utils import get_config
import pandas as pd

config = get_config()
tests_passed = []
tests_failed = []


def run_etl_process(script_name: str):
    result = subprocess.run(["python3", f"{config['etl_script_dir']}/{script_name}"],
                        capture_output=True, text=True)

    if result.returncode != 0:
        print(f"Error running {script_name}: {result.stderr}")
    else:
        print(f"Script {script_name} completed successfully")


def check_transformed_file(file_name: str):
    """Check main output for successfully processed records"""
    file_path = os.path.join(config['transformed_dir'], file_name)
    csv_df = pd.read_csv(file_path)

    # test: 9 good rows
    test_desc = "Transformed file contains 9 rows"
    if len(csv_df) == 9:
        tests_passed.append(test_desc + " (PASSED)")
    else:
        tests_failed.append(test_desc + " (FAILED)")

    # test: good row with quoted address
    test_desc = "Address encolosed in double quotes handled"
    bool_series = (csv_df["student_guid"] == "09D3A997-4763-B7B5-1D31-B48D1B8225A9")
    matching_rows = csv_df[bool_series]
    student_row = matching_rows.iloc[0]
    if student_row["term_address"] == "P.O. Box 938, 7124 Eu St.":
        tests_passed.append(test_desc + " (PASSED)")
    else:
        tests_failed.append(test_desc + " (FAILED)")


def check_bad_records_file(file_name: str):
    """Check records rejected for data quality issues"""
    print("not implemented")


def print_results():
    print(f"{len(tests_passed)} tests passed")
    for test in tests_passed:
        print(f"    {test}")

    print(f"{len(tests_failed)} tests failed")
    for test in tests_failed:
        print(f"    {test}")


def main():
    run_etl_process("transform_students_csv.py")
    check_transformed_file("students_transformed.csv")
    print_results()


if __name__ == '__main__':
    main()
