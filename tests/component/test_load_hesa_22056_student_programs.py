import os
import sys
import subprocess
from utils.data_platform_core import get_config
from TableTester import TableTester

config = get_config()

def run_etl_script():
     # Run ETL script whose output is to be tested.
    etl_script_name = "load_hesa_22056_student_programs.py"
    
    result = subprocess.run(["python3", f"{config['load_script_dir']}/{etl_script_name}"],
                    capture_output=True, text=True)

    if result.returncode != 0:
        print(f"error running {etl_script_name}: {result.stderr}")
    else:
        print(f"script {etl_script_name} completed successfully")


def main():
    # Run ETL process if required
    if "--run-etl" in sys.argv:
        run_etl_script()

    # Declare parameters for test suite
    source_file = "hesa_22056_20240331_student_programs_transformed.csv"
    source_path = os.path.join(config['transformed_dir'], "22056_20240331", source_file)
    target_table = "load_hesa_22056_20240331_student_programs"
    column_mappings = {"student_guid": "student_guid",
                    "email": "email",
                    "program_guid": "program_guid",
                    "program_code": "program_code",
                    "program_name": "program_name",
                    "enrol_date": "enrol_date",
                    "fees_paid": "fees_paid"}

    this_script_name = os.path.basename(__file__)

    # Call test suite
    table_tester = TableTester(
                               target_table=target_table,
                               column_mappings=column_mappings,
                               source_path=source_path,
                               source_table="",
                               caller_name=this_script_name)
    
    table_tester.run_tests()


if __name__ == "__main__":
    main()