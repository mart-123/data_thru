import os
import subprocess
from src.etl.core.etl_utils import get_config
from src.testing.TableTester import TableTester

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
    run_etl_script()

    # Declare parameters for test suite
    source_csv = "student_programs_transformed.csv"
    target_table = "load_hesa_22056_student_programs"
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
                               source_csv=source_csv,
                               source_csv_type="transformed",
                               source_table="",
                               caller_name=this_script_name)
    
    table_tester.run_tests()


if __name__ == "__main__":
    main()