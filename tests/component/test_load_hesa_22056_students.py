import os
import subprocess
from src.etl.core.etl_utils import get_config
from src.testing.TableTester import TableTester

config = get_config()

def run_etl_script():
     # Run ETL script whose output is to be tested.
    etl_script_name = "load_hesa_22056_students.py"
    
    result = subprocess.run(["python3", f"{config['load_script_dir']}/{etl_script_name}"],
                    capture_output=True, text=True)

    if result.returncode != 0:
        print(f"Error running {etl_script_name}: {result.stderr}")
    else:
        print(f"Script {etl_script_name} completed successfully")


def main():
#    run_etl_script()

    # Declare parameters for test suite
    this_script_name = os.path.basename(__file__)
    source_csv = "students_transformed.csv"
    target_table = "load_hesa_22056_students"
    key_column = "student_guid"
    column_mappings = {"student_guid": "student_guid",
                        "first_names": "first_names",
                        "last_name": "last_name",
                        "dob": "dob",
                        "phone": "phone",
                        "email": "email",
                        "home_address": "home_addr",
                        "home_postcode": "home_postcode",
                        "home_country": "home_country",
                        "term_address": "term_addr",
                        "term_postcode": "term_postcode",
                        "term_country": "term_country"}

    # Call test suite
    table_tester = TableTester(
                               target_table=target_table,
                               column_mappings=column_mappings,
                               key_column=key_column,
                               source_csv=source_csv,
                               source_csv_type="transformed",
                               source_table="",
                               caller_name=this_script_name)
    
    table_tester.run_tests()


if __name__ == "__main__":
    main()