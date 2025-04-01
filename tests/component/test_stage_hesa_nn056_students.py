import os
import subprocess
from src.etl.core.etl_utils import get_config
from src.testing.TableTester import TableTester

config = get_config()

def run_etl_script(etl_script_name):
     # Run ETL script whose output is to be tested.
    
    result = subprocess.run(["python3", f"{config['load_script_dir']}/{etl_script_name}"],
                    capture_output=True, text=True)

    if result.returncode != 0:
        print(f"error running {etl_script_name}: {result.stderr}")
    else:
        print(f"script {etl_script_name} completed successfully")


def main():
    # Declare parameters for test suite
    this_script_name = os.path.basename(__file__)
    source_csv = "expected_stage_hesa_nn056_students.csv"
    target_table = "stage_hesa_nn056_students"
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
                        "term_country": "term_country",
                        "ethnicity": "ethnicity",
                        "gender": "gender",
                        "religion": "religion",
                        "sexid": "sexid",
                        "sexort": "sexort",
                        "trans": "trans",
                        "ethnicity_grp1": "ethnicity_grp1",
                        "ethnicity_grp2": "ethnicity_grp2",
                        "ethnicity_grp3": "ethnicity_grp3"
                        }

    # Call test suite
    table_tester = TableTester(
                               target_table=target_table,
                               column_mappings=column_mappings,
                               source_csv=source_csv,
                               source_csv_type="expected",
                               source_table="",
                               caller_name=this_script_name)
    
    table_tester.run_tests()


if __name__ == "__main__":
#    run_etl_script("stage_hesa_nn056_students.py")
    main()
