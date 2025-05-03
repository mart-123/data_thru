import os
import subprocess
from utils.data_platform_core import get_config
from TableTester import TableTester

config = get_config()

def run_etl_model(etl_model_name):
    """Runs ETL model whose output is to be tested."""
    
    result = subprocess.run(
        ["dbt", "run", "--models", etl_model_name],
        cwd=config["dbt_project_dir"],
        capture_output=True, text=True)

    if result.returncode != 0:
        print(f"error running {etl_model_name}: {result.stderr}")
    else:
        print(f"model {etl_model_name} completed successfully")


def main():
    """
    Instantiates TableTester class with source/target dataset definitions,
    then invokes the test process.
    """
    # Declare parameters for test suite
    this_script_name = os.path.basename(__file__)
    source_file = "expected_stage_hesa_nn056_student_programs.csv"
    source_path = os.path.join(config["expected_dir"], source_file)
    target_table = "stage_hesa_nn056_student_programs"
    column_mappings = {
                       "student_guid": "student_guid",
                       "program_guid": "program_guid",
                       "enrol_date": "enrol_date",
                       "fees_paid": "fees_paid",
                       "source_file": "source_file",
                       "hesa_delivery": "hesa_delivery"
                       }

    # Call test suite
    table_tester = TableTester(
                               target_table=target_table,
                               column_mappings=column_mappings,
                               source_path=source_path,
                               source_table="",
                               caller_name=this_script_name)
    
    table_tester.run_tests()


if __name__ == "__main__":
    run_etl_model("stage_hesa_nn056_student_programs")
    main()
