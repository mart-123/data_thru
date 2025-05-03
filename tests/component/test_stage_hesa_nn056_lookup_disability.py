import os
import subprocess
from utils.data_platform_core import get_config
from TableTester import TableTester

config = get_config()

def run_etl_model(etl_model_name):
     # Run ETL model whose output is to be tested    
    result = subprocess.run(
        ["dbt", "run", "--models", etl_model_name],
        cwd=config["dbt_project_dir"],
        capture_output=True, text=True)

    if result.returncode != 0:
        print(f"error running {etl_model_name}: {result.stderr}")
    else:
        print(f"model {etl_model_name} completed successfully")


def main():
    # Declare parameters for test suite
    this_script_name = os.path.basename(__file__)
    source_file = "expected_stage_hesa_nn056_DISABILITY.csv"
    source_path = os.path.join(config['expected_dir'], source_file)
    target_table = "stage_hesa_nn056_lookup_disability"
    column_mappings = {
        "code": "code",
        "label": "label",
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
# Uncomment to run ETL component first
#    run_etl_model("stage_hesa_nn056_lookup_disability")
    main()
