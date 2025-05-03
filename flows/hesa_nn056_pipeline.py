import time
import subprocess
from utils.data_platform_core import get_config

def run_extract_scripts(config):
    """
    Runs all extract scripts. They are all initiated without any
    dependencies between them.
    """
    print("Running extracts...")
    transform_scripts = [
        ("extract_hesa_nn056_students.py", "22056_20240331"),
        ("extract_hesa_nn056_demographics.py", "22056_20240331"),
        ("extract_hesa_nn056_student_programs.py", "22056_20240331"),
        ("extract_hesa_nn056_students.py", "23056_20250331"),
        ("extract_hesa_nn056_demographics.py", "23056_20250331"),
        ("extract_hesa_nn056_student_programs.py", "23056_20250331")
    ]

    all_success = True
    for script, delivery_code in transform_scripts:
        script_path = f"{config['extract_script_dir']}/{script}"
        result = subprocess.run(["python3", script_path, delivery_code],
                       capture_output=True, text=True)

        if result.returncode != 0:
            print(f"Error in {script}: {result.stderr}")
            all_success = False

    if all_success:
        print("Extracts completed successfully")
    else:
        print("Some transforms failed")

    return all_success


def run_load_scripts(config):
    print("Running loads...")

    # Process main load tables
    main_nn056_loads = [
        ("load_hesa_nn056_students.py", "22056_20240331"),
        ("load_hesa_nn056_student_programs.py", "22056_20240331"),
        ("load_hesa_nn056_demographics.py", "22056_20240331"),
        ("load_hesa_nn056_students.py", "23056_20250331"),
        ("load_hesa_nn056_student_programs.py", "23056_20250331"),
        ("load_hesa_nn056_demographics.py", "23056_20250331"),
    ]

    success = True
    for script, delivery_code in main_nn056_loads:
        script_path = f"{config['load_script_dir']}/{script}"
        result = subprocess.run(["python3", script_path, delivery_code], capture_output=True, text=True)

        if result.returncode != 0:
            print(f"Error in {script}: {result.stderr}")
            success = False
            break

    # Process look-up load tables
    nn056_lookups = ["DISABILITY", "ETHNICITY", "GENDERID", "RELIGION", 
                     "SEXID", "SEXORT", "TRANS", "Z_ETHNICGRP1", "Z_ETHNICGRP2", "Z_ETHNICGRP3"]

    nn056_deliveries = ["22056_20240331", "23056_20250331"]

    for lookup_name in nn056_lookups:
        for delivery_code in nn056_deliveries:
            script_path = f"{config['load_script_dir']}/load_hesa_nn056_lookup_table.py"
            result = subprocess.run(["python3", script_path, delivery_code, lookup_name], capture_output=True, text=True)

            if result.returncode != 0:
                print(f"Error in {script}: {result.stderr}")
                success = False
                break

    if success:
        print("Loads completed successfully")
    else:
        print("Some loads failed")

    return success


def run_stage_scripts(config):
    print("Running DBT staging models...")

    # Get start time for duration calculation
    start_time = time.time()

    # Run DBT staging scripts
    result = subprocess.run(
        ["dbt", "run", "--models", "staging"],
        cwd=config["dbt_project_dir"],
        capture_output=True, text=True)

    # Calculate execution time
    execution_time = time.time() - start_time
    print(f"DBT staging ran for {execution_time} seconds")

    # Show DBT output for debugging
    print(f"DBT output:\n{result.stdout}")

    # Report errors
    if result.returncode != 0:
        print(f"Error in DBT staging: {result.stderr}")
        return False

    # Report success
    print("DBT staging completed successfully")
    return True


def run_dimension_scripts(config):
    print("Running DBT dimension models...")

    # Get start time for duration calculation
    start_time = time.time()

    # Run DBT dimension scripts
    result = subprocess.run(
        ["dbt", "run", "--models", "dimensions"],
        cwd=config["dbt_project_dir"],
        capture_output=True, text=True)

    # Calculate execution time
    execution_time = time.time() - start_time
    print(f"DBT dimensions ran for {execution_time} seconds")

    # Show DBT output for debugging
    print(f"DBT output:\n{result.stdout}")

    # Report errors
    if result.returncode != 0:
        print(f"Error in DBT dimensions: {result.stderr}")
        return False

    # Report success
    print("DBT dimensions completed successfully")
    return True


def get_config_task():
    return get_config()


def etl_flow():
    config = get_config_task()

    transform_success = False
    load_success = False
    stage_success = False
    dimension_success = False

    transform_success = run_extract_scripts(config)
    if transform_success:
        load_success = run_load_scripts(config)
        if load_success:
            stage_success = run_stage_scripts(config)
            if stage_success:
                dimension_success = run_dimension_scripts(config)
    
    return {"transform success": transform_success,
            "load success": load_success,
            "stage success": stage_success,
            "dimension success": dimension_success}


def main():
    config = get_config()
    results = etl_flow()

    if all(results.values()):
        print("ETL pipeline completed")
    else:
        print(f"ETL pipeline failed: {results}")

if __name__ == '__main__':
    main()
