import time
import subprocess
from prefect import task, flow
from prefect.tasks import task_input_hash
from datetime import timedelta
from src.etl.core.etl_utils import get_config

@task
def run_extract_scripts(config):
    print("Running extracts...")
    transform_scripts = [
        ("extract_hesa_nn056_students.py", "22056"),
        ("extract_hesa_nn056_demographics.py", "22056"),
        ("extract_hesa_nn056_student_programs.py", "22056"),
        ("extract_hesa_nn056_students.py", "23056"),
        ("extract_hesa_nn056_demographics.py", "23056"),
        ("extract_hesa_nn056_student_programs.py", "23056")
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


@task
def run_load_scripts(config):
    print("Running loads...")
    load_scripts = [
        ("load_hesa_nn056_students.py", "22056"),
        ("load_hesa_nn056_student_programs.py", "22056"),
        ("load_hesa_nn056_demographics.py", "22056"),
        ("load_hesa_nn056_lookup_disability.py", "22056"),
        ("load_hesa_nn056_lookup_ethnicity.py", "22056"),
        ("load_hesa_nn056_lookup_genderid.py", "22056"),
        ("load_hesa_nn056_lookup_religion.py", "22056"),
        ("load_hesa_nn056_lookup_sexid.py", "22056"),
        ("load_hesa_nn056_lookup_sexort.py", "22056"),
        ("load_hesa_nn056_lookup_trans.py", "22056"),
        ("load_hesa_nn056_lookup_z_ethnicgrp1.py", "22056"),
        ("load_hesa_nn056_lookup_z_ethnicgrp2.py", "22056"),
        ("load_hesa_nn056_lookup_z_ethnicgrp3.py", "22056"),
        ("load_hesa_nn056_students.py", "23056"),
        ("load_hesa_nn056_student_programs.py", "23056"),
        ("load_hesa_nn056_demographics.py", "23056"),
        ("load_hesa_nn056_lookup_disability.py", "23056"),
        ("load_hesa_nn056_lookup_ethnicity.py", "23056"),
        ("load_hesa_nn056_lookup_genderid.py", "23056"),
        ("load_hesa_nn056_lookup_religion.py", "23056"),
        ("load_hesa_nn056_lookup_sexid.py", "23056"),
        ("load_hesa_nn056_lookup_sexort.py", "23056"),
        ("load_hesa_nn056_lookup_trans.py", "23056"),
        ("load_hesa_nn056_lookup_z_ethnicgrp1.py", "23056"),
        ("load_hesa_nn056_lookup_z_ethnicgrp2.py", "23056"),
        ("load_hesa_nn056_lookup_z_ethnicgrp3.py", "23056")
    ]

    success = True
    for script, delivery_code in load_scripts:
        script_path = f"{config['load_script_dir']}/{script}"
        result = subprocess.run(["python3", script_path, delivery_code],
                                capture_output=True, text=True)

        if result.returncode != 0:
            print(f"Error in {script}: {result.stderr}")
            success = False
            break

    if success:
        print("Loads completed successfully")
    else:
        print("Some loads failed")

    return success


@task
def run_stage_scripts(config):
    print("Running stages...")
    stage_scripts = [
        "stage_hesa_nn056_students.py",
        "stage_hesa_nn056_programs.py",
        "stage_hesa_nn056_student_programs.py",
        "stage_hesa_nn056_lookup_disability.py",
        "stage_hesa_nn056_lookup_ethnicity.py",
        "stage_hesa_nn056_lookup_genderid.py",
        "stage_hesa_nn056_lookup_religion.py",
        "stage_hesa_nn056_lookup_sexid.py",
        "stage_hesa_nn056_lookup_sexort.py",
        "stage_hesa_nn056_lookup_trans.py",
        "stage_hesa_nn056_lookup_z_ethnicgrp1.py",
        "stage_hesa_nn056_lookup_z_ethnicgrp2.py",
        "stage_hesa_nn056_lookup_z_ethnicgrp3.py"
    ]

    success = True
    for script in stage_scripts:
        result = subprocess.run(["python3", f"{config['stage_script_dir']}/{script}"],
                                capture_output=True, text=True)

        if result.returncode != 0:
            print(f"Error in {script}: {result.stderr}")
            success = False
            break

    if success:
        print("Stages completed successfully")
    else:
        print("Some stages failed")
    return success


@task
def get_config_task():
    return get_config()


@flow(name="ETL Flow")
def etl_flow():
    config = get_config_task()

    transform_success = False
    load_success = False
    stage_success = False

    transform_success = run_extract_scripts(config)
    if transform_success:
        load_success = run_load_scripts(config, wait_for=[transform_success])
        if load_success:
            stage_success = run_stage_scripts(config, wait_for=[load_success])
    
    return {"transform success": transform_success, "load success": load_success, "stage success": stage_success}


def main():
    config = get_config()
    results = etl_flow()

    if all(results.values()):
        print("ETL pipeline completed")
    else:
        print(f"ETL pipeline failed: {results}")

if __name__ == '__main__':
    main()
