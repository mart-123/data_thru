import time
import subprocess
from prefect import task, flow
from prefect.tasks import task_input_hash
from datetime import timedelta
from src.utils.etl_utils import get_config

@task
def run_transform_scripts(config):
    print("Running transforms...")
    transform_scripts = [
        "transform_students_csv.py",
        "transform_demographics_csv.py",
        "transform_student_programs_csv.py"
    ]

    all_success = True
    for script in transform_scripts:
        result = subprocess.run(["python3", f"{config['etl_script_dir']}/{script}"],
                       capture_output=True, text=True)

        if result.returncode != 0:
            print(f"Error in {script}: {result.stderr}")
            all_success = False

    if all_success:
        print("Transforms completed successfully")
    else:
        print("Some transforms failed")

    return all_success


@task
def run_load_scripts(config):
    print("Running loads...")
    load_scripts = [
        "load_hesa_22056_students.py",
        "load_hesa_22056_student_programs.py",
        "load_hesa_22056_demographics.py",
        "load_hesa_22056_lookup_disability.py",
        "load_hesa_22056_lookup_ethnicity.py",
        "load_hesa_22056_lookup_genderid.py",
        "load_hesa_22056_lookup_religion.py",
        "load_hesa_22056_lookup_sexid.py",
        "load_hesa_22056_lookup_sexort.py",
        "load_hesa_22056_lookup_trans.py",
        "load_hesa_22056_lookup_z_ethnicgrp1.py",
        "load_hesa_22056_lookup_z_ethnicgrp2.py",
        "load_hesa_22056_lookup_z_ethnicgrp3.py"
    ]

    success = True
    for script in load_scripts:
        result = subprocess.run(["python3", f"{config['etl_script_dir']}/{script}"],
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
        result = subprocess.run(["python3", f"{config['etl_script_dir']}/{script}"],
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

    transform_success = run_transform_scripts(config)
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
