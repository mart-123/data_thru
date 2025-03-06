import time
import subprocess
from prefect import task, flow
from prefect.tasks import task_input_hash
from datetime import timedelta
from utils.etl_utils import get_config

script_A_running = False

@task
def run_transform_scripts(config):
    print("Running transforms...")
    subprocess.run(["python3", f"{config['etl_script_dir']}/transform_students_csv.py"])
    subprocess.run(["python3", f"{config['etl_script_dir']}/transform_demographics_csv.py"])
    subprocess.run(["python3", f"{config['etl_script_dir']}/transform_student_programs_csv.py"])
    print("Finished transforms")
    return True


@task
def run_load_scripts(config):
    print("Running loads...")
    subprocess.run(["python3", f"{config['etl_script_dir']}/load_hesa_22056_students.py"])
    subprocess.run(["python3", f"{config['etl_script_dir']}/load_hesa_22056_student_programs.py"])
    subprocess.run(["python3", f"{config['etl_script_dir']}/load_hesa_22056_demographics.py"])
    subprocess.run(["python3", f"{config['etl_script_dir']}/load_hesa_22056_lookup_disability.py"])
    subprocess.run(["python3", f"{config['etl_script_dir']}/load_hesa_22056_lookup_ethnicity.py"])
    subprocess.run(["python3", f"{config['etl_script_dir']}/load_hesa_22056_lookup_genderid.py"])
    subprocess.run(["python3", f"{config['etl_script_dir']}/load_hesa_22056_lookup_religion.py"])
    subprocess.run(["python3", f"{config['etl_script_dir']}/load_hesa_22056_lookup_sexid.py"])
    subprocess.run(["python3", f"{config['etl_script_dir']}/load_hesa_22056_lookup_sexort.py"])
    subprocess.run(["python3", f"{config['etl_script_dir']}/load_hesa_22056_lookup_trans.py"])
    subprocess.run(["python3", f"{config['etl_script_dir']}/load_hesa_22056_lookup_z_ethnicgrp1.py"])
    subprocess.run(["python3", f"{config['etl_script_dir']}/load_hesa_22056_lookup_z_ethnicgrp2.py"])
    subprocess.run(["python3", f"{config['etl_script_dir']}/load_hesa_22056_lookup_z_ethnicgrp3.py"])
    print("Finished loads")
    return True


@task
def run_stage_scripts(config):
    print("Running stages...")
    subprocess.run(["python3", f"{config['etl_script_dir']}/stage_hesa_nn056_students.py"])
    subprocess.run(["python3", f"{config['etl_script_dir']}/stage_hesa_nn056_programs.py"])
    subprocess.run(["python3", f"{config['etl_script_dir']}/stage_hesa_nn056_student_programs.py"])
    subprocess.run(["python3", f"{config['etl_script_dir']}/stage_hesa_nn056_lookup_disability.py"])
    subprocess.run(["python3", f"{config['etl_script_dir']}/stage_hesa_nn056_lookup_ethnicity.py"])
    subprocess.run(["python3", f"{config['etl_script_dir']}/stage_hesa_nn056_lookup_genderid.py"])
    subprocess.run(["python3", f"{config['etl_script_dir']}/stage_hesa_nn056_lookup_religion.py"])
    subprocess.run(["python3", f"{config['etl_script_dir']}/stage_hesa_nn056_lookup_sexid.py"])
    subprocess.run(["python3", f"{config['etl_script_dir']}/stage_hesa_nn056_lookup_sexort.py"])
    subprocess.run(["python3", f"{config['etl_script_dir']}/stage_hesa_nn056_lookup_trans.py"])
    subprocess.run(["python3", f"{config['etl_script_dir']}/stage_hesa_nn056_lookup_z_ethnicgrp1.py"])
    subprocess.run(["python3", f"{config['etl_script_dir']}/stage_hesa_nn056_lookup_z_ethnicgrp2.py"])
    subprocess.run(["python3", f"{config['etl_script_dir']}/stage_hesa_nn056_lookup_z_ethnicgrp3.py"])
    print("Finished stages")
    return True


@task
def get_config_task():
    return get_config()


@flow(name="ETL Flow")
def etl_flow():
    config = get_config_task()

    transform_result = run_transform_scripts(config)    # implicit dependency
    load_result = run_load_scripts(config, wait_for=[transform_result])
    stage_result = run_stage_scripts(config, wait_for=[load_result])
    
    return {"transform": transform_result, "load": load_result, "stage": stage_result}


def main():
    config = get_config()
    results = etl_flow()

    if all(results.values()):
        print("ETL pipeline completed")
    else:
        print(f"ETL pipeline failed: {results}")

if __name__ == '__main__':
    main()
