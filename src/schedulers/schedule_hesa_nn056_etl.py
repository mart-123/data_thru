import schedule
import time
import subprocess
import os
from utils.etl_utils import get_config

script_A_running = False

def run_transform_scripts(config):
    global transforms_running
    transforms_running = True
    print("Running transforms...")
    subprocess.run(["python3", f"{config['etl_script_dir']}/transform_students_csv.py"])
    subprocess.run(["python3", f"{config['etl_script_dir']}/transform_demographics_csv.py"])
    subprocess.run(["python3", f"{config['etl_script_dir']}/transform_student_programs_csv.py"])
    print("Finished transforms")
    transforms_running = False


def run_load_scripts(config):
    """Waits for transforms to finish, then invokes load scripts"""
    while transforms_running:
        print("Waiting for transforms to finish...")
        time.sleep(10)  # Check every 10 seconds
    
    global loads_running
    loads_running = True
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

    loads_running = False
    print("Finished loads")


def run_stage_scripts(config):
    global stages_running
    stages_running = True
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
    stages_running = False


def etl_scheduled(config):
    # Schedule script_A to run at 03:00
    schedule.every().day.at("15:00").do(run_transform_scripts(config))

    # Schedule script_B to run at 04:00
    schedule.every().day.at("15:01").do(run_load_scripts(config))

    # Schedule script_B to run at 04:00
    schedule.every().day.at("15:02").do(run_stage_scripts(config))

    # Keep the script running to monitor the schedule
    while True:
        schedule.run_pending()
        time.sleep(1)
    

def etl_immediately(config):
    run_transform_scripts(config)
    run_load_scripts(config)
    run_stage_scripts(config)


def main():
    config = get_config()
    etl_immediately(config)
#    etl_scheduled(config)


if __name__ == '__main__':
    main()
