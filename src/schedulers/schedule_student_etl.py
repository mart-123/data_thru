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
    print("Running loads...")
    subprocess.run(["python3", f"{config['etl_script_dir']}/load_students_to_sql.py"])
    print("Finished loads")


def etl_scheduled(config):
    # Schedule script_A to run at 03:00
    schedule.every().day.at("19:25").do(run_transform_scripts)

    # Schedule script_B to run at 04:00
    schedule.every().day.at("19:26").do(run_load_scripts)

    # Keep the script running to monitor the schedule
    while True:
        schedule.run_pending()
        time.sleep(1)
    

def etl_immediately(config):
    run_transform_scripts(config)
    run_load_scripts(config)


def main():
    config = get_config()
    etl_immediately(config)


if __name__ == '__main__':
    main()
