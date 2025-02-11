import schedule
import time
import subprocess

script_A_running = False

def run_script_A():
    global script_A_running
    script_A_running = True
    print("Running script_A...")
    subprocess.run(["python3", "transform_students_csv.py"])
    print("Finished script_A")
    script_A_running = False

def run_script_B():
    """Script B is invoked on schedule but starts after Script A is finished"""
    while script_A_running:
        print("Waiting for script_A to finish...")
        time.sleep(10)  # Check every 10 seconds
    print("Running script_B...")
    subprocess.run(["python3", "load_students_to_sql.py"])
    print("Finished script_B")

# Schedule script_A to run at 03:00
schedule.every().day.at("19:25").do(run_script_A)

# Schedule script_B to run at 04:00
schedule.every().day.at("19:26").do(run_script_B)

# Keep the script running to monitor the schedule
while True:
    schedule.run_pending()
    time.sleep(1)