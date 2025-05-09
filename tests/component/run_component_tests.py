import os
import sys
import glob
import subprocess


def discover_scripts():
    """
    Builds list of .py scripts (other than itself) in script directory.
    Script names are fully qualified, absolute paths.
    """
    print(f"Discovering test scripts...")

    # Build path to script directory
    script_path = os.path.abspath(__file__)
    script_dir = os.path.dirname(script_path)

    # Get list of test*.py script names. In pipeline order in case
    # of test scripts first running respective pipeline script.
    extract_testers = glob.glob(f"{script_dir}/test_extract_*.py")
    load_testers = glob.glob(f"{script_dir}/test_load_*.py")
    stage_testers = glob.glob(f"{script_dir}/test_stage_*.py")

    script_paths = extract_testers + load_testers + stage_testers

    # Return list
    return script_paths


def run_scripts(script_paths, run_etl_param):
    """
    Iterate through list of test scripts and execute in turn.
    """
    print(f"Running test scripts:")
    results = []

    for script_path in script_paths:
        print(f"    Test script: {script_path}")
        if script_path != __file__:
            result: subprocess.CompletedProcess = subprocess.run(["python3", script_path, run_etl_param], capture_output=True, text=True)
            results.append(result)

    return results


def print_results(results):
    """
    Iterate through results set and print pass/fail outcomes.
    """

    scripts_passed = 0
    scripts_failed = 0

    for result in results:
        cp: subprocess.CompletedProcess = result
        print(f"Test script: {os.path.basename(cp.args[1])}")

        if "0 tests failed" in cp.stdout:
            print("    All test cases passed")
            scripts_passed += 1
        else:
            print(f"    Unexpected results: {cp.stdout}")
            scripts_failed += 1

        print("\n")
    
    print(f"Total test scripts run: {scripts_passed + scripts_failed}")
    print(f"Scripts with failures: {scripts_failed}")


def main():
    run_etl_param = None

    # Argument determines whether each test script first
    # runs its respective pipeline process     
    if "--run-etl" in sys.argv:
        print("Test scripts will run their respective pipeline processes")
        run_etl_param = "--run-etl"
    else:
        print("Test scripts will NOT run their respective pipeline processes")
        run_etl_param = ""

    script_paths = discover_scripts()
    results = run_scripts(script_paths, run_etl_param)
    print_results(results)


if __name__ == "__main__":
    main()

