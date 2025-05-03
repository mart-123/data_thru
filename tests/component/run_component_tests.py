import os
import glob
import subprocess

def discover_scripts():
    """
    Builds list of .py scripts (other than itself) in script directory.
    Script names are fully qualified, absolute paths.
    """
    print(f"Discovering scripts")

    # Build path to script directory
    script_path = os.path.abspath(__file__)
    script_dir = os.path.dirname(script_path)

    # Read all .py script names in current directory
    script_paths = glob.glob(f"{script_dir}/test*.py")

    # Return list
    return script_paths


def run_scripts(script_paths):
    """
    Iterate through list of scripts and execute in turn.
    """
    print(f"Running Scripts:")
    results = []

    for script_path in script_paths:
        print(f"    Script: {script_path}")
        if script_path != __file__:
            result: subprocess.CompletedProcess = subprocess.run(["python3", script_path], capture_output=True, text=True)
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
        print(f"Script: {os.path.basename(cp.args[1])}")

        if "0 tests failed" in cp.stdout:
            print("    All test cases passed")
            scripts_passed += 1
        else:
            print(f"    Results: {cp.stdout}")
            scripts_failed += 1

        print("\n")
    
    print(f"Total test scripts run: {scripts_passed + scripts_failed}")
    print(f"Scripts with failures: {scripts_failed}")


def main():
    script_paths = discover_scripts()
    results = run_scripts(script_paths)
    print_results(results)


if __name__ == "__main__":
    main()

