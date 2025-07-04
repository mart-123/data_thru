"""
ETL utility module providing core functions for HESA data warehouse, including:
        - Config loading (from .env files and JSON configs)
        - Logging setup and configuration
        - Database connection handling
        - Host IP retrieval for WSL2 environments
        - Date validation utilities
"""
import logging
import os
import os.path
import sys
import json
import subprocess
import time
import mysql.connector
from mysql.connector import errorcode
from dotenv import load_dotenv
from datetime import datetime


def get_windows_host_ip():
    """Retrieves Windows host IP address (WSL2 loopback address)."""
    try:
        result = subprocess.run(
            ["grep", "nameserver", "/etc/resolv.conf"], capture_output=True, text=True
        )
        ip_address = result.stdout.split()[1]
        return ip_address
    except Exception as e:
        # As logging hasn't yet been set up, write error to stderr
        print(f"critical error retrieving Windows host IP: {e}", file=sys.stderr)
        raise


def find_dotenv_file():
    """
    Find .env file by checking this script's source directory and parent
    directories. Returns full filepath or raises FileNotFoundError.
    """
    script_dir = os.path.dirname(os.path.abspath(__file__))
    current_dir = script_dir

    for _ in range(4):
        possible_env_file = os.path.join(current_dir, '.env')
        if os.path.isfile(possible_env_file):
            return possible_env_file
        
        current_dir = os.path.dirname(current_dir)

    # If control gets here, .env file was not found.
    raise FileNotFoundError(f"unable to find .env file in {script_dir}")            


def get_config():
    """
    Reads main config file (nested json) and uses its contents to build
    fully qualified file/dir path names in flat 'config' dictionary.

    Returns a dictionary containing application config including:
        - logging directories
        - data directories

    Expects to find main config filepath in .env file in project root.
    """
    try:
        # 0. Create flat config dictionary (to contain working directories)
        config = {}

        # 1. Load basic environment variables (base directory and config filename)
        # Note: .env file load commented out due to container-first approach
        # try:
        #     dotenv_file_path = find_dotenv_file()
        #     load_dotenv(dotenv_file_path, override=True)
        #     print(f"loaded .env from: {dotenv_file_path}")
        # except FileNotFoundError:
        #     print(f"No .env file found, using environment variables directly")

        base_dir = os.getenv("BASE_DIR")
        data_dir = os.getenv("DATA_DIR")
        log_dir = os.getenv("LOG_DIR")
        config_file_path = os.getenv("CONFIG_FILE")
        print(f"using config file: {config_file_path}")

        # 2. Load main config file (contains nested, relative directories)
        with open(config_file_path, "r") as config_file:
            json_config = json.load(config_file)

        # 4. Get base directories (absolute paths)
        scripts_path = os.path.join(base_dir, json_config["paths"]["scripts_base"])
        dbt_path = os.path.join(base_dir, json_config["paths"]["dbt_base"])

        # 5. Declare log directory
        config["log_dir"] = log_dir

        # 6. Declare data directories
        config["deliveries_dir"] = os.path.join(data_dir, json_config["paths"]["deliveries"])
        config["bad_data_dir"] = os.path.join(data_dir, json_config["paths"]["bad_data"])
        config["transformed_dir"] = os.path.join(data_dir, json_config["paths"]["transformed_data"])
        config["expected_dir"] = os.path.join(data_dir, json_config["paths"]["expected_data"])
        config["static_dir"] = os.path.join(data_dir, json_config["paths"]["static_data"])

        # 7. Declare script directories
        config["extract_script_dir"] = os.path.join(scripts_path, json_config["paths"]["extract_scripts"])
        config["load_script_dir"] = os.path.join(scripts_path, json_config["paths"]["load_scripts"])
        config["dbt_project_dir"] = dbt_path

        # Get database settings
#        config["db_host_ip"] = get_windows_host_ip() # only for windows-hosted MySQL connecting from WSL2
#        config["db_host_ip"] = "localhost" # for connecting to dockerised MySQL from host system execution
        config["db_host_ip"] = os.getenv("DB_HOST")
        config["db_port"] = os.getenv("DB_PORT")
        config["db_user"] = os.getenv("DB_USER")
        config["db_pwd"] = os.getenv("DB_PWD")
        config["db_name"] = os.getenv("DB_NAME")
        
        return config

    except Exception as e:
        # As logging hasn't yet been set up, write config error to stderr
        print(f"critical error loading config: {e}", file=sys.stderr)
        raise


def set_up_logging(config, script_name=None):
    """
    Sets up logging (two logs: a 'main' and a 'warnings and upwards').
    Helper function for get_config().
    """
    try:
        os.makedirs(config["log_dir"], exist_ok=True)

        # basic: "%(asctime)s - %(levelname)s - %(pathname)s - %(message)s"
        if script_name:
            log_format = logging.Formatter(f"%(asctime)s - %(levelname)s - {script_name} - %(message)s")
        else:
            log_format = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

        info_log_file = os.path.join(config["log_dir"], "etl_info.log")
        error_log_file = os.path.join(config["log_dir"], "etl_error.log")

        info_handler = logging.FileHandler(info_log_file, mode="a")
        info_handler.setLevel(logging.INFO)
        info_handler.setFormatter(log_format)

        error_handler = logging.FileHandler(error_log_file, mode="a")
        error_handler.setLevel(logging.WARNING)
        error_handler.setFormatter(log_format)

        # Get root logger and clear any existing handlers
        root_logger = logging.getLogger()
        if root_logger.handlers:
            for handler in root_logger.handlers:
                root_logger.removeHandler(handler)

        # Set level and add handlers
        root_logger.setLevel(logging.INFO)  # threshold for logging
        root_logger.addHandler(info_handler)
        root_logger.addHandler(error_handler)

    except Exception as e:
        print(f"error setting up logging: {e}")
        raise


def connect_to_db(config, max_attempts=20, retry_delay=1):
    """
    Connects to MySQL database with retry logic
    
    Args:
        config: Dictionary with DB connection parameters
        max_attempts: Maximum number of connection attempts
        retry_delay: Seconds to wait between attempts
        
    Returns:
        Connection object or raises exception after max attempts
    """
    attempt=0

    while attempt < max_attempts:
        try:
            attempt += 1
            conn = mysql.connector.connect(
                host=config["db_host_ip"],
                port=config["db_port"],
                user=config["db_user"],
                password=config["db_pwd"],
                database=config["db_name"]
            )

            logging.info(
                f"Connected to db: {config['db_name']} host: {config['db_host_ip']} port: {config['db_port']}"
            )
            return conn

        except mysql.connector.Error as err:
            retry_message = f"Connection attempt {attempt}/{max_attempts}"

            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                logging.error(
                    f"{retry_message}: MySQL access denied, check credentials. "
                    f"Host: {config['db_host_ip']} port: {config['db_port']}, db: {config['db_name']}"
                )
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                logging.error(
                    f"{retry_message}: MySQL access denied, check credentials. "
                    f"MySQL database not found. Host: {config['db_host_ip']}, port: {config['db_port']}, db: {config['db_name']}"
                )
            else:
                logging.error(
                    f"{retry_message}: MySQL error: {err}")

            if attempt >= max_attempts:
                logging.error(
                    f"Failed to connect to MySQL after {max_attempts} attempts. "
                    f"Host: {config['db_host_ip']} port: {config['db_port']}, db: {config['db_name']}"
                )

                raise RuntimeError(f"Failed to connect to MySQL after {max_attempts} attempts")
    
            # Wait before retrying
            time.sleep(retry_delay)

def is_valid_date(date_str):
    """Returns 'true' if valid date yyyy-mm-dd. Otherwise false."""
    try:
        datetime.strptime(date_str, "%Y-%m-%d")
        return True
    except ValueError:
        return False
