import logging
import os
import json
import datetime
import subprocess
import mysql.connector
from mysql.connector import errorcode
from dotenv import load_dotenv


def get_windows_host_ip():
    """Retrieves Windows host IP address (WSL2 loopback address)."""
    try:
        result = subprocess.run(
            ["grep", "nameserver", "/etc/resolv.conf"], capture_output=True, text=True
        )
        ip_address = result.stdout.split()[1]
        return ip_address
    except Exception as e:
        logging.critical(f"Error retrieving Windows host IP address: {e}")
        raise


def get_config(env="dev"):
    """
    Reads json config file into 'config' dictionary.
    Appends various working directories and file paths.
    Usage:
        - call from ETL process with config = get_config()
        - add further config[] items for process-specific filepaths, etc
    """
    try:
        # Load project environment variables from .env file
        # (db connection config and base dir/config file paths)
        dotenv_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), '../../.env')
        load_dotenv(dotenv_file)
        base_dir = os.getenv("BASE_DIR")
        config_file_path = os.getenv("CONFIG_FILE")

        # Get config file (mainly directory paths)
        with open(config_file_path, "r") as config_file:
            config = json.load(config_file)

        # Build various directory/file paths from env vars and config values
        config["base_dir"] = base_dir
        config["log_dir"] = os.path.join(base_dir, config["log_dir"], env)
        config["data_dir"] = os.path.join(base_dir, config["data_dir"])
        config["extracts_dir"] = os.path.join(base_dir, config["extracts_dir"])
        config["bad_data_dir"] = os.path.join(base_dir, config["bad_data_dir"])
        config["transformed_dir"] = os.path.join(base_dir, config["transformed_dir"])
        config["lookups_dir"] = os.path.join(base_dir, config["lookups_dir"])
        config["info_log_file_path"] = os.path.join(config["log_dir"], "etl_info.log")
        config["error_log_file_path"] = os.path.join(config["log_dir"], "etl_error.log")
        config["etl_script_dir"] = os.path.join(base_dir, config["etl_script_dir"])
        config["env"] = env

        # Get DB connection config
#        config["db_host_ip"] = get_windows_host_ip() # only for windows-hosted MySQL
        config["db_host_ip"] = "localhost"
        config["db_port"] = os.getenv("DB_PORT")
        config["db_user"] = os.getenv("DB_USER")
        config["db_pwd"] = os.getenv("DB_PWD")
        config["db_name"] = os.getenv("DB_NAME")
        
        return config

    except Exception as e:
        logging.critical(f"Error loading config: {e}")
        raise


def set_up_logging(config):
    """
    Sets up logging (two logs: a 'main' and a 'warnings and upwards').
    Helper function for get_config().
    """
    try:
        os.makedirs(config["log_dir"], exist_ok=True)

        log_format = logging.Formatter(
            "%(asctime)s - %(levelname)s - %(filename)s - %(message)s"
        )

        info_handler = logging.FileHandler(config["info_log_file_path"], mode="a")
        info_handler.setLevel(logging.INFO)
        info_handler.setFormatter(log_format)

        error_handler = logging.FileHandler(config["error_log_file_path"], mode="a")
        error_handler.setLevel(logging.WARNING)
        error_handler.setFormatter(log_format)

        root_logger = logging.getLogger()
        root_logger.setLevel(logging.INFO)  # threshold for writing to log
        root_logger.addHandler(info_handler)
        root_logger.addHandler(error_handler)

    except Exception as e:
        print(f"Error setting up logging: {e}")
        raise


def connect_to_db(config):
    """Connects to MySQL database and returns connection object"""
    try:
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
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            logging.critical(
                f"MySQL access denied, check credentials. Host: {config['db_host_ip']} port: {config['db_port']}, db: {config['db_name']}"
            )
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            logging.critical(
                f"MySQL database not found. Host: {config['db_host_ip']}, port: {config['db_port']}, db: {config['db_name']}"
            )
        else:
            logging.critical(f"MySQL error: {err}")

        # raise RuntimeError(f"Failed db connection. Host: {ip_addr}, port: {config['db_port']}, db: {config['db_name']}")
        raise


def is_valid_date(date_str):
    """Returns 'true' if valid date yyyy-mm-dd. Otherwise false."""
    try:
        datetime.datetime.strptime(date_str, "%Y-%m-%d")
        return True
    except ValueError:
        return False
