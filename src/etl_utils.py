import logging
import os
import json
import datetime


def get_config(env='dev'):
    """
    Reads json config file into 'config' dictionary.
    Appends various working directories and file paths.
    Usage:
        - call from ETL process with config = get_config()
        - add further config[] items for process-specific filepaths, etc
    """
    try:
        # Get db connection config and use to initialise 'config' dictionary
        script_path = os.path.dirname(os.path.abspath(__file__))
        config_path = os.path.join(script_path, '../data/config.json')
        with open(config_path, 'r') as config_file: 
            config = json.load(config_file)

        # Append various directory/file paths to the 'config' dictionary
        script_dir      = os.path.dirname(os.path.abspath(__file__))
        log_dir         = os.path.join(script_dir, f'../logs/{env}')
        data_dir        = os.path.join(script_dir, f'../data')
        extracts_dir    = os.path.join(script_dir, f'../data/extracts')
        bad_data_dir    = os.path.join(script_dir, f'../data/bad_data')
        transformed_dir = os.path.join(script_dir, f'../data/transformed')
        static_dir      = os.path.join(script_dir, f'../data/static')
        config['script_dir']        = script_dir
        config['log_dir']           = log_dir
        config['data_dir']          = data_dir
        config['extracts_dir']      = extracts_dir
        config['bad_data_dir']      = bad_data_dir
        config['transformed_dir']   = transformed_dir
        config['static_dir']        = static_dir
        config['info_log_file_path'] = os.path.join(log_dir, 'etl_info.log')
        config['error_log_file_path'] = os.path.join(log_dir, 'etl_error.log')
        config['env'] = env

        return config

    except Exception as e:
        logging.critical(f"Error opening config {config_path}: {e}")
        raise


def set_up_logging(config):
    """
    Sets up logging (two logs: a 'main' and a 'warnings and upwards').
    Helper function for get_config().
    """
    try:
        os.makedirs(config['log_dir'], exist_ok=True)

        log_format = logging.Formatter("%(asctime)s - %(levelname)s - %(filename)s - %(message)s")

        info_handler = logging.FileHandler(config['info_log_file_path'], mode='a')
        info_handler.setLevel(logging.INFO)
        info_handler.setFormatter(log_format)

        error_handler = logging.FileHandler(config['error_log_file_path'], mode='a')
        error_handler.setLevel(logging.WARNING)
        error_handler.setFormatter(log_format)

        root_logger = logging.getLogger()
        root_logger.setLevel(logging.INFO)   # threshold for writing to log
        root_logger.addHandler(info_handler)
        root_logger.addHandler(error_handler)

    except Exception as e:
        print(f"Error setting up logging: {e}")
        raise


def is_valid_date(date_str):
    """Returns 'true' if valid date yyyy-mm-dd. Otherwise false."""
    try:
        datetime.datetime.strptime(date_str, '%Y-%m-%d')
        return True
    except ValueError:
        return False
    