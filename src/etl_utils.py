import logging
import os
import json


def get_config(env='dev'):
    """
    Reads json config file into 'config' dictionary.
    Appends various working directories and file paths.
    Usage: call from ETL process then add config[] items for process-specific filepaths.
    """
    try:
        # Get db connection config and use to initialise 'config' dictionary
        script_path = os.path.dirname(os.path.abspath(__file__))
        config_path = os.path.join(script_path, '../data/config.json')
        with open(config_path, 'r') as config_file: 
            config = json.load(config_file)

        # Append various directory/file paths to the 'config' dictionary
        script_dir = os.path.dirname(os.path.abspath(__file__))
        log_dir = os.path.join(script_dir, f'../logs/{env}')
        data_dir = os.path.join(script_dir, f'../data')
        config['log_dir'] = log_dir
        config['data_dir'] = data_dir
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

        root_logger = logging.getLogger()
        root_logger.setLevel(logging.INFO)   # threshold for writing to log
        log_format = logging.Formatter("%(asctime)s - %(levelname)s - %(filename)s - %(message)s")

        info_handler = logging.FileHandler(config['info_log_file_path'], mode='a')
        info_handler.setLevel(logging.INFO)
        info_handler.setFormatter(log_format)

        error_handler = logging.FileHandler(config['error_log_file_path'], mode='a')
        error_handler.setLevel(logging.WARNING)
        error_handler.setFormatter(log_format)

        root_logger.addHandler(info_handler)
        root_logger.addHandler(error_handler)

    except Exception as e:
        print(f"Error setting up logging: {e}")
        raise



    