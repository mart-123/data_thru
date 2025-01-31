# mysql is running in windows. Basic server info:
# Server instance: MySQL92
"""
This module creates college database and tables.
"""
import mysql.connector
from mysql.connector import errorcode
import os
import json


def get_config():
    """Reads json config file, returns contents as a dictionary"""
    try:
        script_path = os.path.dirname(os.path.abspath(__file__))
        config_path = os.path.join(script_path, '../data/config.json')
        config_file = open(config_path, 'r')
        config = json.load(config_file)
        return config
    except Exception as e:
        print(f"Error opening config {config_path}: {e}")
        raise



def connect_to_db(config):
    """Connects to MySQL database and returns connection object"""
    print("Connecting to MySQL database...")
    try:
        conn = mysql.connector.connect(
            host=config['db_host'],
            port=config['db_port'],
            user=config['db_user'],
            password=config['db_pwd'],
            database=config['db_name']
            )

        print("Successfully connected to db")
        return conn

    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("MySQL error: access denied, check credentials")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("MySQL error: database not found")
        else:
            print(f"MySQL error: {err}")
        
        # The only place we issue an 'exit' - nothing has yet happened
        exit(1)



def create_load_students_table(cursor):
    """ Creates student load table if not found"""
    print("Creating load_students table...")
    table_name = 'load_students'

    create_student_table = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            student_id INT AUTO_INCREMENT PRIMARY KEY,
            student_guid CHAR(36), -- not PK, only for linking to source systems
            first_names VARCHAR(250),
            last_name VARCHAR(250),
            phone VARCHAR(250),
            email VARCHAR(250),
            home_addr VARCHAR(250),
            home_postcode VARCHAR(50),
            home_country VARCHAR(100),
            term_addr VARCHAR(250),
            term_postcode VARCHAR(50),
            term_country VARCHAR(100),
            dob DATE
            );
        """ 
    try:
        cursor.execute(create_student_table)
        print(f"Successfully checked or created table: {table_name}")
    except mysql.connector.Error as err:
        print(f"Error during creation of load_students: {err}")
        raise



def main():
    """
    Creates tables for the 'college' sample database.
    """
    # Declare here to ensure except/finally work if connection fails
    conn = None

    try:
        print("Creating tables")
        config = get_config()
        conn = connect_to_db(config)
        print("Instantiating cursor...")
        cursor = conn.cursor()
        print("Successfully instantiated cursor")
        create_load_students_table(cursor)
        conn.commit()
        print("Table creation complete")

    except Exception as e:
        print(f"Error during table creation: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()


if __name__ == '__main__':
    main()
