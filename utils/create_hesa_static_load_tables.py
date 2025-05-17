"""
This module creates HESA-related load tables for misc static data files
that are managed by the team and do not originate directly from HESA.
e.g. delivery list, term code list.
"""
import mysql.connector
import traceback
import mysql.connector.cursor
from utils.data_platform_core import get_config, set_up_logging, connect_to_db

def init():
    config = get_config()
    set_up_logging(config)

    return config


def generate_create_statements():
    create_statements = {
        'load_hesa_delivery_metadata':
            """
            CREATE TABLE load_hesa_delivery_metadata (
                delivery_code VARCHAR(36) COMMENT 'Identifies a set of CSV files received from HESA',
                delivery_received DATE COMMENT 'Date on which the CSV files were downloaded/received from HESA',
                collection_ref VARCHAR(36) COMMENT 'Identifies collection (dataset sent by university to HESA) that the delivery relates to',
                collection_sent DATE COMMENT 'Date on which the underlying collection was SENT to HESA',
                delivery_description VARCHAR(250),
                load_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT 'Timestamp of insert',
                source_file VARCHAR(250) COMMENT 'File from which data was loaded'
                )
                COMMENT='List of HESA delivery datasets';
            """,
        'load_hesa_term_codes':
            """
            CREATE TABLE load_hesa_term_codes (
                term_code VARCHAR(36) COMMENT 'University term code',
                term_description VARCHAR(250) COMMENT 'Description of the term',
                standard_start_date DATE COMMENT 'Standard start date of the term',
                standard_end_date DATE COMMENT 'Standard end date of the term',
                load_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT 'Timestamp of insert',
                source_file VARCHAR(250) COMMENT 'File from which data was loaded'
                )
                COMMENT='List of term codes and their date ranges';
            """
    }

    return create_statements



def create_table(cursor: mysql.connector.cursor.MySQLCursor, table_name, create_statement):
    try:
        cursor.execute(f"""
            SELECT COUNT(*)
            FROM information_schema.tables
            WHERE table_name = '{table_name}'        
            """)
        
        if cursor.fetchone()[0] == 1:
            print(f"Table {table_name} : already exists")
        else:
            cursor.execute(create_statement)            
            print(f"Table {table_name} : created")

    except mysql.connector.Error as err:
        print(f"Exception during creation of {table_name}: {err}")
        raise




def main():
    # Declare here to ensure except/finally work if connection fails
    conn = None

    try:
        config = init()
        conn = connect_to_db(config)
        cursor = conn.cursor()
        create_statements = generate_create_statements()

        for table_name, create_statement in create_statements.items():
            create_table(cursor, table_name, create_statement)

        conn.commit()
        print("Table creation complete")

    except Exception as e:
#        print(f"Error during table creation: {e}")
        traceback.print_exc()
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()


if __name__ == '__main__':
    main()
