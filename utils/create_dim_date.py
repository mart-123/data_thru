"""
This module creates HESA-related load tables for misc static data files
that are managed by the team and do not originate directly from HESA.
e.g. delivery list, term code list.
"""
import mysql.connector
import traceback
import mysql.connector.cursor
import logging
import pandas as pd
from datetime import date, timedelta
from utils.data_platform_core import get_config, set_up_logging, connect_to_db


def init():
    config = get_config()
    set_up_logging(config)
    return config


def generate_create_statement(table_name):

    create_statement = f"""
            CREATE TABLE {table_name} (
            dim_date_key VARCHAR(20) COMMENT 'Surrogate key structured DAT_YYYYMMDD for human readability',
            calendar_date DATE,
            year INT COMMENT 'Calendar year (eg. 2025)',
            month INT COMMENT 'Month number (1-12)',
            day INT COMMENT 'Day of month (1-31)',
            day_of_week INT COMMENT 'Day of week (1=Monday, 7=Sunday)',
            day_of_year INT COMMENT 'Day of year (1-366)',
            week_of_year INT COMMENT 'Week within year (1-53, may overlap into new year)',
            quarter INT COMMENT 'Calendar quarter (1-4)',
            day_name VARCHAR(9) COMMENT 'Day name (Sunday, Monday, etc.)',
            month_name VARCHAR(9) COMMENT 'Month name (January, February, etc.)',
            weekend TINYINT COMMENT 'Flag indicating if date is a weekend day (1) or weekday (0)',
            first_day_of_month DATE COMMENT 'First day of the month containing this date',
            last_day_of_month DATE COMMENT 'Last day of the month containing this date',
            PRIMARY KEY (dim_date_key)
            ) COMMENT 'Calendar date dimension for date-based attributes';
            """

    return create_statement


def create_table(cursor: mysql.connector.cursor.MySQLCursor, table_name, create_statement):
    try:
        print(f"Creating dim_date...")
        cursor.execute(f"""
            SELECT COUNT(*)
            FROM information_schema.tables
            WHERE table_name = '{table_name}'        
            """)
        
        if cursor.fetchone()[0] == 1:
            print(f"    Table {table_name} : already exists")
        else:
            cursor.execute(create_statement)            
            print(f"    Table {table_name} : created")

    except mysql.connector.Error as err:
        print(f"Exception during creation of {table_name}: {err}")
        raise


def cleardown_table(cursor, table_name):
    """
    Delete all rows from the load_students table (as a load table
    its contents do not persist over time).
    """
    try:
        print(f"Clearing down dim_date...")
        # Get row count to be displayed after delete is finished
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        row_count = cursor.fetchone()[0]
        
        # Delete all rows from the table (commit logic is in 'main')
        cursor.execute(f"DELETE FROM {table_name}")

        logging.info(f"Deleted {row_count} rows from {table_name}")

    except Exception as e:
        logging.critical(f"Error clearing down table {table_name}: {e}")
        raise


def generate_dates():
    """Generate DataFrame containing all datesfrom/to the given dates."""
    print(f"Generating date range...")
    start_date: date = date(2000, 1, 1)
    end_date: date = date(2030, 12, 31)
    days = (end_date - start_date).days

    date_list = []

    # Build initial dataframe with just the one column
    for i in range(days):
        calc_date = start_date + timedelta(days=i)
        date_list.append(calc_date)

    dates_df = pd.DataFrame(date_list, columns=["calendar_date"])
    dates_df["calendar_date"] = pd.to_datetime(dates_df["calendar_date"])

    # Generate derivative values (year, month, day, month name, etc)
    dates_df["dim_date_key"] = "DAT_" + dates_df["calendar_date"].dt.strftime("%Y%m%d")
    dates_df["day"] = dates_df["calendar_date"].dt.day
    dates_df["month"] = dates_df["calendar_date"].dt.month
    dates_df["year"] = dates_df["calendar_date"].dt.year
    dates_df["day_of_week"] = dates_df["calendar_date"].dt.day_of_week + 1
    dates_df["day_of_year"] = dates_df["calendar_date"].dt.day_of_year
    dates_df["week_of_year"] = dates_df["calendar_date"].dt.isocalendar().week
    dates_df["quarter"] = dates_df["calendar_date"].dt.quarter
    dates_df["weekend"] = dates_df["calendar_date"].dt.weekday.apply(lambda x: 1 if x >= 5 else 0)
    dates_df["day_name"] = dates_df["calendar_date"].dt.day_name()
    dates_df["month_name"] = dates_df["calendar_date"].dt.month_name()
    dates_df["first_day_of_month"] = dates_df["calendar_date"].apply(lambda x: x.replace(day=1))
    dates_df["last_day_of_month"] = dates_df["calendar_date"].apply(lambda x: x.replace(day=x.days_in_month))

    return dates_df


def insert_dates(cursor: mysql.connector.cursor.MySQLCursor, table_name, dates_df: pd.DataFrame):
    print(f"Inserting dates...")
    df_cols = ["dim_date_key", "calendar_date", "day", "month", "year",
               "day_of_week", "day_of_year", "week_of_year",
               "quarter", "weekend", "day_name", "month_name",
               "first_day_of_month", "last_day_of_month"]
    
    # Build list of tuples containing values for insert
    insert_values: list = dates_df[df_cols].values.tolist()

    # Build column name/placeholder strings
    table_cols = df_cols
    insert_cols = ", ".join(table_cols)
    insert_placeholders = ", ".join(["%s"] * len(table_cols))

    insert_cmd = f"""
        INSERT INTO {table_name}
        (
            {insert_cols}
        )
        VALUES
        (
            {insert_placeholders}
        )
        """
    
    cursor.executemany(insert_cmd, insert_values)


def main():
    # Declare here to ensure except/finally work if connection fails
    conn = None

    try:
        config = init()
        conn = connect_to_db(config)
        cursor = conn.cursor()

        table_name = "dim_date"
        create_statement = generate_create_statement(table_name)

        create_table(cursor, table_name, create_statement)
        cleardown_table(cursor, table_name)
        dates_df = generate_dates()
        insert_dates(cursor, table_name, dates_df)

        conn.commit()
        print("dim_date populated.")

    except Exception as e:
        print(f"Error whilst generating dim_date: {e}")
        traceback.print_exc()
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()


if __name__ == '__main__':
    main()
