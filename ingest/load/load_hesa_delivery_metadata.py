
"""
Script to load list of HESA delivery codes with
descriptions and other details.
"""
import os
from utils.data_platform_core import get_config, set_up_logging, is_valid_date
from ingest.core.CsvTableCopier import CsvTableCopier
import pandas as pd


def basic_validation(config: dict, file_path: str):
    """Validation is normally found in the extract scripts, but they are
    designed to filter out bad rows and continue. For static data the
    script should fail and stop the pipeline."""
    df = pd.read_csv(file_path, dtype=str)

    # Check for missing delivery codes
    blank_codes = df["delivery_code"].isna() | df["delivery_code"] == ""
    bad_rows = df[blank_codes]
    if not bad_rows.empty:
        raise ValueError("Blank delivery code(s) found in CSV file") 

    # Check for missing dates
    missing_dates = (df["delivery_received"].isna() | df["delivery_received"] == "" |
                     df["collection_sent"].isna() | df["collection_sent"] == "")
    bad_rows = df[missing_dates]
    if not bad_rows.empty:
        raise ValueError("Delivery or collection date(s) missing in CSV file")

    # Validate dates for yyyy-mm-dd format
    good_delivery_date_series = df["delivery_received"].apply(lambda x: is_valid_date(x))
    bad_rows = ~df[good_delivery_date_series]
    if not bad_rows.empty:
        raise ValueError("Delivery date(s) do not have YYYY-MM-DD format")

    good_collection_date_series = df["collection_sent"].apply(lambda x: is_valid_date(x))
    bad_rows = ~df[good_collection_date_series]
    if not bad_rows.empty:
        raise ValueError("Collection date(s) do not have YYYY-MM-DD format")

    return True


def main():
    """
    Get generic config and set process-specific details.
    (source file, destination table, column name mappings, etc).
    """
    # Fully qualified source file path
    config = get_config()
    source_file = "hesa_static_delivery_metadata.csv"
    source_path = os.path.join(config['static_dir'], source_file)

    # Validate file (as a simple)
    result = basic_validation(config, source_path)

    # Target table and column name mappings
    target_table = f"load_hesa_delivery_metadata"
    column_mappings = {
        "delivery_code": "delivery_code",
        "delivery_received": "delivery_received",
        "collection_ref": "collection_ref",
        "collection_sent": "collection_sent",
        "delivery_description": "delivery_description"
    }

    script_name = os.path.basename(__file__)
    table_copier = CsvTableCopier(source_path, target_table, column_mappings, script_name)
    table_copier.transfer_data()


if __name__ == "__main__":
    main()
