import os
import sys
from utils.data_platform_core import get_config
from ingest.core.CsvTableCopier import CsvTableCopier

def main():
    """Set generic config and process-specific additional (filenames, etc)"""
    delivery_code = sys.argv[1]

    # Fully qualified source file path
    config = get_config()
    source_file = f"hesa_{delivery_code}_students_transformed.csv"
    source_path = os.path.join(config['transformed_dir'], delivery_code, source_file)

    # Target table and column name mappings
    target_table = f"load_hesa_{delivery_code}_students"
    column_mappings = {"student_guid": "student_guid",
                        "first_names": "first_names",
                        "last_name": "last_name",
                        "dob": "dob",
                        "phone": "phone",
                        "email": "email",
                        "home_address": "home_addr",
                        "home_postcode": "home_postcode",
                        "home_country": "home_country",
                        "term_address": "term_addr",
                        "term_postcode": "term_postcode",
                        "term_country": "term_country"}

    script_name = os.path.basename(__file__)
    table_copier = CsvTableCopier(source_path, target_table, column_mappings, script_name)
    table_copier.transfer_data()


if __name__ == "__main__":
    main()