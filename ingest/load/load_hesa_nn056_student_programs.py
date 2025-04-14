import os
import sys
from utils.data_platform_core import get_config
from ingest.core.CsvTableCopier import CsvTableCopier

def main():
    """Set generic config and process-specific additional (filenames, etc)"""
    delivery_code = sys.argv[1]

    # Fully qualified source file path
    config = get_config()
    source_file = f"hesa_{delivery_code}_student_programs_transformed.csv"
    source_path = os.path.join(config['transformed_dir'], delivery_code, source_file)

    # Target table and column name mappings
    target_table = f"load_hesa_{delivery_code}_student_programs"
    column_mappings = {"student_guid": "student_guid",
                    "email": "email",
                    "program_guid": "program_guid",
                    "program_code": "program_code",
                    "program_name": "program_name",
                    "enrol_date": "enrol_date",
                    "fees_paid": "fees_paid"}

    script_name = os.path.basename(__file__)
    table_copier = CsvTableCopier(source_path, target_table, column_mappings, script_name)
    table_copier.transfer_data()


if __name__ == "__main__":
    main()