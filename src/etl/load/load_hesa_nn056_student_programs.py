import os
import sys
from src.etl.core.CsvTableCopier import CsvTableCopier

def main():
    """Set generic config and process-specific additional (filenames, etc)"""
    delivery_code = sys.argv[1]
    source_file = f"hesa_{delivery_code}_student_programs_transformed.csv"
    target_table = f"load_hesa_{delivery_code}_student_programs"
    column_mappings = {"student_guid": "student_guid",
                    "email": "email",
                    "program_guid": "program_guid",
                    "program_code": "program_code",
                    "program_name": "program_name",
                    "enrol_date": "enrol_date",
                    "fees_paid": "fees_paid"}

    script_name = os.path.basename(__file__)
    table_copier = CsvTableCopier("transformed", source_file, target_table, column_mappings, script_name)
    table_copier.transfer_data()


if __name__ == "__main__":
    main()