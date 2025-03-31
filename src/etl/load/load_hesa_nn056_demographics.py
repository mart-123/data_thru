import os
import sys
from src.etl.core.CsvTableCopier import CsvTableCopier

def main():
    """Set generic config and process-specific additional (filenames, etc)"""
    delivery_code = sys.argv[1]
    source_file = f"hesa_{delivery_code}_demographics_transformed.csv"
    target_table = f"load_hesa_{delivery_code}_student_demographics"
    column_mappings = {
        "student_guid": "student_guid",
        "ethnicity": "ethnicity",
        "gender": "gender",
        "religion": "religion",
        "sexid": "sexid",
        "sexort": "sexort",
        "trans": "trans",
        "ethnicity_grp1": "ethnicity_grp1",
        "ethnicity_grp2": "ethnicity_grp2",
        "ethnicity_grp3": "ethnicity_grp3"
    }

    script_name = os.path.basename(__file__)
    table_copier = CsvTableCopier("transformed", source_file, target_table, column_mappings, script_name)
    table_copier.transfer_data()


if __name__ == "__main__":
    main()
