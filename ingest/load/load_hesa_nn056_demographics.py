import os
import sys
from utils.data_platform_core import get_config
from ingest.core.CsvTableCopier import CsvTableCopier

def main():
    """
    Get generic config and set process-specific details.
    (source file, destination table, column name mappings, etc).
    """
    delivery_code = sys.argv[1]

    # Fully qualified source file path
    config = get_config()
    source_file = f"hesa_{delivery_code}_demographics_transformed.csv"
    source_path = os.path.join(config['transformed_dir'], delivery_code, source_file)

    # Target table and column name mappings
    target_table = f"load_hesa_{delivery_code}_demographics"
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
    table_copier = CsvTableCopier(source_path, target_table, column_mappings, script_name)
    table_copier.transfer_data()


if __name__ == "__main__":
    main()
