import os
from src.etl.core.CsvTableCopier import CsvTableCopier

def main():
    """Set generic config and process-specific additional (filenames, etc)"""
    source_file = "hesa_23056_RELIGION.csv"
    target_table = "load_hesa_23056_lookup_religion"
    column_mappings = {"Code": "code", "Label": "label"}

    script_name = os.path.basename(__file__)
    table_copier = CsvTableCopier("lookup", source_file, target_table, column_mappings, script_name)
    table_copier.transfer_data()


if __name__ == "__main__":
    main()