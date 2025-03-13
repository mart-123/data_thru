import os
from src.etl.CsvTableCopier import CsvTableCopier

def main():
    """Set generic config and process-specific additional (filenames, etc)"""
    source_file = "hesa_22056_Z_ETHNICGRP2.csv"
    target_table = "load_hesa_22056_lookup_z_ethnicgrp2"
    column_mappings = {"Code": "code", "Label": "label"}

    script_name = os.path.basename(__file__)
    table_copier = CsvTableCopier("lookup", source_file, target_table, column_mappings, script_name)
    table_copier.transfer_data()


if __name__ == "__main__":
    main()