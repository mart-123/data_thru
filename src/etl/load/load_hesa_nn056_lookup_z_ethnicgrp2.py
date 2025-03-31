import os
import sys
from src.etl.core.CsvTableCopier import CsvTableCopier

def main():
    """Set generic config and process-specific additional (filenames, etc)"""
    delivery_code = sys.argv[1]
    source_file = f"hesa_{delivery_code}_Z_ETHNICGRP2.csv"
    target_table = f"load_hesa_{delivery_code}_lookup_z_ethnicgrp2"
    column_mappings = {"Code": "code", "Label": "label"}

    script_name = os.path.basename(__file__)
    table_copier = CsvTableCopier("lookup", source_file, target_table, column_mappings, script_name)
    table_copier.transfer_data()


if __name__ == "__main__":
    main()