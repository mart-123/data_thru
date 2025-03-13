from src.etl.CsvTableCopier import CsvTableCopier

def main():
    """Set generic config and process-specific additional (filenames, etc)"""
    source_file = "hesa_22056_Z_ETHNICGRP3.csv"
    target_table = "load_hesa_22056_lookup_z_ethnicgrp3"
    column_mappings = {"Code": "code", "Label": "label"}

    table_copier = CsvTableCopier("lookup", source_file, target_table, column_mappings)
    table_copier.transfer_data()


if __name__ == "__main__":
    main()