import os
import sys
from ingest.core.CsvTableCopier import CsvTableCopier
from ingest.core.etl_utils import get_config

def main():
    """Set generic config and process-specific additional (filenames, etc)"""
    delivery_code = sys.argv[1]
    lookup_name = sys.argv[2]
    config = get_config()

    source_file = f"hesa_{delivery_code}_lookup_{lookup_name}.csv"
    source_path = os.path.join(config['deliveries_dir'], delivery_code, source_file)

    target_table = f"load_hesa_{delivery_code}_lookup_{lookup_name.lower()}"
    column_mappings = {"Code": "code", "Label": "label"}

    script_name = os.path.basename(__file__)
    table_copier = CsvTableCopier(source_path, target_table, column_mappings, script_name)
    table_copier.transfer_data()


if __name__ == "__main__":
    main()