"""
Move nn056 look-up codes in nn056 stage table. 
For additional files in 056 schema, UNION selects from respective load tables.
"""
import os
from src.etl.TableCopier import TableCopier


def main():
    source_query = """
                    SELECT t1.code, t1.label, t1.source_file, t1.hesa_delivery
                    FROM load_hesa_22056_lookup_ethnicity t1
                    """    
    source_cols = ['code', 'label', 'source_file', 'hesa_delivery']
    target_table = 'stage_hesa_nn056_lookup_ethnicity'
    target_cols = ['code', 'label', 'source_file', 'hesa_delivery']

    script_name = os.path.basename(__file__)
    table_copier = TableCopier(source_query, source_cols, target_table, target_cols, script_name)
    table_copier.transfer_data()


if __name__ == '__main__':
    main()