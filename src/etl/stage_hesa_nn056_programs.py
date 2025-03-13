"""
Get distinct program details from un-normalised load table, store in nn056 stage table. 
For additional files in 056 schema, UNION selects from respective load tables.
"""
from src.etl.TableCopier import TableCopier
import os

def main():
    source_query = """
                    SELECT DISTINCT t1.program_guid, t1.program_code, t1.program_name, t1.source_file, t1.hesa_delivery
                    FROM load_hesa_22056_student_programs t1
                    """    
    source_cols = ['program_guid', 'program_code', 'program_name', 'source_file', 'hesa_delivery']
    target_table = 'stage_hesa_nn056_programs'
    target_cols = ['program_guid', 'program_code', 'program_name', 'source_file', 'hesa_delivery']

    script_name = os.path.basename(__file__)
    table_copier = TableCopier(source_query, source_cols, target_table, target_cols, script_name)
    table_copier.transfer_data()


if __name__ == '__main__':
    main()