"""
Get student/program links from load table, store in nn056 stage table.
For additional files in 056 schema, UNION selects from respective load tables.
"""
from src.etl.TableCopier import TableCopier

def main():
    source_query = """SELECT t1.student_guid, t1.program_guid, t1.enrol_date,
                      t1.fees_paid, t1.source_file, t1.hesa_delivery
                      FROM load_hesa_22056_student_programs t1"""    
    source_cols = ['student_guid', 'program_guid', 'enrol_date', 'fees_paid', 'source_file', 'hesa_delivery']
    target_table = 'stage_hesa_nn056_student_programs'
    target_cols = ['student_guid', 'program_guid', 'enrol_date', 'fees_paid', 'source_file', 'hesa_delivery']

    table_copier = TableCopier(source_query, source_cols, target_table, target_cols)
    table_copier.transfer_data()


if __name__ == '__main__':
    main()