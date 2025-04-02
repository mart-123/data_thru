"""
Get student/program links from load table, store in nn056 stage table.
For additional files in 056 schema, UNION selects from respective load tables.

NOTE: Run AFTER stage_hesa_nn056_students.py and stage_hesa_nn056_programs.py
to ensure dependencies are satisfied.
"""
from src.etl.core.TableCopier import TableCopier
import os

def main():
    source_query = """
                SELECT 
                    t1.student_guid, 
                    t1.program_guid, 
                    t1.enrol_date,
                    t1.fees_paid, 
                    t1.source_file, 
                    t1.hesa_delivery
                FROM 
                    load_hesa_22056_student_programs t1
                JOIN 
                    stage_hesa_nn056_students s1
                    ON s1.student_guid = t1.student_guid
                    AND s1.hesa_delivery = t1.hesa_delivery
                JOIN 
                    stage_hesa_nn056_programs p1
                    ON p1.program_guid = t1.program_guid
                    AND p1.hesa_delivery = t1.hesa_delivery 
                
                UNION
                
                SELECT 
                    t2.student_guid, 
                    t2.program_guid, 
                    t2.enrol_date,
                    t2.fees_paid, 
                    t2.source_file, 
                    t2.hesa_delivery
                FROM 
                    load_hesa_23056_student_programs t2
                JOIN 
                    stage_hesa_nn056_students s2
                    ON s2.student_guid = t2.student_guid
                    AND s2.hesa_delivery = t2.hesa_delivery
                JOIN 
                    stage_hesa_nn056_programs p2
                    ON p2.program_guid = t2.program_guid
                    AND p2.hesa_delivery = t2.hesa_delivery
            """
    source_cols = ['student_guid', 'program_guid', 'enrol_date', 'fees_paid', 'source_file', 'hesa_delivery']
    target_table = 'stage_hesa_nn056_student_programs'
    target_cols = ['student_guid', 'program_guid', 'enrol_date', 'fees_paid', 'source_file', 'hesa_delivery']

    script_name = os.path.basename(__file__)
    table_copier = TableCopier(source_query, source_cols, target_table, target_cols, script_name)
    table_copier.transfer_data()


if __name__ == '__main__':
    main()