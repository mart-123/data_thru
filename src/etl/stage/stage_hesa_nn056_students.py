"""
Merge all 22056/23056/etc student and demographic load tables into one nn056 stage table. 
For additional files in 056 schema, UNION selects from respective load tables.
"""
from src.etl.core.TableCopier import TableCopier
import os

def main():
    source_query = """
                    SELECT t1.student_guid, t1.first_names, t1.last_name, t1.phone, t1.email, t1.dob,
                        t1.home_addr, t1.home_postcode, t1.home_country, t1.term_addr, t1.term_postcode, t1.term_country,
                        t1.source_file, t1.hesa_delivery,
                        t2.ethnicity, t2.gender, t2.religion, t2.sexid, t2.sexort, t2.trans,
                        t2.ethnicity_grp1, t2.ethnicity_grp2, t2.ethnicity_grp3
                    FROM load_hesa_22056_students t1
                    INNER JOIN load_hesa_22056_student_demographics t2
                            ON t2.student_guid = t1.student_guid
                    
                    UNION
                    
                    SELECT t3.student_guid, t3.first_names, t3.last_name, t3.phone, t3.email, t3.dob,
                        t3.home_addr, t3.home_postcode, t3.home_country, t3.term_addr, t3.term_postcode, t3.term_country,
                        t3.source_file, t3.hesa_delivery,
                        t4.ethnicity, t4.gender, t4.religion, t4.sexid, t4.sexort, t4.trans,
                        t4.ethnicity_grp1, t4.ethnicity_grp2, t4.ethnicity_grp3
                    FROM load_hesa_23056_students t3
                    INNER JOIN load_hesa_23056_student_demographics t4
                            ON t4.student_guid = t3.student_guid;
                    """
    source_cols = ['student_guid', 'first_names', 'last_name', 'phone', 'email', 'dob',
                    'home_addr', 'home_postcode', 'home_country', 'term_addr', 'term_postcode', 'term_country',
                    'source_file', 'hesa_delivery', 'ethnicity', 'gender', 'religion', 'sexid', 'sexort', 'trans',
                    'ethnicity_grp1', 'ethnicity_grp2', 'ethnicity_grp3']

    target_table = 'stage_hesa_nn056_students'

    target_cols = ['student_guid', 'first_names', 'last_name', 'phone', 'email', 'dob',
                    'home_addr', 'home_postcode', 'home_country', 'term_addr', 'term_postcode', 'term_country',
                    'source_file', 'hesa_delivery', 'ethnicity', 'gender', 'religion', 'sexid', 'sexort', 'trans',
                    'ethnicity_grp1', 'ethnicity_grp2', 'ethnicity_grp3']

    script_name = os.path.basename(__file__)
    table_copier = TableCopier(source_query, source_cols, target_table, target_cols, script_name)
    table_copier.transfer_data()


if __name__ == '__main__':
    main()