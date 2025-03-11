"""
Merge all 22056/23056/etc student and demographic load tables into one nn056 stage table. 
"""
from etl.TableCopier import TableCopier

def main():
    source_query =  """
                    SELECT t1.student_guid, t1.first_names, t1.last_name, t1.phone, t1.email, t1.dob,
                        t1.home_addr, t1.home_postcode, t1.home_country, t1.term_addr, t1.term_postcode, t1.term_country,
                        t1.source_file, t1.hesa_delivery,
                        t2.ethnicity, t2.gender, t2.religion, t2.sexid, t2.sexort, t2.trans,
                        t2.ethnicity_grp1, t2.ethnicity_grp2, t2.ethnicity_grp3
                    FROM load_hesa_22056_students t1
                    INNER JOIN load_hesa_22056_student_demographics t2
                            ON t2.student_guid = t1.student_guid;
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

    table_copier = TableCopier(source_query, source_cols, target_table, target_cols)
    table_copier.transfer_data()


if __name__ == '__main__':
    main()