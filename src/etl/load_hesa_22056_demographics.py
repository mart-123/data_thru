from etl.CsvTableCopier import CsvTableCopier

def main():
    """Set generic config and process-specific additional (filenames, etc)"""
    source_file = "demographics_transformed.csv"
    target_table = "load_hesa_22056_student_demographics"
    column_mappings = {
        "student_guid": "student_guid",
        "ethnicity": "ethnicity",
        "gender": "gender",
        "religion": "religion",
        "sexid": "sexid",
        "sexort": "sexort",
        "trans": "trans",
        "ethnicity_grp1": "ethnicity_grp1",
        "ethnicity_grp2": "ethnicity_grp2",
        "ethnicity_grp3": "ethnicity_grp3"
    }

    table_copier = CsvTableCopier("transformed", source_file, target_table, column_mappings)
    table_copier.transfer_data()


if __name__ == "__main__":
    main()
