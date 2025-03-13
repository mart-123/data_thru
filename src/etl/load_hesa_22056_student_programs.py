from src.etl.CsvTableCopier import CsvTableCopier

def main():
    """Set generic config and process-specific additional (filenames, etc)"""
    source_file = "student_programs_transformed.csv"
    target_table = "load_hesa_22056_student_programs"
    column_mappings = {"student_guid": "student_guid",
                    "email": "email",
                    "program_guid": "program_guid",
                    "program_code": "program_code",
                    "program_name": "program_name",
                    "enrol_date": "enrol_date",
                    "fees_paid": "fees_paid"}

    table_copier = CsvTableCopier("transformed", source_file, target_table, column_mappings)
    table_copier.transfer_data()


if __name__ == "__main__":
    main()