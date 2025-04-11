### Creates a clone of students extract, with phone column missing
import pandas as pd
import os

script_dir = os.path.dirname(os.path.abspath(__file__))

# Load CSV file
csv_file_path = os.path.join(script_dir, '../data/students.csv')
df = pd.read_csv(csv_file_path)


# Check that the starting-point file is as expected
expected_columns = ['stu_id','phone','email','home_address','home_postcode','home_country','term_address','term_postcode','term_country','name']
missing_columns = []

for col in expected_columns:
    if col not in df.columns:
        missing_columns.append(col)

if missing_columns:
    raise ValueError(f"Columns missing from csv file: {missing_columns}")


# generate test file with missing phone number column
new_df = df.drop(columns=['phone'])
new_file_path = os.path.join(script_dir,  '../data/students_no_phone_col.csv')
new_df.to_csv(new_file_path, index=False)
