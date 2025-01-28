### Reads 'students' CSV, creates 'students_no_phone_col' CSV
### (with 'phone' col removed).
import pandas as pd
import os

script_dir = os.path.dirname(os.path.abspath(__file__))

# Load the CSV file into a DataFrame
csv_file_path = os.path.join(script_dir, '../data/students.csv')
df = pd.read_csv(csv_file_path)

# Check expected columns are present
expected_columns = ['stu_id','phone','email','home_address','home_postcode','home_country','term_address','term_postcode','term_country','name']
missing_columns = []

for col in expected_columns:
    if col not in df.columns:
        missing_columns.append(col)

if missing_columns:
    raise ValueError(f"Columns missing from csv file: {missing_columns}")

# remove phone number column
new_df = df.drop(columns=['phone'])

# write altered data to new csv file
new_file_path = os.path.join(script_dir,  '../data/students_no_phone_col.csv')
new_df.to_csv(new_file_path, index=False)
