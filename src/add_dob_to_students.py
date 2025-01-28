import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random

# Define the path to the original CSV file
csv_file_path = '../data/students.csv'

# Load the CSV file into a DataFrame
df = pd.read_csv(csv_file_path)

# Function to generate a random date of birth for an adult (18 years or older)
def generate_random_dob(start_date, end_date):
    time_between_dates = end_date - start_date
    days_between_dates = time_between_dates.days
    random_number_of_days = random.randrange(days_between_dates)
    random_date = start_date + timedelta(days=random_number_of_days)
    return random_date

# Define the date range for adults (18 years or older)
end_date = datetime.now() - timedelta(days=18*365)
start_date = end_date - timedelta(days=82*365)  # Assuming maximum age of 100 years

# Generate random dates of birth
dob_list = [generate_random_dob(start_date, end_date).strftime('%Y-%m-%d') for _ in range(len(df))]

# Introduce 5% missing values
num_missing = int(0.05 * len(dob_list))
missing_indices = random.sample(range(len(dob_list)), num_missing)
for idx in missing_indices:
    dob_list[idx] = np.nan

# Add the 'dob' column to the DataFrame
df['dob'] = dob_list

# Save the updated DataFrame back to the CSV file
new_csv_file_path = '../data/students_with_dob.csv'
df.to_csv(new_csv_file_path, index=False)

# Verify the new file
print(f"\nNew file '{new_csv_file_path}' created with the following columns:")
print(pd.read_csv(new_csv_file_path).head())