import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
import os
from src.etl_utils import get_config


def init():
    """Set generic config and process-specific additional (filenames, etc)"""
    config = get_config()

    # Process-specific config (typically filenames)
    config['input_path'] = os.path.join(config['extracts_dir'], 'students_extract.csv')
    config['output_path'] = os.path.join(config['extracts_dir'], 'student_demographics.csv')

    return config


# Function to generate random demographics
def generate_random_demographics():
    ethnicities_values = ['100','101','102','103','104','119','120','121','139','159','160','161','162','163']
    gender_values = ['01','02','98','99']
    religion_values = ['20','21','22','23','29','30','31','98','99']
    sexid_values = ['10','11','12','98','99']
    sexort_values = ['10','11','12','19','98','99']
    trans_values = ['01','02','98','99']

    ethicity = random.choice(ethnicities_values)
    gender = random.choice(gender_values)
    religion = random.choice(religion_values)
    sexid = random.choice(sexid_values)
    sexort = random.choice(sexort_values)
    trans = random.choice(trans_values)

    # Return the single set of demographic values in a dictionary
    return {'ethnicity': ethicity,
            'gender': gender,
            'religion': religion,
            'sexid': sexid,
            'sexort': sexort,
            'trans:': trans}



def main():
    config = init()
    df = pd.read_csv(config['input_path'])

    demographics = [generate_random_demographics() for _ in range(len(df))]

    # Convert 'demographics' list of dictionaries to dataframe
    demographics_df = pd.DataFrame(demographics)
    demographics_df = pd.concat([df['stu_id'], demographics_df], axis=1)

    # Save the updated DataFrame back to the CSV file
    demographics_df.to_csv(config['output_path'], index=False)

    # Verify the new file
    print(f"\nNew file '{config['output_path']}' created with the following columns:")
    print(pd.read_csv(config['output_path']).head())



if __name__ == '__main__':
    main()
