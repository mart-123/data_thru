import pandas as pd
import random
import os
from src.etl.core.etl_utils import get_config


def init():
    """Set generic config and process-specific additional (filenames, etc)"""
    config = get_config()

    # Process-specific config (typically filenames)
    config['input_path'] = os.path.join(config['extracts_dir'], 'students_extract.csv')
    config['output_path'] = os.path.join(config['extracts_dir'], 'demographics_extract.csv')

    return config


# Function to generate random demographics
def generate_random_demographics():
    ethnicity_values = ['103','120','160']    # 'asian - indian or british', 'black - african or british', 'white - british'
    ethnicity_grp1_values = ['01', '01', '02'] # 'bame', 'white'
    ethnicity_grp2_values = ['01', '02', '04'] # 'asian', 'black', 'white'
    ethnicity_grp3_values = ['03', '06', '10'] # 'asian - indian', 'black - african', 'white'
    gender_values = ['01','02','98','99']
    religion_values = ['20','21','22','23','29','30','31','98','99']
    sexid_values = ['10','11','12','98','99']
    sexort_values = ['10','11','12','19','98','99']
    trans_values = ['01','02','98','99']

    ethnicity_idx = random.randint(0, len(ethnicity_values) - 1)
    ethicity = ethnicity_values[ethnicity_idx]
    ethnicity_grp1 = ethnicity_grp1_values[ethnicity_idx]
    ethnicity_grp2 = ethnicity_grp2_values[ethnicity_idx]
    ethnicity_grp3 = ethnicity_grp3_values[ethnicity_idx]
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
            'trans': trans,
            'ethnicity_grp1': ethnicity_grp1,
            'ethnicity_grp2': ethnicity_grp2,
            'ethnicity_grp3': ethnicity_grp3
}


def main():
    config = init()
    # get student extract, as we will need student id to affix to each demographic row
    df = pd.read_csv(config['input_path'], dtype=str)

    # Build list containing a dictionary for each CSV record
    demographics = [generate_random_demographics() for _ in range(len(df))]

    # Convert 'demographics' list of dictionaries to dataframe
    demographics_df = pd.DataFrame(demographics, dtype=str)

    # Affix student id to start of each demographics record
    demographics_df = pd.concat([df['stu_id'], demographics_df], axis=1)

    # Save the updated DataFrame back to the CSV file
    demographics_df.to_csv(config['output_path'], index=False)

    # Verify the new file
    print(f"\nNew file '{config['output_path']}' created with the following columns:")
    print(pd.read_csv(config['output_path'], dtype=str).head())



if __name__ == '__main__':
    main()
