# Functions to Extract, transform (many files into one) and load data back into s3 bucket under a different name


# JSON files into one file

def many2one_json():
    import boto3
    import json

    big_one = []

    s3 = boto3.resource('s3')   

    bucket = s3.Bucket('s3://data-eng-223-final-project/')

     # loop over all files in s3 folder
    for obj in bucket.objects.filter(Prefix='Talent/'):
    
    # check to see if the file is a json file
        if obj.key.endswith('.json'):
            key = obj.key
            body = obj.get()['Body'].read()
            json_data = json.loads(body)
            big_one.append(json_data)
            

    # send to s3 bucket
    s3 = boto3.client('s3')
    s3.put_object(Bucket = 'data-eng-223-final-project', Key='ONE_BIG_JSON.json', Body=json.dumps(big_one))

# read in json file and convert to dataframe
def json2df():
    import pandas as pd
    import boto3
    import json

    s3 = boto3.resource('s3')
    obj = s3.Object('data-eng-223-final-project', 'ONE_BIG_JSON.json')
    body = obj.get()['Body'].read()
    json_data = json.loads(body)
    df = pd.DataFrame(json_data)
    return df


# cleaning the json into normalised csv's
def clean_json():
    import pandas as pd
    import boto3
    import json

    s3 = boto3.resource('s3')
    obj = s3.Object('data-eng-223-final-project', 'ONE_BIG_JSON.json')
    body = obj.get()['Body'].read()
    json_data = json.loads(body)
    df = pd.DataFrame(json_data)
    #df = df.drop(columns=['_id', 'id'])

    # create csv's
    df = json2df()

#############################
    # tech_self_score column into seperate dataframe
    tech_skills = df['tech_self_score'].apply(pd.Series)

    # fill all nan values with 0
    tech_skills = tech_skills.fillna(0)

    # append skill_ to all column names
    tech_skills.columns = ['skill_' + str(col) for col in tech_skills.columns]

    # append to dataframe
    df = df.join(tech_skills)

#############################

    # extract the strengths from the json file#

    the_strengths = df['strengths'].apply(lambda x: x[0:])
    print(the_strengths)


    the_strengths = pd.DataFrame(the_strengths)

    s3 = boto3.client('s3')
    s3.put_object(Bucket = 'data-eng-223-final-project', Key='normal/strengths.csv', Body=the_strengths.to_csv())

#############################
    # extract the weaknesses from the json file

    the_weaknesses = df['weaknesses'].apply(lambda x: x[0:])
    print(the_weaknesses)

    s3 = boto3.client('s3')
    s3.put_object(Bucket = 'data-eng-223-final-project', Key='normal/weaknesses.csv', Body=the_weaknesses.to_csv())

#############################

# create dataframe without tech skills, strengths, and weaknesses from the dataframe

    the_cols = df.columns.tolist()

    
    # remove the columns that are not needed
    the_cols.pop(the_cols.index('tech_self_score'))
    the_cols.pop(the_cols.index('strengths'))
    the_cols.pop(the_cols.index('weaknesses'))

    s3 = boto3.client('s3')
    s3.put_object(Bucket = 'data-eng-223-final-project', Key='normal/JSON_data.csv', Body=df.to_csv())



# CSV files into one file

###############################################################

# Converts Business CSV files to single pandas dataframe

import boto3
import os
import csv

# Connect to boto3
s3_client = boto3.client('s3')


def get_csv(Folder):

    # Converts Business CSV files to single pandas dataframe 

    # List to store the data from each CSV file
    data = []

    # List all objects in the "Academy" directory
    response = s3_client.list_objects_v2(Bucket='data-eng-223-final-project', Prefix='Academy/')

    # Iterate over each object in the directory
    for obj in response['Contents']:
        file_key = obj['Key']
        if file_key.startswith(Folder):
        
            # Download the CSV file from S3
            s3_client.download_file('data-eng-223-final-project', file_key, '/tmp/temp.csv')
        
            # Open the downloaded CSV file and read its contents
            with open('/tmp/temp.csv', 'r') as csv_file:
                csv_reader = csv.reader(csv_file)
                header = next(csv_reader)  # Get the header row
            
                # Read the data row by row and append it to the data list
                for row in csv_reader:
                    data.append(row)

            # Delete the temporary downloaded file
            os.remove('/tmp/temp.csv')

    # Create a DataFrame from the collected data
    df = pd.DataFrame(data, columns=header)

    # Return the DataFrame
    return df 

Folder = 'Academy/Business'

Business_df = get_csv(Folder)

Business_df.head()

##################################################################################

# This function combines CSV files and reformats the data to a normalised format

import pandas as pd
import os

def create_combined_csv(directory):

    # This function combines CSV files and reformats the data to a normalised format

    # Initialize an empty DataFrame to store the combined data
    combined_df = pd.DataFrame()

    # Iterate over the files in the directory
    for file_name in os.listdir(directory):
        if file_name.endswith('.csv'):
            # Construct the full file path
            file_path = os.path.join(directory, file_name)

            # Parse cohort and date from the file name
            split_name = file_name.split('_')
            cohort = split_name[0] + '_' + split_name[1]
            date = split_name[2].split('.')[0]

            # Read the CSV file
            df = pd.read_csv(file_path)

            # Check for length of course
            if 'Analytic_W10' in df.columns:
                weeks = 11
            elif 'Analytic_W9' in df.columns:
                weeks = 10
            else: 
                weeks = 9        

            for week in range (1, weeks):
                # Select the relevant columns for the current week
                week_df = df[['name', 'trainer',
                          f'Analytic_W{week}', f'Independent_W{week}',
                          f'Determined_W{week}', f'Professional_W{week}',
                          f'Studious_W{week}', f'Imaginative_W{week}']].copy()

                # Rename the columns
                week_df.columns = ['name', 'trainer',
                               'Analytic', 'Independent',
                               'Determined', 'Professional',
                               'Studious', 'Imaginative']

                # Add a 'week' column
                week_df['week'] = week

                # Add a 'cohort' column
                week_df['cohort'] = cohort

                # Add a 'date' column
                week_df['date'] = date

                # Append the current week DataFrame to the combined DataFrame
                combined_df = pd.concat([combined_df, week_df], ignore_index=True)

    # Remove rows with any missing values
    combined_df.dropna(inplace=True)

    return combined_df


# Define the path to the directory containing the CSV files
directory = 'Sparta'

df = create_combined_csv(directory)




