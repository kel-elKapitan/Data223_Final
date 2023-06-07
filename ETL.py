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
    tech_skills = pd.DataFrame(tech_skills)
    # append skill_ to all column names
    tech_skills.columns = ['skill_' + str(col) for col in tech_skills.columns]

    # append to dataframe
    df = df.join(tech_skills)

#############################
    # extract the strengths from the json file
    df5 = df.copy() 

    df5 = df5.explode("strengths")

    df5 = df5['strengths'].str.get_dummies()

    df5 = df5.groupby(level=0).sum()


    # rename the columns with the prefix weakness_
    df5.columns = ['strength_' + str(col) for col in df5.columns]

    # append to dataframe
    df = df.join(df5)

#############################
    # extract the weaknesses from the json file
    df6 = df.copy() 

    df6 = df6.explode("weaknesses")

    df6 = df6['weaknesses'].str.get_dummies()

    df6 = df6.groupby(level=0).sum()
    # rename the columns with the prefix weakness_
    df6.columns = ['weakness_' + str(col) for col in df6.columns]

    # append to dataframe
    df = df.join(df6)
    


    
#############################

# create dataframe without tech skills, strengths, and weaknesses from the dataframe

    the_cols = df.columns.tolist()

    
    # remove the columns that are not needed
    the_cols.pop(the_cols.index('tech_self_score'))
    the_cols.pop(the_cols.index('strengths'))
    the_cols.pop(the_cols.index('weaknesses'))
    df = df[the_cols]
    
    s3 = boto3.client('s3')
    s3.put_object(Bucket = 'data-eng-223-final-project', Key='all_TALENT_files_cleaned.csv', Body=df.to_csv())



# CSV files into one file



# Create CSV from JSON




# Create JSON from CSV






