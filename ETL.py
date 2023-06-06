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







# CSV files into one file



# Create CSV from JSON




# Create JSON from CSV






