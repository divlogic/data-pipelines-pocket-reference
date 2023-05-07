import requests
import json
import configparser
import csv
import boto3

# Book api is deprecated, using one that's still up
api_response = requests.get('http://api.open-notify.org/astros.json')

# create a json object from the response content
response_json = json.loads(api_response.content)

all_passes = []
for response in response_json['people']:
    current_astronaut = []
    current_astronaut.append(response['craft'])
    current_astronaut.append(response['name'])
    all_passes.append(current_astronaut)
export_file = "astronaut_export_file.csv"

with open(export_file, 'w') as fp:
    csvw = csv.writer(fp, delimiter='|')
    csvw.writerows(all_passes)
fp.close()

# load the aws_boto_credentials_values
parser = configparser.ConfigParser()
parser.read('pipeline.conf')
access_key = parser.get('aws_boto_credentials', 'access_key')
secret_key = parser.get('aws_boto_credentials', 'secret_key')
bucket_name = parser.get('aws_boto_credentials', 'bucket_name')

s3 = boto3.client(
    's3',
    aws_access_key_id=access_key,
    aws_secret_access_key=secret_key
)

s3_file = export_file

s3.upload_file(export_file, bucket_name, s3_file)
