# Perform a full extract of a demo orders table on my local machine.
# The book used mysql, I went with postgres.
# The book also uses a 'redshift' example, but I think that's
# a bit pricy for a learning exercise
# May require creating a new table like order_warehouse

import psycopg2
import csv
import boto3
import configparser

parser = configparser.ConfigParser()
parser.read('pipeline.conf')

host = parser.get('postgres_config', 'postgres_host')
port = parser.get('postgres_config', 'postgres_port')
database = parser.get('postgres_config', 'postgres_db')
user = parser.get('postgres_config', 'postgres_user')
password = parser.get('postgres_config', 'postgres_password')


conn = psycopg2.connect(dbname=database, user=user,
                        host=host, port=port, password=password)

if conn is None:
    print("Error connecting to the PostgreSQL database")
else:
    print("PostgreSQL connection established!")


max_query = """
SELECT COALESCE(
    MAX(lastupdated),
    '1900-01-01'
)
FROM orders;
"""


local_filename = 'order_extract.csv'

cursor = conn.cursor()

cursor.execute(max_query)
max_result = cursor.fetchone()

if max_result is None:
    print('There is no max result')
    last_updated_warehouse = '1900-01-01'
else:
    last_updated_warehouse = max_result[0]


conn.commit()

extract_query = """
SELECT * FROM orders
WHERE lastupdated > %s;
"""

cursor.execute(extract_query, (last_updated_warehouse,))

results = cursor.fetchall()

with open(local_filename, 'w') as fp:
    csv_w = csv.writer(fp, delimiter='|')
    csv_w.writerows(results)

fp.close()
cursor.close()
conn.close()


# load the aws_boto_credentials values
access_key = parser.get('aws_boto_credentials', 'access_key')
secret_key = parser.get('aws_boto_credentials', 'secret_key')
bucket_name = parser.get('aws_boto_credentials', 'bucket_name')

s3 = boto3.client(
    's3',
    aws_access_key_id=access_key,
    aws_secret_access_key=secret_key
)

s3_file = local_filename

s3.upload_file(local_filename, bucket_name, s3_file)
