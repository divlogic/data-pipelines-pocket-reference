from logging import config
import boto3
import configparser
import psycopg2

parser = configparser.ConfigParser(interpolation=None)
parser.read('pipeline.conf')

dbname = parser.get('aws_creds', 'database')
user = parser.get('aws_creds', 'user')
password = parser.get('aws_creds', 'password')
host = parser.get('aws_creds', 'host')
port = parser.get('aws_creds', 'port')

# connect to the redshift cluster
rs_con = psycopg2.connect(
    dbname=dbname,
    user=user,
    password=password,
    host=host,
    port=port
)

# load the account_id and iam_role from the conf files
account_id = parser.get('aws_boto_credentials', 'account_id')
iam_role = parser.get('aws_creds', 'iam_role')
bucket_name = parser.get('aws_boto_credentials', 'bucket_name')

# run the copy command to load the file into redshift
file_path = ('s3://'
             + bucket_name
             + '/order_extract.csv')
role_string = parser.get('aws_creds', 'arn')

sql = "COPY public.Orders"
sql = sql + " from %s "
sql = sql + "iam_role %s;"

# create a cursor object and execute the copy

cursor = rs_con.cursor()
cursor.execute(sql, (file_path, role_string))

# close the cursor and commit the transaction
cursor.close()
rs_con.commit()

# close the connection
rs_con.close()
