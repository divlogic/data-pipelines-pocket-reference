import boto3
import configparser
import psycopg2

# get db Redshift connection info
parser = configparser.ConfigParser(interpolation=None)
parser.read("pipeline.conf")
dbname = parser.get("aws_creds", "database")
user = parser.get("aws_creds", "user")
password = parser.get("aws_creds", "password")
host = parser.get("aws_creds", "host")
port = parser.get("aws_creds", "port")

# connect to the redshift cluster
rs_conn = psycopg2.connect(
    "dbname=" + dbname
    + " user=" + user
    + " password=" + password
    + " host=" + host
    + " port=" + port)

# load the account_id and iam_role from the conf files
parser = configparser.ConfigParser()
parser.read("pipeline.conf")
account_id = parser.get(
    "aws_boto_credentials",
    "account_id")
arn = parser.get("aws_creds", "arn")

# run the COPY command to ingest into Redshift
bucket_name = parser.get("aws_boto_credentials",
                         "bucket_name")
bucket_name = parser.get("aws_boto_credentials",
                         "bucket_name")
file_path = f"s3://{bucket_name}/dag_run_extract.csv"

sql = """COPY dag_run_history
        (id,dag_id,execution_date,
        state,run_id,external_trigger,
        end_date,start_date)"""
sql = sql + " from '%s' " % file_path
sql = sql + " iam_role '%s';" % arn

print(sql)

# create a cursor object and execute the COPY command
cur = rs_conn.cursor()
cur.execute(sql)

# close the cursor and commit the transaction
cur.close()
rs_conn.commit()

# close the connection
rs_conn.close()
