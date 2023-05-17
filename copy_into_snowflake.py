import snowflake.connector
import configparser

parser = configparser.ConfigParser()
parser.read('pipeline.conf')
username = parser.get('snowflake_creds', 'username')
password = parser.get('snowflake_creds', 'password')
account_name = parser.get('snowflake_creds', 'account_name')

snow_con = snowflake.connector.connect(
    user=username,
    password=password,
    account=account_name,
    database='data_pipelines'
)

sql = """
COPY INTO PUBLIC.Orders
FROM @data_pipelines_book_stage
pattern='order_extract.csv'
"""

cur = snow_con.cursor()
cur.execute('USE ROLE SYSADMIN')
cur.execute('USE DATABASE data_pipelines;')
cur.execute('USE WAREHOUSE compute_wh')

cur.execute(sql)
cur.close()
