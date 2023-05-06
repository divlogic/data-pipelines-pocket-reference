from pymongo import MongoClient
import datetime
import configparser

# load the mongo_config values
parser = configparser.ConfigParser()
parser.read('pipeline.conf')

hostname = parser.get('mongo_config', 'hostname')
username = parser.get('mongo_config', 'username')
password = parser.get('mongo_config', 'password')
database_name = parser.get('mongo_config', 'database')
collection_name = parser.get('mongo_config', 'collection')

# altered the parameter because of an error
uri = ("mongodb://"
       + username
       + ":" + password
       + "@" + hostname
       + "/"
       + database_name
       + "?retryWrites=true&"
       + "w=majority&ssl=false&"
       + "authSource=admin"
       )
print(uri)

mongo_client = MongoClient(uri)

# connect to the db where the collection resides
mongo_db = mongo_client[database_name]

# choose the collection to query documents from
mongo_collection = mongo_db[collection_name]

event_1 = {
    "event_id": 1,
    "event_timestamp": datetime.datetime.today(),
    "event_name": "signup"
}
event_2 = {
    "event_id": 2,
    "event_timestamp": datetime.datetime.today(),
    "event_name": "pageview"
}

event_3 = {
    "event_id": 3,
    "event_timestamp": datetime.datetime.today(),
    "event_name": "login"
}

# insert the 3 documents

mongo_collection.insert_one(event_1)
mongo_collection.insert_one(event_2)
mongo_collection.insert_one(event_3)

mongo_client.close()
