from pymongo import MongoClient, ASCENDING
from pymongo.server_api import ServerApi
import pandas as pd
from pymongo.errors import PyMongoError
import logging

# setting the db name and url
DB_NAME = 'resturant_details'
DB_URL = "mongodb://test:test123@mongodb:27017/restaurant_details?authSource=admin"

# Set up a logger with basic configuration
logging.basicConfig(level=logging.INFO)
def connect():
    try:
        client = MongoClient(DB_URL, serverSelectionTimeoutMS=5000)
        # Verify the connection by attempting to fetch the MongoDB server version
        client.admin.command('ping')
        logging.info("success fully connected to mongodb...")
    except PyMongoError as e:
        print(e)
    return client


# connection to the db and return the cursor
def connect_db(client):
    db = client[DB_NAME]
    return db


# to close the connection
def close_connection(client):
    try:
        client.close()
        logging.info("Successfully closed client")
    except PyMongoError as e:
        logging.error(repr(e))


client = connect()
db = connect_db(client=client)
Store_Status = db['store_status']
Store_Working_Hour = db['store_hours']
Store_TimeZone = db['store_timezone']

# setting the index for fast data retrieval
Store_Working_Hour.create_index([('store_id', ASCENDING)])
Store_Status.create_index([('store_id', ASCENDING)])
Store_TimeZone.create_index([('store_id', ASCENDING)])
