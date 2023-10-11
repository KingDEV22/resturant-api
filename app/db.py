from pymongo import MongoClient, ASCENDING
from pymongo.server_api import ServerApi
import pandas as pd
# from app.constant import DB_URL
from pymongo.errors import PyMongoError
import logging

DB_NAME = 'resturant_details'
DB_URL = "mongodb://test:test123@localhost:27017/restaurant_details?authSource=admin"

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


def connect_db(client):
    db = client[DB_NAME]
    return db


def close_connection(client):
    try:
        client.close()
        logging.info("Successfully closed client")
    except PyMongoError as e:
        logging.error(repr(e))


client = connect()
db = connect_db(client=client)
Store_Status = db['store_status']
Store_Status.create_index([('store_id', ASCENDING)])
Store_Working_Hour = db['store_hours']
Store_Working_Hour.create_index([('store_id', ASCENDING)])
Store_TimeZone = db['store_timezone']
Store_TimeZone.create_index([('store_id', ASCENDING)])
