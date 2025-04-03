import os

from dotenv import load_dotenv
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi

load_dotenv()
CONN_MONGO = os.getenv("MONGO_URL")


class ConnectMongo:
    def connect(self):
        uri = CONN_MONGO
        client = MongoClient(uri, server_api=ServerApi("1"))
        return client
