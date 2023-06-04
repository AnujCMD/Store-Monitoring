import app as app
from pymongo import MongoClient

# MongoDB's connection details
SRV_URL = "mongodb+srv://jedirebels:anujqwerty32@cluster0.qr9uwh7.mongodb.net/your_database?retryWrites=true&w=majority"
MONGO_DB = "store_monitoring"


def connect_mongo():
    # Connect to MongoDB
    client = MongoClient(SRV_URL)
    db = client[MONGO_DB]
    return db
