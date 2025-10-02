# src/q2_verify.py
from pymongo import MongoClient
from pprint import pprint

MONGO_URI = "mongodb://localhost:27017/"   # or Atlas URI if using Atlas
client = MongoClient(MONGO_URI)
db = client["online_retail_db"]

print("transactions:", db.transactions.count_documents({}))
print("customers:", db.customers.count_documents({}))

print("\nSAMPLE transaction:")
pprint(db.transactions.find_one())

print("\nSAMPLE customer:")
pprint(db.customers.find_one())

client.close()
