# src/q4_verify.py
import os
from pymongo import MongoClient
from dotenv import load_dotenv


# Load environment variables from .env file
load_dotenv()

uri = os.getenv("MONGO_URI")
if not uri:
    raise SystemExit("MONGO_URI not set in environment or .env")

client = MongoClient(uri, serverSelectionTimeoutMS=5000)
try:
    info = client.server_info()  # will raise if cannot connect
    print("Connected OK. Server info (partial):")
    print("  version:", info.get("version"))
    # Print the addresses the client is connected to
    print("  client.address:", client.address)
    db = client["online_retail_db"]
    print("transactions count:", db.transactions.count_documents({}))
    print("customers count:", db.customers.count_documents({}))
    print("\nSample transaction doc:")
    print(db.transactions.find_one())
    print("\nSample customer doc:")
    print(db.customers.find_one())
finally:
    client.close()
