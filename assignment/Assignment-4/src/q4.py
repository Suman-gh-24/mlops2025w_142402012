# src/q4.py
import os
import sqlite3
from pathlib import Path
from pymongo import MongoClient, errors as mongo_errors
from dotenv import load_dotenv
load_dotenv()

# Configuration
SQLITE_DB = "online_retail.db"
DB_NAME = "online_retail_db"
MONGO_URI = os.environ.get("MONGO_URI")
if not MONGO_URI:
    raise SystemExit("MONGO_URI environment variable not set. Add it to your .env or export it in the shell.")

# Connect to MongoDB with connection pooling
client = MongoClient(
    MONGO_URI,
    maxPoolSize=50,
    minPoolSize=10
)

# Delete database if it already exists
if DB_NAME in client.list_database_names():
    client.drop_database(DB_NAME)
    print(f"Deleted existing database: {DB_NAME}")

db = client[DB_NAME]

# Fetch data from SQLite
conn = sqlite3.connect(SQLITE_DB)
conn.row_factory = sqlite3.Row
cur = conn.cursor()

query = """
SELECT 
    il.InvoiceLineID,
    il.InvoiceNo,
    il.StockCode,
    il.Quantity,
    il.UnitPrice,
    i.InvoiceDate,
    i.CustomerID,
    i.InvoiceCancelled,
    p.Description,
    co.CountryName
FROM InvoiceLines il
JOIN Invoices i ON il.InvoiceNo = i.InvoiceNo
JOIN Products p ON il.StockCode = p.StockCode
LEFT JOIN Customers c ON i.CustomerID = c.CustomerID
LEFT JOIN Countries co ON c.CountryID = co.CountryID
"""

cur.execute(query)
data = [dict(row) for row in cur.fetchall()]
conn.close()

print(f"Fetched {len(data)} records from SQLite")

# ============================================
# APPROACH 1: Transaction-Centric
# ============================================
transactions = {}

for row in data:
    invoice_no = row['InvoiceNo']
    
    if invoice_no not in transactions:
        transactions[invoice_no] = {
            'invoice_no': invoice_no,
            'invoice_date': row['InvoiceDate'],
            'customer_id': row['CustomerID'],
            'country': row['CountryName'],
            'cancelled': bool(row['InvoiceCancelled']),
            'line_items': [],
            'total_amount': 0.0
        }
    
    line_item = {
        'line_id': row['InvoiceLineID'],
        'stock_code': row['StockCode'],
        'description': row['Description'],
        'quantity': row['Quantity'],
        'unit_price': row['UnitPrice'],
        'line_total': row['Quantity'] * row['UnitPrice']
    }
    
    transactions[invoice_no]['line_items'].append(line_item)
    transactions[invoice_no]['total_amount'] += line_item['line_total']

# Insert transaction-centric documents
transaction_collection = db.transactions
transaction_collection.delete_many({})
transaction_collection.insert_many(list(transactions.values()))

print(f"Inserted {len(transactions)} transaction documents")

# ============================================
# APPROACH 2: Customer-Centric
# ============================================
customers = {}

for row in data:
    customer_id = row['CustomerID']
    
    if not customer_id:
        continue
    
    if customer_id not in customers:
        customers[customer_id] = {
            'customer_id': customer_id,
            'country': row['CountryName'],
            'transactions': [],
            'total_spent': 0.0,
            'total_items_purchased': 0
        }
    
    invoice_no = row['InvoiceNo']
    transaction = None
    for t in customers[customer_id]['transactions']:
        if t['invoice_no'] == invoice_no:
            transaction = t
            break
    
    if not transaction:
        transaction = {
            'invoice_no': invoice_no,
            'invoice_date': row['InvoiceDate'],
            'cancelled': bool(row['InvoiceCancelled']),
            'items': []
        }
        customers[customer_id]['transactions'].append(transaction)
    
    item = {
        'stock_code': row['StockCode'],
        'description': row['Description'],
        'quantity': row['Quantity'],
        'unit_price': row['UnitPrice'],
        'total': row['Quantity'] * row['UnitPrice']
    }
    
    transaction['items'].append(item)
    customers[customer_id]['total_spent'] += item['total']
    customers[customer_id]['total_items_purchased'] += abs(row['Quantity'])

# Insert customer-centric documents
customer_collection = db.customers
customer_collection.delete_many({})
customer_collection.insert_many(list(customers.values()))

print(f"Inserted {len(customers)} customer documents")
