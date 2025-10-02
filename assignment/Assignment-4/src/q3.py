# q3.py
"""
Q3: CRUD performance comparison between transaction-centric and customer-centric models.

Usage:
    python src/q3_crud_compare.py

Config:
    Edit MONGO_URI to point to your MongoDB (local or Atlas) if needed.
"""
import time
import uuid
import random
import sys
from pprint import pprint
from pymongo import MongoClient, errors as mongo_errors

# ---------- CONFIG ----------
MONGO_URI = "mongodb://localhost:27017/"   # change to Atlas URI if needed
DB_NAME = "online_retail_db"
REPEAT = 30          # how many operations to average (adjustable)
WARMUP = 5           # warmup iterations before timing
# ----------------------------

def now():
    return time.perf_counter()

def connect(uri, opts=None):
    opts = opts or {}
    try:
        client = MongoClient(uri, maxPoolSize=50, minPoolSize=10, serverSelectionTimeoutMS=5000, **opts)
        client.admin.command('ping')  # validate connection
        return client
    except Exception as e:
        print("Failed to connect to MongoDB:", e)
        raise

def sample_invoice_nos(db, limit=100):
    # get up to limit invoice_nos from transactions
    docs = list(db.transactions.find({}, {"invoice_no":1}).limit(limit))
    return [d["invoice_no"] for d in docs]

def sample_customer_ids(db, limit=100):
    docs = list(db.customers.find({}, {"customer_id":1}).limit(limit))
    return [d["customer_id"] for d in docs]

def measure(fn, repeat=REPEAT, warmup=WARMUP):
    # warmup
    for _ in range(warmup):
        fn()
    times=[]
    for _ in range(repeat):
        t0=now()
        fn()
        t1=now()
        times.append(t1-t0)
    # return average (ms) and raw list
    avg_ms = (sum(times)/len(times))*1000.0
    return avg_ms, times

def main():
    client = connect(MONGO_URI)
    db = client[DB_NAME]

    # make sure collections exist and have data
    tx_count = db.transactions.count_documents({})
    cust_count = db.customers.count_documents({})
    if tx_count == 0 or cust_count == 0:
        print("collections empty or missing. Make sure Q2 is run first to populate data.")
        print("transactions:", tx_count, "customers:", cust_count)
        client.close()
        sys.exit(1)

    print("Collections ready. transactions:", tx_count, "customers:", cust_count)

    # sample some existing keys to operate on
    invoice_pool = sample_invoice_nos(db, limit=200)
    customer_pool = sample_customer_ids(db, limit=200)
    if not invoice_pool or not customer_pool:
        print("Not enough sample keys. Aborting.")
        client.close()
        sys.exit(1)

    # pick a random existing invoice & customer for read/update/delete tests
    rand_invoice = random.choice(invoice_pool)
    rand_customer = random.choice(customer_pool)

    print("Sample invoice:", rand_invoice, "Sample customer:", rand_customer)

    # ---------- Define operations ----------
    # Transaction-centric operations
    def tx_create():
        # create a small transaction doc
        new_invoice = f"Q3-TX-{uuid.uuid4().hex[:8]}"
        doc = {
            "invoice_no": new_invoice,
            "invoice_date": "2025-10-01T00:00:00",
            "customer_id": rand_customer,
            "country": "Testland",
            "cancelled": False,
            "line_items": [{"line_id":1,"stock_code":"TEST","description":"test","quantity":1,"unit_price":1.0,"line_total":1.0}],
            "total_amount": 1.0
        }
        db.transactions.insert_one(doc)
        # cleanup immediately for fairness
        db.transactions.delete_one({"invoice_no": new_invoice})

    def tx_read():
        db.transactions.find_one({"invoice_no": rand_invoice})

    def tx_update():
        # update customer country across all invoices for a customer (simulate customer-level update)
        db.transactions.update_many({"customer_id": rand_customer}, {"$set":{"country":"UpdLand"}})

    def tx_delete():
        # create and then delete a small doc to measure delete cost
        new_invoice = f"Q3-TX-{uuid.uuid4().hex[:8]}"
        db.transactions.insert_one({"invoice_no": new_invoice, "invoice_date":"2025-10-01T00:00:00", "line_items":[], "total_amount":0.0})
        db.transactions.delete_one({"invoice_no": new_invoice})

    # Customer-centric operations
    def cust_create():
        # create by pushing a new transaction into an existing customer
        new_invoice = f"Q3-CUST-TX-{uuid.uuid4().hex[:8]}"
        tx = {"invoice_no": new_invoice, "invoice_date":"2025-10-01T00:00:00", "cancelled":False, "items":[{"stock_code":"TEST","quantity":1,"unit_price":1.0,"total":1.0}]}
        db.customers.update_one({"customer_id": rand_customer}, {"$push":{"transactions": tx}})
        # cleanup: pull it back
        db.customers.update_one({"customer_id": rand_customer}, {"$pull":{"transactions": {"invoice_no": new_invoice}}})

    def cust_read():
        # find nested transaction by invoice_no using projection
        db.customers.find_one({"customer_id": rand_customer, "transactions.invoice_no": rand_invoice}, {"transactions.$":1})

    def cust_update():
        # update customer country field (single document write)
        db.customers.update_one({"customer_id": rand_customer}, {"$set":{"country":"UpdLand"}})

    def cust_delete():
        # delete (pull) a transaction from a customer's transactions array
        new_invoice = f"Q3-CUST-TX-{uuid.uuid4().hex[:8]}"
        tx = {"invoice_no": new_invoice, "invoice_date":"2025-10-01T00:00:00", "cancelled":False, "items":[]}
        db.customers.update_one({"customer_id": rand_customer}, {"$push":{"transactions": tx}})
        # measure pull
        db.customers.update_one({"customer_id": rand_customer}, {"$pull":{"transactions": {"invoice_no": new_invoice}}})

    # ---------- Timing ----------
    results = {}

    print("\nMeasuring CREATE ...")
    results['create_tx'] = measure(tx_create)
    results['create_cust'] = measure(cust_create)

    print("Measuring READ ...")
    results['read_tx'] = measure(tx_read)
    results['read_cust'] = measure(cust_read)

    print("Measuring UPDATE ...")
    results['update_tx'] = measure(tx_update)
    results['update_cust'] = measure(cust_update)

    print("Measuring DELETE ...")
    results['delete_tx'] = measure(tx_delete)
    results['delete_cust'] = measure(cust_delete)

    # ---------- Print nicely ----------
    def avg_ms(key):
        return results[key][0]

    print("\n=== Q3 CRUD performance summary (avg ms) ===")
    print(f"{'Operation':<12} {'Transaction-centric':>22} {'Customer-centric':>22}")
    print("-"*60)
    print(f"{'Create':<12} {avg_ms('create_tx'):>22.3f} {avg_ms('create_cust'):>22.3f}")
    print(f"{'Read':<12} {avg_ms('read_tx'):>22.3f} {avg_ms('read_cust'):>22.3f}")
    print(f"{'Update':<12} {avg_ms('update_tx'):>22.3f} {avg_ms('update_cust'):>22.3f}")
    print(f"{'Delete':<12} {avg_ms('delete_tx'):>22.3f} {avg_ms('delete_cust'):>22.3f}")

    # optional: print raw values or store to file (skipped)
    client.close()

if __name__ == "__main__":
    main()
