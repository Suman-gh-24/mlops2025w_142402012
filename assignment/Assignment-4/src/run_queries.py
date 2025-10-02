# src/run_queries.py
import sqlite3
from pathlib import Path

DB_NAMES = ["online_retail.db", "IIT_PALAKKAD.db", "online_retail.db".lower()]  # common names we used

# find the DB file in project root
db = None
for name in DB_NAMES:
    p = Path(name)
    if p.exists():
        db = p
        break

if db is None:
    # fallback: find first .db file in cwd
    p = next(Path('.').glob('*.db'), None)
    if p:
        db = p

if db is None:
    print("No .db file found in project root. Run src\\q1.py first.")
    raise SystemExit(1)

print("Using DB:", db)
conn = sqlite3.connect(str(db))
cur = conn.cursor()

queries = [
    ("InvoiceLines count", "SELECT COUNT(*) FROM InvoiceLines;"),
    ("Sample Invoices (5)", "SELECT InvoiceNo, InvoiceDate, CustomerID, InvoiceCancelled FROM Invoices LIMIT 5;"),
    ("Top 10 products by qty", "SELECT StockCode, SUM(Quantity) AS qty_sold FROM InvoiceLines GROUP BY StockCode ORDER BY qty_sold DESC LIMIT 10;"),
    ("Sales by country (joined)", """
        SELECT COALESCE(c.CountryName, 'UNKNOWN') as Country, SUM(il.LineTotal) as total_sales
        FROM InvoiceLines il
        JOIN Invoices i ON il.InvoiceNo = i.InvoiceNo
        LEFT JOIN Customers cu ON i.CustomerID = cu.CustomerID
        LEFT JOIN Countries c ON cu.CountryID = c.CountryID
        GROUP BY c.CountryName
        ORDER BY total_sales DESC
        LIMIT 10;
    """)
]

for title, q in queries:
    print("\n---", title, "---")
    try:
        cur.execute(q)
        rows = cur.fetchall()
        for r in rows:
            print(r)
    except Exception as e:
        print("Query failed:", e)

conn.close()
