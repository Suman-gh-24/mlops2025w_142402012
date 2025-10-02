# src/fixed_load_q1.py
import sqlite3
import pandas as pd
from pathlib import Path

EXCEL_FILE = Path("data/Online Retail.xlsx")   # change if needed
DB_FILE = Path("online_retail.db")
MAX_ROWS = 1000

def safe_int(x, default=None):
    if pd.isna(x): return default
    try:
        return int(x)
    except Exception:
        try:
            return int(float(x))
        except Exception:
            return default

def safe_float(x, default=0.0):
    if pd.isna(x): return default
    try:
        return float(x)
    except Exception:
        return default

def safe_date_iso(x):
    try:
        dt = pd.to_datetime(x, errors='coerce')
        return dt.isoformat() if not pd.isna(dt) else None
    except Exception:
        return None

def main():
    print("Reading Excel:", EXCEL_FILE)
    df = pd.read_excel(EXCEL_FILE, engine="openpyxl")
    # normalize column names
    df.columns = df.columns.str.strip()

    conn = sqlite3.connect(DB_FILE)
    conn.execute("PRAGMA foreign_keys = ON;")   # enable FK enforcement
    cur = conn.cursor()

    # drop and create tables (same schema as friend's, but keep for clarity)
    cur.executescript("""
    DROP TABLE IF EXISTS Countries;
    DROP TABLE IF EXISTS Customers;
    DROP TABLE IF EXISTS Products;
    DROP TABLE IF EXISTS Invoices;
    DROP TABLE IF EXISTS InvoiceLines;

    CREATE TABLE Countries (
        CountryID INTEGER PRIMARY KEY AUTOINCREMENT,
        CountryName TEXT UNIQUE
    );

    CREATE TABLE Customers (
        CustomerID INTEGER PRIMARY KEY,
        CountryID INTEGER,
        FOREIGN KEY (CountryID) REFERENCES Countries(CountryID)
    );

    CREATE TABLE Products (
        StockCode TEXT PRIMARY KEY,
        Description TEXT
    );

    CREATE TABLE Invoices (
        InvoiceNo TEXT PRIMARY KEY,
        InvoiceDate TEXT,
        CustomerID INTEGER,
        InvoiceCancelled INTEGER,
        FOREIGN KEY (CustomerID) REFERENCES Customers(CustomerID)
    );

    CREATE TABLE InvoiceLines (
        InvoiceLineID INTEGER PRIMARY KEY AUTOINCREMENT,
        InvoiceNo TEXT,
        StockCode TEXT,
        Quantity INTEGER,
        UnitPrice REAL,
        LineTotal REAL,
        FOREIGN KEY (InvoiceNo) REFERENCES Invoices(InvoiceNo),
        FOREIGN KEY (StockCode) REFERENCES Products(StockCode)
    );
    """)
    conn.commit()

    inserted = 0
    for _, row in df.iterrows():
        if inserted >= MAX_ROWS:
            break

        invoice_no = str(row.get("InvoiceNo", "")).strip()
        stock_code = str(row.get("StockCode", "")).strip()
        desc = row.get("Description", "")
        qty = safe_int(row.get("Quantity"), default=0)
        invoice_date = safe_date_iso(row.get("InvoiceDate"))
        price = safe_float(row.get("UnitPrice"), default=0.0)
        cust_raw = row.get("CustomerID")
        cust_id = safe_int(cust_raw, default=None)
        country = str(row.get("Country", "")).strip()

        # Countries
        if country == "" or country.lower() == "nan":
            country = None
        if country:
            cur.execute("INSERT OR IGNORE INTO Countries (CountryName) VALUES (?)", (country,))
            cur.execute("SELECT CountryID FROM Countries WHERE CountryName = ?", (country,))
            country_row = cur.fetchone()
            country_id = country_row[0] if country_row else None
        else:
            country_id = None

        # Customers (only if we have a non-None cust_id)
        if cust_id is not None:
            cur.execute("INSERT OR IGNORE INTO Customers (CustomerID, CountryID) VALUES (?, ?)",
                        (cust_id, country_id))

        # Products
        cur.execute("INSERT OR IGNORE INTO Products (StockCode, Description) VALUES (?, ?)",
                    (stock_code, str(desc)))

        # Invoices (mark cancelled if InvoiceNo starts with 'C' OR qty < 0)
        cancelled = 1 if (invoice_no.startswith("C") or (qty < 0)) else 0
        cur.execute("INSERT OR IGNORE INTO Invoices (InvoiceNo, InvoiceDate, CustomerID, InvoiceCancelled) VALUES (?, ?, ?, ?)",
                    (invoice_no, invoice_date, cust_id, cancelled))

        # InvoiceLines
        line_total = qty * price
        cur.execute("""INSERT INTO InvoiceLines (InvoiceNo, StockCode, Quantity, UnitPrice, LineTotal)
                       VALUES (?, ?, ?, ?, ?)""",
                    (invoice_no, stock_code, qty, price, line_total))

        inserted += 1

    conn.commit()

    # Basic verification output
    cur.execute("SELECT COUNT(*) FROM InvoiceLines")
    total_lines = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM Invoices")
    total_invoices = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM Products")
    total_products = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM Customers")
    total_customers = cur.fetchone()[0]

    print(f"Inserted invoice lines (loop count): {inserted}")
    print(f"InvoiceLines rows in DB: {total_lines}")
    print(f"Invoices: {total_invoices}, Products: {total_products}, Customers: {total_customers}")

    conn.close()

if __name__ == "__main__":
    main()
