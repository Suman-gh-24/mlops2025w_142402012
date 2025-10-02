-- schema.sql
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
