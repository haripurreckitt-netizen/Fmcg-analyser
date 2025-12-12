# config.py
from pathlib import Path

# === Base Directories and Paths ===
BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
DATA_RAW_DIR = DATA_DIR / "raw"
DB_PATH = DATA_DIR / "sales.db"

# === Source File Names ===
SALES_FILES = [
    "2024.xlsx",
    "2025.xlsx",
    "2025sep.xlsx"

    # Add more year files here as needed
]
CREDIT_FILE = "Credit_Balances.xlsx"
MARGIN_FILE = "tiles_margin.xlsx"

# ============================================================
# SALES DATA CONFIGURATION
# ============================================================
# These are the EXACT column names from your Excel files
SALES_COLS = [
    'Inv #',        # Invoice number
    'Dl. Date',     # Delivery date (we're NOT using O. Date)
    'Booker',       # Order booker name
    'Cust',         # Customer code
    'Client',       # Customer name
    'Product',      # Product name
    'Net.Qty',      # Net quantity sold
    'Net. Amnt',    # Net amount (revenue)
    'Company',      # Company name
    'Route'         # Route/Territory
]

# Map Excel columns to standardized database column names
SALES_RENAME = {
    'Inv #': 'invoice_number',
    'Dl. Date': 'delivery_date',
    'Booker': 'booker_name',
    'Cust': 'customer_code',
    'Client': 'customer_name',
    'Product': 'product_name',
    'Net.Qty': 'quantity',
    'Net. Amnt': 'amount',
    'Company': 'company',
    'Route': 'route'
}

# Define data types for validation and cleaning
SALES_DTYPES = {
    'invoice_number': 'string',
    'delivery_date': 'date',
    'booker_name': 'string',
    'customer_code': 'string',
    'customer_name': 'string',
    'product_name': 'string',
    'quantity': 'integer',
    'amount': 'integer',
    'company': 'string',
    'route': 'string'
}

# ============================================================
# CREDIT BALANCE DATA CONFIGURATION
# ============================================================
# Exact column names from Credit_Balances.xlsx
CREDIT_COLS = [
    'Code',              # Customer code
    'Balance',           # Outstanding balance
    'Last Invoice on'    # Date of last invoice
]

# Map to standardized names
CREDIT_RENAME = {
    'Code': 'customer_code',
    'Balance': 'balance',
    'Last Invoice on': 'last_invoice_date'
}

# Define data types
CREDIT_DTYPES = {
    'customer_code': 'string',
    'balance': 'integer',
    'last_invoice_date': 'date'
}

# ============================================================
# MARGIN DATA CONFIGURATION
# ============================================================
# Exact column names from tiles_margin.xlsx
MARGIN_COLS = [
    'Invoice #',    # Invoice number
    'Net',          # Net amount (should match sales amount)
    'Profit'        # Profit margin
]

# Map to standardized names
MARGIN_RENAME = {
    'Invoice #': 'invoice_number',
    'Net': 'amount_from_margin',  # Renamed to avoid conflicts during merge
    'Profit': 'profit'
}

# Define data types
MARGIN_DTYPES = {
    'invoice_number': 'string',
    'amount_from_margin': 'integer',
    'profit': 'integer'
}