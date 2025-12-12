# product_loader.py
import pandas as pd
import sqlite3
from pathlib import Path
import config

# --- Configuration ---
DB_PATH = config.DB_PATH
INVENTORY_FILE_PATH = config.DATA_RAW_DIR / "inventory.xlsx"
PRODUCTS_TABLE_NAME = "products"

def update_products():
    """
    Reads the inventory.xlsx file, including a new 'Status' column,
    and saves it to the 'products' table.
    """
    print("\n" + "="*60)
    print("🚀 STARTING PRODUCT & INVENTORY LOADER".center(60))
    print("="*60)

    # 1. Load the inventory data
    try:
        print(f"📂 Loading inventory data from: {INVENTORY_FILE_PATH.name}")
        df = pd.read_excel(INVENTORY_FILE_PATH, engine='openpyxl')
        
        required_cols = {'Name', 'netpcs'}
        if not required_cols.issubset(df.columns):
            missing = required_cols - set(df.columns)
            print(f"❌ ERROR: Missing required columns in inventory file: {', '.join(missing)}")
            return False

    except FileNotFoundError:
        print(f"❌ ERROR: Inventory file not found at '{INVENTORY_FILE_PATH}'")
        return False
    except Exception as e:
        print(f"❌ ERROR: Could not read inventory file. Reason: {e}")
        return False

    # 2. Clean and process the data
    print("🧹 Cleaning and processing product data...")
    
    # Standardize column names
    rename_map = {'Name': 'product_name', 'netpcs': 'stock_quantity'}
    # Check if 'Status' column exists and rename it
    if 'Status' in df.columns:
        rename_map['Status'] = 'status'
        
    df = df.rename(columns=rename_map)

    # Select only the columns we need
    cols_to_keep = ['product_name', 'stock_quantity']
    if 'status' in df.columns:
        cols_to_keep.append('status')
        
    df = df[cols_to_keep]

    # Clean data types
    df['stock_quantity'] = pd.to_numeric(df['stock_quantity'], errors='coerce').fillna(0).astype(int)
    df['product_name'] = df['product_name'].astype(str).str.strip()
    df.dropna(subset=['product_name'], inplace=True)
    df = df[df['product_name'].str.lower() != 'nan']

    # --- NEW LOGIC for STATUS ---
    if 'status' in df.columns:
        # If user provides a status, use it. Fill any blanks with 'Active'.
        print("   ├─ Found 'Status' column in Excel file.")
        df['status'] = df['status'].str.strip().fillna('Active')
    else:
        # Fallback for old files: determine status based on stock.
        print("   ├─ 'Status' column not found. Determining status based on stock quantity.")
        df['status'] = df['stock_quantity'].apply(lambda x: 'Active' if x > 0 else 'Out of Stock')

    # Remove duplicates
    df.drop_duplicates(subset=['product_name'], keep='last', inplace=True)
    
    print(f"   ├─ Processed {len(df)} unique products.")
    print(f"   ├─ Active products: {len(df[df['status'] == 'Active'])}")
    print(f"   ├─ Discontinued products: {len(df[df['status'] == 'Discontinued'])}")
    print(f"   └─ Out of Stock products: {len(df[df['status'] == 'Out of Stock'])}")

    # 3. Save to the database
    try:
        print(f"\n💾 Connecting to database: {DB_PATH}")
        conn = sqlite3.connect(DB_PATH)
        df.to_sql(PRODUCTS_TABLE_NAME, conn, if_exists='replace', index=False)
        conn.close()
        print("\n✅ SUCCESS! Product inventory has been updated in the database.")
        print("="*60 + "\n")
        return True
    except Exception as e:
        print(f"❌ DATABASE ERROR: Could not save product data. Reason: {e}")
        return False

if __name__ == "__main__":
    update_products()
