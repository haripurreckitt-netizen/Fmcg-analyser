# data_loader.py
import pandas as pd
import numpy as np
import sqlite3
import config
import validator

def load_clean_and_merge_data():
    """
    CORRECTED DATA LOADING LOGIC
    
    Priority Order:
    1. Credit Balance file = Master customer list (SOURCE OF TRUTH for balances)
    2. Sales data = Aggregated and merged INTO credit data
    3. Margin data = Merged by invoice
    
    Rules:
    - ONE customer_code = ONE record (from credit file)
    - Balance from credit file is FINAL and ABSOLUTE
    - If customer has no sales, they still exist with their balance
    - Days since = calculated from latest delivery_date in sales_data
    """
    print("\n" + "="*60)
    print("STARTING DATA PROCESSING PIPELINE")
    print("="*60)

    # ========================================
    # STEP 1: Load CREDIT BALANCE DATA FIRST (Master List)
    # ========================================
    try:
        print(f"\n📂 Loading credit balance data (MASTER CUSTOMER LIST)...")
        credit_path = config.DATA_RAW_DIR / config.CREDIT_FILE
        
        # Load ALL columns, not just the ones in config (to get customer_name if it exists)
        credit_df = pd.read_excel(
            credit_path,
            engine='openpyxl'
        )
        
        # Check what columns exist and rename accordingly
        print(f"   ├─ Credit file columns: {list(credit_df.columns)}")
        
        # Apply renaming for columns that exist
        rename_map = {}
        for old_col, new_col in config.CREDIT_RENAME.items():
            if old_col in credit_df.columns:
                rename_map[old_col] = new_col
        
        credit_df = credit_df.rename(columns=rename_map)
        
        # Keep only the columns we need (if they exist)
        required_cols = ['customer_code', 'balance', 'last_invoice_date']
        optional_cols = ['customer_name', 'route']
        
        cols_to_keep = [col for col in required_cols + optional_cols if col in credit_df.columns]
        credit_df = credit_df[cols_to_keep]
        
        print(f"   ├─ Loaded {len(credit_df):,} customer records")

        # Clean credit data (EXCEPT customer_code - will standardize later)
        for col, dtype in config.CREDIT_DTYPES.items():
            if col not in credit_df.columns or col == 'customer_code':  # Skip customer_code
                continue
            if dtype == 'string':
                credit_df[col] = credit_df[col].apply(validator.clean_and_trim_string)
            elif dtype == 'integer':
                credit_df[col] = credit_df[col].apply(validator.clean_and_round_integer)
            elif dtype == 'date':
                credit_df[col] = credit_df[col].apply(validator.parse_date)

        # --- FIX: Invert balance sign for standard accounting ---
        if 'balance' in credit_df.columns:
            print("   ├─ Inverting balance sign (positive = they owe us, negative = we owe them)")
            credit_df['balance'] = credit_df['balance'] * -1
        # --- END FIX ---

        # CRITICAL: Ensure ONE record per customer (remove any duplicates, keep last)
        initial_count = len(credit_df)
        credit_df = credit_df.drop_duplicates(subset=['customer_code'], keep='last')

        duplicates_removed = initial_count - len(credit_df)

        if duplicates_removed > 0:
            print(f"   ├─ ⚠️  Removed {duplicates_removed} duplicate customer codes")
        
        print(f"   └─ ✅ Final master list: {len(credit_df):,} unique customers (before code standardization)")

    except FileNotFoundError:
        print(f"\n❌ ERROR: Credit balance file not found!")
        return None
    except Exception as e:
        print(f"\n❌ ERROR: Could not read credit file - {e}")
        import traceback
        traceback.print_exc()
        return None

    # ========================================
    # STEP 2: Load and Combine Sales Data
    # ========================================
    try:
        all_sales_dfs = []
        print(f"\n📂 Loading {len(config.SALES_FILES)} sales file(s)...")
        
        for sales_file in config.SALES_FILES:
            file_path = config.DATA_RAW_DIR / sales_file
            print(f"   ├─ Reading: {file_path.name}")
            
            df = pd.read_excel(
                file_path,
                usecols=config.SALES_COLS,
                engine='openpyxl'
            )
            df = df.rename(columns=config.SALES_RENAME)
            all_sales_dfs.append(df)
            print(f"   └─ ✅ Loaded {len(df):,} rows")

        sales_df = pd.concat(all_sales_dfs, ignore_index=True)
        print(f"\n✅ Combined sales data: {len(sales_df):,} total rows (before deduplication)")

    except FileNotFoundError as e:
        print(f"\n❌ ERROR: Sales file not found!")
        return None
    except Exception as e:
        print(f"\n❌ ERROR: Could not read sales files - {e}")
        import traceback
        traceback.print_exc()
        return None

    # ========================================
    # STEP 3: Clean Sales Data (EXCEPT customer_code - handle separately)
    # ========================================
    print(f"\n🧹 Cleaning sales data...")
    for col, dtype in config.SALES_DTYPES.items():
        if col not in sales_df.columns or col == 'customer_code':  # Skip customer_code here
            continue
        if dtype == 'string':
            sales_df[col] = sales_df[col].apply(validator.clean_and_trim_string)
        elif dtype == 'integer':
            sales_df[col] = sales_df[col].apply(validator.clean_and_round_integer)
        elif dtype == 'date':
            sales_df[col] = sales_df[col].apply(validator.parse_date)
    
    print(f"   └─ ✅ Sales data cleaned (except customer_code)")

    # ========================================
    # STEP 3B: CRITICAL - Standardize customer codes EARLY
    # ========================================
    print(f"\n🔐 CRITICAL: Standardizing customer codes...")
    
    # Show BEFORE state
    print(f"   BEFORE standardization:")
    print(f"   ├─ Credit sample: {credit_df['customer_code'].head(3).tolist()}")
    print(f"   └─ Sales sample: {sales_df['customer_code'].head(3).tolist()}")
    
    # Convert: float/int -> int -> string (handles 1.0, 1, "1" all the same way)
    # CRITICAL: Direct conversion without validator to avoid None issues
    
    # For credit codes (currently float64)
    credit_df['customer_code'] = credit_df['customer_code'].fillna(-1).astype(int).astype(str)
    credit_df = credit_df[credit_df['customer_code'] != '-1']  # Remove any that were NaN
    
    # For sales codes (currently int64)
    sales_df['customer_code'] = sales_df['customer_code'].astype(str)
    
    # Show AFTER state
    print(f"   AFTER standardization:")
    print(f"   ├─ Credit sample: {credit_df['customer_code'].head(3).tolist()}")
    print(f"   └─ Sales sample: {sales_df['customer_code'].head(3).tolist()}")
    
    # IMMEDIATE verification
    credit_codes_set = set(credit_df['customer_code'].unique())
    sales_codes_set = set(sales_df['customer_code'].unique())
    immediate_matches = len(credit_codes_set & sales_codes_set)
    
    print(f"   ✅ Immediate match test: {immediate_matches} matches")
    if immediate_matches == 0:
        print(f"   ❌ ERROR: Still no matches after standardization!")
        print(f"   Credit codes sample: {list(credit_codes_set)[:5]}")
        print(f"   Sales codes sample: {list(sales_codes_set)[:5]}")
        return None

    # ========================================
    # STEP 4: Remove Duplicate Invoices
    # ========================================
    print(f"\n🔄 Removing duplicate invoices...")
    initial_count = len(sales_df)
    
    sales_df = sales_df.sort_values('delivery_date', ascending=False)
    sales_df = sales_df.drop_duplicates(
        subset=['invoice_number', 'product_name'], 
        keep='first'
    )
    sales_df = sales_df.sort_values('delivery_date', ascending=True)
    
    removed_count = initial_count - len(sales_df)
    print(f"   ├─ Removed {removed_count:,} duplicate product lines")
    print(f"   └─ ✅ Final sales data: {len(sales_df):,} unique product lines")

    # ========================================
    # STEP 5: Analyze Sales Data
    # ========================================
    print(f"\n📊 Analyzing sales...")
    
    positive_sales = sales_df[sales_df['amount'] > 0]
    negative_sales = sales_df[sales_df['amount'] < 0]
    zero_sales = sales_df[sales_df['amount'] == 0]
    
    print(f"   ├─ Positive sales: {len(positive_sales):,} lines (PKR {positive_sales['amount'].sum():,.0f})")
    print(f"   ├─ Returns/Claims: {len(negative_sales):,} lines (PKR {negative_sales['amount'].sum():,.0f})")
    print(f"   ├─ Zero amount: {len(zero_sales):,} lines")
    print(f"   └─ Net sales: PKR {sales_df['amount'].sum():,.0f}")

    # ========================================
    # STEP 6: Aggregate Sales by Customer
    # ========================================
    print(f"\n🔗 Aggregating sales data per customer...")
    
    # Calculate aggregates for each customer
    sales_aggregated = sales_df.groupby('customer_code').agg({
        'amount': 'sum',              # Total sales amount
        'quantity': 'sum',            # Total quantity
        'delivery_date': 'max',       # Latest delivery date
        'invoice_number': 'nunique',  # Count of unique invoices
        'customer_name': 'first',     # Customer name (for reference)
        'route': 'first',             # Route (take first occurrence)
        'booker_name': 'first',       # Booker (take first occurrence)
        'company': 'first'            # Company (take first occurrence)
    }).reset_index()
    
    sales_aggregated.rename(columns={
        'amount': 'total_sales_amount',
        'quantity': 'total_quantity',
        'delivery_date': 'last_delivery_date',
        'invoice_number': 'invoice_count'
    }, inplace=True)
    
    print(f"   └─ ✅ Aggregated sales for {len(sales_aggregated):,} unique customers")

    # ========================================
    # STEP 7: Load Margin/Profit Data
    # ========================================
    try:
        print(f"\n📂 Loading margin data...")
        margin_path = config.DATA_RAW_DIR / config.MARGIN_FILE
        
        margin_df = pd.read_excel(
            margin_path,
            usecols=config.MARGIN_COLS,
            engine='openpyxl'
        ).rename(columns=config.MARGIN_RENAME)
        
        print(f"   └─ ✅ Loaded {len(margin_df):,} invoice margin records")

        # Clean margin data
        for col, dtype in config.MARGIN_DTYPES.items():
            if col not in margin_df.columns:
                continue
            if dtype == 'string':
                margin_df[col] = margin_df[col].apply(validator.clean_and_trim_string)
            elif dtype == 'integer':
                margin_df[col] = margin_df[col].apply(validator.clean_and_round_integer)

        margin_df = margin_df.drop_duplicates(subset=['invoice_number'], keep='last')
        
        # Analyze profit/loss
        profitable = margin_df[margin_df['profit'] > 0]
        loss_making = margin_df[margin_df['profit'] < 0]
        
        print(f"   ├─ Profitable invoices: {len(profitable):,} (PKR {profitable['profit'].sum():,.0f})")
        print(f"   ├─ Loss/Scheme invoices: {len(loss_making):,} (PKR {loss_making['profit'].sum():,.0f})")
        print(f"   └─ Net profit: PKR {margin_df['profit'].sum():,.0f}")

    except FileNotFoundError:
        print(f"\n⚠️  WARNING: Margin file not found")
        margin_df = pd.DataFrame(columns=['invoice_number', 'amount_from_margin', 'profit'])
    except Exception as e:
        print(f"\n❌ ERROR: Could not read margin file - {e}")
        margin_df = pd.DataFrame(columns=['invoice_number', 'amount_from_margin', 'profit'])

    # ========================================
    # STEP 8: Merge Sales INTO Credit (LEFT JOIN)
    # ========================================
    print(f"\n🔗 Merging sales data INTO credit balance (master list)...")
    
    # CRITICAL: Credit is on the LEFT (all customers preserved)
    # Sales aggregated data is merged IN from the right
    final_df = pd.merge(
        credit_df,
        sales_aggregated[['customer_code', 'total_sales_amount', 'total_quantity', 
                         'last_delivery_date', 'invoice_count', 'route', 
                         'booker_name', 'company']],
        on='customer_code',
        how='left'  # Keep ALL customers from credit file
    )
    
    print(f"   └─ ✅ Merged data: {len(final_df):,} customers (same as credit file)")

    # ========================================
    # STEP 9: Merge Detailed Sales Data (for database)
    # ========================================
    print(f"\n🔗 Merging detailed sales with margin data...")
    
    # Merge sales with margin data
    sales_detailed = pd.merge(sales_df, margin_df, on='invoice_number', how='left')
    
    # Merge with credit data to get balance for each transaction
    merge_cols = ['customer_code', 'balance']
    if 'last_invoice_date' in credit_df.columns:
        merge_cols.append('last_invoice_date')
    
    sales_detailed = pd.merge(
        sales_detailed,
        credit_df[merge_cols],
        on='customer_code',
        how='left',
        suffixes=('', '_from_credit')
    )
    
    print(f"   └─ ✅ Detailed sales data: {len(sales_detailed):,} product lines")
    
    # Debug: Check if balance is actually populated
    print(f"\n🔍 Balance Check in Detailed Data:")
    non_zero_balance = sales_detailed[sales_detailed['balance'] != 0]
    print(f"   ├─ Records with non-zero balance: {len(non_zero_balance):,}")
    print(f"   ├─ Total balance in data: PKR {sales_detailed['balance'].sum():,.0f}")
    if len(non_zero_balance) > 0:
        print(f"   └─ Sample balances: {non_zero_balance['balance'].head(5).tolist()}")

    # ========================================
    # STEP 10: Handle Missing Values
    # ========================================
    print(f"\n⚙️  Applying business logic and filling missing values...")
    
    # For customers with NO sales data, fill with appropriate defaults
    final_df['total_sales_amount'] = final_df['total_sales_amount'].fillna(0)
    final_df['total_quantity'] = final_df['total_quantity'].fillna(0)
    final_df['invoice_count'] = final_df['invoice_count'].fillna(0).astype(int)
    final_df['route'] = final_df['route'].fillna('N/A')
    final_df['booker_name'] = final_df['booker_name'].fillna('N/A')
    final_df['company'] = final_df['company'].fillna('N/A')
    
    # For detailed sales data
    sales_detailed['profit'] = sales_detailed['profit'].fillna(0)
    if sales_detailed['balance'].isna().any():
        missing_balance_count = sales_detailed['balance'].isna().sum()
        print(f"   ⚠️  Warning: {missing_balance_count} records have no balance (filling with 0)")
        sales_detailed['balance'] = sales_detailed['balance'].fillna(0)
    
    if 'amount_from_margin' in sales_detailed.columns:
        sales_detailed = sales_detailed.drop(columns=['amount_from_margin'])
    
    print(f"   └─ ✅ Missing values handled")

    # ========================================
    # STEP 11: Calculate Days Since Last Activity
    # ========================================
    print(f"\n📅 Calculating days since last activity...")
    
    from datetime import datetime
    today = datetime.now()
    
    # Use last_delivery_date from sales if available, otherwise use last_invoice_date from credit
    final_df['days_since_last_sale'] = final_df.apply(
        lambda row: (today - row['last_delivery_date']).days 
                    if pd.notna(row['last_delivery_date']) 
                    else (today - row['last_invoice_date']).days 
                    if pd.notna(row['last_invoice_date'])
                    else 999,
        axis=1
    )
    
    print(f"   └─ ✅ Days since calculation complete")

    # ========================================
    # STEP 12: Final Data Quality Summary
    # ========================================
    print(f"\n📊 Final Data Quality Summary:")
    print(f"   ├─ CUSTOMER RECORDS:")
    print(f"   │  ├─ Total unique customers: {len(final_df):,}")
    print(f"   │  ├─ Customers with sales: {len(final_df[final_df['invoice_count'] > 0]):,}")
    print(f"   │  └─ Customers without sales: {len(final_df[final_df['invoice_count'] == 0]):,}")
    print(f"   │")
    print(f"   ├─ CREDIT BALANCES (from credit file - SOURCE OF TRUTH):")
    
    # --- FIX: Update logic for inverted balance (positive = they owe us) ---
    customers_owing_count = len(final_df[final_df['balance'] > 0])
    we_owe_count = len(final_df[final_df['balance'] < 0])
    zero_bal_count = len(final_df[final_df['balance'] == 0])
    
    amount_owing_us = final_df[final_df['balance'] > 0]['balance'].sum()
    amount_we_owe = abs(final_df[final_df['balance'] < 0]['balance'].sum())
    net_balance = final_df['balance'].sum()
    
    print(f"   │  ├─ Customers owing us (Positive): {customers_owing_count:,} (PKR {amount_owing_us:,.0f})")
    print(f"   │  ├─ We owe customers (Negative): {we_owe_count:,} (PKR {amount_we_owe:,.0f})")
    # --- END FIX ---
    print(f"   │  ├─ Zero balance: {zero_bal_count:,}")
    print(f"   │  └─ Net Balance (They owe - We owe): PKR {net_balance:,.0f}")
    print(f"   │")
    print(f"   ├─ SALES DATA (aggregated from transactions):")
    print(f"   │  ├─ Total sales amount: PKR {final_df['total_sales_amount'].sum():,.0f}")
    print(f"   │  ├─ Total quantity: {final_df['total_quantity'].sum():,.0f}")
    print(f"   │  └─ Total invoices: {final_df['invoice_count'].sum():,.0f}")
    print(f"   │")
    print(f"   └─ DETAILED TRANSACTION DATA (for database):")
    print(f"      ├─ Product lines: {len(sales_detailed):,}")
    print(f"      ├─ Unique invoices: {sales_detailed['invoice_number'].nunique():,}")
    # --- FIX: Update logic for inverted balance ---
    print(f"      ├─ Records with balance > 0 (Owe Us): {len(sales_detailed[sales_detailed['balance'] > 0]):,}")
    print(f"      ├─ Records with balance < 0 (We Owe): {len(sales_detailed[sales_detailed['balance'] < 0]):,}")
    print(f"      └─ Total balance in DB data: PKR {sales_detailed.groupby('customer_code')['balance'].first().sum():,.0f}")
    # --- END FIX ---

    print(f"\n✅ DATA PROCESSING COMPLETE!")
    print("="*60 + "\n")
    
    return sales_detailed  # Return detailed transaction data for database


def update_database():
    """Loads clean data and saves to SQLite database."""
    final_data = load_clean_and_merge_data()

    if final_data is None:
        print("\n❌ FAILED: Could not process data. Database not updated.")
        return False

    print(f"💾 Saving to database: {config.DB_PATH}")
    try:
        conn = sqlite3.connect(config.DB_PATH)
        final_data.to_sql('sales_data', conn, if_exists='replace', index=False)
        conn.close()
        
        print(f"✅ SUCCESS! Saved {len(final_data):,} product lines to 'sales_data' table")
        return True
        
    except Exception as e:
        print(f"❌ ERROR: Failed to save to database - {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    print("\n" + "🚀 FMCG Data Loader".center(60))
    success = update_database()
    
    if success:
        print("\n✅ Database update completed successfully!")
    else:
        print("\n❌ Database update failed. Please check errors above.")
