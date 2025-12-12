# routes/customer_deep_dive_routes.py

import sqlite3
import pandas as pd
from flask import render_template, request
from pathlib import Path
from datetime import datetime
from urllib.parse import unquote

DB_PATH = Path(__file__).resolve().parent.parent / "data" / "sales.db"

def _query_db(query, params=()):
    try:
        conn = sqlite3.connect(DB_PATH)
        df = pd.read_sql_query(query, conn, params=params, parse_dates=['delivery_date'])
        conn.close()
        return df
    except Exception as e:
        print(f"❌ Database query error in customer_deep_dive: {e}")
        return pd.DataFrame()

def _get_available_products():
    """Fetch products with stock > 0 and status = 'Active' from products table."""
    try:
        conn = sqlite3.connect(DB_PATH)
        query = "SELECT product_name FROM products WHERE status = 'Active' AND stock_quantity > 0"
        df = pd.read_sql_query(query, conn)
        conn.close()
        return set(df['product_name'].tolist())
    except Exception as e:
        print(f"⚠️ Warning: Could not fetch product inventory. Showing all products. Error: {e}")
        return None

def _calculate_patterns_for_customer(df):
    df = df.sort_values('delivery_date')
    patterns = df.groupby('product_name').agg(
        last_purchase_date=('delivery_date', 'max'),
        total_qty=('quantity', 'sum'),
        total_sales=('amount', 'sum'),
        last_purchase_qty=('quantity', 'last'),
        median_qty=('quantity', 'median'),
        purchase_count=('invoice_number', 'nunique')
    ).reset_index()

    df['days_between'] = df.groupby('product_name')['delivery_date'].diff().dt.days
    median_cycles = df.groupby('product_name')['days_between'].median().rename('median_cycle').reset_index()
    
    patterns = pd.merge(patterns, median_cycles, on='product_name', how='left')

    patterns['median_cycle'] = patterns['median_cycle'].fillna(30)
    patterns['median_qty'] = patterns['median_qty'].fillna(0)
    
    return patterns

def customer_deep_dive(name):
    customer_name = unquote(name)
    
    # 1. GET FILTERS & FETCH CUSTOMER DATA
    company_filter = request.args.get('company', 'all')
    customer_df = _query_db("SELECT * FROM sales_data WHERE customer_name = ?", (customer_name,))

    if customer_df.empty:
        return render_template('error.html', message=f"No data found for customer: {customer_name}")

    available_companies = ['all'] + sorted(customer_df['company'].unique().tolist())
    
    # 2. CALCULATE KPIs
    total_sales = customer_df['amount'].sum()
    total_profit = customer_df.drop_duplicates(subset='invoice_number')['profit'].sum()
    current_balance = customer_df['balance'].iloc[-1] if not customer_df.empty else 0
    first_purchase = customer_df['delivery_date'].min().strftime('%d %b, %Y')
    last_purchase = customer_df['delivery_date'].max().strftime('%d %b, %Y')
    
    kpis = {
        'total_sales': int(total_sales),
        'total_profit': int(total_profit),
        'current_balance': int(current_balance),
        'first_purchase': first_purchase,
        'last_purchase': last_purchase
    }

    # 3. ANALYZE PRODUCT-LEVEL PATTERNS
    product_patterns = _calculate_patterns_for_customer(customer_df)
    
    # FILTER OUT UNAVAILABLE PRODUCTS
    available_products = _get_available_products()
    if available_products is not None:
        product_patterns = product_patterns[product_patterns['product_name'].isin(available_products)]
    
    today = datetime.now()
    product_patterns['days_since_last'] = (today - product_patterns['last_purchase_date']).dt.days
    
    # Define status with 400-day cutoff for seasonal items
    def get_status(row):
        overdue_threshold = 1.2
        is_stock_up = row['last_purchase_qty'] > (row['median_qty'] * 2) if row['median_qty'] > 0 else False
        is_overdue = row['days_since_last'] > (row['median_cycle'] * overdue_threshold)
        is_likely_seasonal = row['days_since_last'] > 400  # NEW: Seasonal/annual items
        
        if is_stock_up:
            return "Stock-Up Purchase", 1
        if is_likely_seasonal:
            return "Seasonal/Annual", 4  # Low priority
        if is_overdue and row['purchase_count'] > 1:
            return "Attention Needed", 3
        return "OK", 2

    product_patterns[['status', 'urgency']] = product_patterns.apply(get_status, axis=1, result_type='expand')
    
    product_company_map = customer_df.drop_duplicates(subset='product_name')[['product_name', 'company']]
    product_patterns = pd.merge(product_patterns, product_company_map, on='product_name', how='left')

    if company_filter != 'all':
        product_patterns = product_patterns[product_patterns['company'] == company_filter]

    product_patterns = product_patterns.sort_values(by=['urgency', 'days_since_last'], ascending=[False, False])

    return render_template('customer_deep_dive.html',
                           title=f"Analysis for {customer_name}",
                           customer_name=customer_name,
                           kpis=kpis,
                           product_list=product_patterns.to_dict('records'),
                           available_companies=available_companies,
                           company_filter=company_filter)