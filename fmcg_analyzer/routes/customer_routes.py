# routes/customer_routes.py

import sqlite3
import pandas as pd
from flask import render_template
from pathlib import Path
from urllib.parse import unquote  # <-- THIS IS THE FIX

DB_PATH = Path(__file__).resolve().parent.parent / "data" / "sales.db"

def customer_details(name):
    """
    Fetches all sales records for a specific customer with correct aggregation.
    
    Shows:
    - All product lines (invoices with multiple products)
    - Total sales = SUM of all product line amounts
    - Credit balance = ONE value (not multiplied)
    - Profit per invoice (grouped correctly)
    """
    name = unquote(name)  # This line will now work
    try:
        conn = sqlite3.connect(DB_PATH)
        
        # Get all product lines for this customer
        query = """
            SELECT * FROM sales_data 
            WHERE customer_name = ?
            ORDER BY delivery_date DESC, invoice_number
        """
        customer_df = pd.read_sql_query(query, conn, params=(name,))
        conn.close()

        if customer_df.empty:
            return render_template('error.html', 
                                 message=f"No data found for customer: {name}")

        # ========================================
        # Calculate Correct Metrics
        # ========================================
        
        # Total Sales = Sum of ALL product line amounts
        total_sales = int(customer_df['amount'].sum())
        
        # Total Quantity = Sum of ALL product quantities
        total_quantity = int(customer_df['quantity'].sum())
        
        # Total Invoices = Count UNIQUE invoice numbers
        total_invoices = customer_df['invoice_number'].nunique()
        
        # Credit Balance = ONE value (take from first row, it's same for all)
        credit_balance = int(customer_df['balance'].iloc[0]) if 'balance' in customer_df.columns else 0
        
        # Total Profit = Sum at INVOICE level (not product line level)
        # Group by invoice first, then sum
        invoice_profits = customer_df.groupby('invoice_number')['profit'].first()
        total_profit = int(invoice_profits.sum())
        
        # Get customer code
        customer_code = customer_df['customer_code'].iloc[0] if 'customer_code' in customer_df.columns else 'N/A'
        
        # ========================================
        # Prepare Invoice Summary (Grouped View)
        # ========================================
        
        # Group by invoice to show invoice-level summary
        invoice_summary = customer_df.groupby('invoice_number').agg({
            'delivery_date': 'first',
            'amount': 'sum',  # Sum all product amounts for this invoice
            'quantity': 'sum',  # Sum all quantities
            'profit': 'first',  # Profit is per invoice, not per product
            'booker_name': 'first',
            'company': 'first'
        }).reset_index()
        
        invoice_summary = invoice_summary.sort_values('delivery_date', ascending=False)
        invoice_list = invoice_summary.to_dict('records')
        
        # ========================================
        # Prepare Product Line Details (Full View)
        # ========================================
        
        # Convert all rows to list of dicts (shows every product line)
        sales_records = customer_df.to_dict('records')

    except Exception as e:
        print(f"❌ Database error in customer_routes: {e}")
        return render_template('error.html', 
                             message=f"Could not load data for {name}")

    return render_template('customer_details.html',
                         customer_name=name,
                         customer_code=customer_code,
                         sales_records=sales_records,
                         invoice_list=invoice_list,  # Grouped by invoice
                         total_sales=total_sales,
                         total_quantity=total_quantity,
                         total_invoices=total_invoices,
                         total_profit=total_profit,
                         credit_balance=credit_balance)