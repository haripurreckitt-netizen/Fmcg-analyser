# routes/invoice_routes.py

import sqlite3
import pandas as pd
from flask import render_template
from pathlib import Path
from scoring import get_customer_scores  # <--- 1. ADD THIS IMPORT

# Define the path to the database file
DB_PATH = Path(__file__).resolve().parent.parent / "data" / "sales.db"

def invoice_details(invoice_id):
    """
    Invoice details - Fetches all data and formats it for invoice_details.html
    """
    try:
        conn = sqlite3.connect(DB_PATH)
        
        query = """
            SELECT * FROM sales_data 
            WHERE invoice_number = ?
        """
        invoice_df = pd.read_sql_query(query, conn, params=(invoice_id,))
        conn.close()

        if invoice_df.empty:
            return render_template('error.html', 
                                 message=f"Invoice not found: {invoice_id}")

        # 1. Get scalar values from the first row (they are the same for all lines)
        first_row = invoice_df.iloc[0]
        total_profit = int(first_row['profit']) # Profit is per-invoice
        customer_name = first_row['customer_name'] # <--- We need this for the score
        delivery_date = first_row['delivery_date']
        booker_name = first_row['booker_name']
        company = first_row['company']
        route = first_row['route']
        
        # 2. Calculate aggregate values
        total_amount = int(invoice_df['amount'].sum())
        total_quantity = int(invoice_df['quantity'].sum())
        total_cost = total_amount - total_profit # Calculate total cost
        
        # 3. Create the list of product items
        items_list = []
        for _, row in invoice_df.iterrows():
            qty = row['quantity']
            amount = row['amount']
            # Calculate unit price, handling division by zero
            unit_price = (amount / qty) if qty != 0 else 0
            
            items_list.append({
                'product': row['product_name'],
                'net_quantity': qty,
                'net_amount': amount,
                'unit_price': unit_price
            })

        # --- 4. NEW: GET THE CUSTOMER'S SCORE ---
        try:
            scores_df = get_customer_scores()
            if not scores_df.empty:
                customer_score_row = scores_df[scores_df['customer_name'] == customer_name]
                if not customer_score_row.empty:
                    credit_score = customer_score_row.iloc[0]['RFM_Score']
                else:
                    credit_score = 0  # Customer exists but has no RFM score (e.g., no sales)
            else:
                credit_score = 0
        except Exception as e:
            print(f"Warning: Could not get credit score for {customer_name}. Error: {e}")
            credit_score = "N/A"
        # --- END OF NEW SECTION ---

        # 5. Build the final 'invoice' dictionary that the template expects
        invoice_data = {
            'invoice_id': invoice_id,
            'customer_name': customer_name,
            'order_date': pd.to_datetime(delivery_date), # Convert to datetime for strftime
            'orderbooker': booker_name,
            'company': company,
            'route': route,
            'credit_score': credit_score, # <--- 5. USE THE REAL SCORE
            
            'total_amount': total_amount,
            'total_quantity': total_quantity,
            'total_cost': total_cost,
            'total_profit': total_profit,
            
            'product_list': items_list # RENAMED KEY
        }

    except Exception as e:
        print(f"❌ Database error in invoice_routes: {e}")
        return render_template('error.html', 
                             message=f"Could not load invoice {invoice_id}. Error: {e}")

    # 6. Pass the single 'invoice' object to the template
    return render_template('invoice_details.html', invoice=invoice_data)