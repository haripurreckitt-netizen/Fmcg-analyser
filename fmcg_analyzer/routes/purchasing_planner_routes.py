# routes/purchasing_planner_routes.py

import sqlite3
import pandas as pd
from flask import render_template, request
from pathlib import Path
from datetime import datetime
from dateutil.relativedelta import relativedelta

DB_PATH = Path(__file__).resolve().parent.parent / "data" / "sales.db"

def purchasing_planner():
    """
    Provides a data-driven purchasing plan, now EXCLUDING discontinued products.
    """
    company_filter = request.args.get('company', 'all')

    try:
        conn = sqlite3.connect(DB_PATH)

        # 1. Get master product list, EXCLUDING any 'Discontinued' items.
        # This is the key change to handle discontinued products.
        inventory_query = """
            SELECT product_name, stock_quantity, status 
            FROM products 
            WHERE status != 'Discontinued'
        """
        master_product_df = pd.read_sql_query(inventory_query, conn)

        # If there are no active products, exit early.
        if master_product_df.empty:
            return render_template('purchasing_planner.html', title='Purchasing Planner', product_list=[], available_companies=[], selected_company=company_filter)

        # (The rest of the logic remains largely the same...)

        # 2. Get the company for each product.
        company_query = """
            SELECT product_name, company
            FROM (
                SELECT product_name, company, ROW_NUMBER() OVER(PARTITION BY product_name ORDER BY delivery_date DESC) as rn
                FROM sales_data
            )
            WHERE rn = 1
        """
        company_df = pd.read_sql_query(company_query, conn)
        
        master_product_df = pd.merge(master_product_df, company_df, on='product_name', how='left')
        master_product_df['company'].fillna('Unknown', inplace=True)

        # 3. Calculate current sales velocity (last 30 days).
        thirty_days_ago = (datetime.now() - relativedelta(days=30)).strftime('%Y-%m-%d')
        velocity_query = f"""
            SELECT product_name, SUM(quantity) as sales_last_30d
            FROM sales_data
            WHERE delivery_date >= '{thirty_days_ago}'
            GROUP BY product_name
        """
        velocity_df = pd.read_sql_query(velocity_query, conn)

        # 4. Calculate seasonal demand.
        current_month = datetime.now()
        start_of_month_ly = (current_month - relativedelta(years=1)).replace(day=1).strftime('%Y-%m-%d')
        end_of_month_ly = (current_month - relativedelta(years=1, months=-1)).replace(day=1) - relativedelta(days=1)
        end_of_month_ly = end_of_month_ly.strftime('%Y-%m-%d')
        
        seasonal_query = f"""
            SELECT product_name, SUM(quantity) as sales_seasonal
            FROM sales_data
            WHERE delivery_date BETWEEN '{start_of_month_ly}' AND '{end_of_month_ly}'
            GROUP BY product_name
        """
        seasonal_df = pd.read_sql_query(seasonal_query, conn)

        # 5. Merge all data.
        final_df = pd.merge(master_product_df, velocity_df, on='product_name', how='left')
        final_df = pd.merge(final_df, seasonal_df, on='product_name', how='left')
        final_df.fillna(0, inplace=True)

        # 6. Apply business logic.
        final_df['projected_demand'] = (final_df['sales_last_30d'] + final_df['sales_seasonal']) / 2
        final_df['days_of_stock_left'] = final_df.apply(
            lambda row: (row['stock_quantity'] / (row['sales_last_30d'] / 30)) if row['sales_last_30d'] > 0 else 999,
            axis=1
        )
        final_df['recommended_purchase'] = (final_df['projected_demand'] - final_df['stock_quantity']).clip(lower=0)
        
        # 7. Update status logic to use the status from the database.
        def get_status(row):
            # The status from the DB ('Active', 'Out of Stock') is now the primary source.
            # We only override for urgency.
            if row['status'] == 'Active' and row['days_of_stock_left'] < 15:
                return "Critical"
            if row['status'] == 'Active' and row['days_of_stock_left'] < 30:
                return "Recommended"
            if row['status'] == 'Active':
                return "Sufficient"
            return row['status'] # Return 'Out of Stock' as is
        
        final_df['status'] = final_df.apply(get_status, axis=1)

        # 8. Handle company filter and sorting.
        available_companies = ['all'] + sorted(final_df['company'].unique().tolist())
        if company_filter != 'all':
            final_df = final_df[final_df['company'] == company_filter]
        final_df = final_df.sort_values(by=['recommended_purchase', 'days_of_stock_left'], ascending=[False, True])

        int_columns = ['stock_quantity', 'sales_last_30d', 'sales_seasonal', 'projected_demand', 'recommended_purchase', 'days_of_stock_left']
        for col in int_columns:
            final_df[col] = final_df[col].astype(int)

        conn.close()
        product_list = final_df.to_dict('records')

    except Exception as e:
        print(f"❌ Database error in purchasing_planner_routes: {e}")
        if "no such table: products" in str(e):
             return render_template('error.html', message="Product inventory data not found. Please run 'python product_loader.py' first.")
        return render_template('error.html', message="Could not load the Purchasing Planner.")

    return render_template('purchasing_planner.html',
                           title='Purchasing Planner',
                           product_list=product_list,
                           available_companies=available_companies,
                           selected_company=company_filter)

