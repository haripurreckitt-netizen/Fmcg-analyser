# routes/company_product_deep_dive.py

import sqlite3
import pandas as pd
from flask import render_template, request, send_file, jsonify
from pathlib import Path
from datetime import datetime
import config
import io

DB_PATH = config.DB_PATH

# === Helper Functions ===
def _query_db(query, params=()):
    try:
        conn = sqlite3.connect(DB_PATH)
        df = pd.read_sql_query(query, conn, params=params, parse_dates=['delivery_date'])
        conn.close()
        return df
    except Exception as e:
        print(f"❌ Database query error: {e}")
        return pd.DataFrame()

def _get_filtered_sales(company=None, route=None, product=None, start_date=None, end_date=None):
    query = "SELECT * FROM sales_data WHERE delivery_date BETWEEN ? AND ?"
    params = [start_date, end_date]

    if company and company != 'all':
        query += " AND company = ?"
        params.append(company)
    if route and route != 'all':
        query += " AND route = ?"
        params.append(route)
    if product and product != 'all':
        query += " AND product_name = ?"
        params.append(product)

    return _query_db(query, params)

def _get_filter_options():
    """Get all unique companies, routes, and products for filter dropdowns"""
    try:
        conn = sqlite3.connect(DB_PATH)
        companies = pd.read_sql_query("SELECT DISTINCT company FROM sales_data ORDER BY company", conn)
        routes = pd.read_sql_query("SELECT DISTINCT route FROM sales_data ORDER BY route", conn)
        products = pd.read_sql_query("SELECT DISTINCT product_name FROM sales_data ORDER BY product_name", conn)
        conn.close()
        
        return {
            'companies': companies['company'].tolist(),
            'routes': routes['route'].tolist(),
            'products': products['product_name'].tolist()
        }
    except Exception as e:
        print(f"❌ Error getting filter options: {e}")
        return {'companies': [], 'routes': [], 'products': []}

def _calculate_pareto(df, group_col):
    grouped = df.groupby(group_col)['amount'].sum().sort_values(ascending=False).reset_index()
    total_sales = grouped['amount'].sum()
    grouped['cumulative'] = grouped['amount'].cumsum()
    grouped['percent'] = grouped['cumulative'] / total_sales * 100
    top_entities = grouped[grouped['percent'] <= 80][group_col].tolist()
    
    # Add formatted amount and percentage columns
    grouped['amount_formatted'] = grouped['amount'].apply(lambda x: f"{int(x):,}")
    grouped['percent_formatted'] = grouped['percent'].apply(lambda x: f"{x:.1f}%")
    
    return top_entities, grouped

def _detect_dropoffs(df):
    today = datetime.now()
    last_purchase = df.groupby('customer_name')['delivery_date'].max().reset_index()
    last_purchase['days_since'] = (today - last_purchase['delivery_date']).dt.days
    dropoffs = last_purchase[last_purchase['days_since'] > 60].sort_values('days_since', ascending=False)
    
    # Add last purchase date formatted
    dropoffs['last_purchase_formatted'] = dropoffs['delivery_date'].dt.strftime('%Y-%m-%d')
    
    return dropoffs

def _calculate_cycle_health(df):
    df = df.sort_values('delivery_date')
    
    # Get last purchase info
    patterns = df.groupby(['customer_name', 'product_name']).agg(
        last_purchase_date=('delivery_date', 'max'),
        last_purchase_qty=('quantity', 'last'),
        median_qty=('quantity', 'median'),
        purchase_count=('invoice_number', 'nunique')
    ).reset_index()

    # Calculate days between purchases
    df['days_between'] = df.groupby(['customer_name', 'product_name'])['delivery_date'].diff().dt.days
    
    # Get median cycle, but only for customers with 2+ purchases
    median_cycles = df[df['days_between'].notna()].groupby(['customer_name', 'product_name'])['days_between'].median().rename('median_cycle').reset_index()
    patterns = pd.merge(patterns, median_cycles, on=['customer_name', 'product_name'], how='left')

    # Use pandas Timestamp.now() for consistency
    today = pd.Timestamp.now()
    patterns['days_since_last'] = (today - pd.to_datetime(patterns['last_purchase_date'])).dt.days
    
    # Fill missing values intelligently
    patterns['median_cycle'] = patterns['median_cycle'].fillna(30)
    patterns['median_qty'] = patterns['median_qty'].fillna(1)

    def get_status(row):
        # Filter out noise: only flag if customer has bought 2+ times
        if row['purchase_count'] < 2:
            return "New Customer"
        
        # Skip if median cycle is unreliable (too short)
        if row['median_cycle'] < 7:
            return "Frequent Buyer"
        
        overdue_threshold = 1.5  # More lenient: 50% over median
        is_stock_up = row['last_purchase_qty'] > (row['median_qty'] * 2) if row['median_qty'] > 0 else False
        is_overdue = row['days_since_last'] > (row['median_cycle'] * overdue_threshold)
        is_likely_seasonal = row['days_since_last'] > 180  # 6 months
        
        # Stock-up means they bought extra, so they won't need more soon
        if is_stock_up and row['days_since_last'] < (row['median_cycle'] * 2):
            return "Stock-Up (OK)"
        
        if is_likely_seasonal:
            return "Seasonal/Inactive"
        
        if is_overdue:
            return "⚠️ Attention Needed"
        
        return "✓ Healthy"

    patterns['status'] = patterns.apply(get_status, axis=1)
    
    # Format dates and numbers
    patterns['last_purchase_formatted'] = pd.to_datetime(patterns['last_purchase_date']).dt.strftime('%Y-%m-%d')
    patterns['median_cycle_formatted'] = patterns['median_cycle'].apply(lambda x: f"{int(x)} days")
    
    # Sort by status priority (Attention Needed first)
    status_priority = {
        '⚠️ Attention Needed': 1, 
        'Seasonal/Inactive': 2, 
        'Stock-Up (OK)': 3, 
        'Frequent Buyer': 4, 
        'New Customer': 5, 
        '✓ Healthy': 6
    }
    patterns['priority'] = patterns['status'].map(status_priority)
    patterns = patterns.sort_values(['priority', 'days_since_last'], ascending=[True, False]).drop('priority', axis=1)
    
    return patterns

def _calculate_affinity(df):
    customer_products = df.groupby('customer_name')['product_name'].apply(set).to_dict()
    product_affinity = {}
    all_products = df['product_name'].unique()

    for product in all_products:
        customers_who_bought = [cust for cust, prods in customer_products.items() if product in prods]
        if len(customers_who_bought) < 2:
            product_affinity[product] = []
            continue

        related_products = set()
        for customer in customers_who_bought:
            related_products.update(customer_products[customer])
        related_products.discard(product)

        affinity_scores = {}
        for related_prod in related_products:
            customers_who_bought_related = [cust for cust, prods in customer_products.items() if related_prod in prods]
            intersection = len(set(customers_who_bought) & set(customers_who_bought_related))
            union = len(set(customers_who_bought) | set(customers_who_bought_related))
            if union > 0:
                similarity = intersection / union
                if similarity > 0.2:
                    affinity_scores[related_prod] = similarity

        # Convert to list of tuples sorted by score
        product_affinity[product] = sorted(
            [(prod, score) for prod, score in affinity_scores.items()],
            key=lambda x: x[1],
            reverse=True
        )[:5]  # Top 5 related products
    
    return product_affinity

def _calculate_trends(df):
    """Calculate month-over-month trends"""
    df['month'] = pd.to_datetime(df['delivery_date']).dt.to_period('M')
    monthly = df.groupby('month').agg(
        total_sales=('amount', 'sum'),
        total_quantity=('quantity', 'sum')
    ).reset_index()
    
    monthly['month'] = monthly['month'].astype(str)
    return monthly.to_dict('records')

def export_excel(df, cycle_health, dropoff_customers):
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        # Export main data
        df.to_excel(writer, index=False, sheet_name='Sales Data')
        
        # Export cycle health
        cycle_health.to_excel(writer, index=False, sheet_name='Cycle Health')
        
        # Export dropoffs
        dropoff_customers.to_excel(writer, index=False, sheet_name='Dropoff Customers')
        
    output.seek(0)
    return send_file(output, download_name="deep_dive_export.xlsx", as_attachment=True)

def company_product_deep_dive():
    selected_company = request.args.get('company', 'all')
    selected_route = request.args.get('route', 'all')
    selected_product = request.args.get('product', 'all')
    start_date = request.args.get('start_date', '2025-09-01')
    end_date = request.args.get('end_date', '2025-09-30')
    export = request.args.get('export', 'false') == 'true'

    # Get filtered data for summary metrics
    df = _get_filtered_sales(selected_company, selected_route, selected_product, start_date, end_date)
    if df.empty:
        return render_template('error.html', message="No data found for selected filters.")

    # Get ALL historical data for cycle calculations (no date filter)
    # This ensures we see full purchase history, not just the filtered period
    df_full_history = _get_filtered_sales(selected_company, selected_route, selected_product, 
                                          start_date='2000-01-01',  # Far past date to get everything
                                          end_date=pd.Timestamp.now().strftime('%Y-%m-%d'))
    
    # Calculate metrics using full history
    cycle_health = _calculate_cycle_health(df_full_history)
    dropoff_customers = _detect_dropoffs(df_full_history)
    
    if export:
        return export_excel(df, cycle_health, dropoff_customers)

    # Get filter options for dropdowns
    filter_options = _get_filter_options()

    # Calculate summary metrics (using filtered period df)
    total_sales = int(df['amount'].sum())
    total_quantity = int(df['quantity'].sum())
    unique_customers = df['customer_name'].nunique()
    unique_products = df['product_name'].nunique()
    unique_routes = df['route'].nunique()
    avg_order_value = int(total_sales / df['invoice_number'].nunique()) if df['invoice_number'].nunique() > 0 else 0

    # Pareto analysis (using filtered period df)
    top_customers, customer_summary = _calculate_pareto(df, 'customer_name')
    top_products, product_summary = _calculate_pareto(df, 'product_name')

    # Route summary (using filtered period df)
    route_summary = df.groupby('route').agg(
        total_sales=('amount', 'sum'),
        total_quantity=('quantity', 'sum'),
        active_customers=('customer_name', 'nunique'),
        product_count=('product_name', 'nunique')
    ).reset_index().sort_values('total_sales', ascending=False)
    
    # Format route summary
    route_summary['total_sales_formatted'] = route_summary['total_sales'].apply(lambda x: f"{int(x):,}")
    route_summary['total_quantity_formatted'] = route_summary['total_quantity'].apply(lambda x: f"{int(x):,}")

    # Product affinity (using filtered period df)
    affinity_map = _calculate_affinity(df)
    
    # Monthly trends (using filtered period df)
    trends = _calculate_trends(df)

    return render_template('company_product_deep_dive.html',
                           title='Company/Product Deep Dive',
                           selected_company=selected_company,
                           selected_route=selected_route,
                           selected_product=selected_product,
                           start_date=start_date,
                           end_date=end_date,
                           filter_options=filter_options,
                           total_sales=f"{total_sales:,}",
                           total_quantity=f"{total_quantity:,}",
                           unique_customers=unique_customers,
                           unique_products=unique_products,
                           unique_routes=unique_routes,
                           avg_order_value=f"{avg_order_value:,}",
                           top_customers=top_customers,
                           top_products=top_products,
                           customer_summary=customer_summary.to_dict('records'),
                           product_summary=product_summary.to_dict('records'),
                           route_summary=route_summary.to_dict('records'),
                           dropoff_customers=dropoff_customers.to_dict('records'),
                           cycle_health=cycle_health.to_dict('records'),
                           affinity_map=affinity_map,
                           trends=trends)