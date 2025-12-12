# routes/planner_routes.py

import sqlite3
import pandas as pd
from flask import render_template, request
from pathlib import Path
from datetime import datetime, timedelta
import sys

# Add parent directory to path for scoring module
sys.path.append(str(Path(__file__).resolve().parent.parent))
from scoring import get_customer_scores

DB_PATH = Path(__file__).resolve().parent.parent / "data" / "sales.db"

# ============================================================
# CONFIGURATION
# ============================================================
CREDIT_BLOCK_THRESHOLD = 100  # Minimum balance to consider (ignore small amounts)
CREDIT_BLOCK_DAYS = 21  # Days without order to consider credit-blocked
ACTION_ITEMS_THRESHOLD = 15  # Days since last order to show in "Action Items Only" view
OVERDUE_MULTIPLIER = 1.2  # 20% over normal cycle = overdue
STOCK_UP_MULTIPLIER = 2  # 2x normal qty = stock-up purchase
MAX_PRODUCTS_PER_CUSTOMER = 3  # Top overdue products to show
SEASONAL_CUTOFF_DAYS = 400  # Products not bought in 400+ days = seasonal/annual

# ============================================================
# DATABASE HELPER
# ============================================================
def _query_db(query, params=()):
    try:
        conn = sqlite3.connect(DB_PATH)
        df = pd.read_sql_query(query, conn, params=params, parse_dates=['delivery_date'])
        conn.close()
        return df
    except Exception as e:
        print(f"⚠️ Database query error in planner_routes: {e}")
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

# ============================================================
# PATTERN ANALYSIS
# ============================================================
def _calculate_customer_patterns(df):
    """Calculates median purchase qty and cycle days for each customer-product pair."""
    df = df.sort_values('delivery_date')
    patterns = df.groupby(['customer_name', 'product_name']).agg(
        last_purchase_date=('delivery_date', 'max'),
        last_purchase_qty=('quantity', 'last'),
        median_qty=('quantity', lambda x: x.median()),
        purchase_count=('invoice_number', 'nunique')
    ).reset_index()

    df['days_between'] = df.groupby(['customer_name', 'product_name'])['delivery_date'].diff().dt.days
    median_cycles = df.groupby(['customer_name', 'product_name'])['days_between'].median().rename('median_cycle').reset_index()
    
    patterns = pd.merge(patterns, median_cycles, on=['customer_name', 'product_name'], how='left')
    
    patterns['median_cycle'] = patterns['median_cycle'].fillna(30)
    patterns['median_qty'] = patterns['median_qty'].fillna(0)
    
    return patterns

def _calculate_trending_products(df, days=30):
    """Calculate products with high purchase frequency in recent period."""
    cutoff_date = datetime.now() - timedelta(days=days)
    recent_df = df[df['delivery_date'] >= cutoff_date]
    
    if recent_df.empty:
        return set()
    
    trending = recent_df.groupby('product_name').agg(
        customer_count=('customer_name', 'nunique'),
        order_count=('invoice_number', 'nunique')
    ).reset_index()
    
    trending_products = trending[
        (trending['customer_count'] >= 3) | (trending['order_count'] >= 5)
    ]['product_name'].tolist()
    
    return set(trending_products)

def _find_product_affinity(df):
    """Calculate which products are commonly bought together."""
    customer_products = df.groupby('customer_name')['product_name'].apply(set).to_dict()
    product_affinity = {}
    all_products = df['product_name'].unique()
    
    for product in all_products:
        customers_who_bought = [cust for cust, prods in customer_products.items() if product in prods]
        
        if len(customers_who_bought) < 2:
            product_affinity[product] = set()
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
        
        product_affinity[product] = set(affinity_scores.keys())
    
    return product_affinity

# ============================================================
# MAIN ROUTE HANDLER
# ============================================================
def route_planner():
    # 1. GET FILTERS
    selected_routes = request.args.getlist('route')
    selected_company = request.args.get('company', 'all')
    view_mode = request.args.get('view', 'action_only')  # 'action_only' or 'full_picture'

    all_routes_df = _query_db("SELECT DISTINCT route FROM sales_data WHERE route IS NOT NULL AND route != '' ORDER BY route")
    available_routes = all_routes_df['route'].tolist()
    
    if not selected_routes:
        selected_routes = [available_routes[0]] if available_routes else []
    
    # 2. FETCH DATA BASED ON FILTERS
    if not selected_routes:
        return render_template('route_planner.html', 
                             title="Route Planner", 
                             available_routes=available_routes, 
                             selected_routes=[], 
                             available_companies=[], 
                             customer_alerts=[], 
                             recovery_list=[],
                             cross_sell=[],
                             view_mode=view_mode)

    route_placeholders = ','.join('?' for r in selected_routes)
    query = f"SELECT * FROM sales_data WHERE route IN ({route_placeholders})"
    
    df = _query_db(query, selected_routes)
    
    available_companies = ['all'] + sorted(df['company'].unique().tolist())
    if selected_company != 'all':
        df = df[df['company'] == selected_company]

    # GET AVAILABLE PRODUCTS & TRENDING PRODUCTS
    available_products = _get_available_products()
    trending_products = _calculate_trending_products(df, days=30)
    product_affinity = _find_product_affinity(df)
    
    # GET CUSTOMER SCORES (for recovery section)
    try:
        customer_scores = get_customer_scores(include_all_columns=False)
    except Exception as e:
        print(f"⚠️ Could not load customer scores: {e}")
        customer_scores = pd.DataFrame()
    
    # ========================================
    # SECTION 1: CUSTOMER ACTION ITEMS
    # ========================================
    customer_alerts = []
    
    if not df.empty:
        today = datetime.now()
        patterns = _calculate_customer_patterns(df)
        
        if available_products is not None:
            patterns = patterns[patterns['product_name'].isin(available_products)]
        
        balances = df.drop_duplicates(subset='customer_name')[['customer_name', 'balance']]
        patterns = pd.merge(patterns, balances, on='customer_name', how='left')
        patterns['days_since_last'] = (today - patterns['last_purchase_date']).dt.days
        patterns['is_trending'] = patterns['product_name'].isin(trending_products)
        
        # Identify overdue products (excluding stock-ups and seasonal items)
        patterns['is_stock_up'] = patterns['last_purchase_qty'] > (patterns['median_qty'] * STOCK_UP_MULTIPLIER)
        patterns['is_seasonal'] = patterns['days_since_last'] > SEASONAL_CUTOFF_DAYS
        
        attention_needed = patterns[
            (~patterns['is_stock_up']) &
            (~patterns['is_seasonal']) &
            (patterns['days_since_last'] > (patterns['median_cycle'] * OVERDUE_MULTIPLIER)) &
            (patterns['purchase_count'] > 1)
        ].copy()
        
        attention_needed['priority'] = attention_needed.apply(
            lambda row: (row['days_since_last'] / row['median_cycle'] * (2 if row['is_trending'] else 1)) if row['median_cycle'] > 0 else 0,
            axis=1
        )
        
        attention_needed = attention_needed.sort_values('priority', ascending=False)
        
        # Group by customer
        for customer_name in df['customer_name'].unique():
            # Get customer's last order date and balance
            customer_last_order = df[df['customer_name'] == customer_name]['delivery_date'].max()
            days_since_last_order = (today - customer_last_order).days
            
            # FILTER BY VIEW MODE
            if view_mode == 'action_only' and days_since_last_order < ACTION_ITEMS_THRESHOLD:
                continue  # Skip customers who ordered recently
            
            customer_data = attention_needed[attention_needed['customer_name'] == customer_name]
            
            # Get customer balance safely
            balance_series = patterns[patterns['customer_name'] == customer_name]['balance']
            if not balance_series.empty:
                customer_balance = balance_series.iloc[0]
            else:
                customer_balance_df = balances[balances['customer_name'] == customer_name]['balance']
                customer_balance = customer_balance_df.iloc[0] if not customer_balance_df.empty else 0.0
            
            # Normalize balance (negative = 0)
            customer_balance = max(0, customer_balance)
            
            # Check if customer is credit-blocked
            is_credit_blocked = (customer_balance > CREDIT_BLOCK_THRESHOLD) and (days_since_last_order >= CREDIT_BLOCK_DAYS)
            
            # Get top 3 most overdue products
            product_alerts = []
            for _, row in customer_data.head(MAX_PRODUCTS_PER_CUSTOMER).iterrows():
                days_overdue = int(row['days_since_last'] - row['median_cycle'])
                product_alerts.append({
                    'product_name': row['product_name'],
                    'days_overdue': days_overdue,
                    'is_trending': row['is_trending']
                })
            
            # Add 1 trending product they used to buy (if not already in top 3)
            if product_alerts:  # Only if they have overdue items
                existing_products = {p['product_name'] for p in product_alerts}
                customer_bought_products = set(patterns[patterns['customer_name'] == customer_name]['product_name'].unique())
                trending_they_bought = trending_products & customer_bought_products - existing_products
                
                if trending_they_bought:
                    # Get the one with best purchase history
                    trending_candidates = patterns[
                        (patterns['customer_name'] == customer_name) & 
                        (patterns['product_name'].isin(trending_they_bought))
                    ].sort_values('purchase_count', ascending=False)
                    
                    if not trending_candidates.empty:
                        best_trending = trending_candidates.iloc[0]
                        product_alerts.append({
                            'product_name': best_trending['product_name'],
                            'days_overdue': int(best_trending['days_since_last'] - best_trending['median_cycle']),
                            'is_trending': True
                        })
            
            # Determine customer status for full picture view
            if days_since_last_order <= 15:
                status_group = 'active'
                status_label = '🟢 Active'
            elif days_since_last_order <= 60:
                status_group = 'drifting'
                status_label = '🟡 Drifting'
            else:
                status_group = 'lost'
                status_label = '🔴 Lost'
            
            # Only add customer if they have alerts
            if product_alerts:
                customer_alerts.append({
                    'name': customer_name,
                    'product_alerts': product_alerts,
                    'days_since_last_order': days_since_last_order,
                    'is_credit_blocked': is_credit_blocked,
                    'balance': customer_balance,
                    'status_group': status_group,
                    'status_label': status_label,
                    'priority': len(product_alerts) + (10 if is_credit_blocked else 0) + (5 if days_since_last_order > 60 else 0)
                })
        
        # Sort by priority
        customer_alerts.sort(key=lambda x: x['priority'], reverse=True)
    
    # ========================================
    # SECTION 2: RECOVERY FOLLOW-UPS
    # ========================================
    recovery_list = []
    
    if not customer_scores.empty and not df.empty:
        # Filter customers on selected routes
        customers_on_route = set(df['customer_name'].unique())
        route_scores = customer_scores[customer_scores['customer_name'].isin(customers_on_route)].copy()
        
        # Recovery criteria: C_Score <= 3 AND balance > 100 AND DSO > 21
        # But we need to calculate DSO ourselves since it might not be in the returned columns
        # Let's use the data we have
        
        # Get last order date for each customer
        last_orders = df.groupby('customer_name')['delivery_date'].max().reset_index()
        last_orders['days_since_last_invoice'] = (datetime.now() - last_orders['delivery_date']).dt.days
        
        # Merge with scores
        route_scores = pd.merge(route_scores, last_orders[['customer_name', 'days_since_last_invoice']], on='customer_name', how='left')
        
        # Filter for recovery candidates
        recovery_candidates = route_scores[
            (route_scores['C_Score'] <= 3) & 
            (route_scores['balance'] > CREDIT_BLOCK_THRESHOLD) &
            (route_scores['weeks_owing'] > 3)  # More than 3 weeks owing (21 days)
        ].copy()
        
        # Sort by worst cases first (highest weeks_owing)
        recovery_candidates = recovery_candidates.sort_values('weeks_owing', ascending=False)
        
        for _, row in recovery_candidates.iterrows():
            recovery_list.append({
                'customer_name': row['customer_name'],
                'days_since_invoice': int(row['days_since_last_invoice']) if pd.notna(row['days_since_last_invoice']) else 0,
                'balance': int(row['balance']),
                'c_score': int(row['C_Score']),
                'weeks_owing': round(row['weeks_owing'], 1),
                'dso_days': int(row['weeks_owing'] * 7)  # Convert weeks to days for display
            })
    
    # ========================================
    # SECTION 3: CROSS-SELL OPPORTUNITIES
    # ========================================
    cross_sell = []
    if not df.empty and available_products:
        customers_on_route = df['customer_name'].unique()

        for customer in customers_on_route:
            bought_products = set(df[df['customer_name'] == customer]['product_name'].unique())
            
            if not bought_products:
                continue
            
            related_products = set()
            for bought_prod in bought_products:
                if bought_prod in product_affinity:
                    related_products.update(product_affinity[bought_prod])
            
            related_products = related_products & available_products
            related_products = related_products - bought_products
            
            if not related_products:
                continue
            
            trending_related = [p for p in related_products if p in trending_products]
            non_trending_related = [p for p in related_products if p not in trending_products]
            
            route_popularity = df['product_name'].value_counts().to_dict()
            trending_related.sort(key=lambda x: route_popularity.get(x, 0), reverse=True)
            non_trending_related.sort(key=lambda x: route_popularity.get(x, 0), reverse=True)
            
            final_suggestions = trending_related + non_trending_related
            
            if final_suggestions:
                opportunities_with_tags = []
                for prod in final_suggestions[:3]:
                    is_hot = prod in trending_products
                    opportunities_with_tags.append({
                        'name': prod,
                        'is_trending': is_hot
                    })
                
                cross_sell.append({
                    'customer_name': customer,
                    'opportunities': opportunities_with_tags
                })

    return render_template('route_planner.html',
                           title='Route Planner',
                           available_routes=available_routes,
                           selected_routes=selected_routes,
                           available_companies=available_companies,
                           selected_company=selected_company,
                           customer_alerts=customer_alerts,
                           recovery_list=recovery_list,
                           cross_sell=cross_sell,
                           view_mode=view_mode)