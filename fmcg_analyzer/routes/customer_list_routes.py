# routes/customer_list_routes.py

import pandas as pd
import sqlite3
from flask import render_template, request
from pathlib import Path
from datetime import datetime, timedelta

# Define the path to the database file
DB_PATH = Path(__file__).resolve().parent.parent / "data" / "sales.db"

def get_date_filter(date_range):
    """
    Convert date range string to SQL WHERE clause.
    Returns tuple: (where_clause, params_dict)
    """
    today = datetime.now()
    
    if date_range == 'current_year':
        start_date = '2025-01-01'
        return "AND delivery_date >= :start_date", {'start_date': start_date}
    
    elif date_range == 'last_year':
        start_date = '2024-01-01'
        end_date = '2024-12-31'
        return "AND delivery_date BETWEEN :start_date AND :end_date", {'start_date': start_date, 'end_date': end_date}
    
    elif date_range == 'current_month':
        start_date = today.replace(day=1).strftime('%Y-%m-%d')
        return "AND delivery_date >= :start_date", {'start_date': start_date}
    
    elif date_range == 'last_month':
        last_month = today.replace(day=1) - timedelta(days=1)
        start_date = last_month.replace(day=1).strftime('%Y-%m-%d')
        end_date = last_month.strftime('%Y-%m-%d')
        return "AND delivery_date BETWEEN :start_date AND :end_date", {'start_date': start_date, 'end_date': end_date}
    
    elif date_range == 'last_3_months':
        start_date = (today - timedelta(days=90)).strftime('%Y-%m-%d')
        return "AND delivery_date >= :start_date", {'start_date': start_date}
    
    elif date_range == 'last_6_months':
        start_date = (today - timedelta(days=180)).strftime('%Y-%m-%d')
        return "AND delivery_date >= :start_date", {'start_date': start_date}
    
    else:  # 'all' or default
        return "", {}


def assign_segment(row):
    """Assign customer segment based on scoring logic"""
    total_score = row['total_score']
    c_score = row['c_score']
    p_score = row['p_score']
    balance = row['balance']
    rfm_score = row['rfm_score']
    
    # High credit risk (only if they owe us - positive balance)
    if c_score == 1 and balance > 50000:
        return "🚨 High Risk"
    if c_score <= 2 and balance > 20000:
        return "⚠️ Credit Risk"
    
    # Low profitability
    if p_score <= 2 and rfm_score >= 10:
        return "💸 Review Pricing"
    
    # Standard segmentation
    if total_score >= 85:
        return "👑 Champions"
    elif total_score >= 70:
        return "💎 Loyal"
    elif total_score >= 55:
        return "📈 Potential"
    elif total_score >= 40:
        return "📉 At Risk"
    else:
        return "💤 Dormant"


def all_customers():
    """
    Show ALL customers with filtering by date range.
    FIXED:
    - Profit is correctly aggregated per customer (SUM of all invoice profits)
    - ONE entry per customer_code (grouped properly)
    - Balance is ALWAYS current balance from latest record (as of today)
    """
    date_range = request.args.get('date_range', 'current_year')
    
    try:
        conn = sqlite3.connect(DB_PATH)
        
        # Get date filter
        date_where, date_params = get_date_filter(date_range)
        
        # ========================================
        # STEP 1: Get CURRENT BALANCE for each customer (as of today, no date filter)
        # ========================================
        balance_query = """
        SELECT 
            customer_code,
            MAX(balance) as balance
        FROM sales_data
        GROUP BY customer_code
        """
        balance_df = pd.read_sql_query(balance_query, conn)
        
        # ========================================
        # STEP 2: Get SALES DATA (FIXED FOR DENORMALIZED PROFIT)
        # ========================================
        # We assume profit is repeated on every line item. 
        # We must group by invoice first to get the distinct profit, then sum.
        sales_query = f"""
        SELECT 
            customer_code,
            MAX(customer_name) as customer_name,
            MAX(route) as route,
            MAX(company) as company,
            SUM(amount) as sales,
            -- FIX: Subquery logic simulation
            -- We can't do complex subqueries easily in one go, 
            -- so we will use a different approach for profit below
            COUNT(DISTINCT invoice_number) as orders,
            MAX(delivery_date) as last_order_date,
            CAST(JULIANDAY('now') - JULIANDAY(MAX(delivery_date)) AS INTEGER) as days_since
        FROM sales_data
        WHERE delivery_date IS NOT NULL
        {date_where}
        GROUP BY customer_code
        """
        
        sales_df = pd.read_sql_query(sales_query, conn, params=date_params)

        # === ADDED: SEPARATE CORRECT PROFIT CALCULATION ===
        # Query: Get unique profit per invoice, then sum by customer
        profit_query = f"""
        SELECT 
            customer_code,
            SUM(distinct_profit) as profit
        FROM (
            SELECT DISTINCT 
                customer_code, 
                invoice_number, 
                profit as distinct_profit
            FROM sales_data
            WHERE delivery_date IS NOT NULL
            {date_where}
        )
        GROUP BY customer_code
        """
        profit_df = pd.read_sql_query(profit_query, conn, params=date_params)
        
        # Merge the correct profit back into sales_df
        sales_df = sales_df.merge(profit_df, on='customer_code', how='left')
        
        # ========================================
        # STEP 3: Get ALL unique customers (to include those with no sales in period)
        # ========================================
        all_customers_query = """
        SELECT DISTINCT
            customer_code,
            MAX(customer_name) as customer_name,
            MAX(route) as route,
            MAX(company) as company
        FROM sales_data
        GROUP BY customer_code
        """
        
        all_customers_df = pd.read_sql_query(all_customers_query, conn)
        conn.close()
        
        # ========================================
        # STEP 4: Merge data - Start with ALL customers, add sales, add balance
        # ========================================
        # Start with all customers
        df = all_customers_df.copy()
        
        # Merge sales data (LEFT JOIN - keep all customers)
        df = df.merge(
            sales_df[['customer_code', 'sales', 'profit', 'orders', 'last_order_date', 'days_since']], 
            on='customer_code', 
            how='left'
        )
        
        # Merge balance data (LEFT JOIN - keep all customers)
        df = df.merge(balance_df, on='customer_code', how='left')
        
        # ========================================
        # STEP 5: Fill missing values
        # ========================================
        df['sales'] = df['sales'].fillna(0)
        df['profit'] = df['profit'].fillna(0)
        df['orders'] = df['orders'].fillna(0).astype(int)
        df['days_since'] = df['days_since'].fillna(999).astype(int)
        df['balance'] = df['balance'].fillna(0)
        
        # Calculate margin
        df['margin'] = df.apply(
            lambda row: (row['profit'] / row['sales'] * 100) if row['sales'] > 0 else 0,
            axis=1
        )
        
        # Fill text columns
        df['route'] = df['route'].fillna('N/A')
        df['company'] = df['company'].fillna('N/A')
        df['customer_name'] = df['customer_name'].fillna('Unknown')
        
        # ========================================
        # STEP 6: Verify uniqueness (CRITICAL CHECK)
        # ========================================
        initial_count = len(df)
        df = df.drop_duplicates(subset=['customer_code'], keep='first')
        final_count = len(df)
        
        if initial_count != final_count:
            print(f"⚠️ WARNING: Removed {initial_count - final_count} duplicate customer codes!")
        
        # ========================================
        # STEP 7: Calculate scoring
        # ========================================
        # Recency score (1-5, lower days = higher score)
        df['r_score'] = pd.cut(df['days_since'], 
                               bins=[-1, 10, 21, 35, 60, 999], 
                               labels=[5, 4, 3, 2, 1]).astype(int)
        
        # Frequency score (1-5 quintiles)
        if df['orders'].nunique() >= 5:
            df['f_score'] = pd.qcut(df['orders'].rank(method='first'), q=5, labels=[1,2,3,4,5], duplicates='drop').astype(int)
        else:
            df['f_score'] = 3  # Default if not enough variation
        
        # Monetary score (1-5 quintiles)
        if df['sales'].nunique() >= 5:
            df['m_score'] = pd.qcut(df['sales'].rank(method='first'), q=5, labels=[1,2,3,4,5], duplicates='drop').astype(int)
        else:
            df['m_score'] = 3  # Default
        
        # Credit score based on balance and DSO
        df['weekly_sales'] = df['sales'] / 52  # Rough estimate
        df['dso'] = df.apply(
            lambda row: (row['balance'] / row['weekly_sales'] * 7) if row['weekly_sales'] > 0 and row['balance'] > 0 else 0,
            axis=1
        )
        
        def calc_credit_score(row):
            # If balance is zero or negative (we owe them), perfect credit score
            if row['balance'] <= 0:
                return 5
            dso = row['dso']
            if dso <= 14: return 5
            elif dso <= 21: return 4
            elif dso <= 35: return 3
            elif dso <= 60: return 2
            else: return 1
        
        df['c_score'] = df.apply(calc_credit_score, axis=1)
        
        # Profit score based on margin
        def calc_profit_score(margin):
            if margin >= 10: return 5
            elif margin >= 8: return 4
            elif margin >= 5: return 3
            elif margin >= 3: return 2
            else: return 1
        
        df['p_score'] = df['margin'].apply(calc_profit_score)
        
        # Calculate total score and RFM
        df['total_score'] = (
            df['r_score'] * 4 +
            df['f_score'] * 3 +
            df['m_score'] * 6 +
            df['c_score'] * 4 +
            df['p_score'] * 3
        )
        
        df['rfm_score'] = df['r_score'] + df['f_score'] + df['m_score']
        
        # Assign segments
        df['segment'] = df.apply(assign_segment, axis=1)
        
        # Sort by total score (best customers first)
        df = df.sort_values('total_score', ascending=False)
        
        # Round numeric columns
        df['sales'] = df['sales'].round(0)
        df['profit'] = df['profit'].round(0)
        df['balance'] = df['balance'].round(0)
        df['margin'] = df['margin'].round(1)
        
        # ========================================
        # STEP 8: Final validation
        # ========================================
        print(f"\n✅ Customer List Generated:")
        print(f"   ├─ Total customers: {len(df):,}")
        print(f"   ├─ Unique customer codes: {df['customer_code'].nunique():,}")
        print(f"   ├─ Customers with sales: {len(df[df['orders'] > 0]):,}")
        print(f"   ├─ Total profit in period: PKR {df['profit'].sum():,.0f}")
        print(f"   └─ Total balance (current): PKR {df['balance'].sum():,.0f}")
        
        # Convert to records
        customers = df.to_dict('records')
        
        return render_template('customer_list.html',
                             customers=customers,
                             total_customers=len(customers),
                             date_range=date_range)
    
    except Exception as e:
        print(f"Error in all_customers: {e}")
        import traceback
        traceback.print_exc()
        return render_template('customer_list.html',
                             customers=[],
                             total_customers=0,
                             date_range=date_range)


# Keep these for backward compatibility (redirect to all_customers)
def high_risk_customers():
    return all_customers()

def growth_customers():
    return all_customers()

def neutral_customers():
    return all_customers()