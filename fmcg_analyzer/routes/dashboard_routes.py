# routes/dashboard_routes.py

import sqlite3
import pandas as pd
from flask import render_template, request
from pathlib import Path
from scoring import get_customer_scores

DB_PATH = Path(__file__).resolve().parent.parent / "data" / "sales.db"

def _query_db(query, params=()):
    """Helper function to execute database queries safely."""
    try:
        conn = sqlite3.connect(DB_PATH)
        df = pd.read_sql_query(query, conn, params=params)
        conn.close()
        return df
    except Exception as e:
        print(f"❌ Database query error: {e}")
        return pd.DataFrame()


def dashboard():
    """
    Main dashboard with proper handling of negative values.
    """
    
    start_date = request.args.get('start', '')
    end_date = request.args.get('end', '')
    
    date_filter = ""
    params = ()
    if start_date and end_date:
        date_filter = "WHERE delivery_date BETWEEN ? AND ?"
        params = (start_date, end_date)
    
    # ========================================
    # 1. Overall KPIs
    # ========================================
    kpi_query = f"""
        SELECT
            COALESCE(SUM(amount), 0) as total_sales,
            COALESCE(SUM(quantity), 0) as total_units,
            COUNT(DISTINCT invoice_number) as total_transactions,
            COUNT(DISTINCT customer_code) as total_customers
        FROM sales_data
        {date_filter}
    """
    kpi_df = _query_db(kpi_query, params)
    
    total_sales = int(kpi_df.iloc[0]['total_sales']) if not kpi_df.empty else 0
    total_units = int(kpi_df.iloc[0]['total_units']) if not kpi_df.empty else 0
    total_transactions = int(kpi_df.iloc[0]['total_transactions']) if not kpi_df.empty else 0
    total_customers = int(kpi_df.iloc[0]['total_customers']) if not kpi_df.empty else 0

    # ========================================
    # 2. Credit Balance
    # ========================================
    credit_query = """
        SELECT 
            customer_code,
            customer_name,
            MAX(balance) as balance,
            MAX(last_invoice_date) as last_invoice_date
        FROM sales_data
        GROUP BY customer_code, customer_name
        ORDER BY balance DESC
    """
    credit_df = _query_db(credit_query)
    
    outstanding_balance = int(credit_df['balance'].sum()) if not credit_df.empty else 0
    credit_customers = credit_df[credit_df['balance'] > 0].head(5).to_dict('records') if not credit_df.empty else []

    # ========================================
    # 3. Customer Scoring
    # ========================================
    scores_df = get_customer_scores()
    
    if not scores_df.empty:
        best_customers_count = len(scores_df[scores_df['RFM_Score'] >= 12])
        at_risk_customers_count = len(scores_df[scores_df['RFM_Score'] <= 6])
        avg_credit_score = round(scores_df['RFM_Score'].mean(), 1)
        
        # Risky customers = Low RFM + POSITIVE balance (they owe us)
        risky_df = scores_df[
            (scores_df['RFM_Score'] <= 7) &
            (scores_df['balance'] > 0)
        ].copy()
        
        if 'balance' not in risky_df.columns:
            risky_df['balance'] = 0
        else:
            risky_df['balance'] = risky_df['balance'].fillna(0)
        
        risky_df = risky_df.sort_values('balance', ascending=False).head(5)
        
        risky_customers = [
            {
                'customer_name': row['customer_name'],
                'credit_score': row['RFM_Score'],
                'balance': int(row['balance']),
                'days_since': int(row['recency'])
            }
            for _, row in risky_df.iterrows()
        ]
    else:
        best_customers_count = 0
        at_risk_customers_count = 0
        avg_credit_score = 0
        risky_customers = []

    high_risk_count = at_risk_customers_count
    growth_potential_count = best_customers_count
    neutral_count = max(0, len(scores_df) - high_risk_count - growth_potential_count) if not scores_df.empty else 0

    # ========================================
    # 4. Monthly Sales Trend
    # ========================================
    sales_trend_query = """
        SELECT 
            STRFTIME('%Y-%m', delivery_date) as month,
            SUM(amount) as sales
        FROM sales_data
        WHERE delivery_date IS NOT NULL
        GROUP BY month
        ORDER BY month
    """
    sales_trend_df = _query_db(sales_trend_query)
    
    if not sales_trend_df.empty:
        sales_trend_df['year'] = pd.to_datetime(sales_trend_df['month']).dt.year
        current_year = sales_trend_df['year'].max()
        last_year = current_year - 1
        
        current_year_df = sales_trend_df[sales_trend_df['year'] == current_year].copy()
        last_year_df = sales_trend_df[sales_trend_df['year'] == last_year].copy()
        
        current_year_df['month_name'] = pd.to_datetime(current_year_df['month']).dt.strftime('%b')
        last_year_df['month_name'] = pd.to_datetime(last_year_df['month']).dt.strftime('%b')
        
        all_months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
        current_dict = dict(zip(current_year_df['month_name'], current_year_df['sales']))
        last_dict = dict(zip(last_year_df['month_name'], last_year_df['sales']))
        
        monthly_sales_chart = {
            'labels': [m for m in all_months if m in current_dict or m in last_dict],
            'data': [current_dict.get(m, 0) for m in all_months if m in current_dict or m in last_dict],
            'data_last_year': [last_dict.get(m, 0) for m in all_months if m in current_dict or m in last_dict]
        }
    else:
        monthly_sales_chart = {'labels': [], 'data': [], 'data_last_year': []}

    # ========================================
    # 5. Orderbooker Performance
    # ========================================
    orderbooker_query = f"""
        SELECT 
            booker_name as orderbooker,
            SUM(amount) as net_amount
        FROM sales_data
        WHERE booker_name IS NOT NULL AND booker_name != ''
        {date_filter.replace('WHERE', 'AND') if date_filter else ''}
        GROUP BY booker_name
        ORDER BY net_amount DESC
    """
    orderbooker_df = _query_db(orderbooker_query, params)
    
    if not orderbooker_df.empty:
        orderbooker_sales = [
            {
                'orderbooker': row['orderbooker'],
                'net_amount': int(row['net_amount'])
            }
            for _, row in orderbooker_df.iterrows()
        ]
        
        top_8_df = orderbooker_df.head(8)
        sorted_orderbookers = {
            'labels': top_8_df['orderbooker'].tolist(),
            'data': top_8_df['net_amount'].tolist()
        }
    else:
        orderbooker_sales = []
        sorted_orderbookers = {'labels': [], 'data': []}

    # ========================================
    # 6. Company Sales Distribution
    # ========================================
    company_query = """
        SELECT 
            STRFTIME('%Y-%m', delivery_date) as month,
            company,
            SUM(amount) as sales
        FROM sales_data
        WHERE delivery_date IS NOT NULL AND company IS NOT NULL AND company != ''
        GROUP BY month, company
        ORDER BY month, sales DESC
    """
    company_df = _query_db(company_query)
    
    if not company_df.empty:
        unique_months = ['All'] + sorted(company_df['month'].unique().tolist())
        
        monthly_by_company_chart = []
        
        all_companies = company_df.groupby('company')['sales'].sum().sort_values(ascending=False)
        monthly_by_company_chart.append({
            'month': 'All',
            'labels': all_companies.index.tolist(),
            'data': all_companies.values.tolist()
        })
        
        for month in unique_months[1:]:
            month_data = company_df[company_df['month'] == month]
            if not month_data.empty:
                monthly_by_company_chart.append({
                    'month': month,
                    'labels': month_data['company'].tolist(),
                    'data': month_data['sales'].tolist()
                })
        
        company_months = unique_months
    else:
        monthly_by_company_chart = [{'month': 'All', 'labels': [], 'data': []}]
        company_months = ['All']

    # ========================================
    # Bundle Data for Template
    # ========================================
    summary = {
        "total_sales": total_sales,
        "total_units": total_units,
        "total_transactions": total_transactions,
        "total_customers": total_customers,
        "outstanding_balance": outstanding_balance,
        "avg_credit_score": avg_credit_score,
        "high_risk_count": high_risk_count,
        "growth_potential_count": growth_potential_count,
        "neutral_count": neutral_count,
        "risky_customers": risky_customers,
        "credit_customers": credit_customers,
        "monthly_sales_chart": monthly_sales_chart,
        "sorted_orderbookers": sorted_orderbookers,
        "monthly_by_company_chart": monthly_by_company_chart,
        "company_months": company_months,
        "orderbooker_sales": orderbooker_sales
    }

    return render_template('dashboard.html', 
                         summary=summary,
                         start_date=start_date,
                         end_date=end_date)


def update_database():
    """Trigger database update."""
    try:
        from data_loader import update_database as run_update
        success = run_update()
        
        if success:
            return "✅ Database updated successfully!"
        else:
            return "❌ Database update failed."
            
    except Exception as e:
        return f"❌ Error: {e}"