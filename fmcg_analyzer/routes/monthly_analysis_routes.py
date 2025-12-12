# routes/monthly_analysis_routes.py

import sqlite3
import pandas as pd
import numpy as np
from flask import render_template, request
from pathlib import Path
from datetime import datetime
from dateutil.relativedelta import relativedelta

DB_PATH = Path(__file__).resolve().parent.parent / "data" / "sales.db"

def _query_db(query, params=()):
    """Helper function to execute database queries safely."""
    try:
        conn = sqlite3.connect(DB_PATH)
        df = pd.read_sql_query(query, conn, params=params, parse_dates=['delivery_date'])
        conn.close()
        return df
    except Exception as e:
        print(f"❌ Database query error in monthly_analysis: {e}")
        return pd.DataFrame()

def monthly_analysis():
    """
    Provides a detailed monthly analysis with corrected profit calculations.
    """
    
    # 1. GET FILTERS AND CALCULATE TIME PERIODS
    # =================================================
    all_months_df = _query_db("SELECT DISTINCT STRFTIME('%Y-%m', delivery_date) as month FROM sales_data ORDER BY month DESC")
    available_months = all_months_df['month'].dropna().tolist()
    if not available_months:
        return render_template('error.html', message="No data available for monthly analysis.")

    selected_month_str = request.args.get('month', available_months[0])
    route_filter = request.args.get('route', 'all')
    selected_month = datetime.strptime(selected_month_str, '%Y-%m')
    today = datetime.now()

    is_current_month = selected_month.year == today.year and selected_month.month == today.month
    day_of_month = today.day if is_current_month else (selected_month + relativedelta(months=1) - relativedelta(days=1)).day
    pro_rata_message = f"(First {day_of_month} Days)" if is_current_month else ""

    current_month_end_date = today if is_current_month else selected_month + relativedelta(months=1) - relativedelta(days=1)
    
    current_month_range = (selected_month.strftime('%Y-%m-01'), current_month_end_date.strftime('%Y-%m-%d'))

    # === FIX START ===
    # Calculate the end date for last year, respecting shorter months (e.g., Feb)
    prev_year_month_base = (selected_month - relativedelta(years=1))
    last_day_of_prev_year_month = (prev_year_month_base + relativedelta(months=1) - relativedelta(days=1)).day
    safe_day_ly = min(day_of_month, last_day_of_prev_year_month)
    last_year_end_date = prev_year_month_base.replace(day=safe_day_ly)
    last_year_range = (prev_year_month_base.strftime('%Y-%m-01'), last_year_end_date.strftime('%Y-%m-%d'))

    # Calculate the end date for last month, respecting shorter months (this was the crash)
    prev_month_base = (selected_month - relativedelta(months=1))
    last_day_of_prev_month = (prev_month_base + relativedelta(months=1) - relativedelta(days=1)).day
    safe_day_lm = min(day_of_month, last_day_of_prev_month)
    last_month_end_date = prev_month_base.replace(day=safe_day_lm)
    last_month_range = (prev_month_base.strftime('%Y-%m-01'), last_month_end_date.strftime('%Y-%m-%d'))


    # 2. FETCH DATA & ROUTES
    # =================================================
    all_routes_df = _query_db("SELECT DISTINCT route FROM sales_data WHERE route IS NOT NULL AND route != '' ORDER BY route")
    available_routes = ['all'] + all_routes_df['route'].tolist()

    query = "SELECT delivery_date, invoice_number, booker_name, customer_name, company, amount, profit, route FROM sales_data WHERE delivery_date BETWEEN ? AND ?"
    current_month_df_unfiltered = _query_db(query, current_month_range)
    last_month_df = _query_db(query, last_month_range)
    last_year_df = _query_db(query, last_year_range)

    if route_filter != 'all':
        current_month_df = current_month_df_unfiltered[current_month_df_unfiltered['route'] == route_filter].copy()
    else:
        current_month_df = current_month_df_unfiltered.copy()

    # 3. CALCULATE KPIs AND MAIN TABLES
    # =================================================
    sales_current = int(current_month_df_unfiltered['amount'].sum())
    sales_last_month = int(last_month_df['amount'].sum())
    sales_last_year = int(last_year_df['amount'].sum())
    
    orderbooker_sales = current_month_df_unfiltered.groupby('booker_name')['amount'].sum().reset_index().sort_values('amount', ascending=False)
    orderbooker_sales['share'] = (orderbooker_sales['amount'] / sales_current * 100) if sales_current else 0

    company_sales_df = pd.merge(current_month_df_unfiltered.groupby('company')['amount'].sum().rename('sales_current'), 
                                last_year_df.groupby('company')['amount'].sum().rename('sales_last_year'), 
                                on='company', how='outer').fillna(0).reset_index()
    company_sales_df['change'] = ((company_sales_df['sales_current'] - company_sales_df['sales_last_year']) / company_sales_df['sales_last_year'] * 100).replace([np.inf, -np.inf], 0)
    company_sales_df = company_sales_df.sort_values('sales_current', ascending=False)
        
    top_customers_by_company = {company: group.groupby('customer_name')['amount'].sum().nlargest(5).reset_index().to_dict('records') 
                                for company, group in current_month_df_unfiltered.groupby('company')}

    # 4. PREPARE DATA FOR DAILY SALES CHART & CUSTOMER TABLES
    # =================================================
    days_in_period = day_of_month
    all_days = list(range(1, days_in_period + 1))

    # Group total sales by day of month (across all routes)
    current_daily = current_month_df_unfiltered.groupby(current_month_df_unfiltered['delivery_date'].dt.day)['amount'].sum()
    last_year_daily = last_year_df.groupby(last_year_df['delivery_date'].dt.day)['amount'].sum()

    # Build daily sales data for bars
    daily_sales = [int(current_daily.get(day, 0)) for day in all_days]
    last_year_sales = [int(last_year_daily.get(day, 0)) for day in all_days]

    # Calculate cumulative totals
    cumulative_current = [sum(daily_sales[:i+1]) for i in range(len(daily_sales))]
    cumulative_last_year = [sum(last_year_sales[:i+1]) for i in range(len(last_year_sales))]

    # Build daily chart data for bar chart with cumulative line
    daily_chart_data = {
        'labels': all_days,
        'datasets': [
            {
                'label': '2024 Sales',
                'data': last_year_sales,
                'backgroundColor': '#d1d5db',  # Light gray for last year bars
                'type': 'bar'
            },
            {
                'label': '2025 Sales',
                'data': daily_sales,
                'backgroundColor': '#6366f1',  # Blue for this year bars
                'type': 'bar'
            },
            {
                'label': '2024 Total',
                'data': cumulative_last_year,
                'borderColor': '#9ca3af',  # Darker gray for last year line
                'backgroundColor': 'rgba(156, 163, 175, 0.2)',
                'fill': False,  # Corrected from 'false' to 'False'
                'type': 'line',
                'borderWidth': 2,
                'pointRadius': 4
            },
            {
                'label': '2025 Total',
                'data': cumulative_current,
                'borderColor': '#3b82f6',  # Darker blue for this year line
                'backgroundColor': 'rgba(59, 130, 246, 0.2)',
                'fill': False,  # Corrected from 'false' to 'False'
                'type': 'line',
                'borderWidth': 2,
                'pointRadius': 4
            }
        ]
    }

    # ==================== FIX STARTS HERE ====================
    # B. Customer Performance Tables (Corrected Profit Logic)
    
    # First, calculate total sales per customer (this is correct as is)
    customer_sales = current_month_df.groupby('customer_name')['amount'].sum().reset_index()
    customer_sales.rename(columns={'amount': 'total_sales'}, inplace=True)

    # Next, calculate total profit correctly by using unique invoices
    # 1. Drop duplicate invoices to ensure we only count profit once per invoice
    unique_invoices_df = current_month_df.drop_duplicates(subset=['invoice_number'])
    # 2. Now, group by customer and sum the profit from the unique invoices
    customer_profit = unique_invoices_df.groupby('customer_name')['profit'].sum().reset_index()
    customer_profit.rename(columns={'profit': 'total_profit'}, inplace=True)

    # Finally, merge sales and correct profit summaries together
    customer_summary_df = pd.merge(customer_sales, customer_profit, on='customer_name', how='left').fillna(0)
    
    # ===================== FIX ENDS HERE =====================

    top_sales_customers = customer_summary_df.nlargest(10, 'total_sales')
    top_profit_customers = customer_summary_df.nlargest(10, 'total_profit')
    least_profit_customers = customer_summary_df.nsmallest(10, 'total_profit')


    # 5. BUNDLE ALL DATA FOR TEMPLATE
    # =================================================
    analysis_data = {
        'selected_month': selected_month.strftime('%B %Y'), 'pro_rata_message': pro_rata_message,
        'sales_current': sales_current, 'sales_last_month': sales_last_month, 'sales_last_year': sales_last_year,
        'orderbooker_sales': orderbooker_sales.to_dict('records'), 'company_sales': company_sales_df.to_dict('records'),
        'top_customers_by_company': top_customers_by_company, 'daily_chart': daily_chart_data,
        'top_sales_customers': top_sales_customers.to_dict('records'),
        'top_profit_customers': top_profit_customers.to_dict('records'),
        'least_profit_customers': least_profit_customers.to_dict('records')
    }

    return render_template('monthly_analysis.html',
                           title='Monthly Analysis',
                           analysis=analysis_data,
                           available_months=available_months,
                           selected_month_str=selected_month_str,
                           available_routes=available_routes,
                           route_filter=route_filter)