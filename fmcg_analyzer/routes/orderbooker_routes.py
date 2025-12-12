# ============================================================
# routes/orderbooker_routes.py
# ============================================================

import sqlite3
import pandas as pd
from flask import render_template, request
from pathlib import Path
from datetime import datetime
from dateutil.relativedelta import relativedelta

DB_PATH = Path(__file__).resolve().parent.parent / "data" / "sales.db"

# ============================================================
# orderbooker_details function
# ============================================================

def orderbooker_details(name):
    """
    Provides a detailed sales performance dashboard for a specific order booker,
    using the layout from 'orderbooker_details.html'.
    """
    try:
        conn = sqlite3.connect(DB_PATH)

        # 1. Get date range from query parameters, default to current month
        today = datetime.now()
        start_date_str = request.args.get('start', today.replace(day=1).strftime('%Y-%m-%d'))
        end_date_str = request.args.get('end', today.strftime('%Y-%m-%d'))
        display_mode = request.args.get('display', 'cards')

        # Convert to datetime objects for calculations
        start_date = datetime.strptime(start_date_str, '%Y-%m-%d')
        end_date = datetime.strptime(end_date_str, '%Y-%m-%d')

        # 2. Define comparison periods
        # Last Month
        last_month_end = start_date - relativedelta(days=1)
        last_month_start = last_month_end.replace(day=1)
        
        # Last Year (Same Month)
        last_year_start = start_date - relativedelta(years=1)
        last_year_end = (last_year_start.replace(day=1) + relativedelta(months=1)) - relativedelta(days=1)
        
        # Helper function to run fast, aggregated queries
        def get_sales_data(start, end, booker):
            # Query for total sales
            query_total = """
                SELECT SUM(amount) FROM sales_data
                WHERE booker_name = ? AND delivery_date BETWEEN ? AND ?
            """
            total_sales_result = conn.execute(query_total, (booker, start.strftime('%Y-%m-%d'), end.strftime('%Y-%m-%d'))).fetchone()
            total_sales = total_sales_result[0] if total_sales_result and total_sales_result[0] is not None else 0

            # Query for top company
            query_top_co = f"""
                SELECT company
                FROM sales_data
                WHERE booker_name = ? AND delivery_date BETWEEN ? AND ?
                GROUP BY company
                ORDER BY SUM(amount) DESC
                LIMIT 1
            """
            top_co_result = conn.execute(query_top_co, (booker, start.strftime('%Y-%m-%d'), end.strftime('%Y-%m-%d'))).fetchone()
            top_company = top_co_result[0] if top_co_result else 'N/A'
            
            return total_sales, top_company

        # 3. Get sales data for all three periods
        current_sales, top_company_current = get_sales_data(start_date, end_date, name)
        last_month_sales, top_company_last = get_sales_data(last_month_start, last_month_end, name)
        last_year_sales, top_company_last_year = get_sales_data(last_year_start, last_year_end, name)

        # 4. Calculate growth percentages
        def calc_growth(current, previous):
            if previous == 0 or previous is None:
                return 100.0 if current > 0 else 0.0
            return ((current - previous) / abs(previous)) * 100 # Use abs() for safety

        growth_vs_last_month = calc_growth(current_sales, last_month_sales)
        growth_vs_last_year = calc_growth(current_sales, last_year_sales)

        # 5. Get breakdowns for the *filtered period*
        # Company Breakdown
        company_query = """
            SELECT company, SUM(amount) as sales
            FROM sales_data
            WHERE booker_name = ? AND delivery_date BETWEEN ? AND ?
            GROUP BY company
            ORDER BY sales DESC
        """
        company_df = pd.read_sql_query(company_query, conn, params=(name, start_date_str, end_date_str))
        company_breakdown = company_df.set_index('company')['sales'].to_dict()

        # Route Breakdown
        route_query = """
            SELECT route, SUM(amount) as sales
            FROM sales_data
            WHERE booker_name = ? AND delivery_date BETWEEN ? AND ?
            GROUP BY route
            ORDER BY sales DESC
        """
        route_df = pd.read_sql_query(route_query, conn, params=(name, start_date_str, end_date_str))
        route_breakdown = route_df.set_index('route')['sales'].to_dict()
        
        # 6. Get Top 10 Customers for the *filtered period*
        top_cust_query = """
            SELECT customer_code, customer_name, SUM(amount) as net_amount
            FROM sales_data
            WHERE booker_name = ? AND delivery_date BETWEEN ? AND ?
            GROUP BY customer_code, customer_name
            ORDER BY net_amount DESC
            LIMIT 10
        """
        top_customers_df = pd.read_sql_query(top_cust_query, conn, params=(name, start_date_str, end_date_str))
        top_customers = top_customers_df.to_dict('records')

        # ============================================================
        # 7. Get Day-wise Profit for the *filtered period* (CORRECTED)
        # ============================================================
        
        # This subquery gets the profit ONCE per invoice, not per product line
        sub_query = """
            SELECT DISTINCT delivery_date, invoice_number, profit
            FROM sales_data
            WHERE booker_name = ? AND delivery_date BETWEEN ? AND ?
        """
        
        # Query for the day-wise list
        profit_query = f"""
            SELECT 
                delivery_date, 
                SUM(profit) as total_profit
            FROM ({sub_query}) AS unique_invoices
            GROUP BY delivery_date
            ORDER BY delivery_date DESC
        """
        profit_df = pd.read_sql_query(profit_query, conn, params=(name, start_date_str, end_date_str))
        
        # Query for the total profit card
        total_profit_query = f"""
            SELECT SUM(profit)
            FROM ({sub_query}) AS unique_invoices
        """
        total_profit_result = conn.execute(total_profit_query, (name, start_date_str, end_date_str)).fetchone()
        total_profit_for_period = int(total_profit_result[0]) if total_profit_result and total_profit_result[0] is not None else 0
        
        # Format for the template
        day_wise_profit_list = [
            {'date': row['delivery_date'], 'profit': row['total_profit']}
            for index, row in profit_df.iterrows()
        ]

        conn.close() 

        # 8. Create the context dictionary your template expects
        context = {
            "title": f"{name} Dashboard",
            "name": name,
            "start_date": start_date_str,
            "end_date": end_date_str,
            "display_mode": display_mode,
            
            "filtered_sales": current_sales, # Main sales number for the filtered period
            
            "current_sales": current_sales,
            "last_month_sales": last_month_sales,
            "last_year_sales": last_year_sales,
            
            "current_month_name": start_date.strftime('%B %Y'), # Simplified name
            "last_month_name": last_month_start.strftime('%B %Y'),
            "last_year_month_name": last_year_start.strftime('%B %Y'),
            
            "top_company_current": top_company_current,
            "top_company_last": top_company_last,
            "top_company_last_year": top_company_last_year,
            
            "growth_vs_last_month": growth_vs_last_month,
            "growth_vs_last_year": growth_vs_last_year,
            
            "day_wise_profit_list": day_wise_profit_list,

            "total_profit_for_period": total_profit_for_period,
        
            "company_breakdown": company_breakdown,
            "route_breakdown": route_breakdown,
            "top_customers": top_customers
        }
        
    except Exception as e:
        print(f"❌ Database error in orderbooker_routes (details): {e}")
        return render_template('error.html', 
                             message=f"Could not load performance data for {name}. Error: {e}")

    # Render the correct template with the full context
    return render_template('orderbooker_details.html', **context)