# routes/profit_routes.py

import sqlite3
import pandas as pd
from flask import render_template
from pathlib import Path

# Define the path to the database file
DB_PATH = Path(__file__).resolve().parent.parent / "data" / "sales.db"

def profit():
    """
    Fetches profit and revenue data, aggregated by month, for display.
    """
    try:
        conn = sqlite3.connect(DB_PATH)
        query = """
            SELECT
                STRFTIME('%Y-%m', delivery_date) AS month,
                SUM(profit) AS total_profit,
                SUM(amount) AS total_revenue
            FROM sales_data
            WHERE delivery_date IS NOT NULL
            GROUP BY month
            ORDER BY month
        """
        profit_df = pd.read_sql_query(query, conn)
        conn.close()

        # Calculate overall totals
        total_profit = profit_df['total_profit'].sum()
        total_revenue = profit_df['total_revenue'].sum()

    except Exception as e:
        print(f"Database error in profit_routes: {e}")
        return render_template('error.html', message="Could not load profit data.")

    context = {
        "total_profit": f"{total_profit:,.0f}",
        "total_revenue": f"{total_revenue:,.0f}",
        "profit_by_month_labels": profit_df['month'].tolist(),
        "profit_by_month_values": profit_df['total_profit'].tolist(),
        "revenue_by_month_values": profit_df['total_revenue'].tolist()
    }

    return render_template('profit.html', **context)