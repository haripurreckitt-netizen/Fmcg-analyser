# routes/credit_list_routes.py

import sqlite3
import pandas as pd
from flask import render_template, request
from pathlib import Path
from datetime import datetime

DB_PATH = Path(__file__).resolve().parent.parent / "data" / "sales.db"

def credit_list():
    """Credit List Route - Displays customer balances with RFM scoring"""
    route_filter = request.args.get('route', 'all')
    sort_by = request.args.get('sort', 'balance')
    order = request.args.get('order', 'desc')
    
    try:
        conn = sqlite3.connect(DB_PATH, detect_types=sqlite3.PARSE_DECLTYPES)

        # Latest snapshot per customer (avoid MAX mistakes)
        credit_query = """
            SELECT 
                customer_code,
                customer_name,
                balance,
                route
            FROM (
                SELECT 
                    customer_code,
                    customer_name,
                    balance,
                    route,
                    delivery_date,
                    ROW_NUMBER() OVER (PARTITION BY customer_code ORDER BY delivery_date DESC) AS rn
                FROM sales_data
            ) AS t
            WHERE rn = 1
        """
        credit_df = pd.read_sql_query(credit_query, conn)
        print(f"📊 Loaded {len(credit_df)} unique customers from database")

        # Build invoice-level correctness, then aggregate per customer
        sales_query = """
            SELECT 
                customer_code,
                invoice_number,
                delivery_date,
                amount,
                quantity,
                profit
            FROM sales_data
        """
        all_sales = pd.read_sql_query(sales_query, conn, parse_dates=['delivery_date'])

        # One row per invoice (profit is per-invoice; avoid double counting)
        invoice_level = all_sales.groupby(['customer_code', 'invoice_number']).agg({
            'delivery_date': 'max',
            'amount': 'sum',
            'quantity': 'sum',
            'profit': 'first'
        }).reset_index()

        # Customer-level aggregation
        sales_agg_df = invoice_level.groupby('customer_code').agg({
            'delivery_date': 'max',
            'amount': 'sum',
            'quantity': 'sum',
            'profit': 'sum',
            'invoice_number': 'count'
        }).reset_index()

        sales_agg_df.rename(columns={
            'delivery_date': 'last_sale_date',
            'amount': 'net_amount',
            'quantity': 'total_quantity',
            'profit': 'total_profit',
            'invoice_number': 'invoice_count'
        }, inplace=True)

        print(f"📊 Aggregated sales for {len(sales_agg_df)} customers")
        print(f"💰 Total profit (corrected): PKR {sales_agg_df['total_profit'].sum():,.0f}")

        # Merge Sales INTO Credit (Left Join)
        final_df = pd.merge(
            credit_df,
            sales_agg_df,
            on='customer_code',
            how='left'
        )

        # Days since last activity
        today = datetime.now()
        final_df['days_since'] = (today - final_df['last_sale_date']).dt.days
        final_df['days_since'] = final_df['days_since'].fillna(999).astype(int)

        # RFM scoring merge
        try:
            from scoring import get_customer_scores
            scores_df = get_customer_scores(include_all_columns=False)

            if not scores_df.empty:
                score_cols = [
                    'customer_code', 
                    'RFM_Score', 
                    'Segment', 
                    'recency',
                    'frequency',
                    'monetary_value',
                    # if your scoring also returns total_profit, include it safely
                    'total_profit'
                ]
                cols_to_merge = [col for col in score_cols if col in scores_df.columns]

                final_df = pd.merge(
                    final_df, 
                    scores_df[cols_to_merge], 
                    on='customer_code', 
                    how='left',
                    suffixes=('', '_score')
                )

                # Rename RFM_Score -> credit_score
                final_df.rename(columns={'RFM_Score': 'credit_score'}, inplace=True)

                # Prefer scoring frequency if present
                if 'frequency' in final_df.columns:
                    final_df['invoice_count'] = final_df['frequency'].fillna(final_df['invoice_count'])

                # Prefer scoring monetary_value (only if non-null)
                if 'monetary_value' in final_df.columns:
                    final_df['net_amount'] = final_df.apply(
                        lambda row: row['monetary_value'] if pd.notnull(row['monetary_value']) else row['net_amount'],
                        axis=1
                    )

                # If scoring has total_profit, prefer it only when non-null
                if 'total_profit_score' in final_df.columns:
                    final_df['total_profit'] = final_df.apply(
                        lambda row: row['total_profit_score'] if pd.notnull(row['total_profit_score']) else row['total_profit'],
                        axis=1
                    )
                elif 'total_profit' in final_df.columns:
                    # keep existing total_profit from aggregation; nothing to do
                    pass

                # Fill defaults
                final_df['credit_score'] = final_df['credit_score'].fillna(0)
                final_df['Segment'] = final_df['Segment'].fillna('UNKNOWN')

                # Recency preference + fallback
                if 'recency' in final_df.columns:
                    final_df['recency'] = final_df['recency'].fillna(final_df['days_since'])
                else:
                    final_df['recency'] = final_df['days_since']
            else:
                raise Exception("scores_df was empty")

        except Exception as e:
            print(f"⚠️ Warning: Could not load scores - {e}. Using defaults.")
            final_df['credit_score'] = 0.0
            final_df['Segment'] = 'UNKNOWN'
            final_df['recency'] = final_df['days_since']

        # Cleaning and types
        final_df['net_amount'] = final_df['net_amount'].fillna(0).astype(int)
        final_df['total_profit'] = final_df['total_profit'].fillna(0).astype(int)
        final_df['total_quantity'] = final_df['total_quantity'].fillna(0).astype(int)
        final_df['invoice_count'] = final_df['invoice_count'].fillna(0).astype(int)
        final_df['balance'] = final_df['balance'].fillna(0).round(0).astype(int)
        final_df['route'] = final_df['route'].fillna('N/A')

        # Filtering
        if route_filter != 'all':
            final_df = final_df[final_df['route'] == route_filter]

        # Sorting
        ascending = (order == 'asc')
        if sort_by in final_df.columns:
            final_df = final_df.sort_values(by=sort_by, ascending=ascending)
        else:
            final_df = final_df.sort_values(by='balance', ascending=False)

        # Routes dropdown
        all_routes_df = pd.read_sql_query(
            "SELECT DISTINCT route FROM sales_data WHERE route IS NOT NULL AND route != ''", 
            conn
        )
        routes = ['all'] + sorted(all_routes_df['route'].unique().tolist())

        conn.close()

        # Summary stats
        total_outstanding = int(final_df['balance'].sum())
        total_profit = int(final_df['total_profit'].sum())
        customers_owing_us = len(final_df[final_df['balance'] > 0])
        customers_we_owe = len(final_df[final_df['balance'] < 0])

        print(f"\n✅ Credit List Summary:")
        print(f"   ├─ Total customers: {len(final_df)}")
        print(f"   ├─ Owing us: {customers_owing_us}")
        print(f"   ├─ We owe: {customers_we_owe}")
        print(f"   ├─ Net outstanding: PKR {total_outstanding:,}")
        print(f"   └─ Total profit: PKR {total_profit:,}")

    except Exception as e:
        print(f"❌ Database error in credit_list_routes: {e}")
        import traceback
        traceback.print_exc()
        return render_template('error.html', message="Could not load credit list.")

    return render_template(
        'credit_list.html',
        title='Credit List',
        customers=final_df.to_dict('records'),
        total_outstanding=total_outstanding,
        total_profit=total_profit,
        customers_owing_us=customers_owing_us,
        customers_we_owe=customers_we_owe,
        routes=routes,
        route_filter=route_filter,
        sort_by=sort_by,
        order=order
    )
