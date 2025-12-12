"""
Customer Scoring Module - RFMCP Analysis
FIXED: Profit calculation now correctly handles invoice-level profit

Key Changes:
1. Query now gets DISTINCT invoice_number + profit pairs
2. Profit is aggregated at invoice level BEFORE customer grouping
3. Comments explain why this matters
"""

import sqlite3
import pandas as pd
import numpy as np
from pathlib import Path

DB_PATH = Path(__file__).resolve().parent / "data" / "sales.db"

# ============================================================
# SCORING CONFIGURATION (Weekly Visit Cycle)
# ============================================================

VISIT_CYCLE_DAYS = 7
DSO_EXCELLENT = 14
DSO_GOOD = 21
DSO_FAIR = 35
DSO_POOR = 60
DSO_CRITICAL = 90

RECENCY_RECENT = 10
RECENCY_NORMAL = 21
RECENCY_CONCERN = 35
RECENCY_RISK = 60
RECENCY_LOST = 90

MARGIN_EXCELLENT = 10
MARGIN_GOOD = 8
MARGIN_FAIR = 5
MARGIN_LOW = 3

# ============================================================
# MAIN SCORING FUNCTION
# ============================================================

def get_customer_scores(include_all_columns=False):
    """
    Calculates RFMCP scores for all customers.
    
    FIXED: Profit is now calculated correctly by:
    1. Getting unique invoice numbers first
    2. Summing profit at invoice level (not product line level)
    3. Then aggregating by customer
    """
    if not DB_PATH.parent.exists():
        print(f"❌ Data directory not found: {DB_PATH.parent}")
        return pd.DataFrame()
    if not DB_PATH.exists():
        print(f"❌ Database file not found: {DB_PATH}")
        return pd.DataFrame()

    try:
        conn = sqlite3.connect(DB_PATH)
        
        # ============================================================
        # CRITICAL FIX: Use a subquery to get ONE row per invoice
        # This prevents counting the same invoice profit multiple times
        # ============================================================
        query = """
            WITH invoice_level AS (
                -- Step 1: Get ONE row per invoice with its profit
                -- (profit is the same for all product lines in an invoice)
                SELECT DISTINCT
                    customer_code,
                    customer_name,
                    invoice_number,
                    delivery_date,
                    profit,
                    balance
                FROM sales_data
                WHERE delivery_date IS NOT NULL
            ),
            product_aggregates AS (
                -- Step 2: Get product-level totals (amount, quantity)
                -- These ARE supposed to be summed across product lines
                SELECT
                    customer_code,
                    customer_name,
                    SUM(amount) as total_amount,
                    SUM(quantity) as total_quantity
                FROM sales_data
                WHERE delivery_date IS NOT NULL
                GROUP BY customer_code, customer_name
            )
            -- Step 3: Combine invoice-level and product-level data
            SELECT
                i.customer_name,
                i.customer_code,
                CAST(JULIANDAY('now') - JULIANDAY(MAX(i.delivery_date)) AS INTEGER) AS recency,
                COUNT(DISTINCT i.invoice_number) AS frequency,
                p.total_amount AS monetary_value,
                SUM(COALESCE(i.profit, 0)) AS total_profit,
                COALESCE(MAX(i.balance), 0) AS balance,
                CAST(JULIANDAY(MAX(i.delivery_date)) - JULIANDAY(MIN(i.delivery_date)) AS INTEGER) AS days_active
            FROM invoice_level i
            JOIN product_aggregates p ON i.customer_code = p.customer_code
            GROUP BY i.customer_name, i.customer_code
            HAVING p.total_amount > 0
        """
        
        df = pd.read_sql_query(query, conn)
        conn.close()

    except Exception as e:
        print(f"❌ Scoring error: {e}")
        return pd.DataFrame()

    if df.empty:
        return pd.DataFrame()

    # Data cleaning
    df['balance'] = pd.to_numeric(df['balance'], errors='coerce').fillna(0)
    df['total_profit'] = pd.to_numeric(df['total_profit'], errors='coerce').fillna(0)
    df['monetary_value'] = pd.to_numeric(df['monetary_value'], errors='coerce').fillna(0)
    df['frequency'] = pd.to_numeric(df['frequency'], errors='coerce').fillna(0).astype(int)
    df['recency'] = pd.to_numeric(df['recency'], errors='coerce').fillna(999).astype(int)
    df['days_active'] = pd.to_numeric(df['days_active'], errors='coerce').fillna(VISIT_CYCLE_DAYS)
    df['days_active'] = df['days_active'].clip(lower=VISIT_CYCLE_DAYS)

    # ========================================
    # BUSINESS METRICS
    # ========================================

    df['weekly_sales'] = (df['monetary_value'] / df['days_active']) * VISIT_CYCLE_DAYS
    df['monthly_sales'] = df['weekly_sales'] * 4

    df['dso'] = np.where(
        (df['weekly_sales'] > 0) & (df['balance'] > 0),
        (df['balance'] / df['weekly_sales']) * VISIT_CYCLE_DAYS,
        0
    )
    df['dso'] = pd.to_numeric(df['dso'], errors='coerce').fillna(0).clip(lower=0, upper=999)
    df['weeks_owing'] = (df['dso'] / VISIT_CYCLE_DAYS)

    # Profit margin (now correct because total_profit is correct)
    df['profit_margin_pct'] = np.where(
        df['monetary_value'] > 0,
        (df['total_profit'] / df['monetary_value']) * 100,
        0
    )

    df['avg_order_value'] = np.where(
        df['frequency'] > 0,
        df['monetary_value'] / df['frequency'],
        0
    )

    df['weeks_since_last_order'] = (df['recency'] / VISIT_CYCLE_DAYS)

    # ========================================
    # INDIVIDUAL SCORES (1-5 scale)
    # ========================================

    df['R_Score'] = create_score(df['recency'], reverse=True)
    df['F_Score'] = create_score(df['frequency'], reverse=False)
    df['M_Score'] = create_score(df['monetary_value'], reverse=False)
    df['C_Score'] = df.apply(calculate_credit_score, axis=1)
    df['P_Score'] = df['profit_margin_pct'].apply(calculate_profit_score)

    # ========================================
    # COMBINED SCORES
    # ========================================

    df['Total_Score'] = (
        df['R_Score'] * 4 +
        df['F_Score'] * 3 +
        df['M_Score'] * 6 +
        df['C_Score'] * 4 +
        df['P_Score'] * 3
    )

    df['RFM_Score'] = df['R_Score'] + df['F_Score'] + df['M_Score']

    # ========================================
    # SEGMENTATION
    # ========================================

    df['Segment'] = df.apply(assign_segment, axis=1)
    df['Risk_Flag'] = df.apply(assign_risk_flag, axis=1)
    df['Priority'] = df.apply(assign_priority, axis=1)

    # ========================================
    # FORMAT & SORT
    # ========================================

    df['weekly_sales'] = pd.to_numeric(df['weekly_sales'], errors='coerce').fillna(0).round(0)
    df['monthly_sales'] = pd.to_numeric(df['monthly_sales'], errors='coerce').fillna(0).round(0)
    df['dso'] = pd.to_numeric(df['dso'], errors='coerce').fillna(0).round(0)
    df['profit_margin_pct'] = pd.to_numeric(df['profit_margin_pct'], errors='coerce').fillna(0).round(1)
    df['avg_order_value'] = pd.to_numeric(df['avg_order_value'], errors='coerce').fillna(0).round(0)
    df['weeks_owing'] = pd.to_numeric(df['weeks_owing'], errors='coerce').fillna(0).round(1)
    df['weeks_since_last_order'] = pd.to_numeric(df['weeks_since_last_order'], errors='coerce').fillna(0).round(1)

    df = df.sort_values('Total_Score', ascending=False).reset_index(drop=True)

    if include_all_columns:
        return df
    else:
        return df[[
            'customer_name', 'customer_code', 'Segment', 'Priority', 'Risk_Flag',
            'Total_Score', 'RFM_Score', 'R_Score', 'F_Score', 'M_Score', 'C_Score', 'P_Score',
            'recency', 'frequency', 'monetary_value', 'balance', 'total_profit',
            'weeks_since_last_order', 'weeks_owing', 'profit_margin_pct'
        ]]

# ============================================================
# SCORING HELPER FUNCTIONS (unchanged)
# ============================================================

def create_score(series, reverse=False):
    """Create 1-5 quintile scores."""
    if series.nunique() < 5:
        if reverse:
            return pd.cut(series.rank(method='first'), bins=min(series.nunique(), 5), labels=range(5, 5-min(series.nunique()), -1), include_lowest=True).astype(int)
        else:
            return pd.cut(series.rank(method='first'), bins=min(series.nunique(), 5), labels=range(1, min(series.nunique()+1, 6)), include_lowest=True).astype(int)
    try:
        if reverse:
            return pd.qcut(series, q=5, labels=[5,4,3,2,1], duplicates='drop').astype(int)
        else:
            return pd.qcut(series.rank(method='first'), q=5, labels=[1,2,3,4,5], duplicates='drop').astype(int)
    except ValueError:
        if reverse:
            return pd.cut(series.rank(method='first'), bins=5, labels=[5,4,3,2,1], include_lowest=True).astype(int)
        else:
            return pd.cut(series.rank(method='first'), bins=5, labels=[1,2,3,4,5], include_lowest=True).astype(int)

def calculate_credit_score(row):
    """Credit score based on DSO."""
    balance = row['balance']
    dso = row['dso']

    if balance <= 0:
        return 5

    if dso <= DSO_EXCELLENT:
        return 5
    elif dso <= DSO_GOOD:
        return 4
    elif dso <= DSO_FAIR:
        return 3
    elif dso <= DSO_POOR:
        return 2
    else:
        return 1

def calculate_profit_score(margin_pct):
    """Profit score based on margin percentage."""
    if margin_pct >= MARGIN_EXCELLENT:
        return 5
    elif margin_pct >= MARGIN_GOOD:
        return 4
    elif margin_pct >= MARGIN_FAIR:
        return 3
    elif margin_pct >= MARGIN_LOW:
        return 2
    else:
        return 1

def assign_segment(row):
    """Assign customer segment."""
    total = row['Total_Score']
    c_score = row['C_Score']
    p_score = row['P_Score']
    balance = row['balance']

    if c_score == 1 and balance > 50000:
        return "🚨 High Risk"
    if c_score <= 2 and balance > 20000:
        return "⚠️ Credit Risk"
    if p_score <= 2 and row['RFM_Score'] >= 10:
        return "💸 Review Pricing"
    if total >= 85:
        return "👑 Champions"
    elif total >= 70:
        return "💎 Loyal"
    elif total >= 55:
        return "📈 Potential"
    elif total >= 40:
        return "📉 At Risk"
    else:
        return "💤 Dormant"

def assign_risk_flag(row):
    """Assign risk flags."""
    flags = []
    if row['C_Score'] <= 2 and row['balance'] > 0:
        flags.append("CREDIT")
    if row['P_Score'] <= 2:
        flags.append("PROFIT")
    if row['R_Score'] <= 2:
        flags.append("INACTIVE")
    return " | ".join(flags) if flags else "OK"

def assign_priority(row):
    """Assign action priority."""
    segment = row['Segment']
    risk = row['Risk_Flag']

    if "🚨" in segment or "CREDIT" in risk:
        return 1
    if "⚠️" in segment or "💸" in segment:
        return 2
    if "📈" in segment or "📉" in segment or "INACTIVE" in risk:
        return 3
    if "👑" in segment or "💎" in segment:
        return 4
    return 5

# ============================================================
# ANALYSIS FUNCTIONS
# ============================================================

def get_segment_summary():
    """Get summary statistics by segment."""
    scores = get_customer_scores(include_all_columns=True)

    if scores.empty:
        return pd.DataFrame()

    summary = scores.groupby('Segment').agg({
        'customer_code': 'count',
        'monetary_value': 'sum',
        'balance': 'sum',
        'total_profit': 'sum',
        'Total_Score': 'mean'
    }).rename(columns={
        'customer_code': 'count',
        'monetary_value': 'total_sales',
        'balance': 'total_balance',
        'total_profit': 'total_profit',
        'Total_Score': 'avg_score'
    })

    summary = summary.round(0)
    summary = summary.sort_values('avg_score', ascending=False)

    return summary

def get_risk_customers(risk_type='CREDIT', limit=50):
    """Get customers with specific risk flags."""
    scores = get_customer_scores(include_all_columns=True)

    if scores.empty:
        return pd.DataFrame()

    if risk_type == 'ALL':
        risky = scores[scores['Risk_Flag'] != 'OK']
    else:
        risky = scores[scores['Risk_Flag'].str.contains(risk_type, na=False)]

    risky = risky.sort_values(['Priority', 'balance'], ascending=[True, False])

    return risky.head(limit)

def get_top_customers(n=20, by='Total_Score'):
    """Get top N customers by specified metric."""
    scores = get_customer_scores(include_all_columns=True)

    if scores.empty:
        return pd.DataFrame()

    if by not in scores.columns:
        by = 'Total_Score'

    return scores.nlargest(n, by)

def get_credit_summary():
    """Get overall credit situation summary."""
    scores = get_customer_scores(include_all_columns=True)

    if scores.empty:
        return {}

    owing = scores[scores['balance'] > 0]
    we_owe = scores[scores['balance'] < 0]

    return {
        'total_customers': len(scores),
        'customers_owing_count': len(owing),
        'customers_owing_amount': owing['balance'].sum(),
        'customers_we_owe_count': len(we_owe),
        'customers_we_owe_amount': abs(we_owe['balance'].sum()),
        'net_balance': scores['balance'].sum(),
        'avg_dso': owing['dso'].mean() if len(owing) > 0 else 0,
        'high_risk_count': len(scores[scores['C_Score'] <= 2]),
        'high_risk_amount': scores[(scores['C_Score'] <= 2) & (scores['balance'] > 0)]['balance'].sum()
    }

# ============================================================
# STANDALONE EXECUTION
# ============================================================

def main():
    """Run scoring as standalone script for testing."""
    CURRENCY = "PKR"
    print("\n" + "="*80)
    print("CUSTOMER SCORING MODULE - Test Run".center(80))
    print("="*80)

    scores = get_customer_scores(include_all_columns=True)

    if scores.empty:
        print("\n❌ No data available. Run data_loader.py first.")
        return

    print(f"\n✅ Scored {len(scores)} customers")

    print("\n📊 SEGMENT DISTRIBUTION:")
    print(scores['Segment'].value_counts())

    print("\n👑 TOP 5 CUSTOMERS:")
    print(scores.head(5)[['customer_name', 'Total_Score', 'Segment', 'monetary_value', 'balance']])

    credit = get_credit_summary()
    print(f"\n💰 CREDIT SUMMARY:")
    print(f"   Customers owing: {credit['customers_owing_count']} ({CURRENCY} {credit['customers_owing_amount']:,.0f})")
    print(f"   Average DSO: {credit['avg_dso']:.0f} days")
    print(f"   High risk: {credit['high_risk_count']} customers ({CURRENCY} {credit['high_risk_amount']:,.0f})")

    print("\n" + "="*80 + "\n")

if __name__ == "__main__":
    main()