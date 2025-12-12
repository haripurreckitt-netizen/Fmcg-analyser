# app.py
from flask import Flask
import logging
import sqlite3
from pathlib import Path

# Import all route functions
from routes.customer_list_routes import all_customers, high_risk_customers, growth_customers, neutral_customers
from routes.orderbooker_routes import orderbooker_details
from routes.dashboard_routes import dashboard, update_database
from routes.credit_list_routes import credit_list
from routes.data_routes import add_data
from routes.profit_routes import profit
from routes.invoice_routes import invoice_details
from routes.monthly_analysis_routes import monthly_analysis
from routes.planner_routes import route_planner
from routes.customer_deep_dive_routes import customer_deep_dive
from routes.purchasing_planner_routes import purchasing_planner
from routes.company_product_deep_dive import company_product_deep_dive
from routes.customer_routes import customer_details  # Missing import

# ============================================================
# Database Initialization
# ============================================================
# Define paths
db_path = Path("data/sales.db")
raw_data_path = Path("data/raw")

# Create directories if they don't exist
db_path.parent.mkdir(parents=True, exist_ok=True)
raw_data_path.mkdir(parents=True, exist_ok=True)

# Create empty database file if it doesn't exist
if not db_path.exists():
    print(f"\n📊 Database not found. Creating blank file at: {db_path}")
    conn = sqlite3.connect(db_path)
    conn.close()
    print("✅ Blank sales.db created successfully")
    print("⚠️  Run 'python data_loader.py' to populate the database with data\n")

# ============================================================
# Flask App Configuration
# ============================================================
app = Flask(__name__)
app.secret_key = 'your-secret-key-change-this-in-production'

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ============================================================
# Route Registration
# ============================================================

# Dashboard & Main Pages
app.route("/")(dashboard)
app.route("/profit")(profit)
app.route("/update_db", methods=["POST"])(update_database)

# Customer Routes - NEW UNIFIED ROUTE
@app.route('/customers/all')
def all_customers_route():
    return all_customers()

# Customer Routes - Legacy (redirect to unified page)
@app.route('/customers/high_risk')
@app.route('/customers/high-risk')
def high_risk():
    return all_customers()  # All go to unified page now

@app.route('/customers/growth')
def growth():
    return all_customers()

@app.route('/customers/neutral')
def neutral():
    return all_customers()

# Individual Customer Detail Page
app.route("/customer/<path:name>")(customer_details)

# Other Detail Pages
app.route("/orderbooker/<name>")(orderbooker_details)
app.route("/invoice/<invoice_id>")(invoice_details)
app.route("/planner")(route_planner)
app.route("/customer_deep_dive/<path:name>")(customer_deep_dive)
app.route("/company_product_deep_dive")(company_product_deep_dive)
app.route("/purchasing_planner")(purchasing_planner)

# List Pages
app.route("/credit_list")(credit_list)

# Data Management
app.route("/add_data", methods=["GET", "POST"])(add_data)

# Monthly Analysis
app.route("/monthly_analysis")(monthly_analysis)


@app.route("/export_invoices/<customer>/<format>")
def export_invoices(customer, format):
    # generate PDF or Excel here
    return f"Exporting {customer} invoices as {format.upper()}"


# ============================================================
# Run Application
# ============================================================
if __name__ == "__main__":
    print("\n" + "="*60)
    print("🚀 FMCG ANALYZER - Starting Flask Application")
    print("="*60)
    print(f"📊 Database: {db_path.absolute()}")
    print(f"📁 Raw Data: {raw_data_path.absolute()}")
    print("="*60 + "\n")
    
    app.run(host='0.0.0.0', debug=True, port=5000)