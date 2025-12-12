# validator.py
import pandas as pd
from datetime import datetime

def clean_and_trim_string(value):
    """
    Cleans string values: strips whitespace, converts to string, handles None/NaN.
    Returns None for missing values, cleaned string otherwise.
    """
    if pd.isna(value) or value is None:
        return None
    return str(value).strip()

def clean_numeric(value):
    """
    Cleans numeric values: handles None/NaN, converts to float.
    Preserves decimal places for amounts like 1234.56
    """
    if pd.isna(value) or value is None:
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None

def clean_and_round_integer(value):
    """
    Cleans and rounds numeric values to integers.
    Use for: invoice numbers, quantities, customer codes, amounts in PKR (no decimals)
    """
    if pd.isna(value) or value is None:
        return None
    try:
        # Convert to float first (handles string numbers), then round and convert to int
        return int(round(float(value)))
    except (ValueError, TypeError):
        return None

def parse_date(value):
    """
    Parses various date formats and returns a standardized datetime object.
    Handles:
    - Excel date numbers
    - String dates in various formats
    - Already parsed datetime objects
    - None/NaN values
    """
    if pd.isna(value) or value is None:
        return None
    
    # If it's already a datetime object, return it
    if isinstance(value, (datetime, pd.Timestamp)):
        return value
    
    # Try to parse using pandas (handles most formats automatically)
    try:
        parsed_date = pd.to_datetime(value, errors='coerce')
        # If parsing failed, parsed_date will be NaT (Not a Time)
        if pd.isna(parsed_date):
            return None
        return parsed_date
    except:
        return None