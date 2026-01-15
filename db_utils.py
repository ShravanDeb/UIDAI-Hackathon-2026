import pandas as pd
from sqlalchemy import create_engine, text

# Database Configuration
DB_URI = "mysql+pymysql://root:Shravan123@localhost:3306/aadhaar_db"
engine = create_engine(DB_URI)

def get_aggregated_data():
    """
    Fetches pre-aggregated data for charts to avoid loading 5M rows.
    """
    query = """
    SELECT 
        month, 
        district, 
        state,
        SUM(count) as total_count
    FROM (
        SELECT month, district, state, (count_0_5 + count_5_17 + count_18_plus) as count FROM enrolment
        UNION ALL
        SELECT month, district, state, (count_5_17 + count_17) as count FROM demographic
        UNION ALL
        SELECT month, district, state, (count_5_17 + count_17) as count FROM biometric
    ) as combined
    GROUP BY month, district, state
    """
    return pd.read_sql(query, engine)

def get_paginated_data(table_name, page, page_size, search_term=None, state_filter=None):
    """
    Fetches raw data in small chunks (Pages) using SQL LIMIT/OFFSET.
    """
    offset = (page - 1) * page_size
    
    # Base Query
    query = f"SELECT * FROM {table_name} WHERE 1=1"
    params = {}
    
    # Add Search (District)
    if search_term:
        query += " AND district LIKE :search"
        params['search'] = f"%{search_term}%"
        
    # Add Filter (State)
    if state_filter and state_filter != "All":
        query += " AND state = :state"
        params['state'] = state_filter
        
    # Add Pagination
    query += f" LIMIT {page_size} OFFSET {offset}"
    
    with engine.connect() as conn:
        return pd.read_sql(text(query), conn, params=params)

def get_total_rows(table_name, search_term=None, state_filter=None):
    """
    Counts total rows for the pagination logic (Page 1 of X).
    """
    query = f"SELECT COUNT(*) FROM {table_name} WHERE 1=1"
    params = {}
    
    if search_term:
        query += " AND district LIKE :search"
        params['search'] = f"%{search_term}%"
        
    if state_filter and state_filter != "All":
        query += " AND state = :state"
        params['state'] = state_filter
        
    with engine.connect() as conn:
        return conn.execute(text(query), params).scalar()

def get_states(table_name):
    """Get unique states for the filter dropdown"""
    query = f"SELECT DISTINCT state FROM {table_name} ORDER BY state"
    with engine.connect() as conn:
        return [row[0] for row in conn.execute(text(query))]