import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text

# Database Configuration
DB_URI = "mysql+pymysql://root:Shravan123@localhost:3306/aadhaar_db"
engine = create_engine(DB_URI)

@st.cache_data(ttl=3600)
def get_main_data():
    """Fetches high-level aggregated data for the Executive Overview."""
    query = """
    SELECT month, district, state, SUM(count) as total_count
    FROM (
        SELECT DATE_FORMAT(date, '%%Y-%%m') as month, district, state, 
               (COALESCE(count_0_5,0) + COALESCE(count_5_17,0) + COALESCE(count_18_plus,0)) as count FROM enrolment
        UNION ALL
        SELECT DATE_FORMAT(date, '%%Y-%%m') as month, district, state, 
               (COALESCE(count_5_17,0) + COALESCE(count_17,0)) as count FROM demographic
        UNION ALL
        SELECT DATE_FORMAT(date, '%%Y-%%m') as month, district, state, 
               (COALESCE(count_5_17,0) + COALESCE(count_17,0)) as count FROM biometric
    ) as combined
    GROUP BY month, district, state
    """
    return pd.read_sql(query, engine)

@st.cache_data(ttl=3600)
def get_detailed_stats():
    """Fetches detailed data for ML analysis."""
    query = """
    SELECT 
        DATE_FORMAT(e.date, '%%Y-%%m') as month, e.state, e.district,
        SUM(COALESCE(e.count_0_5,0) + COALESCE(e.count_5_17,0) + COALESCE(e.count_18_plus,0)) as enrolment_count,
        SUM(COALESCE(d.count_5_17,0) + COALESCE(d.count_17,0)) as demographic_count,
        SUM(COALESCE(b.count_5_17,0) + COALESCE(b.count_17,0)) as biometric_count
    FROM enrolment e
    LEFT JOIN demographic d ON e.date = d.date AND e.district = d.district
    LEFT JOIN biometric b ON e.date = b.date AND e.district = b.district
    GROUP BY month, e.state, e.district
    """
    return pd.read_sql(query, engine).fillna(0)

def get_paginated_data(table_name, page, page_size, search_term=None, state_filter=None):
    """Fetches data in chunks with search and filter capabilities."""
    offset = (page - 1) * page_size
    query = f"SELECT * FROM {table_name} WHERE 1=1"
    params = {}
    if search_term:
        query += " AND district LIKE :search"
        params['search'] = f"%{search_term}%"
    if state_filter and state_filter != "All":
        query += " AND state = :state"
        params['state'] = state_filter
    query += f" LIMIT {page_size} OFFSET {offset}"
    with engine.connect() as conn:
        return pd.read_sql(text(query), conn, params=params)

def get_total_rows(table_name, search_term=None, state_filter=None):
    """Counts total records for pagination."""
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
    """Retrieves unique states for the filter dropdown."""
    query = f"SELECT DISTINCT state FROM {table_name} ORDER BY state"
    with engine.connect() as conn:
        return [row[0] for row in conn.execute(text(query))]