import streamlit as st
import pandas as pd
import plotly.express as px
import math
import numpy as np
from db_utils import get_main_data, get_paginated_data, get_total_rows, get_states, get_detailed_stats
from forecasting import generate_forecast
from metrics import calculate_sli_and_risk

# Professional UI Configuration
st.set_page_config(page_title="Seva-Drishti | UIDAI Analytics", layout="wide")

# Custom CSS for Official Branding (Digital India Theme)
st.markdown("""
<style>
    :root { --gov-blue: #063970; --accent: #ff9933; --bg: #f8fafc; }
    .stApp { background-color: var(--bg); }
    .main-header { 
        background-color: var(--gov-blue); padding: 25px; border-radius: 12px; 
        color: white; border-left: 10px solid var(--accent); margin-bottom: 30px;
    }
    .metric-card {
        background: white; padding: 20px; border-radius: 10px; 
        box-shadow: 0 4px 6px rgba(0,0,0,0.05); border-top: 4px solid var(--gov-blue);
        text-align: center;
    }
    .stButton>button { width: 100%; border-radius: 5px; background-color: var(--gov-blue); color: white; }
    [data-testid="stSidebar"] { background-color: #ffffff; border-right: 1px solid #dee2e6; }
</style>
""", unsafe_allow_html=True)

# --- NAVIGATION ---
with st.sidebar:
    st.image("https://uidai.gov.in/images/logo/uidai_english_logo.svg", width=180)
    st.markdown("---")
    page = st.radio("Management Console", [
        "Executive Overview", 
        "Service Load Analysis", 
        "ML Forecasting", 
        "Data Explorer"
    ])
    st.markdown("---")
    if st.button("🔄 Refresh Master Data"):
        st.cache_data.clear()
        st.rerun()

# --- 1. EXECUTIVE OVERVIEW ---
if page == "Executive Overview":
    st.markdown('<div class="main-header"><h1>Executive Overview</h1><p>Strategic Operations & National Service Trends</p></div>', unsafe_allow_html=True)
    
    df_agg = get_main_data()
    
    # KPIs
    k1, k2, k3, k4 = st.columns(4)
    with k1: st.markdown(f'<div class="metric-card"><h3>Total Services</h3><h2>{df_agg["total_count"].sum():,.0f}</h2></div>', unsafe_allow_html=True)
    with k2: st.markdown(f'<div class="metric-card"><h3>Monthly Avg</h3><h2>{df_agg.groupby("month")["total_count"].sum().mean():,.0f}</h2></div>', unsafe_allow_html=True)
    with k3: st.markdown(f'<div class="metric-card"><h3>Active Districts</h3><h2>{df_agg["district"].nunique()}</h2></div>', unsafe_allow_html=True)
    with k4: st.markdown(f'<div class="metric-card"><h3>Active States</h3><h2>{df_agg["state"].nunique()}</h2></div>', unsafe_allow_html=True)

    st.markdown("### 📈 National Volume Trend")
    trend = df_agg.groupby('month')['total_count'].sum().reset_index()
    fig = px.area(trend, x='month', y='total_count', markers=True, 
                  color_discrete_sequence=['#063970'], template="plotly_white")
    st.plotly_chart(fig, use_container_width=True)

# --- 2. SERVICE LOAD ANALYSIS (RISK) ---
elif page == "Service Load Analysis":
    st.title("🛡️ Risk & Service Load Command Center")
    st.markdown("Weighted analysis of district-level operational stress.")
    
    # Fetch data for risk calculation
    df_detail = get_detailed_stats()
    
    # Prepare inputs for the metrics module
    e_df = df_detail[['district', 'month', 'enrolment_count']].rename(columns={'enrolment_count': 'count'})
    d_df = df_detail[['district', 'month', 'demographic_count']].rename(columns={'demographic_count': 'count'})
    b_df = df_detail[['district', 'month', 'biometric_count']].rename(columns={'biometric_count': 'count'})
    
    risk_df = calculate_sli_and_risk(e_df, d_df, b_df)
    
    # Filters
    c1, c2 = st.columns([1, 3])
    with c1:
        cat_filter = st.multiselect("Filter Risk Category", ["Critical", "High", "Normal"], default=["Critical", "High"])
        filtered_risk = risk_df[risk_df['risk_category'].isin(cat_filter)]
    
    with c2:
        fig = px.scatter(filtered_risk, x="sli_score", y="risk_score", color="risk_category",
                         size="risk_score", hover_name="district",
                         color_discrete_map={"Critical": "#d32f2f", "High": "#f57c00", "Normal": "#388e3c"})
        st.plotly_chart(fig, use_container_width=True)
    
    st.markdown("#### District Stress Rankings")
    st.dataframe(filtered_risk.sort_values("risk_score", ascending=False), use_container_width=True, hide_index=True)

# --- 3. ML FORECASTING ---
elif page == "ML Forecasting":
    st.title("📊 Predictive Service Analytics")
    df_detail = get_detailed_stats()
    
    c1, c2 = st.columns([1, 2])
    with c1:
        metric = st.selectbox("Select Target Metric", ["enrolment_count", "demographic_count", "biometric_count"])
        horizon = st.slider("Forecast Horizon (Months)", 3, 12, 6)
    
    forecast_df, growth = generate_forecast(df_detail, metric, horizon)
    
    if forecast_df is not None:
        fig = px.line(forecast_df, x="month", y=metric, color="type", markers=True,
                     color_discrete_map={"History": "#063970", "Forecast": "#ff9933"})
        st.plotly_chart(fig, use_container_width=True)
        
        st.info(f"Analysis indicates a projected **{growth:+.2f}%** shift in {metric.replace('_', ' ')} over the next {horizon} months.")

# --- 4. DATA EXPLORER (SEARCH & FILTER) ---
elif page == "Data Explorer":
    st.title("📂 Centralized Data Records")
    
    # Interactive Search Header
    c1, c2, c3 = st.columns([1, 1, 2])
    with c1: table = st.selectbox("Dataset", ["enrolment", "demographic", "biometric"])
    with c2: state_filter = st.selectbox("State Filter", ["All"] + get_states(table))
    with c3: search_term = st.text_input("🔍 Search District", placeholder="Type district name to search...")

    # Pagination logic
    PAGE_SIZE = 50
    total_rows = get_total_rows(table, search_term, state_filter)
    total_pages = math.ceil(total_rows / PAGE_SIZE) if total_rows > 0 else 1
    
    curr_page = st.number_input(f"Page (of {total_pages})", min_value=1, max_value=total_pages, step=1)
    
    df_page = get_paginated_data(table, curr_page, PAGE_SIZE, search_term, state_filter)
    
    st.dataframe(df_page, use_container_width=True, hide_index=True)
    st.caption(f"Showing {len(df_page)} of {total_rows} total records.")