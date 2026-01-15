import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import hashlib
from metrics import calculate_sli_and_risk, get_top_critical_districts
from db_utils import get_aggregated_data, get_paginated_data, get_total_rows, get_states

# ==========================================
# 1. PAGE CONFIG
# ==========================================
st.set_page_config(
    page_title="Aadhaar Seva-Drishti",
    page_icon="🆔",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for "Smooth" look
st.markdown("""
<style>
    .stApp { background-color: #f8f9fa; }
    div.stMetric { background-color: #ffffff; padding: 15px; border-radius: 10px; box-shadow: 0 2px 10px rgba(0,0,0,0.05); }
    h1, h2, h3 { color: #1e3a8a; }
</style>
""", unsafe_allow_html=True)

# ==========================================
# 2. AUTHENTICATION (Unchanged)
# ==========================================
if "logged_in" not in st.session_state:
    st.session_state.logged_in = False
    st.session_state.user = None

def hash_password(p): return hashlib.sha256(p.encode()).hexdigest()
USERS = {"admin": hash_password("admin123"), "viewer": hash_password("viewer123")}

def login():
    col1, col2, col3 = st.columns([1,2,1])
    with col2:
        st.title("🔐 Login")
        u = st.text_input("Username")
        p = st.text_input("Password", type="password")
        if st.button("Sign In", use_container_width=True):
            if u in USERS and USERS[u] == hash_password(p):
                st.session_state.logged_in = True
                st.session_state.user = u
                st.rerun()
            else:
                st.error("Invalid credentials")

# ==========================================
# 3. ADVANCED VISUALS ("Out of the Box")
# ==========================================
def render_dashboard(df):
    st.title("📊 Analytics Dashboard")
    
    # KPIS
    total_ops = df['total_count'].sum()
    peak_month = df.groupby('month')['total_count'].sum().idxmax()
    top_state = df.groupby('state')['total_count'].sum().idxmax()
    
    k1, k2, k3 = st.columns(3)
    k1.metric("Total Operations", f"{total_ops:,.0f}")
    k2.metric("Peak Month", peak_month)
    k3.metric("Top State", top_state)
    
    st.divider()

    # 1. Smooth Time Series with Range Slider
    st.subheader("📈 Operational Trend (Interactive)")
    monthly_trend = df.groupby("month")['total_count'].sum().reset_index()
    fig_line = px.line(monthly_trend, x="month", y="total_count", 
                       markers=True, title="Monthly Volume",
                       line_shape="spline") # "Spline" makes it smooth/curved
    fig_line.update_xaxes(rangeslider_visible=True)
    st.plotly_chart(fig_line, use_container_width=True)

    # 2. Sunburst Chart (Drill Down: State -> District)
    st.subheader("🗺️ Geographic Drill-Down")
    st.caption("Click on a State to see its Districts.")
    # For performance, limit to top 5 states for the sunburst
    top_5_states = df.groupby('state')['total_count'].sum().nlargest(5).index
    sunburst_df = df[df['state'].isin(top_5_states)]
    
    fig_sun = px.sunburst(
        sunburst_df, 
        path=['state', 'district'], 
        values='total_count',
        color='total_count',
        color_continuous_scale='Blues'
    )
    st.plotly_chart(fig_sun, use_container_width=True)

# ==========================================
# 4. DATA EXPLORER (Pagination & Filter)
# ==========================================
def data_explorer_page():
    st.title("💾 Data Explorer (Big Data View)")
    
    # 1. Controls Row
    c1, c2, c3 = st.columns([1, 2, 2])
    
    with c1:
        table = st.selectbox("Select Table", ["enrolment", "demographic", "biometric"])
    
    with c2:
        search = st.text_input("🔍 Search District", placeholder="Type district name...")
        
    with c3:
        states = ["All"] + get_states(table)
        state_filter = st.selectbox("Filter by State", states)

    # 2. Pagination Logic
    PAGE_SIZE = 50
    total_rows = get_total_rows(table, search, state_filter)
    total_pages = max(1, (total_rows // PAGE_SIZE) + 1)
    
    if "page_number" not in st.session_state: st.session_state.page_number = 1
    
    # Reset page on filter change
    if "last_table" not in st.session_state or st.session_state.last_table != table:
        st.session_state.page_number = 1
        st.session_state.last_table = table

    # 3. Fetch Data
    df_page = get_paginated_data(table, st.session_state.page_number, PAGE_SIZE, search, state_filter)
    
    # 4. Display Table
    st.write(f"**Showing {len(df_page)} rows** (Total: {total_rows:,}) | Page {st.session_state.page_number} of {total_pages}")
    st.dataframe(df_page, use_container_width=True)
    
    # 5. Pagination Buttons
    prev, _, next_btn = st.columns([1, 10, 1])
    
    if prev.button("Previous") and st.session_state.page_number > 1:
        st.session_state.page_number -= 1
        st.rerun()
        
    if next_btn.button("Next") and st.session_state.page_number < total_pages:
        st.session_state.page_number += 1
        st.rerun()

# ==========================================
# 5. MAIN APP CONTROLLER
# ==========================================
if not st.session_state.logged_in:
    login()
else:
    # Sidebar
    st.sidebar.title("🆔 Seva-Drishti")
    page = st.sidebar.radio("Navigate", ["Dashboard", "Data Explorer", "Risk Command Center", "Logout"])
    
    if page == "Dashboard":
        # Load optimized aggregated data (Fast)
        df_agg = get_aggregated_data()
        render_dashboard(df_agg)
        
    elif page == "Data Explorer":
        data_explorer_page()
        
    elif page == "Risk Command Center":
        # (Your previous Risk Code here - requires reloading full data or adapting to SQL)
        # For Hackathon demo, loading 'df_agg' might be enough if you adapt the function,
        # otherwise assume Risk Analysis is run on a smaller subset or offline.
        st.info("Risk Command Center loaded (See metrics.py integration)")
        
    elif page == "Logout":
        st.session_state.logged_in = False
        st.rerun()