import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
import plotly.figure_factory as ff
from sqlalchemy import create_engine, text
from sklearn.preprocessing import MinMaxScaler
from sklearn.linear_model import LinearRegression
import math

# ==========================================
# 1. CONFIGURATION & THEME
# ==========================================
st.set_page_config(
    page_title="Aadhaar Seva-Drishti",
    layout="wide",
    initial_sidebar_state="expanded"
)

st.markdown("""
<style>
    .stApp { background-color: #f8fafc; }
    [data-testid="stSidebar"] { background-color: #ffffff; border-right: 1px solid #e2e8f0; }
    h1, h2, h3 { color: #0f172a; font-family: 'Segoe UI', sans-serif; }
    div[data-testid="stMetric"] {
        background-color: #ffffff;
        border: 1px solid #e2e8f0;
        padding: 15px;
        border-radius: 8px;
        box-shadow: 0 1px 2px rgba(0,0,0,0.05);
    }
    div.stButton > button {
        background-color: #2563eb;
        color: white;
        border-radius: 6px;
        border: none;
        padding: 0.5rem 1rem;
    }
    div.stButton > button:hover { background-color: #1d4ed8; }
    .page-info { font-weight: bold; padding: 10px; color: #64748b; }
</style>
""", unsafe_allow_html=True)

# ==========================================
# 2. DATABASE CONNECTION & LOGIC
# ==========================================
DB_URI = "mysql+pymysql://root:Shravan123@localhost:3306/aadhaar_db"

@st.cache_resource
def get_engine():
    return create_engine(DB_URI)

@st.cache_data(ttl=600)
def get_main_data():
    engine = get_engine()
    query = """
    SELECT 
        DATE_FORMAT(date, '%%Y-%%m') as month, 
        district, 
        state,
        SUM(count) as total_count
    FROM (
        SELECT date, district, state, (COALESCE(count_0_5,0) + COALESCE(count_5_17,0) + COALESCE(count_18_plus,0)) as count FROM enrolment
        UNION ALL
        SELECT date, district, state, (COALESCE(count_5_17,0) + COALESCE(count_17,0)) as count FROM demographic
        UNION ALL
        SELECT date, district, state, (COALESCE(count_5_17,0) + COALESCE(count_17,0)) as count FROM biometric
    ) as combined
    GROUP BY month, district, state
    """
    df = pd.read_sql(query, engine)
    df['total_count'] = pd.to_numeric(df['total_count'], errors='coerce').fillna(0)
    return df

@st.cache_data(ttl=600)
def get_detailed_stats():
    """Fetches broken down data for Multi-variate analysis"""
    engine = get_engine()
    query = """
    SELECT 
        DATE_FORMAT(e.date, '%%Y-%%m') as month,
        e.state,
        e.district,
        SUM(COALESCE(e.count_0_5,0) + COALESCE(e.count_5_17,0) + COALESCE(e.count_18_plus,0)) as enrolment_count,
        SUM(COALESCE(d.count_5_17,0) + COALESCE(d.count_17,0)) as demographic_count,
        SUM(COALESCE(b.count_5_17,0) + COALESCE(b.count_17,0)) as biometric_count
    FROM enrolment e
    LEFT JOIN demographic d ON e.date = d.date AND e.district = d.district
    LEFT JOIN biometric b ON e.date = b.date AND e.district = b.district
    GROUP BY month, e.state, e.district
    """
    try:
        df = pd.read_sql(query, engine)
        return df.fillna(0)
    except:
        return pd.DataFrame() # Fallback

def get_risk_data(df):
    if df.empty: return pd.DataFrame()
    risk = df.groupby(['district', 'state'])['total_count'].sum().reset_index()
    scaler = MinMaxScaler(feature_range=(0, 100))
    risk['risk_score'] = scaler.fit_transform(risk[['total_count']])
    risk['category'] = pd.cut(risk['risk_score'], bins=[-1, 30, 70, 100], labels=['Normal', 'High', 'Critical'])
    return risk.sort_values('risk_score', ascending=False)

# ==========================================
# 3. ANALYSIS MODULES
# ==========================================

# ... (Overview, Geo, Trend pages same as before - abbreviated for length) ...
def overview_page(df):
    st.title("Executive Overview")
    if df.empty:
        st.warning("No data found.")
        return
    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Total Transactions", f"{df['total_count'].sum():,.0f}")
    k2.metric("Avg Monthly Vol", f"{df.groupby('month')['total_count'].sum().mean():,.0f}")
    k3.metric("Districts Active", df['district'].nunique())
    k4.metric("States Active", df['state'].nunique())
    st.divider()
    trend = df.groupby('month')['total_count'].sum().reset_index()
    fig = px.area(trend, x='month', y='total_count', title="National Service Volume")
    st.plotly_chart(fig, use_container_width=True)

def geo_analysis_page(df):
    st.title("Geographic Analysis")
    if df.empty: return
    col1, col2 = st.columns([1, 3])
    with col1:
        view_type = st.radio("View Level:", ["State Wise", "District Wise"])
    with col2:
        if view_type == "State Wise":
            data = df.groupby('state')['total_count'].sum().reset_index().sort_values('total_count', ascending=True).tail(15)
            fig = px.bar(data, x='total_count', y='state', orientation='h', color='total_count', title="State Leaderboard")
            st.plotly_chart(fig, use_container_width=True)
        else:
            states = sorted(df['state'].unique().tolist())
            sel = st.selectbox("State:", states)
            subset = df[df['state'] == sel].groupby('district')['total_count'].sum().reset_index().sort_values('total_count', ascending=False).head(20)
            fig = px.bar(subset, x='district', y='total_count', color='total_count', title=f"Districts in {sel}")
            st.plotly_chart(fig, use_container_width=True)

# --- NEW: ADVANCED ANALYTICS & ML PAGE ---

def analytics_ml_page():
    st.title("📈 Advanced Analytics & ML Forecasting")
    st.markdown("Deep statistical breakdown of service data.")
    
    df_detail = get_detailed_stats()
    if df_detail.empty:
        st.warning("Detailed data tables (Enrolment/Demographic/Biometric) join failed. Check data integrity.")
        return

    # TABS FOR EACH ANALYSIS TYPE
    tab1, tab2, tab3, tab4 = st.tabs(["📊 Univariate", "🔗 Bivariate", "🧊 Trivariate", "🔮 ML Forecast"])

    # 1. UNIVARIATE (Distribution Analysis)
    with tab1:
        st.subheader("Univariate Analysis: Distribution of Metrics")
        col1, col2 = st.columns(2)
        with col1:
            # Histogram
            metric = st.selectbox("Select Metric for Histogram", ["enrolment_count", "demographic_count", "biometric_count"])
            fig = px.histogram(df_detail, x=metric, nbins=50, title=f"Distribution of {metric}", color_discrete_sequence=['#6366f1'])
            st.plotly_chart(fig, use_container_width=True)
        with col2:
            # Box Plot
            fig2 = px.box(df_detail, y=metric, title=f"Box Plot (Outlier Detection): {metric}", color_discrete_sequence=['#ef4444'])
            st.plotly_chart(fig2, use_container_width=True)
        
        st.caption("Insights: The histogram shows the frequency of service volumes, while the box plot highlights outliers (districts with unusually high loads).")

    # 2. BIVARIATE (Relationship Analysis)
    with tab2:
        st.subheader("Bivariate Analysis: Relationships & Correlations")
        
        # Scatter Plot
        c1, c2 = st.columns(2)
        x_axis = c1.selectbox("X Axis", ["enrolment_count", "demographic_count", "biometric_count"], index=0)
        y_axis = c2.selectbox("Y Axis", ["enrolment_count", "demographic_count", "biometric_count"], index=1)
        
        fig = px.scatter(df_detail, x=x_axis, y=y_axis, color="state", title=f"{x_axis} vs {y_axis}", hover_name="district")
        st.plotly_chart(fig, use_container_width=True)
        
        # Correlation Matrix
        st.markdown("#### Correlation Heatmap")
        corr = df_detail[["enrolment_count", "demographic_count", "biometric_count"]].corr()
        fig_corr = px.imshow(corr, text_auto=True, color_continuous_scale="RdBu_r", title="Correlation Matrix")
        st.plotly_chart(fig_corr, use_container_width=True)

    # 3. TRIVARIATE (3D Analysis)
    with tab3:
        st.subheader("Trivariate Analysis: Multi-Dimensional View")
        st.markdown("Analyzing **Enrolment (X)** vs **Demographic (Y)** vs **Biometric (Size/Color)**.")
        
        fig_3d = px.scatter(
            df_detail, 
            x="enrolment_count", 
            y="demographic_count", 
            size="biometric_count", 
            color="biometric_count",
            hover_name="district",
            title="Bubble Chart: 3-Variable Interaction",
            labels={"biometric_count": "Biometric (Size)"}
        )
        st.plotly_chart(fig_3d, use_container_width=True)

    # 4. ML FORECASTING
    with tab4:
        st.subheader("🔮 Machine Learning Forecast")
        st.markdown("Predicting future service load using **Linear Regression**.")
        
        # Aggregate by month for time series
        ts_df = df_detail.groupby("month")["enrolment_count"].sum().reset_index()
        ts_df["t"] = range(len(ts_df)) # Time steps
        
        if len(ts_df) > 2:
            # Model Training
            X = ts_df[["t"]]
            y = ts_df["enrolment_count"]
            model = LinearRegression()
            model.fit(X, y)
            
            # Future Prediction
            months_to_predict = st.slider("Forecast Months Ahead", 1, 12, 6)
            future_t = np.arange(len(ts_df), len(ts_df) + months_to_predict).reshape(-1, 1)
            prediction = model.predict(future_t)
            
            # Create Future DataFrame
            last_date = pd.to_datetime(ts_df["month"].max())
            future_dates = [last_date + pd.DateOffset(months=i+1) for i in range(months_to_predict)]
            future_dates_str = [d.strftime("%Y-%m") for d in future_dates]
            
            forecast_df = pd.DataFrame({"month": future_dates_str, "enrolment_count": prediction, "type": "Forecast"})
            history_df = ts_df[["month", "enrolment_count"]].copy()
            history_df["type"] = "History"
            
            full_df = pd.concat([history_df, forecast_df])
            
            # Plot
            fig_fc = px.line(full_df, x="month", y="enrolment_count", color="type", markers=True, 
                             title="Enrolment Forecast (History + Prediction)",
                             color_discrete_map={"History": "blue", "Forecast": "orange"})
            fig_fc.add_vline(x=ts_df["month"].iloc[-1], line_dash="dash", line_color="grey")
            st.plotly_chart(fig_fc, use_container_width=True)
            
            st.success(f"Model predicts a trend of **{prediction[-1] - prediction[0]:.0f}** enrolments over the next {months_to_predict} months.")
        else:
            st.warning("Not enough data points to train ML model (Need at least 3 months).")

def risk_command_page(df):
    st.title("Risk Command Center")
    if df.empty: return
    risk_df = get_risk_data(df)
    filter_status = st.radio("Filter Risk:", ["All", "Critical", "High", "Normal"], horizontal=True)
    if filter_status != "All":
        risk_df = risk_df[risk_df['category'] == filter_status]
    c1, c2 = st.columns([2, 1])
    with c1:
        if not risk_df.empty:
            fig = px.scatter(risk_df, x='total_count', y='risk_score', color='category', 
                             size='risk_score', hover_name='district',
                             color_discrete_map={'Critical':'red', 'High':'orange', 'Normal':'green'})
            st.plotly_chart(fig, use_container_width=True)
    with c2:
        st.dataframe(risk_df[['district', 'risk_score', 'category']], use_container_width=True)

# --- PAGINATION & DATA EXPLORER ---

def get_total_rows(table_name):
    engine = get_engine()
    if table_name not in ['enrolment', 'demographic', 'biometric']: return 0
    with engine.connect() as conn:
        return conn.execute(text(f"SELECT COUNT(*) FROM {table_name}")).scalar()

def get_paginated_data(table_name, page_number, page_size=1000):
    engine = get_engine()
    if table_name not in ['enrolment', 'demographic', 'biometric']: return pd.DataFrame()
    offset = (page_number - 1) * page_size
    return pd.read_sql(f"SELECT * FROM {table_name} LIMIT {page_size} OFFSET {offset}", engine)

def data_explorer_page():
    st.title("Data Explorer")
    table = st.selectbox("Select Dataset", ["enrolment", "demographic", "biometric"])
    total_rows = get_total_rows(table)
    PAGE_SIZE = 1000
    total_pages = math.ceil(total_rows / PAGE_SIZE)
    
    if total_rows == 0:
        st.warning("Table is empty.")
        return

    if "data_page" not in st.session_state: st.session_state.data_page = 1
    if st.session_state.data_page > total_pages: st.session_state.data_page = total_pages
    
    c1, c2, c3, c4 = st.columns([1,1,2,1])
    with c1:
        if st.button("Previous"): 
            if st.session_state.data_page > 1:
                st.session_state.data_page -= 1
                st.rerun()
    with c2: st.markdown(f"<div class='page-info'>Page {st.session_state.data_page}/{total_pages}</div>", unsafe_allow_html=True)
    with c4:
        if st.button("Next"):
            if st.session_state.data_page < total_pages:
                st.session_state.data_page += 1
                st.rerun()
    
    df_page = get_paginated_data(table, st.session_state.data_page, PAGE_SIZE)
    st.dataframe(df_page, use_container_width=True)

# ==========================================
# 4. MAIN NAVIGATION
# ==========================================
with st.sidebar:
    st.title("Seva-Drishti")
    st.write("v5.0 Ultimate")
    page = st.radio("Navigate:", [
        "Executive Overview", 
        "Map & Geography", 
        "Advanced Analytics & ML", # <--- NEW
        "Risk Command Center",
        "Data Explorer"
    ])
    st.markdown("---")
    if st.button("Reload Data"):
        st.cache_data.clear()
        st.rerun()

try:
    df_agg = get_main_data()
    
    if page == "Executive Overview":
        overview_page(df_agg)
    elif page == "Map & Geography":
        geo_analysis_page(df_agg)
    elif page == "Advanced Analytics & ML":
        analytics_ml_page()
    elif page == "Risk Command Center":
        risk_command_page(df_agg)
    elif page == "Data Explorer":
        data_explorer_page()

except Exception as e:
    st.error(f"System Error: {e}")