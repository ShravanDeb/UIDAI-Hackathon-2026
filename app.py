import streamlit as st
import pandas as pd
import numpy as np
import hashlib
from sqlalchemy import create_engine
from sklearn.linear_model import LinearRegression
from sklearn.ensemble import IsolationForest

# =====================================================
# PAGE CONFIG
# =====================================================

st.set_page_config(
    page_title="Aadhaar Seva-Drishti",
    page_icon="🆔",
    layout="wide"
)

# =====================================================
# BASIC STYLING (AWESOME LOOK)
# =====================================================

st.markdown("""
<style>
body { background-color: #f5f7fa; }
[data-testid="stSidebar"] { background-color: #0f172a; }
[data-testid="stSidebar"] * { color: white; }
h1, h2, h3 { color: #0f172a; }
</style>
""", unsafe_allow_html=True)

# =====================================================
# USER AUTH (SIMPLE & SAFE)
# =====================================================

def hash_password(p):
    return hashlib.sha256(p.encode()).hexdigest()

USERS = {
    "admin": {"password": hash_password("admin123"), "role": "admin"},
    "viewer": {"password": hash_password("viewer123"), "role": "viewer"}
}

if "logged_in" not in st.session_state:
    st.session_state.logged_in = False
    st.session_state.user = None
    st.session_state.role = None

# =====================================================
# DATABASE (READ-ONLY)
# =====================================================

DB_URI = "mysql+pymysql://root:Shravan123@localhost:3306/aadhaar_db"
engine = create_engine(DB_URI)

def load_table(name):
    return pd.read_sql(f"SELECT * FROM {name}", engine)

# =====================================================
# DATA PREPARATION
# =====================================================

def preprocess_base(df):
    df.columns = df.columns.str.lower().str.strip()
    df['date'] = pd.to_datetime(df['date'])
    df['month'] = df['date'].dt.to_period('M').astype(str)
    df['state'] = df['state'].str.title()
    df['district'] = df['district'].str.title()
    return df

def reshape_enrolment(df):
    df = preprocess_base(df)
    df = df.melt(
        id_vars=['date','month','state','district','pincode'],
        value_vars=['count_0_5','count_5_17','count_18_plus'],
        var_name='age_group',
        value_name='count'
    )
    df['age_group'] = df['age_group'].map({
        'count_0_5':'0-5',
        'count_5_17':'5-17',
        'count_18_plus':'18+'
    })
    return df.dropna()

def reshape_updates(df):
    df = preprocess_base(df)
    df = df.melt(
        id_vars=['date','month','state','district','pincode'],
        value_vars=['count_5_17','count_17'],
        var_name='age_group',
        value_name='count'
    )
    df['age_group'] = df['age_group'].map({
        'count_5_17':'5-17',
        'count_17':'17+'
    })
    return df.dropna()

# =====================================================
# LOAD DATA ONCE
# =====================================================

@st.cache_data
def load_all_data():
    enrol = reshape_enrolment(load_table("enrolment"))
    demo  = reshape_updates(load_table("demographic"))
    bio   = reshape_updates(load_table("biometric"))
    sli   = pd.concat([enrol, demo, bio])
    return enrol, demo, bio, sli

# =====================================================
# ML FUNCTIONS
# =====================================================

def forecast_service_load(sli_df, months=3):
    df = sli_df.groupby("month")["count"].sum().reset_index()
    df["t"] = range(len(df))
    X, y = df[["t"]], df["count"]

    model = LinearRegression()
    model.fit(X, y)

    future_t = np.arange(len(df), len(df)+months).reshape(-1,1)
    return model.predict(future_t)

def detect_anomalies(sli_df):
    df = sli_df.groupby(["month","district"])["count"].sum().reset_index()
    model = IsolationForest(contamination=0.02, random_state=42)
    df["anomaly"] = model.fit_predict(df[["count"]])
    return df[df["anomaly"] == -1]

# =====================================================
# AI INSIGHT (SAFE, RULE-BASED)
# =====================================================

def generate_ai_insight(peak_month, age_group, district):
    return f"""
**AI-Generated Insight**

Aadhaar service demand peaked in **{peak_month}**, driven primarily by enrolments in the
**{age_group} age group**. Districts such as **{district}** consistently experience high
service pressure, indicating sustained population-driven demand.

**Recommendation:**  
UIDAI can proactively deploy enrolment and update infrastructure before peak months to
reduce congestion and improve citizen experience.
"""

# =====================================================
# LOGIN PAGE
# =====================================================

def login_page():
    st.title("🔐 Aadhaar Seva-Drishti Login")
    u = st.text_input("Username")
    p = st.text_input("Password", type="password")

    if st.button("Login"):
        if u in USERS and USERS[u]["password"] == hash_password(p):
            st.session_state.logged_in = True
            st.session_state.user = u
            st.session_state.role = USERS[u]["role"]
            st.rerun()
        else:
            st.error("Invalid credentials")

# =====================================================
# DASHBOARD PAGES
# =====================================================

def overview(enrol, sli):
    st.header("📊 Overview")
    st.metric("Peak Demand Month", sli.groupby("month")["count"].sum().idxmax())
    st.metric("Highest Enrolment Age Group", enrol.groupby("age_group")["count"].sum().idxmax())
    st.metric("Most Stressed District", sli.groupby("district")["count"].mean().idxmax())

def forecast_page(sli):
    st.header("🔮 ML Forecast")
    m = st.slider("Months to forecast", 1, 6, 3)
    f = forecast_service_load(sli, m)
    st.line_chart(f)

def anomaly_page(sli):
    st.header("🚨 Anomaly Detection")
    anomalies = detect_anomalies(sli)
    st.dataframe(anomalies)

def ai_page(enrol, sli):
    st.header("🤖 AI Insights")
    insight = generate_ai_insight(
        sli.groupby("month")["count"].sum().idxmax(),
        enrol.groupby("age_group")["count"].sum().idxmax(),
        sli.groupby("district")["count"].mean().idxmax()
    )
    st.markdown(insight)
    st.info("AI is used only on aggregated data. No individual Aadhaar data is processed.")

def power_bi_page():
    st.header("📈 Power BI Dashboard")
    POWER_BI_URL = "https://app.powerbi.com/reportEmbed?reportId=YOUR_REPORT_ID"
    st.components.v1.iframe(POWER_BI_URL, height=800)

# =====================================================
# MAIN APP
# =====================================================

if not st.session_state.logged_in:
    login_page()
else:
    enrol, demo, bio, sli = load_all_data()

    st.sidebar.title("🆔 Aadhaar Seva-Drishti")
    st.sidebar.write(f"User: {st.session_state.user}")
    st.sidebar.write(f"Role: {st.session_state.role}")

    page = st.sidebar.radio(
        "Navigation",
        ["Overview", "ML Forecast", "Anomaly Detection", "AI Insights", "Power BI", "Logout"]
    )

    if page == "Overview":
        overview(enrol, sli)
    elif page == "ML Forecast":
        forecast_page(sli)
    elif page == "Anomaly Detection":
        anomaly_page(sli)
    elif page == "AI Insights":
        ai_page(enrol, sli)
    elif page == "Power BI":
        power_bi_page()
    elif page == "Logout":
        st.session_state.logged_in = False
        st.rerun()
