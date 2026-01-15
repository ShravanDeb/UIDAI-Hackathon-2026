# ============================================================
# Aadhaar Seva-Drishti : Final Analysis Script
# Data Source : MySQL (aadhaar_db)
# Tables      : enrolment, demographic, biometric
# ============================================================

import pandas as pd
import mysql.connector
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np

# ------------------------------------------------------------
# 1. DATABASE CONFIG
# ------------------------------------------------------------

DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "Shravan123",
    "database": "aadhaar_db",
    "port": 3306
}

# ------------------------------------------------------------
# 2. MYSQL CONNECTION
# ------------------------------------------------------------

def get_connection():
    return mysql.connector.connect(**DB_CONFIG)

# ------------------------------------------------------------
# 3. LOAD TABLE FROM MYSQL
# ------------------------------------------------------------

def load_table(table_name):
    conn = get_connection()
    df = pd.read_sql(f"SELECT * FROM {table_name}", conn)
    conn.close()
    return df

# ------------------------------------------------------------
# 4. COMMON PREPROCESSING
# ------------------------------------------------------------

def preprocess_base(df):
    df.columns = df.columns.str.lower().str.strip()
    df['date'] = pd.to_datetime(df['date'])
    df['month'] = df['date'].dt.to_period('M').astype(str)
    df['state'] = df['state'].str.strip().str.title()
    df['district'] = df['district'].str.strip().str.title()
    return df

# ------------------------------------------------------------
# 5. RESHAPE ENROLMENT TABLE (WIDE → LONG)
# ------------------------------------------------------------

def reshape_enrolment(df):
    df = preprocess_base(df)

    df_long = df.melt(
        id_vars=['date', 'month', 'state', 'district', 'pincode'],
        value_vars=['count_0_5', 'count_5_17', 'count_18_plus'],
        var_name='age_group',
        value_name='count'
    )

    df_long['age_group'] = df_long['age_group'].replace({
        'count_0_5': '0-5',
        'count_5_17': '5-17',
        'count_18_plus': '18+'
    })

    return df_long.dropna(subset=['count'])

# ------------------------------------------------------------
# 6. RESHAPE UPDATE TABLES (DEMOGRAPHIC & BIOMETRIC)
# ------------------------------------------------------------

def reshape_updates(df):
    df = preprocess_base(df)

    df_long = df.melt(
        id_vars=['date', 'month', 'state', 'district', 'pincode'],
        value_vars=['count_5_17', 'count_17'],
        var_name='age_group',
        value_name='count'
    )

    df_long['age_group'] = df_long['age_group'].replace({
        'count_5_17': '5-17',
        'count_17': '17+'
    })

    return df_long.dropna(subset=['count'])

# ------------------------------------------------------------
# 7. LOAD & TRANSFORM DATA
# ------------------------------------------------------------

print("Loading data from MySQL...")

enrol_df = reshape_enrolment(load_table("enrolment"))
demo_df  = reshape_updates(load_table("demographic"))
bio_df   = reshape_updates(load_table("biometric"))

print("Data loaded and transformed successfully.")

# ------------------------------------------------------------
# 8. UNIVARIATE ANALYSIS – TEMPORAL TRENDS
# ------------------------------------------------------------

monthly_enrol = enrol_df.groupby('month')['count'].sum()

plt.figure(figsize=(10,5))
monthly_enrol.plot(marker='o')
plt.title("Monthly Aadhaar Enrolment Trend")
plt.xlabel("Month")
plt.ylabel("Total Enrolments")
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()

# ------------------------------------------------------------
# 9. AGE-WISE DISTRIBUTION
# ------------------------------------------------------------

age_dist = enrol_df.groupby('age_group')['count'].sum()

plt.figure(figsize=(6,4))
age_dist.plot(kind='bar')
plt.title("Age-wise Aadhaar Enrolment Distribution")
plt.ylabel("Total Enrolments")
plt.tight_layout()
plt.show()

# ------------------------------------------------------------
# 10. STATE-WISE ENROLMENT COMPARISON
# ------------------------------------------------------------

state_enrol = enrol_df.groupby('state')['count'].sum().sort_values(ascending=False)

plt.figure(figsize=(10,5))
state_enrol.head(10).plot(kind='bar')
plt.title("Top 10 States by Aadhaar Enrolment")
plt.ylabel("Total Enrolments")
plt.tight_layout()
plt.show()

# ------------------------------------------------------------
# 11. DISTRICT × MONTH HEATMAP (STRESS VISUALISATION)
# ------------------------------------------------------------

heatmap_df = (
    enrol_df.groupby(['district', 'month'])['count']
    .sum()
    .unstack(fill_value=0)
)

plt.figure(figsize=(12,6))
sns.heatmap(heatmap_df, cmap="YlOrRd")
plt.title("District-wise Aadhaar Enrolment Intensity")
plt.tight_layout()
plt.show()

# ------------------------------------------------------------
# 12. BIOMETRIC UPDATE LIFECYCLE ANALYSIS
# ------------------------------------------------------------

bio_trend = (
    bio_df.groupby(['month', 'age_group'])['count']
    .sum()
    .unstack()
)

plt.figure(figsize=(10,5))
bio_trend.plot(marker='o')
plt.title("Biometric Updates by Age Group (Lifecycle Pattern)")
plt.ylabel("Update Count")
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()

# ------------------------------------------------------------
# 13. ANOMALY DETECTION (Z-SCORE METHOD)
# ------------------------------------------------------------

district_mean = enrol_df.groupby('district')['count'].mean()
district_std  = enrol_df.groupby('district')['count'].std()

enrol_df['z_score'] = (
    enrol_df['count'] - enrol_df['district'].map(district_mean)
) / enrol_df['district'].map(district_std)

anomalies = enrol_df[enrol_df['z_score'].abs() > 3]

print("\nAnomalous District-Month Records (Sample):")
print(anomalies[['month', 'district', 'count', 'z_score']].head())

# ------------------------------------------------------------
# 14. SERVICE LOAD INTENSITY (SLI)
# ------------------------------------------------------------

sli_df = pd.concat([enrol_df, demo_df, bio_df])

sli = (
    sli_df.groupby(['month', 'district'])['count']
    .sum()
    .reset_index()
)

high_stress = (
    sli.groupby('district')['count']
    .mean()
    .sort_values(ascending=False)
    .head(10)
)

plt.figure(figsize=(8,4))
high_stress.plot(kind='bar')
plt.title("High Service Load Intensity Districts")
plt.ylabel("Average Service Load")
plt.tight_layout()
plt.show()

# ------------------------------------------------------------
# 15. AUTO-GENERATED INSIGHTS (FOR REPORT)
# ------------------------------------------------------------

print("\nKEY INSIGHTS FOR UIDAI REPORT")

print("Peak Service Demand Month:",
      sli.groupby('month')['count'].sum().idxmax())

print("Highest Enrolment Age Group:",
      enrol_df.groupby('age_group')['count'].sum().idxmax())

print("Most Stressed District:",
      high_stress.idxmax())

# ------------------------------------------------------------
# END OF SCRIPT
# ------------------------------------------------------------
