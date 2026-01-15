import pandas as pd
import mysql.connector
import glob
import os
from prettytable import PrettyTable
import sys

# ==========================================
# 1. DATABASE CONFIG
# ==========================================

DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "Shravan123",
    "port": 3306
}

DB_NAME = "aadhaar_db"
DATA_DIR = "Data"

# ==========================================
# 2. DATABASE CONNECTION
# ==========================================

def get_connection(database=None):
    config = DB_CONFIG.copy()
    if database:
        config["database"] = database
    return mysql.connector.connect(**config)

# ==========================================
# 3. CHECK IF DATABASE EXISTS
# ==========================================

server_conn = get_connection()
server_cursor = server_conn.cursor()

server_cursor.execute("SHOW DATABASES")
databases = [db[0] for db in server_cursor.fetchall()]

if DB_NAME in databases:
    print(f"\nDatabase '{DB_NAME}' already exists.\n")

    conn = get_connection(DB_NAME)
    cursor = conn.cursor()

    summary = PrettyTable(["Table Name", "Total Rows"])

    for table in ["enrolment", "demographic", "biometric"]:
        cursor.execute(f"SHOW TABLES LIKE '{table}'")
        if cursor.fetchone():
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            summary.add_row([table, cursor.fetchone()[0]])
        else:
            summary.add_row([table, "Table not found"])

    print(summary)
    conn.close()
    server_conn.close()

    sys.exit("ETL skipped to prevent duplicate insertion.")

# ==========================================
# 4. CREATE DATABASE & TABLES (FIRST RUN)
# ==========================================

server_cursor.execute(f"CREATE DATABASE {DB_NAME}")
print(f"Database '{DB_NAME}' created successfully.")
server_conn.close()

conn = get_connection(DB_NAME)
cursor = conn.cursor()

# Enrolment table
cursor.execute("""
CREATE TABLE enrolment (
    id INT AUTO_INCREMENT PRIMARY KEY,
    date DATE,
    state VARCHAR(100),
    district VARCHAR(100),
    pincode VARCHAR(10),
    count_0_5 INT,
    count_5_17 INT,
    count_18_plus INT
)
""")

# Demographic table
cursor.execute("""
CREATE TABLE demographic (
    id INT AUTO_INCREMENT PRIMARY KEY,
    date DATE,
    state VARCHAR(100),
    district VARCHAR(100),
    pincode VARCHAR(10),
    count_5_17 INT,
    count_17 INT
)
""")

# Biometric table
cursor.execute("""
CREATE TABLE biometric (
    id INT AUTO_INCREMENT PRIMARY KEY,
    date DATE,
    state VARCHAR(100),
    district VARCHAR(100),
    pincode VARCHAR(10),
    count_5_17 INT,
    count_17 INT
)
""")

conn.commit()
print("Tables created successfully.")

# ==========================================
# 5. CLEANING & INSERT FUNCTIONS
# ==========================================

def fix_date(df):
    df["date"] = pd.to_datetime(df["date"], dayfirst=True, errors="coerce")
    df["date"] = df["date"].dt.strftime("%Y-%m-%d")
    return df

def insert_data(df, table, columns):
    query = f"""
    INSERT INTO {table} ({','.join(columns)})
    VALUES ({','.join(['%s'] * len(columns))})
    """
    df = df.where(pd.notnull(df), None)
    cursor.executemany(query, df[columns].values.tolist())
    conn.commit()

# ==========================================
# 6. PROCESS CSV FOLDERS
# ==========================================

def process(folder, table, rename_map, columns):
    files = glob.glob(os.path.join(DATA_DIR, folder, "*.csv"))
    print(f"\nProcessing {folder} ({len(files)} files)")

    for file in files:
        df = pd.read_csv(file)
        df = fix_date(df)
        df = df.rename(columns=rename_map)
        df = df[columns]

        insert_data(df, table, columns)
        print(f"  {os.path.basename(file)} : {len(df)} rows inserted")

# ==========================================
# 7. RUN ETL (ONLY FIRST TIME)
# ==========================================

process(
    "Enrolment",
    "enrolment",
    {
        "age_0_5": "count_0_5",
        "age_5_17": "count_5_17",
        "age_18_greater": "count_18_plus"
    },
    ["date", "state", "district", "pincode",
     "count_0_5", "count_5_17", "count_18_plus"]
)

process(
    "Demographic",
    "demographic",
    {
        "demo_age_5_17": "count_5_17",
        "demo_age_17_": "count_17"
    },
    ["date", "state", "district", "pincode",
     "count_5_17", "count_17"]
)

process(
    "Biometric",
    "biometric",
    {
        "bio_age_5_17": "count_5_17",
        "bio_age_17_": "count_17"
    },
    ["date", "state", "district", "pincode",
     "count_5_17", "count_17"]
)

# ==========================================
# 8. FINAL SUMMARY
# ==========================================

summary = PrettyTable(["Table Name", "Total Rows"])

for table in ["enrolment", "demographic", "biometric"]:
    cursor.execute(f"SELECT COUNT(*) FROM {table}")
    summary.add_row([table, cursor.fetchone()[0]])

print("\nETL COMPLETED SUCCESSFULLY\n")
print(summary)

conn.close()
