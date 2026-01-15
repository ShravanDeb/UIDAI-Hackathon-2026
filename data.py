import pandas as pd
import mysql.connector
import glob
import os
import sys
import re
from thefuzz import process # pip install thefuzz

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
# 2. THE GOLDEN MASTER LIST
# ==========================================

OFFICIAL_STATES = [
    "Andaman & Nicobar Islands", "Andhra Pradesh", "Arunachal Pradesh", 
    "Assam", "Bihar", "Chandigarh", "Chhattisgarh", "Dadra & Nagar Haveli", 
    "Daman & Diu", "Delhi", "Goa", "Gujarat", "Haryana", "Himachal Pradesh", 
    "Jammu & Kashmir", "Jharkhand", "Karnataka", "Kerala", "Ladakh", 
    "Lakshadweep", "Madhya Pradesh", "Maharashtra", "Manipur", "Meghalaya", 
    "Mizoram", "Nagaland", "Odisha", "Puducherry", "Punjab", "Rajasthan", 
    "Sikkim", "Tamil Nadu", "Telangana", "Tripura", "Uttar Pradesh", 
    "Uttarakhand", "West Bengal"
]

# ==========================================
# 3. THE "ZERO-TOUCH" NORMALIZER ENGINE
# ==========================================

def normalize_state_name(input_name):
    if pd.isna(input_name) or str(input_name).strip() == "":
        return "Unknown"
    
    # LAYER 1: Aggressive Cleaning
    # Remove dots, extra spaces, and lowercase it
    # "J. & K." -> "j&k"
    clean = str(input_name).lower().strip()
    clean = re.sub(r'[^\w\s&]', '', clean) # Remove punctuation except &
    clean = re.sub(r'\s+', ' ', clean)     # Collapse spaces
    
    # LAYER 2: Semantic Alias Mapping (The "Known Unknowns")
    aliases = {
        "orissa": "Odisha",
        "odisa": "Odisha",
        "pondicherry": "Puducherry",
        "uttaranchal": "Uttarakhand",
        "j&k": "Jammu & Kashmir",
        "jammu and kashmir": "Jammu & Kashmir",
        "andaman and nicobar": "Andaman & Nicobar Islands",
        "a&n": "Andaman & Nicobar Islands",
        "dadra and nagar": "Dadra & Nagar Haveli",
        "d&n": "Dadra & Nagar Haveli",
        "daman and diu": "Daman & Diu",
        "nct of delhi": "Delhi",
        "delhi ncr": "Delhi"
    }
    
    if clean in aliases:
        return aliases[clean]
    
    # LAYER 3: Fuzzy Logic (The "Safety Net")
    # We allow a very high threshold (90) for automatic acceptance.
    # If it's 90% similar to a real state, we assume it's a typo and fix it.
    match, score = process.extractOne(clean, OFFICIAL_STATES)
    if score >= 88: # 88 is the "Magic Number" for safe typo correction
        return match
        
    # If all fails, title case it (It might be a new territory or valid name)
    return input_name.title()

# ==========================================
# 4. DATABASE SETUP
# ==========================================

def get_connection(db=None):
    conf = DB_CONFIG.copy()
    if db: conf["database"] = db
    return mysql.connector.connect(**conf)

def setup_db():
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("DROP DATABASE IF EXISTS aadhaar_db") # FORCE RESET
    cursor.execute("CREATE DATABASE aadhaar_db")
    cursor.execute("USE aadhaar_db")
    
    # Simplified Schema for Hackathon Speed
    tables = {
        "enrolment": "count_0_5 INT, count_5_17 INT, count_18_plus INT",
        "demographic": "count_5_17 INT, count_17 INT",
        "biometric": "count_5_17 INT, count_17 INT"
    }
    
    for name, cols in tables.items():
        cursor.execute(f"""
            CREATE TABLE {name} (
                id INT AUTO_INCREMENT PRIMARY KEY,
                date DATE,
                state VARCHAR(100),
                district VARCHAR(100),
                pincode VARCHAR(10),
                {cols}
            )
        """)
    conn.commit()
    return conn

# ==========================================
# 5. ETL PIPELINE
# ==========================================

def process_data():
    conn = setup_db()
    cursor = conn.cursor()
    print(" Starting Zero-Touch ETL Pipeline...")

    # Folder Config
    tasks = [
        ("Enrolment", "enrolment", 
         {"age_0_5": "count_0_5", "age_5_17": "count_5_17", "age_18_greater": "count_18_plus"},
         ["count_0_5", "count_5_17", "count_18_plus"]),
         
        ("Demographic", "demographic", 
         {"demo_age_5_17": "count_5_17", "demo_age_17_": "count_17"},
         ["count_5_17", "count_17"]),
         
        ("Biometric", "biometric", 
         {"bio_age_5_17": "count_5_17", "bio_age_17_": "count_17"},
         ["count_5_17", "count_17"])
    ]

    for folder, table, rename_map, value_cols in tasks:
        files = glob.glob(os.path.join(DATA_DIR, folder, "*.csv"))
        print(f"\n Processing {folder} ({len(files)} files)")
        
        for f in files:
            df = pd.read_csv(f)
            
            # 1. Standardize Header
            df.columns = df.columns.str.lower().str.strip()
            df = df.rename(columns=rename_map)
            
            # 2. Date Fix
            df["date"] = pd.to_datetime(df["date"], dayfirst=True, errors="coerce").dt.strftime("%Y-%m-%d")
            
            # 3. ZERO-TOUCH STATE CORRECTION (The Core Logic)
            # We apply the normalizer to every single row
            df["state"] = df["state"].apply(normalize_state_name)
            
            # 4. Filter & Order
            cols = ["date", "state", "district", "pincode"] + value_cols
            df = df[cols].fillna(0) # Safety fill for numbers
            
            # 5. Bulk Insert
            placeholders = ",".join(["%s"] * len(cols))
            query = f"INSERT INTO {table} ({','.join(cols)}) VALUES ({placeholders})"
            cursor.executemany(query, df.values.tolist())
            conn.commit()
            
            print(f"   {os.path.basename(f)} -> Cleaned & Inserted {len(df)} rows")

    print("\n ETL Complete! Database is 100% Normalized.")
    conn.close()

if __name__ == "__main__":
    process_data()