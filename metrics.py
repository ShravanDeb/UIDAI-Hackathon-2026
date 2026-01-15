import pandas as pd
import numpy as np
from sklearn.preprocessing import MinMaxScaler

def calculate_sli_and_risk(enrol_df, demo_df, bio_df):
    """
    Calculates Service Load Intensity (SLI) and Risk Scores for each district.
    Input DFs must have 'district', 'month', and 'count' columns.
    """
    # 1. Aggregate by District & Month
    # We rename 'count' to specific types to avoid confusion after merging
    e_agg = enrol_df.groupby(['district', 'month'])['count'].sum().reset_index().rename(columns={'count': 'enrol_count'})
    d_agg = demo_df.groupby(['district', 'month'])['count'].sum().reset_index().rename(columns={'count': 'demo_count'})
    b_agg = bio_df.groupby(['district', 'month'])['count'].sum().reset_index().rename(columns={'count': 'bio_count'})

    # 2. Merge all datasets (Outer join ensures we don't lose districts active in only one service)
    merged = e_agg.merge(d_agg, on=['district', 'month'], how='outer') \
                  .merge(b_agg, on=['district', 'month'], how='outer') \
                  .fillna(0)

    # 3. Calculate Service Load Intensity (SLI) - Weighted Effort
    # Weights: Biometric (1.5) > Enrolment (1.0) > Demographic (0.8)
    merged['sli_score'] = (merged['enrol_count'] * 1.0) + \
                          (merged['bio_count'] * 1.5) + \
                          (merged['demo_count'] * 0.8)

    # 4. Calculate Risk Score (Normalized 0-100)
    # This standardizes the score so you can compare small and large districts fairly
    scaler = MinMaxScaler(feature_range=(0, 100))
    # Reshape is needed for the scaler
    merged['risk_score'] = scaler.fit_transform(merged[['sli_score']])

    # 5. Categorize Risk
    # Critical = Top 10% of stress, High = >50 score, Normal = Rest
    threshold = merged['risk_score'].quantile(0.90)
    
    conditions = [
        (merged['risk_score'] >= threshold),
        (merged['risk_score'] >= 50) & (merged['risk_score'] < threshold),
        (merged['risk_score'] < 50)
    ]
    choices = ['Critical', 'High', 'Normal']
    
    merged['risk_category'] = np.select(conditions, choices, default='Normal')
    
    return merged

def get_top_critical_districts(sli_df):
    """
    Returns the top 10 most critical districts based on the most recent month's data.
    """