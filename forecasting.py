import pandas as pd
import numpy as np
import streamlit as st
from sklearn.linear_model import LinearRegression

@st.cache_data
def generate_forecast(df, target_col, months_to_predict=6):
    """Predicts future service load using Linear Regression."""
    if df.empty or len(df) < 3:
        return None, 0

    # Aggregate by month for time-series training
    ts_df = df.groupby("month")[target_col].sum().reset_index()
    ts_df['month_dt'] = pd.to_datetime(ts_df['month'])
    ts_df = ts_df.sort_values('month_dt')
    ts_df["t"] = range(len(ts_df))

    # Model Training
    X = ts_df[["t"]]
    y = ts_df[target_col]
    model = LinearRegression()
    model.fit(X, y)

    # Future Prediction
    future_t = np.arange(len(ts_df), len(ts_df) + months_to_predict).reshape(-1, 1)
    prediction = model.predict(future_t)

    # Build Forecast Dataframe
    last_date = ts_df["month_dt"].max()
    future_dates = [last_date + pd.DateOffset(months=i+1) for i in range(months_to_predict)]
    
    forecast_df = pd.DataFrame({
        "month": [d.strftime("%Y-%m") for d in future_dates],
        target_col: prediction,
        "type": "Forecast"
    })

    history_df = ts_df[["month", target_col]].copy()
    history_df["type"] = "History"
    
    full_df = pd.concat([history_df, forecast_df]).reset_index(drop=True)
    growth = ((prediction[-1] - y.iloc[-1]) / y.iloc[-1] * 100) if y.iloc[-1] != 0 else 0
    
    return full_df, growth