"""HW2 Q2.2 - End-to-end stock watchlist volatility pipeline.

fetch_data  ->  analytics_summary  ->  volatility_alert
"""
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

# choose your own 
TICKERS = ["NVDA", "TSLA", "SPY"]      # any 3 stocks / ETFs / cryptos (e.g. "BTC-USD")
ALERT_THRESHOLD_PCT = 3.0              # alert if the latest day's swing >= 3%
N_DAYS = 30                            # last 30 trading days


DATA_DIR = os.path.expanduser("~/airflow/data")
PRICES_CSV = os.path.join(DATA_DIR, "watchlist_prices.csv")
SUMMARY_CSV = os.path.join(DATA_DIR, "watchlist_summary.csv")


def fetch_data():
    """Step 1: download the last 30 trading days of daily prices for each ticker."""
    import pandas as pd
    import yfinance as yf

    os.makedirs(DATA_DIR, exist_ok=True)
    frames = []
    for ticker in TICKERS:
        hist = yf.Ticker(ticker).history(period="3mo", interval="1d", auto_adjust=False)
        if hist.empty:
            raise ValueError(f"No data returned for {ticker}")
        hist = hist.tail(N_DAYS).reset_index()
        hist["Date"] = pd.to_datetime(hist["Date"]).dt.strftime("%Y-%m-%d")
        hist["Ticker"] = ticker
        frames.append(hist[["Date", "Ticker", "Open", "High", "Low", "Close", "Volume"]])
        print(f"{ticker}: {len(hist)} rows, {hist['Date'].iloc[0]} -> {hist['Date'].iloc[-1]}")

    prices = pd.concat(frames, ignore_index=True)
    prices.to_csv(PRICES_CSV, index=False)
    print(f"Saved {len(prices)} rows to {PRICES_CSV}")


def analytics_summary():
    """Step 2: 30-day return and average daily swing per ticker -> watchlist_summary.csv"""
    import pandas as pd

    prices = pd.read_csv(PRICES_CSV)
    prices["Swing %"] = (prices["High"] - prices["Low"]) / prices["Open"] * 100

    rows = []
    for ticker, g in prices.groupby("Ticker", sort=False):
        g = g.sort_values("Date")
        initial_close = g["Close"].iloc[0]
        latest_close = g["Close"].iloc[-1]
        rows.append({
            "Ticker": ticker,
            "Start Date": g["Date"].iloc[0],
            "End Date": g["Date"].iloc[-1],
            "Initial Close": round(initial_close, 2),
            "Latest Close": round(latest_close, 2),
            "30d Return %": round((latest_close - initial_close) / initial_close * 100, 2),
            "Avg Daily Swing %": round(g["Swing %"].mean(), 2),
            "Latest Swing %": round(g["Swing %"].iloc[-1], 2),
        })

    summary = pd.DataFrame(rows)
    summary.to_csv(SUMMARY_CSV, index=False)
    print(summary.to_string(index=False))
    print(f"Saved summary to {SUMMARY_CSV}")


def volatility_alert():
    """Step 3: alert if any ticker's latest-day swing exceeded the threshold."""
    import pandas as pd

    summary = pd.read_csv(SUMMARY_CSV)
    alerts = summary[summary["Latest Swing %"] >= ALERT_THRESHOLD_PCT]
    if alerts.empty:
        print(f"All assets traded within normal volatility ranges "
              f"(threshold {ALERT_THRESHOLD_PCT}%).")
    else:
        for _, r in alerts.iterrows():
            print(f"ALERT: {r['Ticker']} experienced high volatility "
                  f"({r['Latest Swing %']:.1f}% swing) on {r['End Date']}")


with DAG(
    "q22_watchlist_pipeline",
    default_args={"owner": "student", "retries": 1, "retry_delay": timedelta(minutes=1)},
    description="Stock watchlist volatility pipeline",
    schedule="@daily",
    start_date=datetime(2026, 10, 1),
    catchup=False,
    tags=["hw2"],
) as dag:
    t_fetch = PythonOperator(task_id="fetch_data", python_callable=fetch_data)
    t_summary = PythonOperator(task_id="analytics_summary", python_callable=analytics_summary)
    t_alert = PythonOperator(task_id="volatility_alert", python_callable=volatility_alert)

    t_fetch >> t_summary >> t_alert
