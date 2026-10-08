"""HW2 Section 3 - Mini-project example: "Tech vs Crypto Risk Monitor"

Data source : Yahoo Finance (yfinance), 6 months of daily prices for 5 assets
Architecture: Airflow on a GCP VM -> pandas analytics -> CSV + PNG charts in ~/airflow/data/mini
Analysis    : daily returns, 20-day rolling volatility (annualised), max drawdown,
              correlation matrix, 20/50-day moving-average trend signal

            fetch_prices
           /            \\
  compute_metrics     make_charts
           \\            /
            write_report
"""
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

ASSETS = ["AAPL", "MSFT", "NVDA", "BTC-USD", "ETH-USD"]   # change freely
OUT_DIR = os.path.expanduser("~/airflow/data/mini")
PRICES = os.path.join(OUT_DIR, "close_prices.csv")
METRICS = os.path.join(OUT_DIR, "risk_metrics.csv")
CORR = os.path.join(OUT_DIR, "correlation.csv")
REPORT = os.path.join(OUT_DIR, "report.txt")


def fetch_prices():
    import pandas as pd
    import yfinance as yf

    os.makedirs(OUT_DIR, exist_ok=True)
    closes = {}
    for a in ASSETS:
        h = yf.Ticker(a).history(period="6mo", interval="1d")
        h.index = pd.to_datetime(h.index).tz_localize(None).normalize()
        closes[a] = h["Close"]
        print(f"{a}: {len(h)} rows")
    # crypto trades every day, stocks only on weekdays -> keep common dates
    df = pd.DataFrame(closes).dropna()
    df.to_csv(PRICES, index_label="Date")
    print(f"saved {df.shape} to {PRICES}")


def compute_metrics():
    import numpy as np
    import pandas as pd

    px = pd.read_csv(PRICES, index_col="Date", parse_dates=True)
    ret = px.pct_change().dropna()

    rows = []
    for a in px.columns:
        running_max = px[a].cummax()
        drawdown = (px[a] / running_max - 1).min()
        ma20, ma50 = px[a].rolling(20).mean().iloc[-1], px[a].rolling(50).mean().iloc[-1]
        rows.append({
            "Asset": a,
            "6m Return %": round((px[a].iloc[-1] / px[a].iloc[0] - 1) * 100, 2),
            "Ann. Volatility %": round(ret[a].std() * np.sqrt(252) * 100, 2),
            "Latest 20d Vol %": round(ret[a].tail(20).std() * np.sqrt(252) * 100, 2),
            "Max Drawdown %": round(drawdown * 100, 2),
            "Trend (MA20 vs MA50)": "UP" if ma20 > ma50 else "DOWN",
        })
    metrics = pd.DataFrame(rows)
    metrics.to_csv(METRICS, index=False)
    ret.corr().round(3).to_csv(CORR)
    print(metrics.to_string(index=False))
    print("\nCorrelation of daily returns:\n", ret.corr().round(2))


def make_charts():
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import numpy as np
    import pandas as pd

    px = pd.read_csv(PRICES, index_col="Date", parse_dates=True)
    ret = px.pct_change().dropna()

    (px / px.iloc[0] * 100).plot(figsize=(10, 5), title="Growth of 100 (6 months)")
    plt.ylabel("Indexed price"); plt.tight_layout()
    plt.savefig(os.path.join(OUT_DIR, "growth.png")); plt.close()

    (ret.rolling(20).std() * np.sqrt(252) * 100).plot(figsize=(10, 5),
                                                      title="20-day rolling volatility (annualised %)")
    plt.tight_layout(); plt.savefig(os.path.join(OUT_DIR, "rolling_vol.png")); plt.close()

    corr = ret.corr()
    fig, ax = plt.subplots(figsize=(6, 5))
    im = ax.imshow(corr, vmin=-1, vmax=1, cmap="RdBu_r")
    ax.set_xticks(range(len(corr))); ax.set_xticklabels(corr.columns, rotation=45)
    ax.set_yticks(range(len(corr))); ax.set_yticklabels(corr.columns)
    for i in range(len(corr)):
        for j in range(len(corr)):
            ax.text(j, i, f"{corr.iloc[i, j]:.2f}", ha="center", va="center", fontsize=8)
    fig.colorbar(im); ax.set_title("Correlation of daily returns")
    fig.tight_layout(); fig.savefig(os.path.join(OUT_DIR, "correlation.png")); plt.close(fig)
    print("charts saved in", OUT_DIR)


def write_report():
    import pandas as pd

    m = pd.read_csv(METRICS)
    riskiest = m.sort_values("Ann. Volatility %", ascending=False).iloc[0]
    best = m.sort_values("6m Return %", ascending=False).iloc[0]
    lines = [
        f"Risk report generated {datetime.now():%Y-%m-%d %H:%M}",
        m.to_string(index=False),
        "",
        f"Most volatile asset: {riskiest['Asset']} ({riskiest['Ann. Volatility %']}% annualised)",
        f"Best 6-month performer: {best['Asset']} ({best['6m Return %']}%)",
    ]
    with open(REPORT, "w") as f:
        f.write("\n".join(lines))
    print("\n".join(lines))


with DAG(
    "mini_project_risk_monitor",
    default_args={"owner": "student", "retries": 1, "retry_delay": timedelta(minutes=1)},
    schedule="@daily",
    start_date=datetime(2026, 10, 1),
    catchup=False,
    tags=["hw2", "mini-project"],
) as dag:
    a = PythonOperator(task_id="fetch_prices", python_callable=fetch_prices)
    b = PythonOperator(task_id="compute_metrics", python_callable=compute_metrics)
    c = PythonOperator(task_id="make_charts", python_callable=make_charts)
    d = PythonOperator(task_id="write_report", python_callable=write_report)
    a >> [b, c] >> d
