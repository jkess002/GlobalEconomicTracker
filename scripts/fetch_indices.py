import yfinance as yf
import sqlite3
import pandas as pd
import sys
from datetime import datetime, timezone

INDICES = [
    {"country": "USA", "name": "S&P 500", "ticker": "^GSPC"},
    {"country": "UK", "name": "FTSE 100", "ticker": "^FTSE"},
    {"country": "Germany", "name": "DAX", "ticker": "^GDAXI"},
    {"country": "Japan", "name": "Nikkei 225", "ticker": "^N225"},
    {"country": "China", "name": "Hang Seng", "ticker": "^HSI"},
    {"country": "France", "name": "CAC 40", "ticker": "^FCHI"},
    {"country": "Italy", "name": "FTSE MIB", "ticker": "FTSEMIB.MI"},
    {"country": "Canada", "name": "S&P/TSX Comp.", "ticker": "^GSPTSE"},
    {"country": "Australia", "name": "ASX 200", "ticker": "^AXJO"},
    {"country": "South Korea", "name": "KOSPI Composite Index", "ticker": "^KS11"},
    {"country": "India", "name": "Nifty 50", "ticker": "^NSEI"},
    {"country": "Brazil", "name": "Bovespa", "ticker": "^BVSP"},
    {"country": "Mexico", "name": "IPC Mexico", "ticker": "^MXX"},
    {"country": "Indonesia", "name": "IDX Composite", "ticker": "^JKSE"},
    {"country": "South Africa", "name": "Satrixx 40 ETF", "ticker": "STX40.JO"},
]

COMMODITIES = [
    {"name": "Gold", "ticker": "GC=F"},
    {"name": "Silver", "ticker": "SI=F"},
    {"name": "Platinum", "ticker": "PL=F"},
    {"name": "Crude Oil (WTI)", "ticker": "CL=F"},
    {"name": "Brent Crude", "ticker": "BZ=F"},
    {"name": "Natural Gas", "ticker": "NG=F"},
    {"name": "Copper", "ticker": "HG=F"},
]


def fetch_index_data(period="90d"):
    records = []
    hist_date_range = yf.download(
        [idx["ticker"] for idx in INDICES],
        period=period,
        interval="1d",
        group_by='ticker',
        auto_adjust=True,
        progress=False
    )

    for idx in INDICES:
        try:
            hist = hist_date_range[idx["ticker"]]["Close"].dropna()
            for date, close in hist.items():
                timestamp = date.strftime("%Y-%m-%d %H:%M:%S")
                records.append((timestamp, idx["country"], idx["name"], idx["ticker"], close))
        except Exception as e:
            print(f"⚠️ Failed to fetch index: {idx['ticker']} - {e}")

    return pd.DataFrame(records, columns=["timestamp", "country", "index_name", "ticker", "close"])


def fetch_commodity_data(period="90d"):
    records = []
    for item in COMMODITIES:
        try:
            ticker = yf.Ticker(item["ticker"])
            hist = ticker.history(period=period, interval="1d")["Close"].dropna()
            if hist.empty:
                print(f"⚠️ No data for {item['ticker']}")
                continue

            for date, close in hist.items():
                if not isinstance(date, pd.Timestamp):
                    continue
                timestamp = date.strftime("%Y-%m-%d %H:%M:%S")
                records.append((timestamp, item["name"], item["ticker"], close))

        except Exception as e:
            print(f"⚠️ Error fetching {item['ticker']}: {e}")

    return pd.DataFrame(records, columns=["timestamp", "name", "ticker", "close"])

def save_to_sqlite(backfill=False):
    if backfill:
        db = "db/global_economic_tracker.sqlite"
    else:
        db = "../db/global_economic_tracker.sqlite"
    con = sqlite3.connect(db)
    cursor = con.cursor()

    cursor.execute("""
            CREATE TABLE IF NOT EXISTS index_history (
                timestamp TEXT,
                country TEXT,
                index_name TEXT,
                ticker TEXT,
                close REAL
            )
        """)

    df_index = fetch_index_data("90d" if backfill else "1d")
    df_index.to_sql("index_history", con, if_exists="append", index=False)
    print(f"✅ Saved {len(df_index)} index records.")

    cursor.execute("""
            CREATE TABLE IF NOT EXISTS commodity_prices (
                timestamp TEXT,
                name TEXT,
                ticker TEXT,
                close REAL
            )
        """)

    df_commodities = fetch_commodity_data("90d" if backfill else "5d")
    df_commodities.to_sql("commodity_prices", con, if_exists="append", index=False)
    print(f"✅ Saved {len(df_commodities)} commodity records.")

    con.close()

if __name__ == "__main__":
    backfill = "--backfill" in sys.argv
    if backfill:
        print("📦 Running 90-day backfill...")
    else:
        print("📅 Fetching current data...")
    save_to_sqlite(backfill=backfill)
