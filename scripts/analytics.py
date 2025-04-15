import sqlite3
import pandas as pd
import os

DB_PATH = "../db/global_economic_tracker.sql"

def load_data(table_name: str) -> pd.DataFrame:
    con = sqlite3.connect(DB_PATH)
    df = pd.read_sql(f"SELECT * FROM {table_name}", con, parse_dates=["timestamp"])
    con.close()
    return df.sort_values(["ticker", "timestamp"])

def compute_analytics(df: pd.DataFrame) -> pd.DataFrame:
    df["daily_return"] = df.groupby("ticker")["close"].pct_change()
    df["rolling_avg"] = df.groupby("ticker")["close"].transform(lambda x: x.rolling(window=7).mean())
    df["rolling_volatility"] = df.groupby("ticker")["daily_return"].transform(lambda x: x.rolling(window=7).std())

    def max_drawdown(series):
        peak = series.expanding(min_periods=1).max()
        drawdown = (series - peak) / peak
        return drawdown

    df["drawdown"] = df.groupby("ticker")["close"].transform(max_drawdown)
    return df

def compute_sharpe_ratios(df: pd.DataFrame) -> pd.DataFrame:
    df = df.dropna(subset=["daily_return"])
    grouped = df.groupby("ticker")["daily_return"]
    sharpe = grouped.mean() / grouped.std()
    return sharpe.reset_index().rename(columns={"daily_return": "sharpe_ratio"})

def compute_correlation_matrix(df: pd.DataFrame) -> pd.DataFrame:
    pivot = df.pivot(index="timestamp", columns="ticker", values="daily_return")
    return pivot.corr()

def save_output(df: pd.DataFrame, name: str):
    os.makedirs("output", exist_ok=True)
    path = f"output/{name}_analytics.csv"
    df.to_csv(path, index=False)
    print(f"📄 Saved: {path}")

if __name__ == "__main__":
    print("📊 Computing analytics for indices...")
    index_df = load_data("index_history")
    index_analytics = compute_analytics(index_df)
    save_output(index_analytics, "index")

    print("🛢️ Computing analytics for commodities...")
    commodity_df = load_data("commodity_prices")
    commodity_analytics = compute_analytics(commodity_df)
    save_output(commodity_analytics, "commodity")

    print("📈 Calculating Sharpe ratios...")
    sharpe_index = compute_sharpe_ratios(index_analytics)
    sharpe_commodity = compute_sharpe_ratios(commodity_analytics)
    sharpe_index.to_csv("output/index_sharpe_ratios.csv", index=False)
    sharpe_commodity.to_csv("output/commodity_sharpe_ratios.csv", index=False)
    print("✅ Saved Sharpe ratios")

    print("📊 Calculating correlation matrices...")
    corr_index = compute_correlation_matrix(index_analytics)
    corr_commodity = compute_correlation_matrix(commodity_analytics)
    corr_index.to_csv("output/index_correlation_matrix.csv")
    corr_commodity.to_csv("output/commodity_correlation_matrix.csv")
    print("✅ Saved correlation matrices")

