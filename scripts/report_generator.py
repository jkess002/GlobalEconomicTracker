import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import base64
from io import BytesIO
import os

OUTPUT_DIR = "output"
REPORT_FILE = os.path.join(OUTPUT_DIR, "daily_report.html")

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

NAME_MAP = {item["ticker"]: item["name"] for item in INDICES + COMMODITIES}

def fig_to_base64(fig):
    buf = BytesIO()
    fig.savefig(buf, format="png", bbox_inches="tight")
    buf.seek(0)
    return base64.b64encode(buf.read()).decode("utf-8")

def styled_df_to_html(df, title, color_column=None):
    styled = df.style.set_table_attributes('class="styled-table"').format({
        "Daily Return": "{:.2%}",
        "Sharpe Ratio": "{:.2f}",
        "Z-Score": "{:.2f}",
        "Volatility Percentile": "{:.2f}"
    })
    if color_column:
        def highlight_row(row):
            color = '#d4f4dd' if row[color_column] > 0 else '#f9d0c4'
            return ['background-color: {}'.format(color) if col == color_column else '' for col in row.index]
        styled = styled.apply(highlight_row, axis=1)
    return f"<div class='table-block'><h2>{title}</h2>{styled.to_html(index=False)}</div>"

def calculate_z_scores(df):
    z_df = df.copy()
    z_df["z_score"] = z_df.groupby("ticker")["daily_return"].transform(
        lambda x: (x - x.rolling(7).mean()) / x.rolling(7).std()
    )
    return z_df

def calculate_volatility_percentile(df):
    df = df.copy()
    df["rolling_volatility"] = df.groupby("ticker")["daily_return"].transform(lambda x: x.rolling(7).std())
    def percentile_rank(x):
        return x.rank(pct=True).iloc[-1] * 100 if not x.dropna().empty else None
    percentile_df = df.groupby("ticker")["rolling_volatility"].apply(percentile_rank).reset_index()
    percentile_df.columns = ["ticker", "volatility_pctile"]
    return percentile_df

def create_correlation_heatmap(csv_path, title):
    df = pd.read_csv(csv_path, index_col=0)
    df.rename(columns=NAME_MAP, index=NAME_MAP, inplace=True)
    fig, ax = plt.subplots(figsize=(10, 8))
    sns.heatmap(df, annot=True, cmap="coolwarm", center=0, fmt=".2f", ax=ax)
    ax.set_title(title)
    return fig_to_base64(fig)

def create_rolling_plot(csv_path, title):
    df = pd.read_csv(csv_path, parse_dates=["timestamp"])
    df["label"] = df["ticker"].map(NAME_MAP)
    df = df[df["timestamp"] >= df["timestamp"].max() - pd.Timedelta(days=30)]
    df_grouped = df.groupby("label")
    fig, ax = plt.subplots(figsize=(10, 6))
    for label, group in df_grouped:
        min_val = group["rolling_avg"].min()
        max_val = group["rolling_avg"].max()
        normalized = (group["rolling_avg"] - min_val) / (max_val - min_val)
        ax.plot(group["timestamp"], normalized, label=label)
    ax.set_title(title)
    ax.set_xlabel("Date")
    ax.set_ylabel("Normalized 7-day Rolling Avg")
    ax.legend(loc="center left", bbox_to_anchor=(1, 0.5), fontsize="x-small")
    plt.tight_layout()
    return fig_to_base64(fig)

def generate_combined_table(perf_path, sharpe_path, title):
    perf = pd.read_csv(perf_path, parse_dates=["timestamp"])
    perf = calculate_z_scores(perf)
    vol_df = calculate_volatility_percentile(perf)

    latest = perf["timestamp"].max()
    latest_df = perf[perf["timestamp"] == latest][["ticker", "daily_return", "z_score"]].dropna()
    latest_df = latest_df.merge(vol_df, on="ticker", how="left")

    sharpe = pd.read_csv(sharpe_path)
    merged = latest_df.merge(sharpe, on="ticker")
    merged["name"] = merged["ticker"].map(NAME_MAP)
    merged = merged[["name", "ticker", "daily_return", "sharpe_ratio", "z_score", "volatility_pctile"]]

    merged = merged.rename(columns={
        "name": "Name",
        "ticker": "Ticker",
        "daily_return": "Daily Return",
        "sharpe_ratio": "Sharpe Ratio",
        "z_score": "Z-Score",
        "volatility_pctile": "Volatility Percentile"
    })
    merged = merged.sort_values("Daily Return", ascending=False)
    return styled_df_to_html(merged, f"{title} — Daily Change, Sharpe, Z-Score & Volatility", color_column="Daily Return")

def generate_report():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    html_parts = ["""
    <html>
    <head>
        <style>
            body { font-family: Arial, sans-serif; }
            .styled-table {
                border-collapse: collapse;
                margin: 10px;
                font-size: 0.9em;
                min-width: 400px;
                border: 1px solid #ddd;
            }
            .styled-table th, .styled-table td {
                padding: 8px 12px;
                border: 1px solid #ddd;
                text-align: right;
            }
            h2 { color: #333; }
            img { max-width: 100%; height: auto; }
            .flex-container {
                display: flex;
                flex-wrap: wrap;
                justify-content: space-between;
            }
            .table-block {
                flex: 0 0 48%;
                margin-bottom: 20px;
            }
        </style>
    </head>
    <body>
    <h1>📊 Global Economic Tracker — Daily Report</h1>
    <div class="flex-container">
    """]

    html_parts.append(generate_combined_table(
        f"{OUTPUT_DIR}/index_analytics.csv", f"{OUTPUT_DIR}/index_sharpe_ratios.csv", "Indices"
    ))
    html_parts.append(generate_combined_table(
        f"{OUTPUT_DIR}/commodity_analytics.csv", f"{OUTPUT_DIR}/commodity_sharpe_ratios.csv", "Commodities"
    ))
    html_parts.append("</div>")

    for name in ["index", "commodity"]:
        heatmap_img = create_correlation_heatmap(f"{OUTPUT_DIR}/{name}_correlation_matrix.csv", f"Correlation Matrix ({name.title()})")
        html_parts.append(f"<h2>Correlation Matrix ({name.title()})</h2><img src='data:image/png;base64,{heatmap_img}' />")

    for name in ["index", "commodity"]:
        rolling_img = create_rolling_plot(f"{OUTPUT_DIR}/{name}_analytics.csv", f"7-Day Rolling Averages ({name.title()})")
        html_parts.append(f"<h2>7-Day Rolling Averages ({name.title()})</h2><img src='data:image/png;base64,{rolling_img}' />")

    html_parts.append("</body></html>")

    with open(REPORT_FILE, "w", encoding="utf-8") as f:
        f.write("\n".join(html_parts))

    print(f"✅ Report generated: {REPORT_FILE}")

if __name__ == "__main__":
    generate_report()