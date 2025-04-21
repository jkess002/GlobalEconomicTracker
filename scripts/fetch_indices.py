import yfinance as yf
from kafka import KafkaProducer
from sqlalchemy import create_engine, text
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import Table, MetaData
import pandas as pd
import sys, os, psycopg2, json
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

def insert_on_conflict(engine, table_name, df, unique_cols):
    metadata = MetaData(bind=engine)
    metadata.reflect()
    table = Table(table_name, metadata, autoload_with=engine)
    stmt = insert(table).values(df.to_dict(orient="records"))
    stmt = stmt.on_conflict_do_nothing(index_elements=unique_cols)
    with engine.begin() as conn:
        conn.execute(stmt)

def table_has_data(conn, table_name):
    result = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
    count = result.scalar()
    return count > 0

def save_to_postgres(backfill=False):
    user = os.getenv("POSTGRES_USER")
    password = os.getenv("POSTGRES_PASSWORD")
    host = os.getenv("POSTGRES_HOST")
    port = os.getenv("POSTGRES_PORT")
    dbname = os.getenv("POSTGRES_DB")


    engine = create_engine(f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}")
    conn = engine.connect()

    producer = KafkaProducer(
        bootstrap_servers=os.getenv("KAFKA_BROKER", "localhost:9092"),
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

    with conn.begin():
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS index_prices (
                timestamp TIMESTAMP,
                country TEXT,
                name TEXT,
                ticker TEXT,
                price DOUBLE PRECISION
            )
        """))

        conn.execute(text("""
            DO $$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1 FROM pg_constraint
                    WHERE conname = 'index_prices_unique'
                ) THEN
                    ALTER TABLE index_prices
                    ADD CONSTRAINT index_prices_unique UNIQUE (timestamp, ticker);
                END IF;
            END
            $$;
        """))

        index_has_data = table_has_data(conn, "index_prices")
        commodity_has_data = table_has_data(conn, "commodity_prices")
        df_index = fetch_index_data("90d" if backfill or not index_has_data else "3d")
        df_index.columns = ["timestamp", "country", "name", "ticker", "price"]
        insert_on_conflict(engine, "index_prices", df_index, ["timestamp", "ticker"])
        for _, row in df_index.iterrows():
            producer.send("index_data", row.to_dict())
        print(f"✅ Saved {len(df_index)} index records.")

        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS commodity_prices (
                timestamp TIMESTAMP,
                name TEXT,
                ticker TEXT,
                price DOUBLE PRECISION
            )
        """))

        conn.execute(text("""
            DO $$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1 FROM pg_constraint
                    WHERE conname = 'commodity_prices_unique'
                ) THEN
                    ALTER TABLE commodity_prices
                    ADD CONSTRAINT commodity_prices_unique UNIQUE (timestamp, ticker);
                END IF;
            END
            $$;
        """))

        df_commodities = fetch_commodity_data("90d" if backfill or not commodity_has_data else "3d")
        df_commodities.columns = ["timestamp", "name", "ticker", "price"]
        insert_on_conflict(engine, "commodity_prices", df_commodities, ["timestamp", "ticker"])
        for _, row in df_commodities.iterrows():
            producer.send("commodity_data", row.to_dict())
        producer.flush()

        print(f"✅ Saved {len(df_commodities)} commodity records.")

    conn.close()

if __name__ == "__main__":
    backfill = "--backfill" in sys.argv
    print("📦 Running 90-day backfill..." if backfill else "📅 Fetching current data...")
    save_to_postgres(backfill=backfill)
