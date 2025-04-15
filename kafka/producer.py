import json
from kafka import KafkaProducer
import pandas as pd
from datetime import datetime
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from scripts.fetch_indices import INDICES, COMMODITIES

INDEX_TICKERS = {d["ticker"] for d in INDICES}
COMMODITY_TICKERS = {d["ticker"] for d in COMMODITIES}

KAFKA_TOPIC = "economic.daily.raw"
KAFKA_BROKER = "localhost:9092"
DATA_PATH = "output/index_analytics.csv"

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    retries=5,
    retry_backoff_ms=2000,
)

def stream_to_kafka():
    df = pd.read_csv(DATA_PATH, parse_dates=["timestamp"])
    latest = df["timestamp"].max()
    latest_df = df[df["timestamp"] == latest]

    for _, row in latest_df.iterrows():
        asset_type = (
            "index" if row["ticker"] in INDEX_TICKERS
            else "commodity" if row["ticker"] in COMMODITY_TICKERS
            else "unknown"
        )
        message = {
            "timestamp": row["timestamp"].strftime("%Y-%m-%d"),
            "ticker": row["ticker"],
            "price": round(row["price"], 2) if "price" in row else None,
            "daily_return": round(row["daily_return"], 6),
            "type": asset_type
        }
        composite_key = f"{message['type']}|{message['ticker']}"
        producer.send(KAFKA_TOPIC, key=composite_key.encode("utf-8"), value=message)
        print(f"✅ Sent: {message}")

    producer.flush()
    print("📤 Kafka streaming complete.")

if __name__ == "__main__":
    stream_to_kafka()
