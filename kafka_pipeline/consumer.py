import json
import sqlite3

from kafka import KafkaConsumer


def consume_messages():
    consumer = KafkaConsumer(
        "economic.daily.raw",
        bootstrap_servers="localhost:9092",
        auto_offset_reset='latest',
        enable_auto_commit=True,
        group_id='economic-data-consumer',
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        key_deserializer=lambda x: x.decode('utf-8') if x else None
    )

    print("📥 Listening for messages...\n")

    db = "../db/global_economic_tracker.sqlite"
    con = sqlite3.connect(db)
    cursor = con.cursor()

    cursor.execute("""
                   CREATE TABLE IF NOT EXISTS raw_prices
                   (
                       timestamp
                       TEXT,
                       ticker
                       TEXT,
                       price
                       REAL,
                       daily_return
                       REAL,
                       type
                       TEXT
                   )
                   """)

    try:
        for message in consumer:
            # key = message.key
            value = message.value

            cursor.execute(
                """
                INSERT INTO raw_prices (timestamp, ticker, price, daily_return, type)
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    value.get("timestamp"),
                    value.get("ticker"),
                    value.get("price"),
                    value.get("daily_return"),
                    value.get("type"),
                )
            )

            con.commit()

    except KeyboardInterrupt:
        print("🛑 Stopping consumer (KeyboardInterrupt)")
    except Exception as e:
        print(f"❌ Error: {e}")
    finally:
        consumer.close()
        con.close()
        print("👋 Consumer connection closed.")


if __name__ == "__main__":
    consume_messages()
