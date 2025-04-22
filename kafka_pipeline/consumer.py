import json
import os
import sqlite3
import threading
import time

import requests
from kafka import KafkaConsumer

DEBOUNCE_SECONDS = 15
AIRFLOW_DAG_TRIGGER_URL = "http://localhost:8080/api/v1/dags/global_econ_dbt_pipeline/dagRuns"
AIRFLOW_AUTH = (os.getenv("AIRFLOW_USER"), os.getenv("AIRFLOW_PASS"))

last_message_time = time.time()
dag_triggered = False


def trigger_airflow():
    global dag_triggered # noqa: F824
    dag_triggered = True
    print("🛫 Triggering Airflow DAG...")
    try:
        response = requests.post(AIRFLOW_DAG_TRIGGER_URL, auth=AIRFLOW_AUTH, json={})
        print(f"📡 Airflow response: {response.status_code} - {response.text}")
    except Exception as e:
        print(f"❌ Failed to trigger DAG: {e}")


def debounce_trigger():
    global last_message_time, dag_triggered # noqa: F824
    while True:
        if time.time() - last_message_time > DEBOUNCE_SECONDS and not dag_triggered:
            trigger_airflow()
        time.sleep(1)


def consume_messages():
    global last_message_time, dag_triggered # noqa: F824

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

    threading.Thread(target=debounce_trigger, daemon=True).start()

    try:
        for message in consumer:
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
            last_message_time = time.time()
            dag_triggered = False

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
