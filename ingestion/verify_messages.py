import json
import os

from dotenv import load_dotenv
from kafka import KafkaConsumer

def main():
    load_dotenv()

    consumer = KafkaConsumer(
        os.getenv("KAFKA_TOPIC", "market-data"),
        bootstrap_servers=os.getenv(
            "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"
        ),
        auto_offset_reset="earliest",
        group_id="market-data-verification",
        consumer_timeout_ms=10000,
        value_deserializer=lambda data: json.loads(
            data.decode("utf-8")
        ),
    )

    count = 0

    try:
        for message in consumer:
            print(message.value)
            count += 1
    finally:
        consumer.close()

    print(f"Read {count} observations")


if __name__ == "__main__":
    main()
