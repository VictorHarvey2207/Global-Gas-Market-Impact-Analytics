import json
import os

from kafka import KafkaProducer

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "market-data"

def create_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda record: json.dumps(record).encode("utf-8"),
    )


def publish(producer, source, indicator, observation_date, value):
    if value is None:
        return False

    try:
        if str(value).strip() in ("", "."):
            return False

        numeric_value = float(value)

        if numeric_value != numeric_value:  # NaN check
            return False

        record = {
            "source": source,
            "indicator": indicator,
            "date": str(observation_date)[:10],
            "value": numeric_value,
        }

        producer.send(KAFKA_TOPIC, value=record)
        return True

    except (TypeError, ValueError):
        return False
