import json
from datetime import datetime
from kafka import KafkaProducer
from messify import messify, shuffle_buffer

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "market-data"

_buffer = []

def create_producer():
    return KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,value_serializer=lambda record: json.dumps(record).encode("utf-8"))

def publish(producer, source, indicator, observation_date, value,
            series_id=None, extra=None):
    record = {
        "source": source,
        "indicator": indicator,
        "series_id": series_id,
        "date": None if observation_date is None else str(observation_date),
        "value": None if value is None else str(value),
        "ingested_at": datetime.utcnow().isoformat(),
    }
    if extra:
        record.update(extra)
    _buffer.extend(messify(record))
    return True

def drain(producer):
    records = shuffle_buffer(list(_buffer))
    _buffer.clear()
    for record in records:
        producer.send(KAFKA_TOPIC, value=record)
    print(f"Sent {len(records)} messages to Kafka (incl. injected duplicates)")
    return len(records)
