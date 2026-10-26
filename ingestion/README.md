Run all this from `ingestion/` directory

## 1. Install dependencies

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```
Or if you're using conda: 
```bash
conda create -n gas_analysis
conda activate gas_analysis

```

## 2. Start Kafka and check status

```bash
docker compose up -d
docker compose ps
```

## 3. Create kafka topic (aka the log where records are stored)

The topic name is market-data.

```bash
docker exec market-data-kafka /opt/kafka/bin/kafka-topics.sh --create --if-not-exists --topic market-data --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

## 4. Fetch data and publish to Kafka

```bash
python run_all.py
```

This fetches data from Yahoo Finance, FRED, and EIA, then publishes to Kafka.

## 5. Verify Kafka messages

```bash
python verify_messages.py
```

## 6. Run Spark

Return to parent directory:

```bash
cd ..
```

Run Spark consumer:

```bash
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6 consumer.py
```
Spark 3.5.6 with Scala 2.12. Kafka connector version must match installed Spark and Scala versions.
