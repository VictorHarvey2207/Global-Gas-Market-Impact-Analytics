from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, from_json, to_date
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
)

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "market-data"

OUTPUT_PATH = "output/market_data"

SCHEMA = StructType([
    StructField("source", StringType(), True),
    StructField("indicator", StringType(), True),
    StructField("date", StringType(), True),
    StructField("value", DoubleType(), True),
])

def read_market_data(spark: SparkSession) -> DataFrame:

    kafka_df = (
        spark.read
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")
        .option("endingOffsets", "latest")
        .load()
    )

    json_df = kafka_df.select(
        col("value").cast("string").alias("json")
    )

    market_df = (
        json_df
        .select(from_json(col("json"), SCHEMA).alias("data"))
        .select("data.*")
        .withColumn("date", to_date(col("date"), "yyyy-MM-dd"))
        .filter(
            col("source").isNotNull()
            & col("indicator").isNotNull()
            & col("date").isNotNull()
            & col("value").isNotNull()
        )
        .dropDuplicates(["source", "indicator", "date"])
    )

    return market_df


def main():
    spark = (
        SparkSession.builder
        .appName("MarketDataConsumer")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    try:
        market_df = read_market_data(spark)

        print("Market data schema:")
        market_df.printSchema()

        print("Sample observations:")
        market_df.orderBy("date", "indicator").show(30, truncate=False)

        (
            market_df.write
            .mode("overwrite")
            .parquet(OUTPUT_PATH)
        )

        print(f"Processed snapshot saved to {OUTPUT_PATH}")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
