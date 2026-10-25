"""Spark lifecycle belongs to entry points, never to transformation functions."""

from pyspark.sql import SparkSession

from .config import Config


def create_spark(config: Config) -> SparkSession:
    builder = (SparkSession.builder.appName("GlobalGasMarket-OilForecast")
               .config("spark.sql.session.timeZone", "UTC")
               .config("spark.sql.shuffle.partitions", str(config.shuffle_partitions)))
    if config.spark_master:
        builder = builder.master(config.spark_master)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark
