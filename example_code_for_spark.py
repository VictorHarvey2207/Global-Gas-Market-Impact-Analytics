from pyspark.sql import SparkSession
from consumer import read_market_data

spark = SparkSession.builder.appName("MarketDataProcessing").getOrCreate()

df = read_market_data(spark)

# processing logic starts here.
df.groupBy("indicator").count().show()

spark.stop()
