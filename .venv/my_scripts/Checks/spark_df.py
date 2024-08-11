from pyspark.sql import SparkSession
from pyspark.sql.functions import col, Column

# Initialize Spark session
spark = SparkSession.builder \
    .appName("CryptoDBSession") \
    .master("local[*]") \
    .config("spark.jars", "/path/to/postgresql-42.2.20.jar") \
    .config("spark.sql.execution.pyspark.udf.faulthandler.enabled", "true") \
    .config("spark.python.worker.faulthandler.enabled", "true") \
    .config("spark.executor.memory", "2g") \
    .getOrCreate()

# PostgreSQL connection properties
url = "jdbc:postgresql://localhost:5433/cryptodb"
properties = {
    "user": "shahar",
    "password": "64478868",
    "driver": "org.postgresql.Driver"
}

# Assuming crypto_df is already defined and loaded with data
# Define crypto_df variable
crypto_df = None

# Select data according to the specified schema
crypto_df = crypto_df.select(
    col("id").alias("currency"),
    col("last_updated").alias("timestamp"),
    col("current_price").alias("price"),
    col("total_volume").alias("volume"),
    col("market_cap")
)

# Write data to PostgreSQL
crypto_df.write.jdbc(url=url, table="public.crypto_data", mode="append", properties=properties)

# Stop the Spark session
spark.stop()