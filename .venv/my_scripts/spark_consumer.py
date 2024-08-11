from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType

# Define the schema for the Kafka message
schema = StructType([
    StructField("id", StringType(), True),
    StructField("last_updated", LongType(), True),
    StructField("current_price", DoubleType(), True),
    StructField("total_volume", DoubleType(), True),
    StructField("market_cap", DoubleType(), True)
])

# Initialize Spark session with Kafka package
spark = SparkSession.builder \
    .appName("CryptoStreamProcessor") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2") \
    .getOrCreate()

# Set log level to WARN to reduce verbosity
spark.sparkContext.setLogLevel("WARN")

# Read from Kafka
crypto_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "crypto_topic") \
    .load()

# Process the data
crypto_df = crypto_df.withColumn("data", from_json(col("value").cast("string"), schema)).select("data.*")

# Select relevant columns and rename them
crypto_df = crypto_df.select(
    col("id").alias("currency"),
    col("last_updated").alias("timestamp"),
    col("current_price").alias("price"),
    col("total_volume").alias("volume"),
    col("market_cap")
)

# Show the contents of crypto_df
crypto_df.show()

# Function to write data to PostgreSQL
def write_to_postgresql(df):
    df.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/postgres") \
        .option("dbtable", "public.crypto_data") \
        .option("user", "shahar") \
        .option("password", "64478868") \
        .mode("append") \
        .save()

# Write the stream to PostgreSQL
crypto_df.writeStream \
    .foreachBatch(write_to_postgresql) \
    .outputMode("append") \
    .start() \
    .awaitTermination()