import os

# Set Hadoop home directory
os.environ['HADOOP_HOME'] = 'C:\\hadoop'
os.environ['PATH'] += os.pathsep + os.path.join(os.environ['HADOOP_HOME'], 'bin')

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

# Initialize Spark Session with the correct Kafka package version
spark = SparkSession.builder \
    .appName("CryptoKafkaConsumer") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2") \
    .getOrCreate()

# Define schema for JSON data
schema = StructType([
    StructField("id", StringType(), True),
    StructField("symbol", StringType(), True),
    StructField("name", StringType(), True),
    StructField("current_price", DoubleType(), True),
    StructField("market_cap", DoubleType(), True),
    StructField("total_volume", DoubleType(), True),
    StructField("last_updated", TimestampType(), True)
])

# Read data from Kafka
crypto_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "crypto_bitcoin,crypto_ethereum,crypto_solana,crypto_tether") \
    .load()

# Convert value column from binary to string
crypto_df = crypto_df.selectExpr("CAST(value AS STRING)")

# Parse JSON and extract relevant fields
crypto_df = crypto_df.withColumn("data", from_json(col("value"), schema)).select("data.*")

# Select relevant columns and rename them
crypto_df = crypto_df.select(
    col("id").alias("currency"),
    col("last_updated").alias("timestamp"),
    col("current_price").alias("price"),
    col("total_volume").alias("volume"),
    col("market_cap")
)

# Function to write data to PostgreSQL
def write_to_postgresql(df):
    df.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/cryptodb") \
        .option("dbtable", "public.cryptodb") \
        .option("user", "shahar") \
        .option("password", "64478868") \
        .mode("append") \
        .save()

# Function to write data to Elasticsearch
def write_to_elasticsearch(df):
    df.write \
        .format("org.elasticsearch.spark.sql") \
        .option("es.nodes", "localhost") \
        .option("es.port", "9200") \
        .option("es.resource", "crypto_data/_doc") \
        .mode("append") \
        .save()

# Write data to both PostgreSQL and Elasticsearch
def write_to_sinks(df, epoch_id):
    write_to_postgresql(df)
    write_to_elasticsearch(df)

# Start the streaming query
query = crypto_df.writeStream \
    .foreachBatch(write_to_sinks) \
    .option("checkpointLocation", "/path/to/checkpoint/dir") \
    .start()

query.awaitTermination()
