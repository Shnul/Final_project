import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

try:
    # Initialize Spark Session with Kafka package
    spark = SparkSession.builder \
        .appName("CryptoKafkaConsumer") \
        .config("spark.master", "local[*]") \
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
        .option("kafka.bootstrap.servers", "course-kafka:9092") \
        .option("subscribe", "crypto_bitcoin") \
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

    # Function to write data to Elasticsearch
    def write_to_elasticsearch(df, epoch_id):
        try:
            df.write \
                .format("org.elasticsearch.spark.sql") \
                .option("es.nodes", "localhost") \
                .option("es.port", "9200") \
                .option("es.resource", "crypto_data/_doc") \
                .mode("append") \
                .save()
            logger.info(f"Batch {epoch_id} written to Elasticsearch successfully.")
        except Exception as e:
            logger.error(f"Error writing batch {epoch_id} to Elasticsearch: {e}")

    # Write data to Elasticsearch
    query = crypto_df.writeStream \
        .foreachBatch(write_to_elasticsearch) \
        .start()

    query.awaitTermination()

except Exception as e:
    logger.error(f"Error in streaming process: {e}")
