import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

try:
    spark = SparkSession.builder \
        .appName("CryptoKafkaConsumer") \
        .config("spark.master", "local[*]") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2,org.elasticsearch:elasticsearch-spark-30_2.12:8.1.2") \
        .getOrCreate()

    schema = StructType([
        StructField("id", StringType(), True),
        StructField("symbol", StringType(), True),
        StructField("name", StringType(), True),
        StructField("current_price", DoubleType(), True),
        StructField("market_cap", DoubleType(), True),
        StructField("total_volume", DoubleType(), True),
        StructField("last_updated", TimestampType(), True)
    ])

    crypto_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "course-kafka:9092") \
        .option("subscribe", "crypto_bitcoin,crypto_ethereum,crypto_solana,crypto_tether") \
        .load()

    crypto_df = crypto_df.selectExpr("CAST(value AS STRING)")

    crypto_df = crypto_df.withColumn("data", from_json(col("value"), schema)).select("data.*")

    crypto_df = crypto_df.select(
        col("id").alias("currency"),
        col("last_updated").alias("timestamp"),  # Directly use last_updated as timestamp
        col("current_price").alias("price"),
        col("total_volume").alias("volume"),
        col("market_cap")
    )

    def write_to_elasticsearch(df, epoch_id):
        try:
            df.write \
                .format("org.elasticsearch.spark.sql") \
                .option("es.nodes", "http://elasticsearch:9200") \
                .option("es.resource", "crypto_data/_doc") \
                .option("es.mapping.id", "currency") \
                .option("es.write.operation", "upsert") \
                .mode("append") \
                .save()
            logger.info(f"Batch {epoch_id} written to Elasticsearch successfully.")
        except Exception as e:
            logger.error(f"Error writing batch {epoch_id} to Elasticsearch: {e}")

    query = crypto_df.writeStream \
        .foreachBatch(write_to_elasticsearch) \
        .option("checkpointLocation", "s3a://elastic-checkpoint-2/") \
        .start()

    query.awaitTermination()

except Exception as e:
    logger.error(f"Error in streaming process: {e}")
