from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

spark = SparkSession.builder \
    .appName("CryptoKafkaConsumer") \
    .config("spark.master", "local[*]") \
    .config("spark.jars", "/opt/driver/postgresql-42.5.6.jar") \
    .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2') \
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
    .option("subscribe", "crypto_bitcoin") \
    .load()

crypto_df = crypto_df.selectExpr("CAST(value AS STRING)")

crypto_df = crypto_df.withColumn("data", from_json(col("value"), schema)).select("data.*")

crypto_df = crypto_df.select(
    col("id").alias("currency"),
    col("last_updated").alias("dates"),
    col("current_price").alias("price"),
    col("total_volume").alias("volume"),
    col("market_cap")
)

def write_to_postgresql(df, epoch_id):
    if df.count() == 0:
        print(f"Batch {epoch_id} is empty, nothing to write.")
    else:
        print(f"Batch {epoch_id} has {df.count()} records, writing to PostgreSQL.")
        df.show()  # Display the data in the current batch
        df.write \
            .format("jdbc") \
            .option("url", "jdbc:postgresql://postgres:5432/postgres") \
            .option("driver", "org.postgresql.Driver") \
            .option("dbtable", "crypto_data") \
            .option("user", "postgres") \
            .option("password", "postgres") \
            .mode("append") \
            .save()

# Write data to PostgreSQL
crypto_df.writeStream \
    .foreachBatch(write_to_postgresql) \
    .start() \
    .awaitTermination()
