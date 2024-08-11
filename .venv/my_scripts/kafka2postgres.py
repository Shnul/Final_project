from pyspark.sql import SparkSession

def write_to_elasticsearch(df):
    # Your code to write to Elasticsearch
    pass

def write_to_postgresql(df):
    # Your code to write to PostgreSQL
    pass

def write_to_sinks(df, epoch_id):
    write_to_postgresql(df)
    write_to_elasticsearch(df)

spark = SparkSession.builder \
    .appName("KafkaSparkConsumer") \
    .getOrCreate()

# Read from Kafka
crypto_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "crypto_topic") \
    .load()

# Start the streaming query
query = crypto_df.writeStream \
    .foreachBatch(write_to_sinks) \
    .option("checkpointLocation", "/path/to/checkpoint/dir") \
    .start()

query.awaitTermination()