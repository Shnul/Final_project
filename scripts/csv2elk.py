from pyspark.sql import SparkSession
import logging

# Initialize logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("MinioToElasticsearch") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.2.0,org.elasticsearch:elasticsearch-spark-30_2.12:7.13.2") \
    .getOrCreate()

# Set up Minio (S3) configurations
hadoop_conf = spark._jsc.hadoopConfiguration()
hadoop_conf.set("fs.s3a.endpoint", "http://localhost:9000")
hadoop_conf.set("fs.s3a.access.key", "lfL3OAHlQCeFnqec")
hadoop_conf.set("fs.s3a.secret.key", "3Kl7FmJ40o5jG9L63cm9F9XZFOgtOMiM")
hadoop_conf.set("fs.s3a.path.style.access", "true")
hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
hadoop_conf.set("fs.s3a.connection.ssl.enabled", "false")

logger.info("Minio (S3) configurations set.")

try:
    # Read data from Minio
    df = spark.read.csv("s3a://finalproject/csvfiles/")
    logger.info("Data read from Minio successfully.")

    # Process the data (if needed)
    # For example, let's just show the data
    df.show()

    # Set up Elasticsearch configurations
   # es_write_conf = {
   #     "es.nodes": "localhost",
   #     "es.port": "9200",
   #     "es.resource": "test1/_doc",
   #     "es.input.json": "true"
   # }

    # Write data to Elasticsearch
   # df.write \
   #     .format("org.elasticsearch.spark.sql") \
   #     .options(**es_write_conf) \
   #     .mode("overwrite") \
   #     .save()
   # logger.info("Data written to Elasticsearch successfully.")

except Exception as e:
    logger.error("An error occurred: %s", e)

finally:
    # Stop the Spark session
    spark.stop()
    ##logger.info("Spark session stopped.")
