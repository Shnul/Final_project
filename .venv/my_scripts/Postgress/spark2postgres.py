from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType
import os

# Set environment variables for PySpark
os.environ['PYSPARK_DRIVER_PYTHON'] = 'python'
os.environ['PYSPARK_PYTHON'] = 'python'

# Print environment variables for debugging
print(f"JAVA_HOME: {os.environ.get('JAVA_HOME')}")
print(f"PATH: {os.environ.get('PATH')}")

# Initialize Spark session
spark = SparkSession.builder \
    .appName("CryptoDataToPostgres") \
    .config("spark.jars", "file:///C:/path/to/postgresql-42.2.20.jar") \
    .getOrCreate()

# Sample data
crypto_data = [
    {"id": "bitcoin", "symbol": "btc", "name": "Bitcoin", "current_price": 60978, "market_cap": 1206769468840},
    {"id": "ethereum", "symbol": "eth", "name": "Ethereum", "current_price": 2611.41, "market_cap": 314265507159},
    # Add more data as needed
]

# Define schema
schema = StructType([
    StructField("id", StringType(), True),
    StructField("symbol", StringType(), True),
    StructField("name", StringType(), True),
    StructField("current_price", DoubleType(), True),
    StructField("market_cap", LongType(), True)
])

# Preprocess data to ensure correct types
for record in crypto_data:
    record["current_price"] = float(record["current_price"])

# Create DataFrame
crypto_df = spark.createDataFrame(crypto_data, schema)

# Function to write DataFrame to PostgreSQL
def write_to_postgresql(df):
    try:
        df.write \
            .format("jdbc") \
            .option("url", "jdbc:postgresql://localhost:5432/cryptodb") \
            .option("dbtable", "crypto_data") \
            .option("user", "shahar") \
            .option("password", "64478868") \
            .option("driver", "org.postgresql.Driver") \
            .save()
        print("Data written to PostgreSQL successfully.")
    except Exception as e:
        print(f"Error writing to PostgreSQL: {e}")

# Write DataFrame to PostgreSQL
write_to_postgresql(crypto_df)

# Stop Spark session
spark.stop()