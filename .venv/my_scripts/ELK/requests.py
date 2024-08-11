import requests
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Initialize Spark session
spark = SparkSession.builder \
    .appName("API to PostgreSQL") \
    .config("spark.jars", "path/to/postgresql-42.2.20.jar") \
    .getOrCreate()

# Fetch data from API
response = requests.get("https://api.example.com/data")
data = response.json()

# Define schema for the data
schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("email", StringType(), True)
])

# Create DataFrame from JSON data
df = spark.createDataFrame(data, schema=schema)

# Define PostgreSQL connection properties
db_url = "jdbc:postgresql://localhost:5433/cryptodb"
db_properties = {
    "user": "shahar",
    "password": "64478868",
    "driver": "org.postgresql.Driver"
}

# Write DataFrame to PostgreSQL table
df.write.jdbc(url=db_url, table="users", mode="append", properties=db_properties)

# Stop Spark session
spark.stop()