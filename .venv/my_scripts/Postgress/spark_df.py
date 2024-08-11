from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder \
    .appName("CryptoDBSession") \
    .master("local[*]") \
    .getOrCreate()

# Example DataFrame
data = [("Alice", 1), ("Bob", 2)]
columns = ["name", "id"]
df = spark.createDataFrame(data, columns)

# Register DataFrame as a temporary view
df.createOrReplaceTempView("users")

# Execute SQL query
result_df = spark.sql("SELECT * FROM users")
result_df.show()

# Show tables
spark.sql("SHOW TABLES").show()

# Stop the Spark session
spark.stop()