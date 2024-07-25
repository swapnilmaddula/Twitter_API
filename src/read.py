from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Elsevier") \
    .getOrCreate()

# Read the JSON file
json_path = "data/source_data/dataset1.json"  # Update this path as needed
df = spark.read.json(json_path)

# Filter out rows where the Twitter field is NULL or contains NULL values
df = df.where(col("twitter").isNotNull())

# Select specific fields from the nested JSON in the "twitter" column
df = df.select(
    col("twitter.created_at").alias("tweet_created_at"),
    col("twitter.text").alias("tweet_text"),
    col("twitter.id").alias("id"),
    col("twitter.user.name").alias("user_name"),
    col("twitter.user.followers_count").alias("followers_count")
)
df = df.where(col("id") != "NULL").where(col("created_at") != '')

# Show the contents of the DataFrame
df.write.mode("overwrite").csv("data/json.csv")

