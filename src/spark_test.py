from pyspark.sql import SparkSession

# Initialize a Spark session
spark = SparkSession.builder \
    .appName("DataTransformationExample") \
    .getOrCreate()

# Sample data
data = [
    ("John", "Doe", 30),
    ("Jane", "Smith", 25),
    ("Sam", "Brown", 35)
]

# Define the schema
columns = ["FirstName", "LastName", "Age"]

# Create a DataFrame
df = spark.createDataFrame(data, columns)

# Show the DataFrame
df.show()

spark.stop()