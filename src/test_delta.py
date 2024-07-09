from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from delta.tables import DeltaTable

# Initialize Spark session with Delta support
spark = SparkSession.builder \
    .appName("DeltaTableExample") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
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
print("Original DataFrame:")
df.show()

# Define the path for the Delta table
delta_table_path = "data/delta-table"

# Write the DataFrame to a Delta table
df.write.format("delta").mode("overwrite").save(delta_table_path)

# Create a DeltaTable object
delta_table = DeltaTable.forPath(spark, delta_table_path)

# Print the Delta table data
print("Delta Table DataFrame:")
delta_table.toDF().show()

# Perform some transformations
# Select specific columns
df_select = delta_table.toDF().select("FirstName", "Age")
print("Selected Columns:")
df_select.show()

# Filter rows based on a condition
df_filtered = delta_table.toDF().filter(delta_table.toDF().Age > 30)
print("Filtered Rows:")
df_filtered.show()

# Add a new column
df_with_new_column = delta_table.toDF().withColumn("IsAdult", col("Age") > 18)
print("DataFrame with New Column:")
df_with_new_column.show()

# Group by and aggregate
df_grouped = delta_table.toDF().groupBy("LastName").count()
print("Grouped and Aggregated DataFrame:")
df_grouped.show()

# Perform a simple transformation
df_transformed = delta_table.toDF().withColumn("AgeInFiveYears", col("Age") + 5)
print("Transformed DataFrame:")
df_transformed.show()

# Optionally, write the transformed DataFrame back to the Delta table
df_transformed.write.format("delta").mode("overwrite").save(delta_table_path)

# Stop the Spark session
spark.stop()
