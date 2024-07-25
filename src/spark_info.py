from pyspark.sql import SparkSession

# Create Spark session
spark = SparkSession.builder.appName("ClusterInfo").getOrCreate()

# Get Spark context
sc = spark.sparkContext

# Get executor details
executor_info = sc._jsc.sc().getExecutorMemoryStatus().items()

total_cores = sc.defaultParallelism
total_memory = sum(memory for node, (memory, _) in executor_info) / (1024 * 1024 * 1024)  # Convert bytes to GB

print(f"Total Cores: {total_cores}")
print(f"Total Memory: {total_memory:.2f} GB")

# Stop the Spark session
spark.stop()
