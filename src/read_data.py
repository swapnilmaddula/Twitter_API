from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Elsevier").getOrCreate()

def read_csv(file_path):
    tweet_data = spark.read.csv(file_path, header=True, inferSchema=True)
    return tweet_data

x = read_csv("data/silver/tweet_data/part-00000-5334d6c2-d037-4d69-a017-aed98fc2790e-c000.csv")

x.show()