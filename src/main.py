from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("Elsevier").getOrCreate()

class Top5Trends:
    
    def __init__(self):
        pass

    def read_csv(self, filepath):
        tweets = spark.read.csv(filepath, header=True, inferSchema=True)
        self = self.tweets
        return tweets
    
    def process_tweets(self):
        
    
# Instantiate the class
top5trends_instance = Top5Trends()

# Read the CSV file
df = top5trends_instance.read_csv("data/tweet_data.csv")
