from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, collect_list, concat_ws

spark = SparkSession.builder.appName("Elsevier").getOrCreate()

class Top5Trends:
    
    def __init__(self):
        self.tweets = None

    def read_csv(self, filepath):
        self.tweets = spark.read.csv(filepath, header=True, inferSchema=True)
        return self.tweets
    
    def identify_trending_topics(self):
        grouped_tweets = self.tweets.withColumn("date", to_date("created_at"))
        grouped_tweets = grouped_tweets.groupBy("date").agg(concat_ws(",", collect_list("content")).alias("concatenated_content"))
        grouped_tweets.show()
        return grouped_tweets


top5trends_instance = Top5Trends()

df = top5trends_instance.read_csv("data/tweet_data.csv")
grouped_df = top5trends_instance.identify_trending_topics()
