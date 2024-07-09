from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, collect_list, concat_ws
from stop_words import get_stop_words
import nltk
from collections import Counter

spark = SparkSession.builder.appName("Elsevier").getOrCreate()

nltk.download('punkt')
nltk.download('averaged_perceptron_tagger')

class Top5Trends:
    
    def __init__(self):
        self.tweets = None

    def read_csv(self, filepath):
        self.tweets = spark.read.csv(filepath, header=True, inferSchema=True)
        return self.tweets
    
    def obtain_list_of_ignore_words(self, language):
        stop_words = get_stop_words(language)
        return stop_words
    
    def identify_trending_topics(self):
        grouped_tweets = self.tweets.withColumn("date", to_date("created_at"))
        grouped_tweets = grouped_tweets.groupBy("date").agg(concat_ws(",", collect_list("content")).alias("concatenated_content"))

        grouped_data = grouped_tweets.collect()

        stop_words = self.obtain_list_of_ignore_words("dutch")
        word_counter = Counter()
        for row in grouped_data:
            date = row['date']
            concatenated_content = row['concatenated_content']
            tokens = nltk.word_tokenize(concatenated_content)
            filtered_tokens = [i for i in tokens if i not in stop_words]
            word_counter.update(filtered_tokens)
            print(filtered_tokens)

        most_common_words = word_counter.most_common(5)
        print("Top 5 trending words:")
        for word, count in most_common_words:
            print(f"{word}: {count}")
        
        return grouped_tweets

# Usage
top5trends_instance = Top5Trends()
df = top5trends_instance.read_csv("data/tweet_data.csv")
grouped_df = top5trends_instance.identify_trending_topics()
