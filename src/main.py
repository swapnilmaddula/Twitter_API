from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import to_date, collect_list, concat_ws, udf, StringType
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
        grouped_tweets = self.tweets.withColumn("date", F.to_date("created_at"))
        grouped_tweets = grouped_tweets.groupBy("date").agg(F.concat_ws(",", F.collect_list("content")).alias("concatenated_content"))
        
        stop_words = self.obtain_list_of_ignore_words("dutch")
        
        def extract_trending_topics(content):
            tokens = nltk.word_tokenize(content)
            filtered_tokens = [token for token in tokens if token.lower() not in stop_words and token.isalpha()]
            word_counter = Counter(filtered_tokens)
            most_common_words = word_counter.most_common(5)
            most_common_words_string = ','.join([word for word, count in most_common_words])
            return most_common_words_string
        
        extract_trending_topics_udf = F.udf(extract_trending_topics, StringType())
        
        grouped_tweets = grouped_tweets.withColumn('trending_topics', extract_trending_topics_udf(F.col('concatenated_content')))
    
        return grouped_tweets


top5trends_instance = Top5Trends()
df = top5trends_instance.read_csv("data/tweet_data.csv")
grouped_df = top5trends_instance.identify_trending_topics()
print(grouped_df)