from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import to_date, collect_list, concat_ws, udf, StringType
import nltk
from collections import Counter

spark = SparkSession.builder.appName("Elsevier").getOrCreate()

nltk.download('punkt')
nltk.download('averaged_perceptron_tagger')
nltk.download('stopwords')

class Top5Trends:
    
    def __init__(self, filepath):
        self.filepath = filepath
        self.tweets = None

    def read_csv(self):
        tweet_data = spark.read.csv(self.filepath, header=True, inferSchema=True)
        return tweet_data
    
    def obtain_list_of_ignore_words(self, language):
        stop_words = nltk.corpus.stopwords.words(language)
        return stop_words
    
    def identify_trending_topics(self):
        tweet_data = self.read_csv()
        grouped_tweets = tweet_data.withColumn("date", F.to_date("created_at"))
        grouped_tweets = grouped_tweets.groupBy("date").agg(F.concat_ws(",", F.collect_list("content")).alias("concatenated_content"))
        stop_words = self.obtain_list_of_ignore_words("dutch")
        
        def extract_trending_topics(content):
            tokens = nltk.word_tokenize(content)
            filtered_tokens = [w for w in tokens if not w.lower() in stop_words]
            word_counter = Counter(filtered_tokens)
            most_common_words = word_counter.most_common(5)
            most_common_words_string = ','.join([word for word, count in most_common_words])
            return most_common_words_string
        
        extract_trending_topics_udf = F.udf(extract_trending_topics, StringType())
        
        grouped_tweets = grouped_tweets.withColumn('trending_topics', extract_trending_topics_udf(F.col('concatenated_content')))
        return grouped_tweets


top5trends_instance = Top5Trends("data/silver/tweet_data/part-00000-5334d6c2-d037-4d69-a017-aed98fc2790e-c000.csv")
grouped_df = top5trends_instance.identify_trending_topics()
