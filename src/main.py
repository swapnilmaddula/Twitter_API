#import identify_trending_topics
import read_source_data

load_tweet_data = read_source_data.LoadTweetData()

load_tweet_data.read_json_file('data/source_data/dataset1.json')
load_tweet_data.parse_json_objects()
load_tweet_data.incremental_load('data/silver/tweet_data')