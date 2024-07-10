import identify_trending_topics
import read_source_data

def main(source_file_path, silver_path, gold_path):
    load_tweet_data = read_source_data.LoadTweetData()

    load_tweet_data.read_json_file('data/source_data/dataset1.json')
    load_tweet_data.parse_json_objects()
    load_tweet_data.incremental_load('data/silver/tweet_data')

    file_path = "data/silver/tweet_data/*.csv"
    top5trends_instance = identify_trending_topics.Top5Trends(file_path)
    grouped_df = top5trends_instance.identify_trending_topics()
    grouped_df.show()

if __name__ == "__main__":
    main()

