import identify_trending_topics
import read_source_data

def main(source_file_path, silver_path, gold_path):
    load_tweet_data = read_source_data.LoadTweetData(file_path_source='data/source_data/dataset1.json', folder_path_silver= 'data/silver/tweet_data' )
    load_tweet_data.incremental_load()

    file_path = "data/silver/tweet_data/*.csv"
    top5trends_instance = identify_trending_topics.Top5Trends(file_path)
    grouped_df = top5trends_instance.identify_trending_topics()
    grouped_df.show()

if __name__ == "__main__":
    main()

