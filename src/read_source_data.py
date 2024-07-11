import json
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType


class LoadTweetData:

    def __init__(self, file_path_source, folder_path_silver):
        self.data = []
        self.new_data = []
        self.file_path_source = file_path_source 
        self.folder_path_silver = folder_path_silver 
        print(self.file_path_source)
        self.spark = SparkSession.builder.appName("Elsevier").getOrCreate()
        
    def read_json_file_as_dataframe(self, file_path):
        with open(file_path, 'r', encoding='utf-8') as file:
            data = file.readlines()
        json_lines = data
        rows = []
        for line in json_lines:
            try:
                tweet = json.loads(line.strip())
                created_at_raw = tweet['interaction']['created_at']
                created_at = datetime.strptime(created_at_raw, '%a, %d %b %Y %H:%M:%S +0000').isoformat()
                content = tweet['interaction']['content']
                tweet_id = tweet['twitter']['id']
                rows.append([created_at, content, tweet_id])
            except json.JSONDecodeError:
                print(f"Error decoding JSON: {line}")
            except KeyError as e:
                print(f"Missing key {e} in JSON: {line}")
            except ValueError as e:
                print(f"Error parsing date: {created_at_raw} in JSON: {line}")
        return rows

    def incremental_load(self):
        rows = self.read_json_file_as_dataframe(self.file_path_source)
        schema = StructType([
            StructField("created_at", StringType(), True),
            StructField("content", StringType(), True),
            StructField("tweet_id", StringType(), True)  # Use StringType instead of int()
        ])
        
        try:
            # Read all CSV files in the folder_path_silver directory
            source_data = self.spark.read.csv(path=f"{self.folder_path_silver}/*.csv", header=True, schema=schema)
        except Exception as e:
            print(f"Error reading source data: {e}")
            source_data = self.spark.createDataFrame([], schema)
        
        if rows:
            new_data = self.spark.createDataFrame(rows, schema)
            
            if source_data.count() > 0:
                source_data_ids = source_data.select("tweet_id")
                print(source_data)
                new_data_updated = new_data.join(source_data_ids, on="tweet_id", how="left_anti")
            
            if new_data.count() > 0:
                updated_tweet_data = source_data.union(new_data)
            else:
                updated_tweet_data = source_data
            
            try:
                updated_tweet_data.write.csv(path=self.folder_path_silver, header=True, mode="overwrite")
                print("Data written successfully")
            except Exception as e:
                print(f"Error writing data: {e}")
        else:
            print("No new data to load")