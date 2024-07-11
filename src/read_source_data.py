import json
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType


class LoadTweetData:

    def __init__(self, file_path_source, folder_path_silver):
        self.data = []
        self.new_data = []
        self = self.file_path_source
        self = self.folder_path_silver
        spark = SparkSession.builder.appName("Elsevier").getOrCreate()

    def read_json_file(self):
        with open(self.file_path_source, 'r', encoding='utf-8') as file:
            data = file.readlines()
        self.data = data
        return data

    def parse_json_objects(self):
        json_lines = self.data
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
        self.new_data = rows
        return rows

    def incremental_load(self):
        try:
            schema = StructType([
                StructField("created_at", StringType(), True),
                StructField("content", StringType(), True),
                StructField("tweet_id", StringType(), True)
            ])
            source_data = self.spark.read.csv(self.folder_path_silver, header=True, schema=schema)
        except Exception as e:
            print(f"Error reading source data: {e}")
            source_data = self.spark.createDataFrame([], schema)
        
        new_data = self.spark.createDataFrame(self.new_data, schema)
        
        if not source_data.rdd.isEmpty():
            source_data_ids = source_data.select("tweet_id").distinct()
            new_data = new_data.join(source_data_ids, on="tweet_id", how="left_anti")
        
        updated_tweet_data = source_data.union(new_data)
        try:
            updated_tweet_data.write.csv(self.folder_path_silver, header=True, mode="overwrite")
        except Exception as e:
            print(e)
        print("data written successfully")
