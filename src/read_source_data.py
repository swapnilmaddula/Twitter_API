import json
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType

class LoadTweetData:

    def __init__(self, file_path_source, folder_path_silver):
        self.new_data = []
        self.file_path_source = file_path_source 
        self.folder_path_silver = folder_path_silver 
        self.spark = SparkSession.builder.appName("Elsevier").getOrCreate()
        
    def read_json_file(self, file_path):
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
        rows = self.read_json_file(self.file_path_source)
        schema = StructType([
            StructField("created_at", StringType(), True),
            StructField("content", StringType(), True),
            StructField("tweet_id", StringType(), True)  
        ])
        
        new_data = self.spark.createDataFrame(rows, schema)

        try:
            source_data = self.spark.read.csv(path=f"{self.folder_path_silver}/*.csv", header=True, schema=schema)
            source_data.createOrReplaceTempView("source_data")
        except Exception as e:
            print(f"Error reading source data: {e}")
            source_data = self.spark.createDataFrame([], schema)
            source_data.createOrReplaceTempView("source_data")

        new_data.createOrReplaceTempView("new_data")

        # SQL query to merge new data into existing data
        merged_data = self.spark.sql("""
            SELECT new_data.created_at, new_data.content, new_data.tweet_id
            FROM new_data
            LEFT ANTI JOIN source_data
            ON new_data.tweet_id = source_data.tweet_id
            UNION
            SELECT * FROM source_data
        """)
        merged_data = merged_data.dropDuplicates()
        try:
            merged_data.write.mode("overwrite").option("quoteAll", "true").csv(path=self.folder_path_silver, header=True)
            print("Data written successfully")
        except Exception as e:
            print(f"Error writing data: {e}")