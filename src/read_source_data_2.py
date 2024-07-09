from pyspark.sql import SparkSession
from datetime import datetime
import json

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Elsevier") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

class ReadSourceData:
    
    def __init__(self, filepath):
        self.filepath = filepath

    def read_json_file(self):
        with open(self.filepath, "r", encoding="utf-8") as file:
            data = file.readlines()
        self.data = data

    def parse_json_objects(self):
        rows = []
        for line in self.data:
            try:
                tweet = json.loads(line.strip())
                created_at_raw = tweet['interaction']['created_at']
                created_at = datetime.strptime(created_at_raw, '%a, %d %b %Y %H:%M:%S +0000').isoformat()
                content = tweet['interaction']['content']
                rows.append([created_at, content])
            except json.JSONDecodeError:
                print(f"Error decoding JSON: {line}")
            except KeyError as e:
                print(f"Missing key {e} in JSON: {line}")
            except ValueError as e:
                print(f"Error parsing date: {created_at_raw} in JSON: {line}")
        self.rows = rows

    def write_to_delta(self, target_path):
        columns = ["Created_At", "Content"]
        df = spark.createDataFrame(self.rows, columns)
        df.write.format("delta").mode("overwrite").save(target_path)

# Initialize class
filepath = "data/dataset1.json"  
target_path = "data/tweet_content" 

read_source_data = ReadSourceData(filepath)
read_source_data.read_json_file()
Rows = read_source_data.parse_json_objects()

print(Rows)

#read_source_data.write_to_delta(target_path)

spark.stop()
