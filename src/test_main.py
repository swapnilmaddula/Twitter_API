from main import main
import pytest
import read_source_data
import identify_trending_topics
import json
import datetime
from pyspark import SparkSession
# 1. test for spam data - Validation
    # test def: silver table defined only with spam content
    # expectation: no rows should be written to the gold layer

spark = SparkSession.builder.appName("Elsevier").getOrCreate()

def Test_1_main():

    silver_path = "test_data/silver"
    gold_path = "test_data/gold/top5trends"

    top5trends = identify_trending_topics.Top5Trends(filepath_silver=f"{silver_path}/*.csv", folderpath_gold=gold_path)
    top5trends.identify_trending_topics()

    gold_table = spark.read.csv(path = gold_path+ "/*csv")

    length_of_table = gold_table.count()

    assert(length_of_table) == 0

if __name__ == "__main__":
    Test_1_main()
    print("Test passed!")
    
    






    



    


    
