import pytest
import identify_trending_topics
from pyspark.sql import SparkSession
# 1. test for spam data - Validation
    # test def: silver table defined only with spam content
    # expectation: no trending topics should be written to the gold layer

spark = SparkSession.builder.appName("Elsevier").getOrCreate()

def test_main():

    silver_path = "test_data/silver"
    gold_path = "test_data/gold/top5trends"

    top5trends = identify_trending_topics.Top5Trends(filepath_silver=f"{silver_path}/*.csv", folderpath_gold=gold_path)
    top5trends.identify_trending_topics()

    gold_table = spark.read.csv(path = gold_path+ "/*csv", header = True)

    gold_table.show

    gold_table.createOrReplaceTempView('data')

    row_count = spark.sql('SELECT COUNT(*) FROM data WHERE LENGTH(TRENDING_TOPICS)> 0').collect()[0][0]

    print(row_count)
    
    assert(row_count) == 0


if __name__ == "__main__":
    pytest.main([__file__])
    








    



    


    
