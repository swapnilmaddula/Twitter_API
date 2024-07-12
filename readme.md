#Twitter Trend Analysis App

This repo contains the code for an app to parse tweet date and identify 5 most common trends

The app can be executed by cloning the directory and running the command > docker-compose up --build

the dockertext file is used to install java, spark 3.5.1, and dependencies in requirements.txt

The structure of the directory is as follows:
1) "src" folder contains the main code in python and pyspark
2) the test_main.py file contains the unit test (described in the file)
3) The data folder contains three layers according to the medallion architecture: source, silver and gold

Description of the app:

1) The "read_source_data.py" imports the source data and identifies the three attributes from the api result: Tweet_ID, content and date created at. 
    The tweets with no ids are rejected
2) The parsed and cleaned data is stored in the silver layer. This intermediate layer also provides scope for future analysis requirements: for instance, demographics. 
3) The data is also loaded incrementally
4) The cleaned data is then processed by the "identify_trending_topics.py" script to identify 5 most trending topics for each day
    This is achieved by aggregating the content by the date, and then cleaning the content for characters and emojis. 
    Subsequently, the most common words are counted and appended
5) The final data is written into the gold layer. 

There is also a github actions workflow file that can be used to provision this code on any cluster, and its also possible to mount a storage and provide the paths explcitly to the main.py script. 

.
├── Dockerfile
├── data
│   ├── gold
│   ├── silver
│   └── source_data
├── directory_structure.txt
├── docker-compose.yml
├── entrypoint.sh
├── readme.md
├── requirements.txt
├── src
│   ├── identify_trending_topics.py
│   ├── main.py
│   ├── read_source_data.py
│   ├── readme.md
│   └── test_main.py
└── test_data
    ├── gold
    │   └── top5trends
    ├── silver
    │   └── spam.csv
    └── source_data

13 directories, 24 files
