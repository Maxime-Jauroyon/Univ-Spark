# Univ-Spark

Downloading the Reddit API : pip install praw

Downloading the NLP API : pip install textblob

Downloading pyspark : pip install pyspark

Link where we created our account we used : https://www.reddit.com/prefs/apps 
(which currently don't work so the old link https://old.reddit.com/prefs/apps/ is what we really used)

Spark Streaming Programming Guide : https://spark.apache.org/docs/latest/streaming-programming-guide.html

How to run (local):
first launch the server who will send the Reddit data : python3 server.py localhost 8888
then you can run the notebook

How to run (docker):
first launch the server who will send the Reddit data : spark-submit server.py localhost 8888
then you can run the notebook