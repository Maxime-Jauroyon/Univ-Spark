# Real-Time Sentiment Analysis of Reddit Data using Spark Streaming

## Description
This project showcases how to leverage Spark Streaming to perform real-time data processing. We utilized the Reddit API to source data and the PRAW library to access it.

The data is transmitted from a server to a client, which processes it in real-time. The primary objective of this project is to conduct sentiment analysis on the Reddit data and cluster it, followed by plotting.

To analyze sentiment, we employed the TextBlob library, and for real-time data processing, we utilized the Spark Streaming library.

Overall, this project provides a practical example of real-time data processing and sentiment analysis using Spark Streaming, PRAW, and TextBlob libraries.

## Requirements
```
pip install praw
```

```
pip install textblob
```

```
pip install pyspark
```

## Files
- `server.py` : the server who will send the Reddit data
- `process.ipynb` : the notebook who will receive the Reddit data and process it

## How to run
1. Sign in to your reddit account (If you don't have account, you need to create one) and create an app to get the credentials : https://www.reddit.com/prefs/apps

2. Modify the `server.py` file with your credentials
3. Launch the server who will send the Reddit data : `python3 server.py`
4. Open the notebook `process.ipynb` and run it

## Notes
To get the Reddit data we used the PRAW library : https://praw.readthedocs.io/en/latest/







<!-- Link where we created our account we used : https://www.reddit.com/prefs/apps 
(which currently don't work so the old link https://old.reddit.com/prefs/apps/ is what we really used)

Spark Streaming Programming Guide : https://spark.apache.org/docs/latest/streaming-programming-guide.html

How to run (local):
first launch the server who will send the Reddit data : python3 server.py localhost 8888
then you can run the notebook

How to run (docker):
first launch the server who will send the Reddit data : spark-submit server.py localhost 8888
then you can run the notebook -->