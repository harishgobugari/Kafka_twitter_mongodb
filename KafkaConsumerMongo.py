import re
from kafka import KafkaConsumer
from pymongo import MongoClient
from textblob import TextBlob
from json import loads

consumer = KafkaConsumer(
    'topic name',
     bootstrap_servers=['localhost:9092'],
     auto_offset_reset='earliest',
     enable_auto_commit=True,
     value_deserializer=lambda x: loads(x.decode('utf-8')))


#connecting to mongodb
myclient = MongoClient("mongodb://localhost:27017/")
mydb = myclient["database name"]
mycol = mydb["collection name"]

#getting sentiment score
def get_tweet_sentiment(tweet): 

        tweet = ' '.join(re.sub("(@[A-Za-z0-9]+)|([^0-9A-Za-z \t])|(\w+:\/\/\S+)", " ", tweet).split())
        analysis = TextBlob(tweet)
        # set sentiment 
        if analysis.sentiment.polarity > 0: 
            return 'positive'
        elif analysis.sentiment.polarity == 0: 
            return 'neutral'
        else: 
            return 'negative'

# Send data to MongoDB
for message in consumer:
        message = message.value
        message['tweet'] = get_tweet_sentiment(message['tweet'])
        mycol.insert(message)
        


