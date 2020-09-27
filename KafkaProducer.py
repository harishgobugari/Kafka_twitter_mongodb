import tweepy
import twitterconfig
from json import dumps
from kafka import KafkaProducer, KafkaClient

# authorization tokens
consumer_key = twitterconfig.consumer_key
consumer_secret = twitterconfig.consumer_secret
access_key = twitterconfig.access_key
access_secret = twitterconfig.access_secret

# StreamListener class inherits from tweepy.StreamListener and overrides on_status/on_error methods.
class StreamListener(tweepy.StreamListener):
    
    def on_status(self, status):
		data = {'tweet' :status.text,
				'tweetedTime' :status.created_at,
				'device' :status.source,
				'userLocation' :status.user.location,
				'verifiedUser' :status.user.verified,
				'followers' :status.user.followers_count,
				'friendsCount' :status.user.friends_count,
				'publicLists' :status.user.listed_count,
				'likesCount' :status.user.favourites_count,
				'statusCount' :status.user.statuses_count,
				'accountCreatedtime' :status.user.created_at,
				'retweetCount' :status.retweet_count,
				'tweetLikecount' :status.favorite_count,
				'tweetLanguage' :status.lang,
				'mediaType' :status.entities['media'][0]['type'] if 'media' in status.entities.keys() else 'None'}
    	producer.send("topic name", value=data)
        
if __name__ == "__main__":
	producer = KafkaProducer(bootstrap_servers="localhost:9092", value_serializer=lambda x: dumps(x, default=str).encode('utf-8'))                     
	auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
	auth.set_access_token(access_key, access_secret)
	api = tweepy.API(auth)
	streamListener = StreamListener()
	stream = tweepy.Stream(auth=api.auth, listener=streamListener, tweet_mode='extended')
	tags = ["enter the tweet tag"]
	stream.filter(track=tags)





