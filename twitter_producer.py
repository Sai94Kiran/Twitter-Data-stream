import tweepy
from kafka import KafkaProducer
import json

# Twitter API credentials
consumer_key = 'your-consumer-key'
consumer_secret = 'your-consumer-secret'
access_token = 'your-access-token'
access_token_secret = 'your-access-token-secret'

# Kafka configuration
KAFKA_TOPIC = 'twitter'
KAFKA_SERVER = 'localhost:9092'

# Set up Kafka producer
producer = KafkaProducer(
    bootstrap_servers=[KAFKA_SERVER],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

# Set up Tweepy stream listener
class TwitterStreamListener(tweepy.StreamListener):
    def on_data(self, data):
        tweet = json.loads(data)
        producer.send(KAFKA_TOPIC, tweet)
        print(f"Tweet sent to Kafka: {tweet['text']}")
        return True

    def on_error(self, status):
        print(f"Error: {status}")

# Set up Tweepy API authentication
auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
api = tweepy.API(auth)

# Set up stream listener
stream_listener = TwitterStreamListener()
stream = tweepy.Stream(auth=api.auth, listener=stream_listener)

# Filter tweets by keywords
stream.filter(track=['Python', 'Kafka', 'Streaming'])

# If you want to track tweets by user
# stream.filter(follow=['user_id'])
