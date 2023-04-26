import tweepy
import json

import secrets

from kafka.producer import KafkaProducer

# Twitter API authentication
bearer_token = secrets.bearer_token

# Create an OAuth2 user handler using the bearer token
auth = tweepy.OAuth2BearerHandler(bearer_token)
# Create the Twitter API client
api = tweepy.API(auth)

# Kafka producer configuration
topic = "BigData"
brokers = "localhost:9092"

# Create the Kafka producer
producer = KafkaProducer(bootstrap_servers=brokers)

# Define the search query
query = ["machine learning", "data", "artificial intelligence"]



for keyword in query:
    # Get the tweets from Twitter
    tweets = api.search_tweets(q=keyword, tweet_mode = 'extended')
    # Iterate over the tweets and send them to the Kafka topic
    # Note that these tweets are not being filtered in any way so output may not be very nice!
    for tweet in tweets:
        # Get the full text of the tweet
        text = tweet.full_text

        # Convert the tweet text to a JSON string
        tweet_json = json.dumps(text)

        # Send the JSON string to the Kafka topic
        producer.send(topic, tweet_json.encode())
        producer.flush()
