import requests
import json
from requests_oauthlib import OAuth1
from kafka import KafkaProducer
from time import sleep

# Twitter API credentials
consumer_key = "9umd8dC74Tmu2kZoDRwhxYz5H"
consumer_secret = "QVfavLVmlThGaUsTMO6fPx55YtRGWOrcsAd4mTbcYqUBnZcgAk"
access_token = "1649677592574803968-LEdWZVu1ZyJfVLCx62uIavRdZoLrzE"
access_token_secret = "20MWn8pj2xgpKw6gtbLQuFc0aNHpsQ4U5bEKbDmb2dpzi"

# Set up the Kafka producer
producer = KafkaProducer(bootstrap_servers='localhost:9092', value_serializer=lambda v: json.dumps(v).encode('utf-8'))

# Set up Twitter API authentication
auth = OAuth1(consumer_key, consumer_secret, access_token, access_token_secret)

# Replace the following with the keywords you want to track
keywords = ['tesla', 'bitcoin', 'spacex']
keywords_encoded = ','.join(keywords)

stream_url = f"https://api.twitter.com/2/tweets/search/stream?expansions=author_id&tweet.fields=created_at&user.fields=username&query={keywords_encoded}"

while True:
    try:
        response = requests.get(stream_url, auth=auth, stream=True)

        for line in response.iter_lines():
            if line:
                json_data = json.loads(line)
                tweet_data = json_data['data']
                user_data = json_data['includes']['users'][0]

                formatted_data = {
                    'id': tweet_data['id'],
                    'text': tweet_data['text'],
                    'created_at': tweet_data['created_at'],
                    'user_id': user_data['id'],
                    'user_name': user_data['username'],
                }

                producer.send('twitter_topic', value=formatted_data)
    except Exception as e:
        print(f"Error: {e}")
        sleep(30)  # Wait for 30 seconds before trying again
import requests
import json
from requests_oauthlib import OAuth1
from kafka import KafkaProducer
from time import sleep

# Replace the following with your own Twitter API keys and secrets
consumer_key = "your_consumer_key"
consumer_secret = "your_consumer_secret"
access_token = "your_access_token"
access_token_secret = "your_access_token_secret"

# Set up the Kafka producer
producer = KafkaProducer(bootstrap_servers='localhost:9092', value_serializer=lambda v: json.dumps(v).encode('utf-8'))

# Set up Twitter API authentication
auth = OAuth1(consumer_key, consumer_secret, access_token, access_token_secret)

# Replace the following with the keywords you want to track
keywords = ['keyword1', 'keyword2', 'keyword3']
keywords_encoded = ','.join(keywords)

stream_url = f"https://api.twitter.com/2/tweets/search/stream?expansions=author_id&tweet.fields=created_at&user.fields=username&query={keywords_encoded}"

while True:
    try:
        response = requests.get(stream_url, auth=auth, stream=True)

        for line in response.iter_lines():
            if line:
                json_data = json.loads(line)
                tweet_data = json_data['data']
                user_data = json_data['includes']['users'][0]

                formatted_data = {
                    'id': tweet_data['id'],
                    'text': tweet_data['text'],
                    'created_at': tweet_data['created_at'],
                    'user_id': user_data['id'],
                    'user_name': user_data['username'],
                }

                producer.send('twitter_topic', value=formatted_data)
    except Exception as e:
        print(f"Error: {e}")
        sleep(30)  # Wait for 30 seconds before trying again
