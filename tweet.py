# Importing Tweepy and time
import tweepy
import time

# Credentials (INSERT YOUR KEYS AND TOKENS IN THE STRINGS BELOW)
api_key = "9umd8dC74Tmu2kZoDRwhxYz5H"
api_secret = "QVfavLVmlThGaUsTMO6fPx55YtRGWOrcsAd4mTbcYqUBnZcgAk"
bearer_token = r"AAAAAAAAAAAAAAAAAAAAAIWrmwEAAAAAkmuPzRW%2Ft6m9o%2FHtInBO8Bh51vc%3D9xfxMBROkcM8RPCVvpROu3gAR9XjOIKNxs9i0ediIx5OVWA5Ti"
access_token = "1649677592574803968-LEdWZVu1ZyJfVLCx62uIavRdZoLrzE"
access_token_secret = "20MWn8pj2xgpKw6gtbLQuFc0aNHpsQ4U5bEKbDmb2dpzi"

# Gainaing access and connecting to Twitter API using Credentials
client = tweepy.Client(bearer_token, api_key, api_secret, access_token, access_token_secret)

auth = tweepy.OAuth1UserHandler(api_key, api_secret, access_token, access_token_secret)
api = tweepy.API(auth)

search_terms = ["python", "programming", "coding"]

# Bot searches for tweets containing certain keywords
class MyStream(tweepy.StreamingClient):

    # This function gets called when the stream is working
    def on_connect(self):

        print("Connected")


    # This function gets called when a tweet passes the stream
    def on_tweet(self, tweet):

        # Displaying tweet in console
        if tweet.referenced_tweets == None:
            print(tweet.text)
            client.like(tweet.id)

            # Delay between tweets
            time.sleep(0.5)
        

# Creating Stream object
stream = MyStream(bearer_token=bearer_token)

# Adding terms to search rules
# It's important to know that these rules don't get deleted when you stop the
# program, so you'd need to use stream.get_rules() and stream.delete_rules()
# to change them, or you can use the optional parameter to stream.add_rules()
# called dry_run (set it to True, and the rules will get deleted after the bot
# stopped running).
for term in search_terms:
    stream.add_rules(tweepy.StreamRule(term))

# Starting stream
stream.filter(tweet_fields=["referenced_tweets"])