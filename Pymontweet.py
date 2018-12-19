from __future__ import print_function
import tweepy
import json
from pymongo import MongoClient
 
MONGO_HOST= 'mongodb://localhost/twitterdb'  
 
WORDS = ['#bigdata', '#hadoop', '#AI', '#datascience', '#machinelearning', '#iot']
 
CONSUMER_KEY = "KEY"
CONSUMER_SECRET = "SECRET"
ACCESS_TOKEN = "TOKEN"
ACCESS_TOKEN_SECRET = "TOKEN_SECRET"
 
 
class StreamListener(tweepy.StreamListener):    
 
    def on_connect(self):
        print("You are now connected to the streaming API.")
    def on_error(self, status_code):
        print('An Error has occured: ' + repr(status_code))
        return False
    def on_data(self, data):
        try:
            client = MongoClient(MONGO_HOST)
            db = client.twitterdb
            datajson = json.loads(data)
            
            created_at = datajson['created_at']
 
            print("Tweet collected at " + str(created_at))
            db.twitter_search.insert(datajson)
        except Exception as e:
           print(e)
 
auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)
#Set up the listener. The 'wait_on_rate_limit=True' is needed to help with Twitter API rate limiting.
listener = StreamListener(api=tweepy.API(wait_on_rate_limit=True))
streamer = tweepy.Stream(auth=auth, listener=listener)
print("Tracking: " + str(WORDS))
streamer.filter(track=WORDS)
