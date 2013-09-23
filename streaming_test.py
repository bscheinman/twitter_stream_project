#!/usr/bin/python

from RecordStorage import MongoTweetStorage
import twitter_utils
from settings import *

hashtags = ['android', 'iphone']
exclude = set(['gameinsight'])

api = twitter_utils.get_api(CONSUMER_KEY, CONSUMER_SECRET, ACCESS_TOKEN, ACCESS_TOKEN_SECRET)

tweets_stored = 0
with MongoTweetStorage(MONGO_HOST, MONGO_PORT, MONGO_DB) as mongo:
    for tweet in twitter_utils.get_tweets(api, hashtags=hashtags, exclude_hashtags=exclude):
        if mongo.store_record(tweet):
            tweets_stored += 1
            if MAX_TWEETS is not None and tweets_stored >= MAX_TWEETS:
                break

