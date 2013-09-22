#!/usr/bin/python

from TwitterAPI import TwitterAPI

from settings import *
from RecordStorage import MongoTweetStorage

api = TwitterAPI(CONSUMER_KEY, CONSUMER_SECRET, ACCESS_TOKEN, ACCESS_TOKEN_SECRET)
stream = api.request('statuses/sample')
tweets_stored = 0

with MongoTweetStorage(MONGO_HOST, MONGO_PORT, MONGO_DB) as mongo:
    for tweet in stream:
        if mongo.store_record(tweet):
            tweets_stored += 1
            if MAX_TWEETS is not None and tweets_stored >= MAX_TWEETS:
                break

