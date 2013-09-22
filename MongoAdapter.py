from dateutil import parser as date_parser
import pymongo

TWITTER_TIME_FORMAT = '%a %b %d %H:%M:%S %z %Y'

def parse_twitter_time(twitter_time_str):
    return date_parser.parse(twitter_time_str)


class MongoAdapter:
    '''
        It is recommended to use this class using python's 'with' syntax.
        Otherwise, be sure to either provide None for the constructor's
        buffer_size argument (which will disable buffering altogether),
        or, preferably, call commit_tweets() when you are done using
        the adapter.
    '''

    # set buffer_size to None to immediately commit all tweets
    def __init__(self, host, port, buffer_size=1000):
        # TODO: deal with errors or bad input here
        self._connection = pymongo.MongoClient(host, port)
        db = self._connection.twitter_stream
        self._tweets_table = db.tweets
        self._buffered_tweets = []
        self._buffer_size = buffer_size


    def __enter__(self):
        return self


    def __exit__(self, typ, value, traceback):
        self.commit_tweets()


    def include_tweet(self, raw_tweet):
        '''
            only include tweets that are relevant to our use case
        '''

        # only consider tweets in english
        entities = raw_tweet.get('entities', None)
        if not entities or not entities.get('hashtags', None):
            return False

        # only consider tweets with at least one hashtag
        if raw_tweet.get('lang', None) != 'en':
            return False

        return True


    def extract_tweet(self, raw_tweet):
        '''
            the raw json that twitter returns has a lot of extraneous data,
            so we should convert it to a format containing only what we need
        '''
        tweet = {}
        tweet['twitter_id'] = raw_tweet['id']
        tweet['time'] = parse_twitter_time(raw_tweet['created_at'])
        tweet['user_id'] = raw_tweet['user']['id']
        tweet['text'] = raw_tweet['text']
    
        # don't care about indices of hashtags or urls
        tweet['hashtags'] = [tag['text'] for tag in raw_tweet['entities']['hashtags']]
        tweet['urls'] = [url['expanded_url'] for url in raw_tweet['entities']['urls']]

        # keep all location information to deal with later
        tweet['location'] = raw_tweet['place']

        return tweet


    def store_tweet(self, raw_tweet):
        if not self.include_tweet(raw_tweet):
            return False

        try:
            tweet = self.extract_tweet(raw_tweet)
        except Exception as e:
            print 'error processing tweet: %s' % str(e)
            return False
        else:
            self._buffered_tweets.append(tweet)
            if self._buffer_size is None or len(self._buffered_tweets) >= self._buffer_size:
                self.commit_tweets()
            return True


    def commit_tweets(self):
        if not self._buffered_tweets:
            return
        try:
            self._tweets_table.insert(self._buffered_tweets)
        except Exception as e:
            print 'error writing %i tweets to database' % len(self._buffered_tweets)
        else:
            print 'stored %i tweets' % len(self._buffered_tweets)
        finally:
            # TODO: we probably don't want to ditch all buffered tweets
            # just because the db commit failed, but for now this is fine
            self._buffered_tweets = []

