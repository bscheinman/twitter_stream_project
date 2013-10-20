from dateutil import parser as date_parser
import pymongo


def parse_twitter_time(twitter_time_str):
    return date_parser.parse(twitter_time_str)


class RecordStorage(object):

    def include_record(self, raw_record):
        '''
            Returns True if this record should be stored, and False otherwise
        '''
        return True


    def extract_record(self, raw_record):
        '''
            Transform the raw record into a form appropriate for this storage mechanism
        '''
        return raw_record


    def store_record(self, raw_record):
        if not self.include_record(raw_record):
            return False

        try:
            record = self.extract_record(raw_record)
        except Exception as e:
            print 'error processing record: %s' % str(e)
            return False
        else:
            return record is not None and self._store_record_impl(record)


    def _store_record_impl(self, record):
        '''
            This method must be overridden to perform the actual storage logic
            appropriate for the derived type.  It should return True if the
            record was successfully stored and False otherwise.
        '''
        raise NotImplementedError()


class MongoStorage(RecordStorage):
    '''
        It is recommended to use this class using python's 'with' syntax.
        Otherwise, be sure to either provide None for the constructor's
        buffer_size argument (which will disable buffering altogether),
        or, preferably, call commit_records() when you are done using
        the adapter.
    '''

    # set buffer_size to None to immediately commit all tweets
    def __init__(self, host, port, db_name, table_name, buffer_size=1000):
        # TODO: deal with errors or bad input here
        self._connection = pymongo.MongoClient(host, port)
        db = self._connection[db_name]
        self._table = db[table_name]
        self._buffered_records = []
        self._buffer_size = buffer_size


    def __enter__(self):
        return self


    def __exit__(self, typ, value, traceback):
        self.commit_records()


    def _store_record_impl(self, record):
        self._buffered_records.append(record)
        if self._buffer_size is None or len(self._buffered_records) >= self._buffer_size:
            self.commit_records()
        return True


    def commit_records(self):
        if not self._buffered_records:
            return
        try:
            self._table.insert(self._buffered_records)
        except Exception as e:
            print 'error writing %i records to database' % len(self._buffered_records)
        else:
            print 'stored %i records' % len(self._buffered_records)
        finally:
            # TODO: we probably don't want to ditch all buffered tweets
            # when the db commit fails, but for now this is fine
            self._buffered_records = []


class MongoTweetStorage(MongoStorage):

    def __init__(self, host, port, db_name, **kwargs):
        super(MongoTweetStorage, self).__init__(host, port, db_name, 'tweets', **kwargs)

    
    def include_record(self, raw_tweet):

        # only consider tweets with at least one hashtag
        entities = raw_tweet.get('entities', None)
        if not entities or not entities.get('hashtags', None):
            return False

        # only consider tweets in english
        if raw_tweet.get('lang', None) != 'en':
            return False

        # only consider original tweets
        if raw_tweet.get('retweeted_status', None):
            return False

        return True


    def extract_record(self, raw_tweet):
        '''
            The raw json that Twitter returns has a lot of extraneous data,
            so we should convert it to a format containing only what we need.
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


