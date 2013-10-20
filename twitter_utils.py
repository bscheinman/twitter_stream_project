from TwitterAPI import TwitterAPI


def get_api(consumer_key, consumer_secret, access_token, access_token_secret):
    return TwitterAPI(consumer_key, consumer_secret, access_token, access_token_secret)


def get_tweets(api, hashtags=[], users=[], exclude_hashtags=[]):
    query_params = { 'language': 'en' }
    exclude_hashtags = set([tag.lower() for tag in exclude_hashtags])
    or_func = lambda x, y: x or y
    def get_hashtags(t):
        return t['entities'].get('hashtags', []) if 'entities' in t else []
    invalid_hashtag = lambda t: t in exclude_hashtags
    
    if hashtags:
        # this will search for tweets with ANY of the provided hashtags
        query_params['track'] = ','.join('%23' + tag for tag in hashtags)
    if users:
        query_params['follow'] = ','.join(users)

    stream = api.request('statuses/filter', query_params)
    for tweet in stream:
        # Twitter's api will also return tweets that were in reply to
        # or retweets of the specified users.  We only want to provide
        # tweets that were created by these users.
        if users and tweet['user']['id'] not in users:
            continue

        # Certain hashtags are associated with a ton of auto-generated
        # tweets that we're not interested in, so we'll want to exclude those
        if exclude_hashtags:
            if reduce(or_func,
                      map(lambda t: invalid_hashtag(t['text'].lower()),
                          get_hashtags(tweet)),
                      False):
                continue

        yield tweet

    # TODO: For longer running programs, we might want to provide a way
    # to close the stream when we're done, but for current purposes it
    # shouldn't be necessary.
