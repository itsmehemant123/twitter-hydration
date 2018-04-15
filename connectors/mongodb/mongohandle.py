from mongoengine import *
from .tweet import Tweet

class MongoHandle:
    def __init__(self, config):
        self.connection = connect(config['nosql']['db'], host=config['nosql']['host'], port=config['nosql']
                ['port'], username=config['nosql']['username'], password=config['nosql']['password'])

    def write(self, tweets):
        for tweet in tweets:
            db_tweet = Tweet.objects(tweet_id=tweet['id']).upsert_one(tweet_id = tweet['id'], body = tweet)
            db_tweet.save()

    def check(self, tweet_id):
        try:
            tweets = Tweet.objects(tweet_id=tweet_id)
            if (len(tweets) > 0):
                return True
        except:
            pass

        return False

    def clean(self):
        self.connection.close()
