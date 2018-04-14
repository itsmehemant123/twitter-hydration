from mongoengine import *
from .tweet import Tweet

class MongoHandle:
    def __init__(self, config):
        connect(config['nosql']['db'], host=config['nosql']['host'], port=config['nosql']
                ['port'], username=config['nosql']['username'], password=config['nosql']['password'])

    def write(self, tweet):
        db_tweet = Tweet(tweet_id=tweet['id'])
        db_tweet.body = tweet
        db_tweet.save()
