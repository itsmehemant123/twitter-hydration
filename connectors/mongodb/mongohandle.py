import time
import logging
from mongoengine import *
from .tweet import Tweet

class MongoHandle:
    def __init__(self, config):
        start1 = time.time()
        self.connection = connect(config['nosql']['db'], host=config['nosql']['host'], port=config['nosql']
                ['port'], username=config['nosql']['username'], password=config['nosql']['password'])
        end1 = time.time()
        logging.info('Connected to Mongo in: %.2f seconds.' % (end1 - start1))
        self.written_tweets = [tweet['tweet_id'] for tweet in Tweet.objects]
        end2 = time.time()
        logging.info('Fetched existing tweets from database in: %.2f seconds.' % (end2 - end1))

    def write(self, tweets, source_file):
        for tweet in tweets:
            db_tweet = Tweet.objects(tweet_id=tweet['id']).upsert_one(tweet_id = tweet['id'], body = tweet, source_file = source_file)
            db_tweet.save()

    def is_written(self, tweet_id):
        if (tweet_id in self.written_tweets):
            return True

        return False

    def clean(self):
        self.connection.close()
