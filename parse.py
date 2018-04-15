import os
import json
import logging
from connectors.mongodb.mongohandle import MongoHandle
from twarc import Twarc

logging.basicConfig(level=logging.INFO)

with open('./config/config.json') as data_file:
    config = json.load(data_file)

handle = MongoHandle(config)
t = Twarc(config['twitter']['consumer_key'], config['twitter']['consumer_secret'],
          config['twitter']['access_token'], config['twitter']['access_token_secret'])

for source_file in os.listdir('./source'):
    tweet_ids = open('./source/' + source_file)
    new_tweet_ids = []
    for line in tweet_ids:
        if (not handle.check(line)):
            new_tweet_ids.append(line)

    handle.write(t.hydrate(new_tweet_ids))
    tweet_ids.close()

handle.clean()
