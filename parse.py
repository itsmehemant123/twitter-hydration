import os
import json
import logging
from connectors.mongodb.mongohandle import MongoHandle
from twarc import Twarc

logging.basicConfig(level=logging.INFO)

with open('./config/config.json') as data_file:
    config = json.load(data_file)

logging.info('Finished parsing config.')

handle = MongoHandle(config)
logging.info('Initialized the Mongo connection.')

t = Twarc(config['twitter']['consumer_key'], config['twitter']['consumer_secret'],
          config['twitter']['access_token'], config['twitter']['access_token_secret'])
logging.info('Initialized Twitter connection.')

for source_file in os.listdir('./' + config['source_folder']):
    logging.info('Preparing to hydrate: ' + source_file)
    tweet_ids = open('./' + config['source_folder'] + '/' + source_file)
    new_tweet_ids = []
    for line in tweet_ids:
        if (not handle.check(line)):
            new_tweet_ids.append(line)

    handle.write(t.hydrate(new_tweet_ids))
    tweet_ids.close()
    logging.info('Finished hydrating: ' + source_file)

logging.info('Finished hydration task.')
handle.clean()
