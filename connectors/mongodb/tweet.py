from mongoengine import *

class Tweet(DynamicDocument):
    tweet_id = IntField(max_length=200, required=True)
    source_file = StringField(max_length=150)
