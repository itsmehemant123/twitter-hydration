import json
import logging

class FileHandle:
    def __init__(self, config):
        self.destination = open(config['file']['name'], 'w')

