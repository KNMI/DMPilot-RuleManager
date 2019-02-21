"""
This module implements a MongoDB session manager as a _fake singleton_.

Instead of calling the MongoManager() constructor, use the
mongoSession variable, that is already created and connected to Mongo
when the module is loaded.

Example
-------

```
from mongomanager import mongoSession
...
mongoSession.save('collection_name', document)
```
"""

from configuration import config

# MongoDB driver for Python
from pymongo import MongoClient


class MongoManager():
    """
    Class MongoManager
    Container for MongoDB connection
    """

    def __init__(self):
        """
        def MongoManager.__init__
        Initializes the class
        """

        self.client = None
        self.database = None

    def connect(self):
        """
        def MongoManager.connect
        Opens connection to the database
        """

        if self.client is not None:
            return

        self.client = MongoClient(
            config["MONGO"]["HOST"],
            config["MONGO"]["PORT"]
        )

        # Use the wfrepo database
        self.database = self.client.wfrepo

        if 'AUTHENTICATE' in config['MONGO'] and config['MONGO']['AUTHENTICATE']:
            self.database.authenticate(config['MONGO']['USER'], config['MONGO']['PASS'])

    def save(self, collection, document):
        """Save a document in a collection."""
        self.database[collection].save(document)


mongoSession = MongoManager()
mongoSession.connect()
