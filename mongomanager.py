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
mongoSession.save("collection_name", document)
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

        # Authenticate against the database
        if "AUTHENTICATE" in config["MONGO"] and config["MONGO"]["AUTHENTICATE"]:
            self.database.authenticate(config["MONGO"]["USER"], config["MONGO"]["PASS"])

    def findOne(self, collection, query):
        """
        def MongoManager::findOne
        Finds a document in a collection
        """

        self.database[collection].find_one(query)

    def save(self, collection, document):
        """
        def MongoManager::save
        Saves a document to a collection
        """

        self.database[collection].save(document)

    def setMetadataDocument(self, document):
        """
        def MongoManager::saveMetadataDocument
        Saves a waveform metadata document
        """

        self.save("daily_streams", document)

    def getMetadataDocument(self, SDSFile):
        """
        def MongoManager::getMetadataDocument
        Returns a waveform metadata document
        """

        return self.findOne("daily_streams", {"fileId": SDSFile.filename})

mongoSession = MongoManager()
mongoSession.connect()
