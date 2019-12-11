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

import logging
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

        # Initialize logger
        self.logger = logging.getLogger('RuleManager')
        self.logger.debug("Initializing a new Mongo Session.")

        self.client = None
        self.database = None

    def connect(self):
        """
        def MongoManager.connect
        Creates a connection to the database
        """

        if self.client is not None:
            return

        self.client = MongoClient(
            config["MONGO"]["HOST"],
            config["MONGO"]["PORT"]
        )

        # Use the wfrepo database
        self.database = self.client[config["MONGO"]["DATABASE"]]

        # Authenticate against the database
        if "AUTHENTICATE" in config["MONGO"] and config["MONGO"]["AUTHENTICATE"]:
            self.database.authenticate(config["MONGO"]["USER"], config["MONGO"]["PASS"])

    def findOne(self, collection, query):
        """
        def MongoManager::findOne
        Finds a document in a collection
        """

        return self.database[collection].find_one(query)

    def findMany(self, collection, query):
        """
        def MongoManager::findMany
        Finds a document in a collection
        """

        return self.database[collection].find(query)

    def deleteOne(self, collection, query):
        """
        def MongoManager::deleteOne
        Delete a document in a collection
        """

        return self.database[collection].delete_one(query)

    def deleteMany(self, collection, query):
        """
        def MongoManager::deleteMany
        Delete many documents in a collection
        """

        return self.database[collection].delete_many(query)

    def save(self, collection, document, overwrite=True):
        """
        def MongoManager::save
        Saves a document to a collection
        """

        # First, delete all documents related to this file
        if overwrite:
            res = self.database[collection].delete_many({"fileId": document["fileId"]})
            if res.acknowledged and res.deleted_count > 0:
                self.logger.debug("Deleted %d documents from '%s' collection with fileId = %s" % (
                                    res.deleted_count, collection, document["fileId"]))

        # Second, insert new document
        res = self.database[collection].insert_one(document)
        if res.acknowledged:
            self.logger.debug("Inserted document into '%s' collection with fileId = %s" % (
                                collection, document["fileId"]))

    def saveDCDocument(self, document):
        """
        def MongoManager::saveDCDocument
        Saves a Dublin Core metadata document
        """

        self.save(config["MONGO"]["DC_METADATA_COLLECTION"], document)

    def getDCDocument(self, SDSFile):
        """
        def MongoManager::getDCDocument
        Returns a Dublin Core metadata document corresponding to a file
        """

        return self.findOne(config["MONGO"]["DC_METADATA_COLLECTION"],
                            {"fileId": SDSFile.filename})

    def deleteDCDocument(self, SDSFile):
        """Updates the Dublin Core metadata document corresponding to a file
        marking it as deleted."""

        return (self.database[config["MONGO"]["DC_METADATA_COLLECTION"]]
                .update_many({"fileId": SDSFile.filename},
                             {'$set': {
                                 'irods_path': 'DELETED',
                                 'checksum': 'DELETED'
                             }}))

    def setMetadataDocument(self, document):
        """
        def MongoManager::saveMetadataDocument
        Saves a waveform metadata document
        """

        self.save(config["MONGO"]["WF_METADATA_COLLECTION"], document)

    def getMetadataDocument(self, SDSFile):
        """
        def MongoManager::getMetadataDocument
        Returns a waveform metadata document corresponding to a file
        """

        return self.findOne(config["MONGO"]["WF_METADATA_COLLECTION"],
                            {"fileId": SDSFile.filename})

    def deleteMetadataDocument(self, SDSFile):
        """
        def MongoManager::deleteMetadataDocument
        Delete one waveform metadata document corresponding to a file
        """

        return self.deleteOne(config["MONGO"]["WF_METADATA_COLLECTION"],
                              {"fileId": SDSFile.filename})

    def savePPSDDocument(self, document):
        """Saves a PPSD metadata document."""

        self.save(config["MONGO"]["PPSD_METADATA_COLLECTION"], document, overwrite=False)

    def getPPSDDocuments(self, SDSFile):
        """Returns a PPSD document corresponding to a file."""

        return self.findMany(config["MONGO"]["PPSD_METADATA_COLLECTION"],
                            {"fileId": SDSFile.filename})

    def deletePPSDDocuments(self, SDSFile):
        """Deletes the PPSD documents corresponding to a file marking it as
        deleted."""

        res = self.deleteMany(config["MONGO"]["PPSD_METADATA_COLLECTION"],
                              {"fileId": SDSFile.filename})
        self.logger.debug("Deleted %d documents from '%s' collection with fileId = %s" % (
                            res.deleted_count, config["MONGO"]["PPSD_METADATA_COLLECTION"],
                            SDSFile.filename))
        return res


mongoSession = MongoManager()
mongoSession.connect()
