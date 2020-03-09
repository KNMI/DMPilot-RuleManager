"""
This module implements a MongoDB session manager as a `dict`
holding one session to each collection in `config`.

Instead of calling the MongoManager() constructor, use the
mongo_pool variable, that is already created and connected to Mongo
when the module is loaded.

Example
-------

```
from mongomanager import mongo_pool
...
mongo_pool.setMetadataDocument("collection_name", document)
```
"""

import logging
from configuration import config

# MongoDB driver for Python
from pymongo import MongoClient


class MongoSession():
    """Container for a MongoDB session. It is plugged to a single collection.

    Attributes
    ----------
    client : `pymongo.MongoClient`
    database : `pymongo.database`
    collection : `pymongo.collection`

    Parameters
    ----------
    config_dict : `dict`
        The information needed to connect to the database. Needs to have the
        following fields:
        - ``HOST``: The database hostname or IP. (`str`)
        - ``PORT``: Port to connect to MongoDB. (`int`)
        - ``DATABASE``: Database name. (`str`)
        - ``COLLECTION``: Collection name. (`str`)
        - ``AUTHENTICATE``: Whether or not the databse uses authentication. (`bool`)
        - ``USER``: Username. Only needed when authenticating. (`str`)
        - ``PASS``: Password. Only needed when authenticating. (`str`)
    """

    def __init__(self, config_dict):
        # Session will not be initialized yet
        self.client = None
        self.database = None
        self.collection = None

        # DB location
        self._host = config_dict["HOST"]
        self._port = config_dict["PORT"]
        self._db_name = config_dict["DATABASE"]
        self._collection_name = config_dict["COLLECTION"]

        # Authentication info
        self._authenticate = config_dict.get("AUTHENTICATE", False)
        self._user = config_dict.get("USER", None)
        self._pass = config_dict.get("PASS", None)

        # Initialize logger
        self._logger = logging.getLogger('RuleManager')
        self._logger.debug("Initializing a new Mongo Session on %s:%s." % (self._host, self._port))

    def connect(self):
        """Creates a connection to the database. If the object already has a
        connection, does nothing."""

        if self.client is not None:
            return

        # Create a connection
        self.client = MongoClient(self._host, self._port, retryWrites=False)
        self.database = self.client[self._db_name]
        self.collection = self.database[self._collection_name]

        # Authenticate against the database
        if self._authenticate:
            self.database.authenticate(self._user, self._pass)

    def findOne(self, query):
        """Finds a single document in the collection."""
        return self.collection.find_one(query)

    def findMany(self, query):
        """Finds documents in the collection."""
        return self.collection.find(query)

    def deleteOne(self, query):
        """Deletes a single document from the collection."""
        return self.collection.delete_one(query)

    def deleteMany(self, query):
        """Deletes many documents from the collection."""
        return self.collection.delete_many(query)

    def save(self, document, overwrite=True):
        """Saves a document."""

        # First, delete all documents related to this file
        if overwrite:
            res = self.collection.delete_many({"fileId": document["fileId"]})
            if res.acknowledged and res.deleted_count > 0:
                self._logger.debug("Deleted %d documents from '%s' collection with fileId = %s" % (
                                     res.deleted_count, self._collection_name, document["fileId"]))

        # Second, insert new document
        res = self.collection.insert_one(document)
        if res.acknowledged:
            self._logger.debug("Inserted document into '%s' collection with fileId = %s" % (
                                 self._collection_name, document["fileId"]))

    def saveMany(self, documents):
        """Save a list of documents."""
        return self.collection.insert_many(documents)


class MongoManager():
    """Stores all the MongoDB sessions from the configuration. Reads all
    MongoDB information from config and connects to all sessions at
    load time.

    Attributes
    ----------
    sessions : `dict` (`str` -> `MongoSession`)
        Holds all the pairs of session name and session object.
    """

    def __init__(self):
        self._logger = logging.getLogger('RuleManager')
        self.sessions = {db_info['NAME']: MongoSession(db_info) for db_info in config['MONGO']}

        # TODO: connect later
        for session_name in self.sessions:
            self.sessions[session_name].connect()

    def saveDCDocument(self, document):
        """Saves a Dublin Core metadata document."""

        self.sessions["Dublin Core"].save(document)

    def getDCDocument(self, SDSFile):
        """Returns a Dublin Core metadata document corresponding to a file."""

        return self.sessions["Dublin Core"].findOne({"fileId": SDSFile.filename})

    def deleteDCDocument(self, SDSFile):
        """Updates the Dublin Core metadata document corresponding to a file
        marking it as deleted."""

        return self.sessions["Dublin Core"].collection.update_many(
            {"fileId": SDSFile.filename},
            {'$set': {
                'irods_path': 'DELETED',
                'checksum': 'DELETED'
            }})

    def setMetadataDocument(self, document):
        """Saves a waveform metadata document."""

        self.sessions["WFCatalog"].save(document)

    def getMetadataDocument(self, SDSFile):
        """Returns a waveform metadata document corresponding to a file."""

        return self.sessions["WFCatalog"].findOne({"fileId": SDSFile.filename})

    def deleteMetadataDocument(self, SDSFile):
        """Delete one waveform metadata document corresponding to a file."""

        return self.sessions["WFCatalog"].deleteOne({"fileId": SDSFile.filename})

    def savePPSDDocument(self, document):
        """Saves a PPSD metadata document."""

        self.sessions["PPSD"].save(document, overwrite=False)

    def savePPSDDocuments(self, documents):
        """Save a list of PPSD metadata documents."""
        res = self.sessions["PPSD"].saveMany(documents)
        self._logger.debug("Inserted %d documents in the PPSD collection", len(res.inserted_ids))

    def getPPSDDocuments(self, SDSFile):
        """Return the PPSD documents that correspond to a file."""
        return list(self.sessions["PPSD"].findMany({"fileId": SDSFile.filename}))

    def deletePPSDDocuments(self, SDSFile):
        """Delete the PPSD documents corresponding to a file."""
        res = self.sessions["PPSD"].deleteMany({"fileId": SDSFile.filename})
        self._logger.debug("Deleted %d documents from PPSD collection with fileId = %s" % (
                             res.deleted_count, SDSFile.filename))
        return res


mongo_pool = MongoManager()
