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
mongo_pool.set_wfcatalog_daily_document("collection_name", document)
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
        self._logger = logging.getLogger("RuleManager")
        self._logger.debug("Initializing a new Mongo Session on %s:%s." % (
                                                        self._host, self._port))

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

    def find_one(self, query):
        """Finds a single document in the collection."""
        doc = self.collection.find_one(query)
        if doc is not None:
            self._logger.debug("Found 1 document in '%s' collection",
                               self._collection_name)
        return doc

    def find_many(self, query):
        """Finds documents in the collection."""
        count = self.collection.count_documents(query)
        cur = self.collection.find(query)
        self._logger.debug("Found %d document(s) in '%s' collection",
                           count, self._collection_name)
        return cur

    def delete_one(self, query):
        """Deletes a single document from the collection."""
        res = self.collection.delete_one(query)
        if res.acknowledged:
            self._logger.debug("Deleted %d document(s) from '%s' collection",
                               res.deleted_count, self._collection_name)

    def delete_many(self, query):
        """Deletes many documents from the collection."""
        res = self.collection.delete_many(query)
        if res.acknowledged:
            self._logger.debug("Deleted %d document(s) from '%s' collection",
                               res.deleted_count, self._collection_name)

    def save(self, document, overwrite=True):
        """Saves a document."""

        # First, delete all documents related to this file
        if overwrite:
            res = self.collection.delete_many({"fileId": document["fileId"]})
            if res.acknowledged and res.deleted_count > 0:
                self._logger.debug("Deleted %d document(s) from '%s' collection",
                                   res.deleted_count, self._collection_name)

        # Second, insert new document
        res = self.collection.insert_one(document)
        if res.acknowledged:
            self._logger.debug("Inserted 1 document into '%s' collection",
                               self._collection_name)

    def save_many(self, documents):
        """Save a list of documents."""
        res = self.collection.insert_many(documents)
        if res.acknowledged:
            self._logger.debug("Inserted %d document(s) into '%s' collection",
                               len(res.inserted_ids), self._collection_name)


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
        self.sessions = {db_info["NAME"]: MongoSession(db_info)
                         for db_info in config["MONGO"]}

        # TODO: connect later
        for session_name in self.sessions:
            self.sessions[session_name].connect()

    def save_dc_document(self, document):
        """Saves a Dublin Core document."""
        self.sessions["Dublin Core"].save(document)

    def get_dc_document(self, SDSFile):
        """Returns a Dublin Core document corresponding to a file."""
        return self.sessions["Dublin Core"].find_one({"fileId": SDSFile.filename})

    def delete_dc_document(self, SDSFile):
        """Updates the Dublin Core document corresponding to a file
        marking it as deleted."""

        return self.sessions["Dublin Core"].collection.update_many(
            {"fileId": SDSFile.filename},
            {"$set": {
                "irods_path": "DELETED",
                "checksum": "DELETED"
            }})

    def set_wfcatalog_daily_document(self, document):
        """Saves a WFCatalog-daily document."""
        self.sessions["WFCatalog-daily"].save(document)

    def get_wfcatalog_daily_document(self, sds_file):
        """Returns a WFCatalog-daily document corresponding to a file."""
        return self.sessions["WFCatalog-daily"].find_one({"fileId": sds_file.filename})

    def delete_wfcatalog_daily_document(self, sds_file):
        """Delete one WFCatalog-daily document corresponding to a file."""
        return self.sessions["WFCatalog-daily"].delete_one({"fileId": sds_file.filename})

    def save_wfcatalog_segments_documents(self, documents):
        """Save a list of WFCatalog-segments documents."""
        self.sessions["WFCatalog-segments"].save_many(documents)

    def get_wfcatalog_segments_documents(self, sds_file):
        """Return the WFCatalog-segments documents that correspond to a file."""
        return list(self.sessions["WFCatalog-segments"].find_many(
            {"fileId": sds_file.filename}))

    def delete_wfcatalog_segments_documents(self, sds_file):
        """Delete the WFCatalog-segments documents corresponding to a file."""
        self.sessions["WFCatalog-segments"].delete_many({"fileId": sds_file.filename})

    def save_ppsd_document(self, document):
        """Saves a PPSD document."""
        self.sessions["PPSD"].save(document, overwrite=False)

    def save_ppsd_documents(self, documents):
        """Save a list of PPSD documents."""
        self.sessions["PPSD"].save_many(documents)

    def get_ppsd_documents(self, sds_file):
        """Return the PPSD documents that correspond to a file."""
        return list(self.sessions["PPSD"].find_many({"fileId": sds_file.filename}))

    def delete_ppsd_documents(self, sds_file):
        """Delete the PPSD documents corresponding to a file."""
        self.sessions["PPSD"].delete_many({"fileId": sds_file.filename})

mongo_pool = MongoManager()
