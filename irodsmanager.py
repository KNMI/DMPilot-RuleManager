import irods
import os
from irods.session import iRODSSession
from irods.models import DataObject

from configuration import config

class IRODSManager():
    """
    Class IRODSManager
    Manages session to iRODS
    """

    root = config["IRODS_ROOT"]

    def __init__(self):

        self.session = None
        self.connect()

    def connect(self):
        """
        def IRODSManager::connect
        Connects an iRODSSession
        """

        if self.session is not None:
            return

        self.session = irods.session.iRODSSession(
            host=config["IRODS"]["HOST"],
            port=config["IRODS"]["PORT"],
            user=config["IRODS"]["USER"],
            password=config["IRODS"]["PASS"],
            zone=config["IRODS"]["ZONE"]
        )

    def disconnect(self):
        """
        def IRODSManager::disconnect
        The iRODSSession class is a context manager and calls this function during __exit__
        See: https://github.com/irods/python-irodsclient/blob/master/irods/session.py
        """

        self.session.cleanup()

    def getCollection(self, path):
        return self.session.collections.get(path)

    def createCollection(self, collection):
        self.session.collections.create(collection)

    def getDataObjects(self, path):
        return self.getCollection(path).data_objects

    def createDataObject(self, SDSFile):
        """
        def IRODSManager::createDataObject
        Registers a data object to iRODS
        """

        # Create the collection if it does not exist
        self.createCollection(SDSFile.irodsDirectory)

        # Add the data object
        self.session.data_objects.put(SDSFile.filepath, SDSFile.irodsPath)
      

    def dataObjectExists(self, SDSFile):
        """
        def IRODSManager::createDataObject
        Registers a data object to iRODS
        XXX TODO
        """

        return self.session.query(DataObject.name, DataObject.checksum).filter(DataObject.name == SDSFile.filename)

I = IRODSManager()
I.connect()
