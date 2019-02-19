import os

# Lots of iRODS imports
import irods
from irods.session import iRODSSession
from irods.models import DataObject
from irods.exception import DataObjectDoesNotExist
import irods.keywords as kw

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

        # Open a session
        self.session = iRODSSession(
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

        # Attempt to get the data object
        dataObject = self.getDataObject(SDSFile)
        if dataObject is not None:
            return

        # Checksum of file did not change vs. iRODS checksum
        if dataObject.checksum == "sha2:%s" % SDSFile.checksum:
            return

        # Create the collection if it does not exist
        self.createCollection(SDSFile.irodsDirectory)

        # Some put options
        options = {
            kw.RESC_NAME_KW: "compResc",
            kw.REG_CHKSUM_KW: True
        }

        # Add the data object
        self.session.data_objects.put(SDSFile.filepath, SDSFile.irodsPath, **options)
      

    def purgeTemporaryFile(self, SDSFile):
        """
        def IRODSManager::purgeTemporaryFile
        Purges an SDSFile from the temporary archive if it exists in iRODS
        """

        dataObject = self.getDataObject(SDSFile)

        # Not in iRODS: do not purge from disk
        if dataObject is None:
            return

        # Yeah let's be careful with this..
        # os.remove(SDSFile.filepath)

    def getDataObject(self, SDSFile):
        """
        def IRODSManager::createDataObject
        Retrieves a data object from iRODS and returns None if it does not exist
        """

        # Attempt to get the file from iRODS
        # If it does not exists an exception is raised and we return None
        try:
          return self.session.data_objects.get(SDSFile.irodsPath) 
        except DataObjectDoesNotExist:
          return None

I = IRODSManager()
I.connect()
