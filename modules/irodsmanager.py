"""
This module implements an iRODS session manager as a _fake singleton_.

Instead of calling the IRODSManager() constructor, use the
irodsSession variable, that is already created and connected to iRODS
when the module is loaded.

Example
-------

```
from irodsmanager import irodsSession
irodsSession.createCollection(sdsFile.irodsDirectory)
irodsSession.createDataObject(sdsFile)
```
"""

import os
import logging

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

        # Initialize logger
        self.logger = logging.getLogger(__name__)
        self.logger.debug("Initializing a new iRODS Session.")

        self.session = None
        self.connect()

    def connect(self):
        """
        def IRODSManager::connect
        Creates a iRODSSession to connect to iRODS
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

        self.logger.debug("Disconnecting the iRODS Session.")

        if self.session is None:
            return

        self.session.cleanup()

    def getCollection(self, path):
        """Returns the collection named by `path`."""
        return self.session.collections.get(path)

    def createCollection(self, collection):
        """Creates a collection in the iRODS catalog. Does nothing if it is already there."""
        self.session.collections.create(collection)

    def getDataObjects(self, path):
        return self.getCollection(path).data_objects

    def createDataObject(self, SDSFile,
                         rescName="demoResc",
                         purgeCache=False,
                         registerChecksum=False):
        """
        def IRODSManager::createDataObject
        Inserts an SDS data object into iRODS at a collection given by
        `SDSFile.irodsDirectory`.

        Parameters
        ----------
        SDSFile : `SDSFile`
            An SDS file descriptor.
        rescName : `str`, optional
            Name of the resource to save the data object.
        purgeCache : `bool`, optional
            Whether or not to purge the cache, in case the resource is compound.
        registerChecksum : `bool`, optional
            Whether or not to register the SHA256 checksum of the object in iCAT.
        """

        # Create the collection if it does not exist
        self.createCollection(SDSFile.irodsDirectory)

        # Attempt to get the data object
        dataObject = self.getDataObject(SDSFile)
        if dataObject is not None:

            # Checksum of file did not change vs. iRODS checksum
            if dataObject.checksum == SDSFile.checksum:
                self.logger.debug("File already registered, cancelling ingestion.")
                return

        # Some put options
        options = {
            kw.RESC_NAME_KW: rescName,
            kw.PURGE_CACHE_KW: purgeCache,
            kw.REG_CHKSUM_KW: registerChecksum
        }

        # Add the data object
        self.session.data_objects.put(SDSFile.filepath, SDSFile.irodsPath, **options)

    def deleteDataObject(self, SDSFile, force=False):
        """
        def IRODSManager::removeDataObject
        Delete an SDS data object from iRODS at a collection given by
        `SDSFile.irodsDirectory`.

        Parameters
        ----------
        SDSFile : `SDSFile`
            An SDS file descriptor.
        force : `bool`
            Whether to force the deletion.
        """

        # Attempt to get the data object
        dataObject = self.getDataObject(SDSFile)
        if dataObject is None:
            self.logger.debug("File not registered, cancelling deletion.")
            return

        # Unlink the data object
        dataObject.unlink(force=force)

    def remotePut(self, SDSFile, rootCollection,
                  rescName="demoResc",
                  purgeCache=False,
                  registerChecksum=False):
        """
        Inserts an SDS data object into iRODS at a collection rooted at `rootCollection`.

        Parameters
        ----------
        SDSFile : `SDSFile`
            An SDS file descriptor.
        rootCollection : `str`
            The root collection to save the data object.
        rescName : `str`, optional
            Name of the resource to save the data object.
        purgeCache : `bool`, optional
            Whether or not to purge the cache, in case the resource is compound.
        registerChecksum : `bool`, optional
            Whether or not to register the SHA256 checksum of the object in iCAT.
        """

        # Create the collection if it does not exist
        self.createCollection(SDSFile.customDirectory(rootCollection))

        # Some put options
        options = {
            kw.RESC_NAME_KW: rescName,
            kw.PURGE_CACHE_KW: purgeCache,
            kw.REG_CHKSUM_KW: registerChecksum
        }

        # Add the data object
        self.session.data_objects.put(SDSFile.filepath,
                                      SDSFile.customPath(rootCollection),
                                      **options)

    def getDataObject(self, SDSFile, rootCollection=None):
        """
        def IRODSManager::createDataObject
        Retrieves a data object from iRODS and returns None if it does not exist

        Parameters
        ----------
        rootCollection : `str`, optional
            The archive's root collection.
        """

        # Attempt to get the file from iRODS
        # If it does not exists an exception is raised and we return None
        try:
            if rootCollection is None:
                return self.session.data_objects.get(SDSFile.irodsPath)
            return self.session.data_objects.get(SDSFile.customPath(rootCollection))
        except DataObjectDoesNotExist:
            return None

    def exists(self, SDSFile, rootCollection=None):
        """Check whether the file is registered in iRODS.

        Parameters
        ----------
        rootCollection : `str`, optional
            The archive's root collection.
        """
        if rootCollection is None:
            return self.session.data_objects.exists(SDSFile.irodsPath)
        return self.session.data_objects.exists(SDSFile.customPath(rootCollection))


irodsSession = IRODSManager()
irodsSession.connect()
