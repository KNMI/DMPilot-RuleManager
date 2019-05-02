"""
Collection of policies that either return True or False
depending on whether conditions are met
"""

from datetime import datetime, timedelta

from modules.irodsmanager import irodsSession
from modules.mongomanager import mongoSession
from modules.dublincore import dublinCore

def assertQualityCondition(options, SDSFile):
    """
    def assertQualityCondition
    Asserts that the SDSFile quality is in options
    """
    return SDSFile.quality in options["qualities"]

def assertIRODSNotExistCondition(options, SDSFile):
    """
    def assertIRODSNotExistCondition
    Asserts that the SDSFile is not in iRODS
    """
    # The file was not already ingested by iRODS
    return not irodsSession.exists(SDSFile)

def assertWFCatalogExistsCondition(options, SDSFile):

    # Extract the current metadata object from the database
    metadataObject = mongoSession.getMetadataDocument(SDSFile)

    # Document exists and has the same hash: it exists
    return (metadataObject is not None) and (metadataObject["checksum"] == SDSFile.checksum)

def assertWFCatalogNotExistsCondition(options, SDSFile):

    return not assertWFCatalogExistsCondition(options, SDSFile)

def assertIRODSExistsCondition(options, SDSFile):
    return irodsSession.exists(SDSFile)

def assertModificationTimeYoungerThan(options, SDSFile):
    """
    def assertIRODSNotExistCondition
    Asserts that the SDSFile is not in iRODS
    """
    return SDSFile.modified > (datetime.now() - timedelta(days=options["days"]))

def assertModificationTimeOlderThan(options, SDSFile):
    """
    def assertIRODSNotExistCondition
    Asserts that the SDSFile is not in iRODS
    """
    return SDSFile.modified < (datetime.now() - timedelta(days=options["days"]))

def assertDCMetadataExistsCondition(options, SDSFile):

    # Get the existing Dublin Core Object
    dublinCoreObject = dublinCore.getDCMetadata(SDSFile)

    return (dublinCoreObject is not None) and (dublinCoreObject["checksum"] == SDSFile.checksum)

def assertDCMetadataNotExistsCondition(options, SDSFile):

    return not assertDCMetadataExistsCondition(options, SDSFile)
