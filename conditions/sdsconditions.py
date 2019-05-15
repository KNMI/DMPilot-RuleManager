"""
Collection of conditions that either return True or False
depending on whether conditions are met
"""

import logging
from datetime import datetime, timedelta

from modules.irodsmanager import irodsSession
from modules.mongomanager import mongoSession
from modules.dublincore import dublinCore

# Initialize logger
logger = logging.getLogger(__name__)

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
    if metadataObject is not None:
        exists = True
        if metadataObject["checksum"] == SDSFile.checksum:
            same_hash = True
            logger.debug("File %s does exist in WFCatalog, with same checksum (%s)." % (
                            SDSFile.filename, SDSFile.checksum))
        else:
            same_hash = False
            logger.debug("File %s does exist in WFCatalog, but with a different checksum (%s vs %s)." % (
                            SDSFile.filename, metadataObject["checksum"], SDSFile.checksum))
    else:
        exists = False
        logger.debug("File %s does not exist in WFCatalog." % SDSFile.filename)

    return exists and same_hash

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


    # Document exists and has the same hash: it exists
    if dublinCoreObject is not None:
        exists = True
        if dublinCoreObject["checksum"] == SDSFile.checksum:
            same_hash = True
            logger.debug("File %s does exist in DublinCoreMetadata, with same checksum (%s)." % (
                            SDSFile.filename, SDSFile.checksum))
        else:
            same_hash = False
            logger.debug("File %s does exist in DublinCoreMetadata, but with a different checksum (%s vs %s)." % (
                            SDSFile.filename, dublinCoreObject["checksum"], SDSFile.checksum))
    else:
        exists = False
        logger.debug("File %s does not exist in DublinCoreMetadata." % SDSFile.filename)

    return exists and same_hash

def assertDCMetadataNotExistsCondition(options, SDSFile):

    return not assertDCMetadataExistsCondition(options, SDSFile)
