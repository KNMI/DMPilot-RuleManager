"""
Collection of conditions that either return True or False
depending on whether conditions are met
"""

import logging
from datetime import datetime, timedelta
import os

from modules.dublincore import getDCMetadata
from orfeus.sdsfile import SDSFile

from modules.irodsmanager import irodsSession
from modules.mongomanager import mongoSession

# Initialize logger
logger = logging.getLogger(__name__)


def assertQualityCondition(options, sds_file):
    """Asserts that the SDSFile quality is in options."""
    return sds_file.quality in options["qualities"]


def assertIRODSExistsCondition(options, sds_file):
    return irodsSession.exists(sds_file)


def assertIRODSNotExistCondition(options, sds_file):
    """Asserts that the SDSFile is not in iRODS."""
    # The file was not already ingested by iRODS
    return not irodsSession.exists(sds_file)


def assertWFCatalogExistsCondition(options, sds_file):

    # Extract the current metadata object from the database
    metadataObject = mongoSession.getMetadataDocument(sds_file)

    # Document exists and has the same hash: it exists
    if metadataObject is not None:
        exists = True
        if metadataObject["checksum"] == sds_file.checksum:
            same_hash = True
            logger.debug("File %s does exist in WFCatalog, with same checksum (%s)." % (
                            sds_file.filename, sds_file.checksum))
        else:
            same_hash = False
            logger.debug("File %s does exist in WFCatalog, but with a different checksum (%s vs %s)." % (
                            sds_file.filename, metadataObject["checksum"], sds_file.checksum))
    else:
        exists = False
        logger.debug("File %s does not exist in WFCatalog." % sds_file.filename)

    return exists and same_hash


def assertWFCatalogNotExistsCondition(options, sds_file):

    return not assertWFCatalogExistsCondition(options, sds_file)


def assertModificationTimeYoungerThan(options, sds_file):
    return sds_file.modified > (datetime.now() - timedelta(days=options["days"]))


def assertModificationTimeOlderThan(options, sds_file):
    return sds_file.modified < (datetime.now() - timedelta(days=options["days"]))


def assertDCMetadataExistsCondition(options, sds_file):

    # Get the existing Dublin Core Object
    dublinCoreObject = getDCMetadata(sds_file)

    # Document exists and has the same hash: it exists
    if dublinCoreObject is not None:
        exists = True
        if dublinCoreObject["checksum"] == sds_file.checksum:
            same_hash = True
            logger.debug("File %s does exist in DublinCoreMetadata, with same checksum (%s)." % (
                            sds_file.filename, sds_file.checksum))
        else:
            same_hash = False
            logger.debug("File %s does exist in DublinCoreMetadata, but with a different checksum (%s vs %s)." % (
                            sds_file.filename, dublinCoreObject["checksum"], sds_file.checksum))
    else:
        exists = False
        logger.debug("File %s does not exist in DublinCoreMetadata." % sds_file.filename)

    return exists and same_hash


def assertDCMetadataNotExistsCondition(options, sds_file):

    return not assertDCMetadataExistsCondition(options, sds_file)


def assertPrunedFileExistsCondition(options, sds_file):
    """Aserts that the pruned version of the SDS file is in the temporary archive."""

    # Create a phantom SDSFile with a different quality idenfier
    qualityFile = SDSFile(sds_file.filename, sds_file.archiveRoot)
    qualityFile.quality = "Q"
    return os.path.exists(qualityFile.filepath)


def assertTempArchiveExistCondition(options, SDSFile):
    """Assert that the file exists in the temporary archive."""
    return os.path.isfile(SDSFile.filepath)


def assertFileReplicatedCondition(options, sds_file):
    """Assert that the file has been replicated.

    Checks replication by verifying that the file is present in iRODS,
    in the given remote collection. Does not check checksum.

    Parameters
    ----------
    options : `dict`
        The rule's options.
        - ``replicationRoot``: Root replication collection (`str`)
    sds_file : `SDSFile`
        The file being processed.
    """
    return irodsSession.doesFederatedDataObjectExist(sds_file,
                                                     options["replicationRoot"])


def assertPIDCondition(options, sds_file):
    """Assert that a PID was assigned to the file on iRODS."""
    return irodsSession.getPID(sds_file) is not None


def assertReplicaPIDCondition(options, sds_file):
    """Assert that a PID was assigned to the file on the replication iRODS."""
    return irodsSession.getPID(sds_file, rootCollection=options["replicationRoot"]) is not None
