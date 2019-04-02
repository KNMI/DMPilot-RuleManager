"""
Module that houses all the rule handler for SDS daily files.

Every rule should be implemented as a module funcion with exactly two arguments:
1) a `dict` that holds the options for the rule, and
2) the item that is subject to the rule, in this case, a `SDSFile` object.
"""

import logging
from datetime import datetime, timedelta

from modules.irodsmanager import irodsSession
from modules.mongomanager import mongoSession
from modules.wfcatalog import collector
from modules.dublincore import dublinCore
from modules.psdcollector import psdCollector

logger = logging.getLogger(__name__)


def psdMetadata(self, options, SDSFile):
    """Handler for PSD calculation.
    TODO XXX

    Parameters
    ----------
    options : `dict`
        The rule's options.
    SDSFile : `SDSFile`
        The file to be processed.
    """

    print(psdCollector.process(SDSFile))


def prune(options, SDSFile):
    """Handler for the file pruning/repacking rule.

    Parameters
    ----------
    options : `dict`
        The rule's options.
        - ``repackRecordSize``: The new record size (`int`)
        - ``removeOverlap``: Whether or not to remove overlaps (`bool`)
        - ``qualities``: Quality codes of the files to be processed (`list` of `str`)
    SDSFile : `SDSFile`
        The file to be processed.
    """

    # Prune the file to a .Q quality file in the temporary archive
    SDSFile.prune(recordLength=options["repackRecordSize"],
                  removeOverlap=options["removeOverlap"])


def ingestion(options, SDSFile):
    """Handler for the ingestion rule.

    Parameters
    ----------
    options : `dict`
        The rule's options.
        - ``days``: Maximum age of the file to be ingested (`int`)
        - ``daysIgnoreOlderThan``: (`int`)
        - ``rescName``: Name of the iRODS resource to save the object (`str`)
        - ``purgeCache``: Whether or not to purge the cache,
                          in case the resource is compound (`bool`)
        - ``qualities``: Quality codes of the files to be processed (`list` of `str`)
    SDSFile : `SDSFile`
        The file to be processed.
    """

    logger.info("Ingesting file %s." % SDSFile.filename)

    # Check the modification time of the file
    if SDSFile.modified < (datetime.now() - timedelta(days=options["days"])):
        return logger.info("File too old, cancelling ingestion.")

    # The file was already ingested by iRODS
    if irodsSession.exists(SDSFile):
        return logger.info("File already present, cancelling ingestion.")

    # Attempt to ingest to iRODS
    irodsSession.createDataObject(SDSFile,
                                  rescName="compResc",
                                  purgeCache=True,
                                  registerChecksum=True)

    # Check if checksum is saved
    logger.info(irodsSession.getDataObject(SDSFile).checksum)


def federatedIngestion(options, SDSFile):
    """Handler for a federated ingestion rule. Puts the object in a given
    root collection, potentially in a federated zone.

    Parameters
    ----------
    options : `dict`
        The rule's options.
        - ``remoteRoot``: Name of the root collection to put the object (`str`)
        - ``qualities``: Quality codes of the files to be processed (`list` of `str`)
    SDSFile : `SDSFile`
        The file to be processed.
    """

    logger.info("Ingesting file %s." % SDSFile.customPath(options["remoteRoot"]))

    # Attempt to ingest to iRODS
    irodsSession.remotePut(SDSFile,
                           options["remoteRoot"],
                           purgeCache=True,
                           registerChecksum=True)


def purge(options, SDSFile):
    """Handler for the temporary archive purge rule.

    Parameters
    ----------
    options : `dict`
        The rule's options.
        - ``daysPurgeAfter``: Number of days after which the file should be deleted (`int`)
        - ``deleteEmptyFiles``: Whether to delete files with no samples regardless of age (`bool`)
    SDSFile : `SDSFile`
        The file to be processed.
    """

    # If configured: files with file size 0 need to be deleted
    if options["deleteEmptyFiles"] and SDSFile.size == 0:
        logger.info("Purging empty file %s." % SDSFile.filename)
        return irodsSession.purgeTemporaryFile(SDSFile)

    # Check if the file modification date is after the configured limit and must be kept
    if SDSFile.created > (datetime.now() - timedelta(days=options["daysPurgeAfter"])):
        return

    # Some other configurable rules
    logger.info("Purging file %s." % SDSFile.filename)
    irodsSession.purgeTemporaryFile(SDSFile)


def dcMetadata(options, SDSFile):
    """Process and save Dublin Core metadata of an SDS file.

    Parameters
    ----------
    options : `dict`
        The rule's options.
        - ``qualities``: Quality codes of the files to be processed (`list` of `str`)
    SDSFile : `SDSFile`
        The file to be processed.
    """

    logger.info("Dublin Core metadata for %s." % SDSFile.filename)

    # Get the existing Dublin Core Object
    dublinCoreObject = dublinCore.getDCMetadata(SDSFile)

    if dublinCoreObject is not None:
        if dublinCoreObject["checksum"] == SDSFile.checksum:
            return logger.info("DC metadata already exists for %s." % SDSFile.filename)

    document = dublinCore.extractDCMetadata(SDSFile)

    # Save to the database
    if document:
        mongoSession.saveDCDocument(document)
        logger.info("Saved DC metadata for %s." % SDSFile.filename)


def waveformMetadata(options, SDSFile):
    """Handler for the WFCatalog metadata rule.
    TODO XXX

    Parameters
    ----------
    options : `dict`
        The rule's options.
        - ``qualities``: Quality codes of the files to be processed (`list` of `str`)
    SDSFile : `SDSFile`
        The file to be processed.
    """

    dataObject = irodsSession.getDataObject(SDSFile)

    # There is no data object available in iRODS
    if dataObject is None:
        return logger.info("IRODS Data Object does not exist.")

    # Check checksum of SDSFile against what is in the database
    metadataObject = mongoSession.getMetadataDocument(SDSFile)
    if metadataObject is not None:
        if metadataObject["checksum"] == SDSFile.checksum:
            return logger.info("Metadata already exists and hash did not change.")

    document = collector.getMetadata(SDSFile)
    if document is None:
      return logger.error("Could not get the waveform metadata.")

    logger.info("Adding new waveform metadata for %s." % SDSFile.filename)

    # Save the metadata document
    mongoSession.setMetadataDocument(document)


def testPrint(options, sdsFile):
    """Prints the filename."""
    logger.info(sdsFile.filename)
    logger.info(sdsFile.directory)
    logger.info(sdsFile.irodsDirectory)
