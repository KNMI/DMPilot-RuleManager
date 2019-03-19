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
        - ``qualities``: Quality codes of the files to be processed (`list` of `str`)
    SDSFile : `SDSFile`
        The file to be processed.
    """

    if SDSFile.quality not in options["qualities"]:
        return

    # Prune the file
    SDSFile.prune(options["repackRecordSize"])


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

    # Check if qualities need to be checked
    if SDSFile.quality not in options["qualities"]:
        return

    logger.info("Ingesting file: " + SDSFile.filename)

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

    # Check if qualities need to be checked
    if SDSFile.quality not in options["qualities"]:
        return

    logger.info("Ingesting file: " + SDSFile.customPath(options["remoteRoot"]))

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

    # Files with size 0 need to be deleted regardless
    if options["deleteEmptyFiles"] and SDSFile.size == 0:
        logger.info("Purging empty file: " + SDSFile.filename)
        return irodsSession.purgeTemporaryFile(SDSFile)

    # We can check time modified etc etc..
    if SDSFile.created > (datetime.now() - timedelta(days=7)):
        return

    # Some other configurable rules
    logger.info("Purging file: " + SDSFile.filename)
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

    # Check if qualities need to be checked
    if SDSFile.quality not in options["qualities"]:
        return

    logger.info("Dublin Core metadata for " + SDSFile.filename)

    if dublinCore.getDCMetadata(SDSFile) is not None:
        logger.info("DC metadata already exists for " + SDSFile.filename)
        return

    document = dublinCore.extractDCMetadata(SDSFile)

    # Save to the database
    if document:
        mongoSession.saveDCDocument(document)
        logger.info("Saved DC metadata for " + SDSFile.filename)


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

    # Check if qualities need to be checked
    if SDSFile.quality not in options["qualities"]:
        return

    if mongoSession.getMetadataDocument(SDSFile) is not None:
        return

    logger.info(collector.getMetadata(SDSFile))


def testPrint(options, sdsFile):
    """Prints the filename."""
    logger.info(sdsFile.filename)
