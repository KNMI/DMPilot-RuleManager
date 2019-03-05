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


def ingestion(options, SDSFile):
    """Handler for the ingestion rule.

    Parameters
    ----------
    options : `dict`
        The rule's options.
        - ``days``: Maximum age of the file to be ingested (`int`)
        - ``prune``: Whether or not to prune the file (`bool`)
        - ``repackRecordSize``: (`int`)
        - ``daysIgnoreOlderThan``: (`int`)
        - ``rescName``: Name of the iRODS resource to save the object (`str`)
        - ``purgeCache``: Whether or not to purge the cache,
                          in case the resource is compound (`bool`)
    SDSFile : `SDSFile`
        The file to be processed.
    """

    logger.info("Ingesting file: " + SDSFile.filename)

    # Check the modification time of the file
    if SDSFile.modified < (datetime.now() - timedelta(days=options["days"])):
        logger.info("File too old, cancelling ingestion.")
        return

    # The file was already ingested by iRODS
    if isIngested(SDSFile):
        logger.info("File already present, cancelling ingestion.")
        return

    # A prune is requested
    if options["prune"]:
        logger.info("Prune is requested.")

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
    SDSFile : `SDSFile`
        The file to be processed.
    """

    logger.info("Ingesting file: " + SDSFile.customPath(options["remoteRoot"]))

    # Attempt to ingest to iRODS
    irodsSession.remotePut(SDSFile,
                           options["remoteRoot"],
                           purgeCache=True,
                           registerChecksum=True)


def isIngested(SDSFile):
    """Stateless check to see if the file exists in iRODS?
    TODO XXX
    """

    return irodsSession.exists(SDSFile)


def purge(options, SDSFile):
    """Handler for the temporary archive purge rule.

    Parameters
    ----------
    options : `dict`
        The rule's options.
        - ``daysPurgeAfter``: Number of days after which the file should be deleted (`int`)
    SDSFile : `SDSFile`
        The file to be processed.
    """

    # We can check time modified etc etc..
    if SDSFile.created > (datetime.now() - timedelta(days=7)):
        return

    # Some other configurable rules
    logger.info("Purging file: " + SDSFile.filename)
    irodsSession.purgeTemporaryFile(SDSFile)


def dcMetadata(options, sdsFile):
    """Process and save Dublin Core metadata of an SDS file.

    Parameters
    ----------
    options : `dict`
        The rule's options.
    SDSFile : `SDSFile`
        The file to be processed.
    """

    logger.info("Dublin Core metadata for " + sdsFile.filename)

    if dublinCore.getDCMetadata(sdsFile) is not None:
        logger.info("DC metadata already exists for " + sdsFile.filename)
        return

    document = dublinCore.extractDCMetadata(sdsFile)

    # Save to the database
    if document:
        mongoSession.saveDCDocument(document)
        logger.info("Saved DC metadata for " + sdsFile.filename)


def waveformMetadata(options, SDSFile):
    """Handler for the WFCatalog metadata rule.
    TODO XXX

    Parameters
    ----------
    options : `dict`
        The rule's options.
    SDSFile : `SDSFile`
        The file to be processed.
    """
    if mongoSession.getMetadataDocument(SDSFile) is not None:
        return

    logger.info(collector.getMetadata(SDSFile))


def testPrint(options, sdsFile):
    """Prints the filename."""
    logger.info(sdsFile.filename)
