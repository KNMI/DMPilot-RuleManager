"""
Module that houses all the rule handlers for SDS daily files.

Every rule should be implemented as a module function with exactly two arguments:
1) a `dict` that holds the options for the rule, and
2) the item that is subject to the rule, in this case, a `SDSFile` object.
"""

import logging
from modules.wfcatalog import getWFMetadata
from modules.dublincore import extractDCMetadata

from modules.irodsmanager import irodsSession
from modules.mongomanager import mongoSession
from modules.psdcollector import psdCollector

logger = logging.getLogger(__name__)


def psdMetadataRule(options, SDSFile):
    """Handler for PSD calculation.
    TODO XXX

    Parameters
    ----------
    options : `dict`
        The rule's options.
    SDSFile : `SDSFile`
        The file to be processed.
    """

    if SDSFile.isPressureChannel:
        # Store in psd.seismic
        print(psdCollector.process(SDSFile))
    else:
        # Store in psd.infra
        print(psdCollector.process(SDSFile))


def pruneRule(options, SDSFile):
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

    logger.debug("Pruning file %s." % SDSFile.filename)

    # Prune the file to a .Q quality file in the temporary archive
    SDSFile.prune(recordLength=options["repackRecordSize"],
                  removeOverlap=options["removeOverlap"])

    logger.debug("Pruned file %s." % SDSFile.filename)


def ingestionRule(options, SDSFile):
    """Handler for the ingestion rule.

    Parameters
    ----------
    options : `dict`
        The rule's options.
        - ``rescName``: Name of the iRODS resource to save the object (`str`)
        - ``purgeCache``: Whether or not to purge the cache,
                          in case the resource is compound (`bool`)
    SDSFile : `SDSFile`
        The file to be processed.
    """

    logger.debug("Ingesting file %s." % SDSFile.filename)

    # Attempt to ingest to iRODS
    irodsSession.createDataObject(SDSFile,
                                  rescName="compResc",
                                  purgeCache=True,
                                  registerChecksum=True)

    # Check if checksum is saved
    logger.debug("Ingested file %s with checksum '%s'" % (
            SDSFile.filename, irodsSession.getDataObject(SDSFile).checksum))


def pidRule(options, SDSFile):
    """Handler for the PID assignment rule.

    Parameters
    ----------
    options : `dict`
        The rule's options.
    SDSFile : `SDSFile`
        The file to be processed.
    """

    logger.debug("Assigning PID to file %s." % SDSFile.filename)

    # Attempt to assign PID
    is_new, pid = irodsSession.assignPID(SDSFile)

    if is_new is None:
        logger.error("Error while assigning PID to file %s." % SDSFile.filename)
    elif is_new:
        logger.info("Assigned PID %s to file %s." % (pid, SDSFile.filename))
    elif not is_new:
        logger.info("File %s was already previously assigned PID %s." % (SDSFile.filename, pid))


def replicationRule(options, SDSFile):
    """Handler for the PID assignment rule.

    Parameters
    ----------
    options : `dict`
        The rule's options.
        - ``replicationRoot``: Root replication collection (`str`)
    SDSFile : `SDSFile`
        The file to be processed.
    """

    logger.debug("Replicating file %s." % SDSFile.filename)

    # Attempt to replicate file
    success, response = irodsSession.eudatReplication(SDSFile, options["replicationRoot"])

    if success:
        logger.debug("Replicated file %s to collection %s." % (SDSFile.filename,
                                                               options["replicationRoot"]))
    else:
        logger.error("Error replicating file %s: %s" % (SDSFile.filename, response))


def deleteArchiveRule(options, SDSFile):
    """Handler for the rule that deletes a file from the iRODS archive.

    Parameters
    ----------
    options : `dict`
        The rule's options.
    SDSFile : `SDSFile`
        The description of the file to be deleted.
    """

    logger.debug("Deleting file %s." % SDSFile.filename)

    # Attempt to delete from iRODS
    irodsSession.deleteDataObject(SDSFile)

    # Check if checksum is saved
    logger.debug("Deleted file %s." % SDSFile.filename)


def federatedIngestionRule(options, SDSFile):
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

    logger.debug("Ingesting file %s." % SDSFile.customPath(options["remoteRoot"]))

    # Attempt to ingest to iRODS
    irodsSession.remotePut(SDSFile,
                           options["remoteRoot"],
                           purgeCache=True,
                           registerChecksum=True)

    logger.debug("Ingested file %s" % SDSFile.customPath(options["remoteRoot"]))


def purgeRule(options, SDSFile):
    """Handler for the temporary archive purge rule.

    Parameters
    ----------
    options : `dict`
        The rule's options.
    SDSFile : `SDSFile`
        The file to be processed.
    """

    # Some other configurable rules
    logger.debug("Purging file %s from temporary archive." % SDSFile.filename)
    try:
        # Yeah let's be careful with this..
        # os.remove(SDSFile.filepath)
        logger.debug("Purged file %s from temporary archive." % SDSFile.filename)
    except OSError as e:
        if e.errno != errno.ENOENT:
            raise
        logger.debug("File %s not present in temporary archive." % SDSFile.filename)


def dcMetadataRule(options, SDSFile):
    """Process and save Dublin Core metadata of an SDS file.

    Parameters
    ----------
    options : `dict`
        The rule's options.
        - ``qualities``: Quality codes of the files to be processed (`list` of `str`)
    SDSFile : `SDSFile`
        The file to be processed.
    """

    logger.debug("Saving Dublin Core metadata for %s." % SDSFile.filename)

    # Get the existing Dublin Core Object
    document = extractDCMetadata(SDSFile, irodsSession.getPID(SDSFile).upper())

    # Save to the database
    if document:
        mongoSession.saveDCDocument(document)
        logger.debug("Saved Dublin Core metadata for %s." % SDSFile.filename)


def deleteDCMetadataRule(options, SDSFile):
    """Delete Dublin Core metadata of an SDS file.

    Parameters
    ----------
    options : `dict`
        The rule's options.
    SDSFile : `SDSFile`
        The file to be processed.
    """

    logger.debug("Deleting Dublin Core metadata for %s." % SDSFile.filename)

    mongoSession.deleteDCDocument(SDSFile)
    logger.debug("Deleted Dublin Core metadata for %s." % SDSFile.filename)


def waveformMetadataRule(options, SDSFile):
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

    # Get waveform metadata
    document = getWFMetadata(SDSFile)
    if document is None:
        return logger.error("Could not get the waveform metadata.")

    logger.debug("Saving waveform metadata for %s." % SDSFile.filename)

    # Save the metadata document
    mongoSession.setMetadataDocument(document)
    logger.debug("Saved waveform metadata for %s." % SDSFile.filename)


def deleteWaveformMetadataRule(options, SDSFile):
    """Delete waveform metadata of an SDS file.

    Parameters
    ----------
    options : `dict`
        The rule's options.
    SDSFile : `SDSFile`
        The file to be processed.
    """

    logger.debug("Deleting waveform metadata for %s." % SDSFile.filename)

    mongoSession.deleteMetadataDocument(SDSFile)
    logger.debug("Deleted waveform metadata for %s." % SDSFile.filename)


def removeFromDeletionDatabaseRule(options, SDSFile):
    """Removes the file from the deletion database.

    To be used after a successful deletion of the file from all desired archives.
    """

    logger.debug("Removing deletion entry for %s." % SDSFile.filename)

    from core.database import deletion_database
    deletion_database.remove(SDSFile)

    logger.debug("Removed deletion entry for %s." % SDSFile.filename)


def testPrint(options, sdsFile):
    """Prints the filename."""
    logger.info(sdsFile.filename)
    logger.info(sdsFile.directory)
    logger.info(sdsFile.irodsDirectory)
