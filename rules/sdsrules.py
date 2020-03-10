"""
Module that houses all the rule handlers for SDS daily files.

Every rule should be implemented as a module function with exactly two arguments:
1) a `dict` that holds the options for the rule, and
2) the item that is subject to the rule, in this case, a `SDSFile` object.
"""

import logging
import os
import shutil

from modules.wfcatalog import getWFMetadata
from modules.dublincore import extractDCMetadata

import modules.s3manager as s3manager
from modules.irodsmanager import irodsSession
from modules.mongomanager import mongo_pool
from modules.psd2.psd import PSDCollector

logger = logging.getLogger('RuleManager')


def ppsdMetadataRule(options, SDSFile):
    """Handler for PPSD calculation.

    Parameters
    ----------
    options : `dict`
        The rule's options.
    SDSFile : `SDSFile`
        The file to be processed.
    """

    logger.debug("Computing PPSD metadata for %s." % SDSFile.filename)

    # Process PPSD
    documents = PSDCollector(connect_sql=False).process(SDSFile, cache_response=False)

    # Save to the database
    mongo_pool.deletePPSDDocuments(SDSFile)
    mongo_pool.savePPSDDocuments(documents)
    logger.debug("Saved PPSD metadata for %s." % SDSFile.filename)


def deletePPSDMetadataRule(options, SDSFile):
    """Delete PPSD metadata of an SDS file.

    Parameters
    ----------
    options : `dict`
        The rule's options.
    SDSFile : `SDSFile`
        The file to be processed.
    """

    logger.debug("Deleting PPSD metadata for %s." % SDSFile.filename)
    mongo_pool.deletePPSDDocuments(SDSFile)
    logger.debug("Deleted PPSD metadata for %s." % SDSFile.filename)


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


def ingestionIrodsRule(options, SDSFile):
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


def ingestionS3Rule(options, SDSFile):
    """Handler for the ingestion rule.

    Parameters
    ----------
    options : `dict`
        The rule's options.
    SDSFile : `SDSFile`
        The file to be processed.
    """
    logger.debug("Ingesting file %s." % SDSFile.filename)

    # Upload file to S3
    s3manager.put(SDSFile)

    # Check if checksum is saved
    logger.debug("Ingested file %s with checksum '%s'" % (
            SDSFile.filename, SDSFile.checksum))


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


def addPidToWFCatalogRule(options, SDSFile):
    """Updates the WFCatalog with the file PID from the local iRODS archive.

    Parameters
    ----------
    options : `dict`
        The rule's options.
    SDSFile : `SDSFile`
        The file to be processed.
    """

    logger.debug("Updating WFCatalog with the PID of file %s." % SDSFile.filename)

    pid = irodsSession.getPID(SDSFile)

    if pid is not None:
        mongo_pool.update_many({"fileId": SDSFile.filename},
                               {"$set": {"dc_identifier": pid}})
        logger.info("Entry for file %s updated with PID %s." % (SDSFile.filename, pid))
    else:
        logger.error("File %s has no PID." % SDSFile.filename)


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
    except FileNotFoundError:
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
        mongo_pool.saveDCDocument(document)
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

    logger.debug("Marking %s as deleted in Dublin Core metadata." % SDSFile.filename)
    mongo_pool.deleteDCDocument(SDSFile)
    logger.debug("Marked %s as deleted in Dublin Core metadata." % SDSFile.filename)


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
    mongo_pool.setMetadataDocument(document)
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

    mongo_pool.deleteMetadataDocument(SDSFile)
    logger.debug("Deleted waveform metadata for %s." % SDSFile.filename)


def removeFromDeletionDatabaseRule(options, SDSFile):
    """Removes the file from the deletion database.

    To be used after a successful deletion of the file from all desired archives.
    """

    logger.debug("Removing deletion entry for %s." % SDSFile.filename)

    from core.database import deletion_database
    deletion_database.remove(SDSFile)

    logger.debug("Removed deletion entry for %s." % SDSFile.filename)


def printWithMessage(options, sdsFile):
    """Prints the filename followed by a message.

    Parameters
    ----------
    options : `dict`
        The rule's options.
        - ``message``: Message to print (`str`)
    SDSFile : `SDSFile`
        The file to be processed.
    """
    print(sdsFile.filename, options["message"])


def quarantineRule(options, sdsFile):
    """Moves the file to another directory where it can be further analyzed by a human.

    It should be called with the .Q file, but acts on both of them. It moves the raw
    .D data file, and deletes the .Q file. Fails if there is already a .D file
    with same name already quarantined.

    Parameters
    ----------
    options : `dict`
        The rule's options.
        - ``quarantine_path``: Directory for the quarantine area (`str`)
        - ``dry_run``: If True, doesn't move/delete the files (`bool`)
    SDSFile : `SDSFile`
        The file to be processed.
    """

    # Move the raw .D file
    d_file_path = os.path.join(sdsFile.archiveRoot,
                               sdsFile.custom_quality_subdir('D'),
                               sdsFile.custom_quality_filename('D'))
    dest_dir = os.path.join(options['quarantine_path'],
                            sdsFile.custom_quality_subdir('D'))
    if options['dry_run']:
        logger.info('Would move %s to %s.', d_file_path, dest_dir)
    else:
        os.makedirs(dest_dir, exist_ok=True)
        shutil.move(d_file_path, dest_dir)
        logger.info('Moved %s to %s.', d_file_path, dest_dir)

    # Delete the .Q file
    q_file_path = sdsFile.filepath
    if options['dry_run']:
        logger.info('Would remove %s.', q_file_path)
    else:
        os.remove(q_file_path)
        logger.info('Removed %s.', q_file_path)

    # TODO: Report


def testPrint(options, sdsFile):
    """Prints the filename."""
    logger.info(sdsFile.filename)
    logger.info(sdsFile.directory)
    logger.info(sdsFile.irodsDirectory)
