"""
Collection of conditions that either return True or False
depending on whether conditions are met
"""

import logging
from datetime import datetime, timedelta
import os

from orfeus.sdsfile import SDSFile

import modules.s3manager as s3manager
from modules.irodsmanager import irodsSession
from modules.mongomanager import mongo_pool

# Initialize logger
logger = logging.getLogger('RuleManager')


def assertQualityCondition(options, sds_file):
    """Asserts that the SDSFile quality is in options."""
    return sds_file.quality in options["qualities"]


def assertIRODSExistsCondition(options, sds_file):
    return irodsSession.exists(sds_file)


def assertS3ExistsCondition(options, sds_file):
    if "check_checksum" in options:
        if options["check_checksum"]:
            return (s3manager.exists(sds_file)
                    and s3manager.get_checksum(sds_file) == sds_file.checksum)
        else:
            return s3manager.exists(sds_file)
    return s3manager.exists(sds_file)


def assertIRODSNotExistCondition(options, sds_file):
    """Asserts that the SDSFile is not in iRODS."""
    # The file was not already ingested by iRODS
    return not irodsSession.exists(sds_file)


def _assertExistsAndHashesinDocument(sds_file, document, db_name="mongoDB",
                            use_checksum_prev=False, use_checksum_next=False):
    """Asserts that the document exists with the same checksum(s) as SDSFile"""

    # Document exists and has the same hash: it exists
    if document is not None:
        exists = True
        if document["checksum"] == sds_file.checksum:
            if use_checksum_prev and \
                    document["checksum_prev"] != sds_file.previous.checksum:
                same_hash = False
                msg = ("File %s does exist in %s, but the previous file has a "
                       "different checksum (%s vs %s).") % (
                       sds_file.filename, db_name, document["checksum_prev"],
                       sds_file.previous.checksum)
            elif use_checksum_next and \
                    document["checksum_next"] != sds_file.next.checksum:
                same_hash = False
                msg = ("File %s does exist in %s, but the next file has a "
                       "different checksum (%s vs %s).") % (
                       sds_file.filename, db_name, document["checksum_next"],
                       sds_file.next.checksum)
            else:
                same_hash = True
                checksums_str = "self: %s" % (sds_file.checksum)
                if use_checksum_prev:
                    checksums_str += ", prev: %s" % (sds_file.previous.checksum)
                if use_checksum_next:
                    checksums_str += ", next: %s" % (sds_file.next.checksum)
                msg = "File %s does exist in %s, with same checksums (%s)." % (
                                sds_file.filename, db_name, checksums_str)
        else:
            same_hash = False
            msg = ("File %s does exist in %s, but with a different checksum "
                   "(%s vs %s).") % (sds_file.filename, db_name,
                                     document["checksum"], sds_file.checksum)
    else:
        exists = False
        msg = "File %s does not exist in %s." % (sds_file.filename, db_name)

    logger.debug(msg)
    return exists and same_hash

def assertWFCatalogExistsCondition(options, sds_file):

    # Extract the current metadata object from the database
    metadataObject = mongo_pool.getWFCatalogDailyDocument(sds_file)

    # Document exists and has the same hash: it exists
    return _assertExistsAndHashesinDocument(sds_file, metadataObject,
                                            db_name="WFCatalog",
                                            use_checksum_prev=True)


def assertWFCatalogNotExistsCondition(options, sds_file):

    return not assertWFCatalogExistsCondition(options, sds_file)


def assertModificationTimeYoungerThan(options, sds_file):
    """Assert that the file was last modified less than `options['days']` days ago.

    Parameters
    ----------
    options : `dict`
        The rule's options.
        - ``days``: The number of days used to test the file against (`int`)
    sds_file : `SDSFile`
        The file being processed.

    """
    return sds_file.modified > (datetime.now() - timedelta(days=options["days"]))


def assertModificationTimeOlderThan(options, sds_file):
    """Assert that the file was last modified more than `options['days']` days ago.

    Parameters
    ----------
    options : `dict`
        The rule's options.
        - ``days``: The number of days used to test the file against (`int`)
    sds_file : `SDSFile`
        The file being processed.

    """
    return sds_file.modified < (datetime.now() - timedelta(days=options["days"]))


def assertDataTimeYoungerThan(options, sds_file):
    """Assert that the date the file data corresponds to (in the filename) is less than `options['days']` days ago.

    Parameters
    ----------
    options : `dict`
        The rule's options.
        - ``days``: The number of days used to test the file against (`int`)
    sds_file : `SDSFile`
        The file being processed.

    """
    return sds_file.start > (datetime.now() - timedelta(days=options["days"]))


def assertDataTimeOlderThan(options, sds_file):
    """Assert that the date the file data corresponds to (in the filename) is more than `options['days']` days ago.

    Parameters
    ----------
    options : `dict`
        The rule's options.
        - ``days``: The number of days used to test the file against (`int`)
    sds_file : `SDSFile`
        The file being processed.

    """
    return sds_file.start < (datetime.now() - timedelta(days=options["days"]))


def assertDCMetadataExistsCondition(options, sds_file):

    # Get the existing Dublin Core Object
    dublinCoreObject = mongo_pool.getDCDocument(sds_file)

    # Document exists and has the same hash: it exists
    return _assertExistsAndHashesinDocument(sds_file, dublinCoreObject,
                                            db_name="DublinCoreMetadata")

def assertDCMetadataNotExistsCondition(options, sds_file):

    return not assertDCMetadataExistsCondition(options, sds_file)


def assertPPSDMetadataExistsCondition(options, sds_file):
    """Assert that the PPSD metadata related to the file is present in the database.

    Returns
    -------
    `bool`
        `True` if there are PPSD documents in the database, all with
        the same checksum of `sds_file`, and if there are records
        containing the checksum of the previous (next) day file, these
        match the ones of `sds_file.previous`
        (`sds_file.next`). `False` otherwise.

    """

    # Get the existing PPSD documents for this file
    ppsd_documents = mongo_pool.getPPSDDocuments(sds_file)

    # Document exists and has the same hash: it exists
    if ppsd_documents:
        # Get all checksums
        checksum = {doc['checksum'] for doc in ppsd_documents}
        checksum_prev = {doc['checksum_prev'] for doc in ppsd_documents
                         if 'checksum_prev' in doc}
        checksum_next = {doc['checksum_next'] for doc in ppsd_documents
                         if 'checksum_next' in doc}

        # Verify they are all unique
        if len(checksum) != 1 or len(checksum_prev) != 1 or len(checksum_next) != 1:
            logger.debug("PPSD data exists for file %s, but with inconsistent checksums." % (
                            sds_file.filename))
            return False

        # There is only one of each checksum, we can access them directly
        checksum = checksum.pop()
        checksum_prev = checksum_prev.pop()
        checksum_next = checksum_next.pop()

        # Get checksums of the neighboring files if they exist
        sds_checksum_prev = sds_file.previous
        if sds_checksum_prev is not None:
            sds_checksum_prev = sds_checksum_prev.checksum
        sds_checksum_next = sds_file.next
        if sds_checksum_next is not None:
            sds_checksum_next = sds_checksum_next.checksum

        # Detect if any one of the checksums is different
        if checksum != sds_file.checksum:
            logger.debug("PPSD data exists for file %s, but with a different checksum (%s vs %s)." % (
                            sds_file.filename, checksum, sds_file.checksum))
            return False
        if checksum_prev != sds_checksum_prev:
            logger.debug("PPSD data exists for file %s, but with a different checksum_prev (%s vs %s)." % (
                            sds_file.filename, checksum_prev, sds_checksum_prev))
            return False
        if checksum_next != sds_checksum_next:
            logger.debug("PPSD data exists for file %s, but with a different checksum_next (%s vs %s)." % (
                            sds_file.filename, checksum_next, sds_checksum_next))
            return False

        # Finally, report success
        logger.debug("PPSD data exists for file %s, with the same checksums." % (
            sds_file.filename))
        return True

    # No document exists in DB for the file
    else:
        logger.debug("PPSD data does not exist for file %s." % sds_file.filename)
        return False


def assertPPSDMetadataNotExistsCondition(options, sds_file):

    return not assertPPSDMetadataExistsCondition(options, sds_file)


def assertPrunedFileExistsCondition(options, sds_file):
    """Assert that the pruned version of the SDS file is in the temporary archive."""
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
    return irodsSession.federatedExists(sds_file, options["replicationRoot"])


def assertPIDCondition(options, sds_file):
    """Assert that a PID was assigned to the file on iRODS."""
    return irodsSession.getPID(sds_file) is not None


def assertReplicaPIDCondition(options, sds_file):
    """Assert that a PID was assigned to the file on the replication
    iRODS. Also returns False if the file is not present at the replica
    site."""
    return irodsSession.getFederatedPID(sds_file, options["replicationRoot"]) is not None
