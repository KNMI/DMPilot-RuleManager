"""
Collection of conditions that either return True or False
depending on whether conditions are met
"""

import logging
from datetime import datetime, timedelta
import os

from sds.sdsfile import SDSFile

import modules.s3manager as s3manager
from modules.irodsmanager import irods_session
from modules.mongomanager import mongo_pool

# Initialize logger
logger = logging.getLogger("RuleManager")


def assert_quality_condition(options, sds_file):
    """Assert that the SDSFile quality is in a list given in the options.

    Parameters
    ----------
    options : `dict`
        The rule's options.
        - ``qualities``: List of qualities to accept (`list` of `str`)
    sds_file : `SDSFile`
        The file being processed.

    """
    return sds_file.quality in options["qualities"]


def assert_irods_exists_condition(options, sds_file):
    return irods_session.exists(sds_file)


def assert_s3_exists_condition(options, sds_file):
    """Assert that the file is archived in S3.

    Parameters
    ----------
    options : `dict`
        The rule's options.
        - ``check_checksum``: Whether or not to verify the file checksum (`bool`, default `True`)
    sds_file : `SDSFile`
        The file being processed.

    """
    if "check_checksum" in options:
        if options["check_checksum"]:
            return (s3manager.exists(sds_file)
                    and s3manager.get_checksum(sds_file) == sds_file.checksum)
        else:
            return s3manager.exists(sds_file)
    return s3manager.exists(sds_file)


def _assert_exists_and_hashes_in_document(sds_file, document, db_name="mongoDB",
                                          use_checksum_prev=False, use_checksum_next=False):
    """Assert that the document exists with the same checksum(s) as SDSFile.

    Parameters
    ----------
    sds_file : `SDSFile`
        The file being processed.
    document : `dict`
        The document corresponding to `sds_file`. Can be `None` in case it doesn't exist.
    db_name : `str`
        The database name to be used in logs (default `"mongoDB"`).
    use_checksum_prev : `bool`
        Whether or not to check the document's `checksum_prev` against the checksum
        of `sds_file.previous`. Ignored if `sds_file.previous.checksum` is `None`
        (default `False`).
    use_checksum_next : `bool`
        Whether or not to check the document's `checksum_next` against the checksum
        of `sds_file.next`. Ignored if `sds_file.next.checksum` is `None` (default `False`).

    Returns
    -------
    `bool`

    """
    if document is not None:
        # Document exists and has the same hash: it exists
        exists = True
        if document["checksum"] == sds_file.checksum:
            if (use_checksum_prev
                    and sds_file.previous.checksum is not None
                    and document["checksum_prev"] != sds_file.previous.checksum):
                same_hash = False
                msg = ("File %s does exist in %s, but the previous file has a "
                       "different checksum (%s vs %s).") % (
                       sds_file.filename, db_name, document["checksum_prev"],
                       sds_file.previous.checksum)
            elif (use_checksum_next
                  and sds_file.next.checksum is not None
                  and document["checksum_next"] != sds_file.next.checksum):
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


def assert_wfcatalog_exists_condition(options, sds_file):
    """Assert that the file metadata is present in the WFCatalog.

    Parameters
    ----------
    options : `dict`
        The rule's options.
        - ``check_checksum``: Whether or not to compare checksums (`bool`, default `True`)
    sds_file : `SDSFile`
        The file being processed.

    """
    # Extract the current metadata object from the database
    metadata_object = mongo_pool.get_wfcatalog_daily_document(sds_file)

    # In case we don't care about the hashes, we can exit here
    if ("check_checksum" in options
            and options["check_checksum"] is False
            and metadata_object is not None):
        logger.debug("File %s exists in WFCatalog, checksum not verified."
                     % (sds_file.filename))
        return True

    # Document exists and has the same hash: it exists
    return _assert_exists_and_hashes_in_document(sds_file, metadata_object,
                                                 db_name="WFCatalog",
                                                 use_checksum_prev=True)


def _get_neighbor(sds_file, neighbor):
    """Get the "current", "next", or "previous" SDSFile in relation to a given SDSFile.

    Returns
    -------
    `SDSFile` or `None`
        The requested `SDSFile` or `None` if the file does not exist.

    """
    queried_file = None
    if neighbor == "current":
        queried_file = sds_file
    elif neighbor == "next":
        queried_file = sds_file.next
    elif neighbor == "previous":
        queried_file = sds_file.previous

    if os.path.isfile(queried_file.filepath):
        return queried_file
    else:
        return None


def assert_modification_time_newer_than(options, sds_file):
    """Assert that the file was last modified less than `options["days"]` days ago.

    In case the file does not exist, returns False.

    Parameters
    ----------
    options : `dict`
        The rule's options.
        - ``days``: The number of days used to test the file against (`int`)
        - ``apply_to``: Which file to apply the rule ---
                        "previous" | "current" (default) | "next"
    sds_file : `SDSFile`
        The file being processed.

    """
    file_to_apply = _get_neighbor(sds_file, options.get("apply_to", "current"))
    if file_to_apply is None:
        return False
    return file_to_apply.modified > (datetime.now() - timedelta(days=options["days"]))


def assert_modification_time_older_than(options, sds_file):
    """Assert that the file was last modified more than `options["days"]` days ago.

    In case the file does not exist, returns True.

    Parameters
    ----------
    options : `dict`
        The rule's options.
        - ``days``: The number of days used to test the file against (`int`)
        - ``apply_to``: Which file to apply the rule ---
                        "previous" | "current" (default) | "next"
    sds_file : `SDSFile`
        The file being processed.

    """
    file_to_apply = _get_neighbor(sds_file, options.get("apply_to", "current"))
    if file_to_apply is None:
        return True
    return file_to_apply.modified < (datetime.now() - timedelta(days=options["days"]))


def assert_data_time_newer_than(options, sds_file):
    """Assert that the date the file data corresponds to (in the filename) is less than
    `options["days"]` days ago.

    In case the file does not exist, returns False.

    Parameters
    ----------
    options : `dict`
        The rule's options.
        - ``days``: The number of days used to test the file against (`int`)
        - ``apply_to``: Which file to apply the rule ---
                        "previous" | "current" (default) | "next"
    sds_file : `SDSFile`
        The file being processed.

    """
    file_to_apply = _get_neighbor(sds_file, options.get("apply_to", "current"))
    if file_to_apply is None:
        return False
    return file_to_apply.start > (datetime.now() - timedelta(days=options["days"]))


def assert_data_time_older_than(options, sds_file):
    """Assert that the date the file data corresponds to (in the filename) is more than
    `options["days"]` days ago.

    In case the file does not exist, returns True.

    Parameters
    ----------
    options : `dict`
        The rule's options.
        - ``days``: The number of days used to test the file against (`int`)
        - ``apply_to``: Which file to apply the rule ---
                        "previous" | "current" (default) | "next"
    sds_file : `SDSFile`
        The file being processed.

    """
    file_to_apply = _get_neighbor(sds_file, options.get("apply_to", "current"))
    if file_to_apply is None:
        return True
    return file_to_apply.start < (datetime.now() - timedelta(days=options["days"]))


def assert_dc_metadata_exists_condition(options, sds_file):

    # Get the existing Dublin Core Object
    dublin_core_object = mongo_pool.get_dc_document(sds_file)

    # Document exists and has the same hash: it exists
    return _assert_exists_and_hashes_in_document(sds_file, dublin_core_object,
                                                 db_name="DublinCoreMetadata")


def assert_ppsd_metadata_exists_condition(options, sds_file):
    """Assert that the PPSD metadata related to the file is present in the database.

    Parameters
    ----------
    options : `dict`
        The rule's options.
        - ``check_checksum``: Whether or not to compare checksums (`bool`, default `True`)
    sds_file : `SDSFile`
        The file being processed.

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
    ppsd_documents = mongo_pool.get_ppsd_documents(sds_file)

    # Document exists and has the same hash: it exists
    if ppsd_documents:
            # In case we don't care about the hashes, we can exit here
        if "check_checksum" in options and options["check_checksum"] is False:
            logger.debug("PPSD data exists for file %s, checksum not verified."
                         % (sds_file.filename))
            return True

        # Get all checksums
        checksum = {doc["checksum"] for doc in ppsd_documents}
        checksum_prev = {doc["checksum_prev"] for doc in ppsd_documents
                         if "checksum_prev" in doc}
        checksum_next = {doc["checksum_next"] for doc in ppsd_documents
                         if "checksum_next" in doc}

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


def assert_pruned_file_exists_condition(options, sds_file):
    """Assert that the pruned version of the SDS file is in the temporary archive."""
    # Create a phantom SDSFile with a different quality idenfier
    quality_file = SDSFile(sds_file.filename, sds_file.archive_root)
    quality_file.quality = "Q"
    return os.path.exists(quality_file.filepath)


def assert_temp_archive_exist_condition(options, sds_file):
    """Assert that the file exists in the temporary archive."""
    return os.path.isfile(sds_file.filepath)


def assert_file_replicated_condition(options, sds_file):
    """Assert that the file has been replicated.

    Checks replication by verifying that the file is present in iRODS,
    in the given remote collection. Does not check checksum.

    Parameters
    ----------
    options : `dict`
        The rule's options.
        - ``replication_root``: Root replication collection (`str`)
    sds_file : `SDSFile`
        The file being processed.
    """
    return irods_session.federated_exists(sds_file, options["replication_root"])


def assert_pid_condition(options, sds_file):
    """Assert that a PID was assigned to the file on iRODS."""
    return irods_session.get_pid(sds_file) is not None


def assert_replica_pid_condition(options, sds_file):
    """Assert that a PID was assigned to the file on the replication
    iRODS. Also returns False if the file is not present at the replica
    site."""
    return irods_session.get_federated_pid(sds_file, options["replication_root"]) is not None
