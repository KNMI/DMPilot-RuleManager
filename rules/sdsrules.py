"""
Module that houses all the rule handlers for SDS daily files.

Every rule should be implemented as a module function with exactly two arguments:
1) a `dict` that holds the options for the rule, and
2) the item that is subject to the rule, in this case, a `SDSFile` object.
"""

import logging
import os
import shutil

from botocore.exceptions import CredentialRetrievalError
from boto3.exceptions import S3UploadFailedError
from core.exceptions import ExitPipelineException

from modules.wfcatalog import get_wf_metadata
from modules.dublincore import extract_dc_metadata

import modules.s3manager as s3manager
from modules.irodsmanager import irods_session
from modules.mongomanager import mongo_pool
from modules.psd2.psd import PSDCollector

logger = logging.getLogger("RuleManager")


def ppsd_metadata_rule(options, sds_file):
    """Handler for PPSD calculation.

    Parameters
    ----------
    options : `dict`
        The rule's options.
    sds_file : `SDSFile`
        The file to be processed.
    """

    logger.debug("Computing PPSD metadata for %s." % sds_file.filename)

    # Process PPSD
    documents = PSDCollector(connect_sql=False).process(sds_file, cache_response=False)

    # Save to the database
    mongo_pool.delete_ppsd_documents(sds_file)
    mongo_pool.save_ppsd_documents(documents)
    logger.debug("Saved PPSD metadata for %s." % sds_file.filename)


def delete_ppsd_metadata_rule(options, sds_file):
    """Delete PPSD metadata of an SDS file.

    Parameters
    ----------
    options : `dict`
        The rule's options.
    sds_file : `SDSFile`
        The file to be processed.
    """

    logger.debug("Deleting PPSD metadata for %s." % sds_file.filename)
    mongo_pool.delete_ppsd_documents(sds_file)
    logger.debug("Deleted PPSD metadata for %s." % sds_file.filename)


def prune_rule(options, sds_file):
    """Handler for the file pruning/repacking rule.

    Due to the way `SDSFile.prune()` runs `dataselect` as its first step, always sorts
    the records, independently of other options configured. On the other hand, `msrepack`
    only runs when `repack` is set to True.

    Parameters
    ----------
    options : `dict`
        The rule's options.
        - ``cut_boundaries``: Whether or not to cut the file at the day boundaries (`bool`)
        - ``repack``: Whether or not to repack records (`bool`)
        - ``repack_record_size``: The new record size if `repack` is `True` (`int`)
        - ``remove_overlap``: Whether or not to remove overlaps (`bool`)
    sds_file : `SDSFile`
        The file to be processed.
    """

    logger.debug("Pruning file %s." % sds_file.filename)

    # Prune the file to a .Q quality file in the temporary archive
    sds_file.prune(cut_boundaries=options["cut_boundaries"],
                   repack=options["repack"],
                   record_length=options["repack_record_size"],
                   remove_overlap=options["remove_overlap"])

    logger.debug("Pruned file %s." % sds_file.filename)


def ingestion_irods_rule(options, sds_file):
    """Handler for the ingestion rule.

    Parameters
    ----------
    options : `dict`
        The rule's options.
        - ``resc_name``: Name of the iRODS resource to save the object (`str`)
        - ``purge_cache``: Whether or not to purge the cache,
                           in case the resource is compound (`bool`)
    sds_file : `SDSFile`
        The file to be processed.
    """

    logger.debug("Ingesting file %s." % sds_file.filename)

    # Attempt to ingest to iRODS
    irods_session.create_data_object(sds_file,
                                     resc_name="compResc",
                                     purge_cache=True,
                                     register_checksum=True)

    # Check if checksum is saved
    logger.debug("Ingested file %s with checksum '%s'" % (
        sds_file.filename, irods_session.get_data_object(sds_file).checksum))


def ingestion_s3_rule(options, sds_file):
    """Handler for the ingestion rule.

    Parameters
    ----------
    options : `dict`
        The rule's options.
        - ``exit_on_failure``: Whether or not to exit the pipeline when the upload fails (`bool`)
    sds_file : `SDSFile`
        The file to be processed.

    Raises
    ------
    `ExitPipelineException`
        Raised when upload fails and `exitOnFailure` is `True`.
    """
    logger.debug("Ingesting file %s." % sds_file.filename)

    try:
        # Upload file to S3
        s3manager.put(sds_file)
    except (CredentialRetrievalError, S3UploadFailedError) as e:
        if options["exit_on_failure"]:
            raise ExitPipelineException(True, str(e))
        else:
            raise

    # Check if checksum is saved
    logger.debug("Ingested file %s with checksum '%s'" % (
        sds_file.filename, sds_file.checksum))


def delete_s3_rule(options, sds_file):
    """Handler for the rule that deletes a file from the S3 archive.

    Parameters
    ----------
    options : `dict`
        The rule's options.
        - ``dry_run``: If True, doesn't delete the file (`bool`, default `False`)
    sds_file : `SDSFile`
        The description of the file to be deleted.

    """
    if "dry_run" in options and options["dry_run"]:
        logger.info("Would delete file %s from S3." % sds_file.filename)
    else:
        logger.debug("Deleting file %s from S3." % sds_file.filename)

        # Attempt to delete from S3
        s3manager.delete(sds_file)

        logger.debug("Deleted file %s from S3." % sds_file.filename)


def pid_rule(options, sds_file):
    """Handler for the PID assignment rule.

    Parameters
    ----------
    options : `dict`
        The rule's options.
    sds_file : `SDSFile`
        The file to be processed.
    """

    logger.debug("Assigning PID to file %s." % sds_file.filename)

    # Attempt to assign PID
    is_new, pid = irods_session.assign_pid(sds_file)

    if is_new is None:
        logger.error("Error while assigning PID to file %s." % sds_file.filename)
    elif is_new:
        logger.info("Assigned PID %s to file %s." % (pid, sds_file.filename))
    elif not is_new:
        logger.info("File %s was already previously assigned PID %s." % (sds_file.filename, pid))


def add_pid_to_wfcatalog_rule(options, sds_file):
    """Updates the WFCatalog with the file PID from the local iRODS archive.

    Parameters
    ----------
    options : `dict`
        The rule's options.
    sds_file : `SDSFile`
        The file to be processed.
    """

    logger.debug("Updating WFCatalog with the PID of file %s." % sds_file.filename)

    pid = irods_session.get_pid(sds_file)

    if pid is not None:
        mongo_pool.update_many({"fileId": sds_file.filename},
                               {"$set": {"dc_identifier": pid}})
        logger.info("Entry for file %s updated with PID %s." % (sds_file.filename, pid))
    else:
        logger.error("File %s has no PID." % sds_file.filename)


def replication_rule(options, sds_file):
    """Handler for the PID assignment rule.

    Parameters
    ----------
    options : `dict`
        The rule's options.
        - ``replication_root``: Root replication collection (`str`)
    sds_file : `SDSFile`
        The file to be processed.
    """

    logger.debug("Replicating file %s." % sds_file.filename)

    # Attempt to replicate file
    success, response = irods_session.eudat_replication(sds_file, options["replication_root"])

    if success:
        logger.debug("Replicated file %s to collection %s." % (sds_file.filename,
                                                               options["replication_root"]))
    else:
        logger.error("Error replicating file %s: %s" % (sds_file.filename, response))


def delete_archive_rule(options, sds_file):
    """Handler for the rule that deletes a file from the iRODS archive.

    Parameters
    ----------
    options : `dict`
        The rule's options.
    sds_file : `SDSFile`
        The description of the file to be deleted.
    """

    logger.debug("Deleting file %s." % sds_file.filename)

    # Attempt to delete from iRODS
    irods_session.delete_data_object(sds_file)

    # Check if checksum is saved
    logger.debug("Deleted file %s." % sds_file.filename)


def federated_ingestion_rule(options, sds_file):
    """Handler for a federated ingestion rule. Puts the object in a given
    root collection, potentially in a federated zone.

    Parameters
    ----------
    options : `dict`
        The rule's options.
        - ``remote_root``: Name of the root collection to put the object (`str`)
        - ``qualities``: Quality codes of the files to be processed (`list` of `str`)
    sds_file : `SDSFile`
        The file to be processed.
    """

    logger.debug("Ingesting file %s." % sds_file.custom_path(options["remote_root"]))

    # Attempt to ingest to iRODS
    irods_session.remote_put(sds_file,
                             options["remote_root"],
                             purge_cache=True,
                             register_checksum=True)

    logger.debug("Ingested file %s" % sds_file.custom_path(options["remote_root"]))


def purge_rule(options, sds_file):
    """Handler for the temporary archive purge rule.

    Parameters
    ----------
    options : `dict`
        The rule's options.
    sds_file : `SDSFile`
        The file to be processed.
    """

    # Some other configurable rules
    logger.debug("Purging file %s from temporary archive." % sds_file.filename)
    try:
        os.remove(sds_file.filepath)
        logger.debug("Purged file %s from temporary archive." % sds_file.filename)
    except FileNotFoundError:
        logger.debug("File %s not present in temporary archive." % sds_file.filename)


def dc_metadata_rule(options, sds_file):
    """Process and save Dublin Core metadata of an SDS file.

    Parameters
    ----------
    options : `dict`
        The rule's options.
        - ``qualities``: Quality codes of the files to be processed (`list` of `str`)
    sds_file : `SDSFile`
        The file to be processed.
    """

    logger.debug("Saving Dublin Core metadata for %s." % sds_file.filename)

    # Get the existing Dublin Core Object
    document = extract_dc_metadata(sds_file, irods_session.get_pid(sds_file).upper())

    # Save to the database
    if document:
        mongo_pool.save_dc_document(document)
        logger.debug("Saved Dublin Core metadata for %s." % sds_file.filename)


def delete_dc_metadata_rule(options, sds_file):
    """Delete Dublin Core metadata of an SDS file.

    Parameters
    ----------
    options : `dict`
        The rule's options.
        - ``dry_run``: If True, doesn't delete the data (`bool`, default `False`)
    sds_file : `SDSFile`
        The file to be processed.
    """

    if "dry_run" in options and options["dry_run"]:
        logger.info("Would delete all Dublin Core metadata for %s." % sds_file.filename)
    else:
        logger.debug("Marking %s as deleted in Dublin Core metadata." % sds_file.filename)
        mongo_pool.delete_dc_document(sds_file)
        logger.debug("Marked %s as deleted in Dublin Core metadata." % sds_file.filename)


def waveform_metadata_rule(options, sds_file):
    """Handler for the WFCatalog metadata rule.
    TODO XXX

    Parameters
    ----------
    options : `dict`
        The rule's options.
    sds_file : `SDSFile`
        The file to be processed.
    """

    # Get waveform metadata
    (doc_daily, docs_segments) = get_wf_metadata(sds_file)

    logger.debug("Saving waveform metadata for %s." % sds_file.filename)

    # Save the daily metadata document
    mongo_pool.set_wfcatalog_daily_document(doc_daily)

    # Save the continuous segments documents
    if docs_segments is None:
        return logger.debug("No continuous segments to save for %s." % sds_file.filename)
    else:
        mongo_pool.delete_wfcatalog_segments_documents(sds_file)
        mongo_pool.save_wfcatalog_segments_documents(docs_segments)

    logger.debug("Saved waveform metadata for %s." % sds_file.filename)

def delete_waveform_metadata_rule(options, sds_file):
    """Delete waveform metadata of an SDS file.

    Parameters
    ----------
    options : `dict`
        The rule's options.
        - ``dry_run``: If True, doesn't delete the data (`bool`, default `False`)
    sds_file : `SDSFile`
        The file to be processed.
    """

    if "dry_run" in options and options["dry_run"]:
        logger.info("Would delete all waveform metadata for %s." % sds_file.filename)
    else:
        logger.debug("Deleting waveform metadata for %s." % sds_file.filename)
        mongo_pool.delete_wfcatalog_daily_document(sds_file)
        mongo_pool.delete_wfcatalog_segments_documents(sds_file)
        logger.debug("Deleted waveform metadata for %s." % sds_file.filename)


def remove_from_deletion_database_rule(options, sds_file):
    """Removes the file from the deletion database.

    To be used after a successful deletion of the file from all desired archives.
    """

    logger.debug("Removing deletion entry for %s." % sds_file.filename)

    from core.database import deletion_database
    deletion_database.remove(sds_file)

    logger.debug("Removed deletion entry for %s." % sds_file.filename)


def print_with_message(options, sds_file):
    """Prints the filename followed by a message.

    Parameters
    ----------
    options : `dict`
        The rule's options.
        - ``message``: Message to print (`str`)
    sds_file : `SDSFile`
        The file to be processed.
    """
    print(sds_file.filename, options["message"])


def quarantine_raw_file_rule(options, sds_file):
    """Moves the file to another directory where it can be further analyzed by a human.

    This does not check any related file to quarantine along.

    Parameters
    ----------
    options : `dict`
        The rule's options.
        - ``quarantine_path``: Directory for the quarantine area (`str`)
        - ``dry_run``: If True, doesn't move the files (`bool`, default `False`)
    sds_file : `SDSFile`
        The file to be processed.

    Raises
    ------
    `ExitPipelineException`
        Raised after file is quarantined (every time rule is executed).

    """
    try:
        # Move the raw file
        source_path = sds_file.filepath
        dest_dir = sds_file.custom_directory(options["quarantine_path"])
        if "dry_run" in options and options["dry_run"]:
            logger.info("Would move %s to %s/", source_path, dest_dir)
        else:
            os.makedirs(dest_dir, exist_ok=True)
            shutil.move(source_path, dest_dir)
            logger.info("Moved %s to %s/", source_path, dest_dir)

        # TODO: Report

    except (KeyError, shutil.Error, PermissionError) as ex:
        raise ExitPipelineException(True, str(ex))

    raise ExitPipelineException(False, "File quarantined")


def quarantine_pruned_file_rule(options, sds_file):
    """Moves the file to another directory where it can be further analyzed by a human.

    It should be called with the .Q file, but acts on both of them. It moves the raw
    .D data file, and deletes the .Q file. Fails if there is already a .D file
    with same name already quarantined.

    Parameters
    ----------
    options : `dict`
        The rule's options.
        - ``quarantine_path``: Directory for the quarantine area (`str`)
        - ``dry_run``: If True, doesn't move/delete the files (`bool`, default `False`)
    sds_file : `SDSFile`
        The file to be processed.

    Raises
    ------
    `ExitPipelineException`
        Raised after file is quarantined (every time rule is executed).

    """
    try:
        # Move the raw .D file
        d_file_path = os.path.join(sds_file.archive_root,
                                   sds_file.custom_quality_subdir("D"),
                                   sds_file.custom_quality_filename("D"))
        dest_dir = os.path.join(options["quarantine_path"],
                                sds_file.custom_quality_subdir("D"))
        if "dry_run" in options and options["dry_run"]:
            logger.info("Would move %s to %s/", d_file_path, dest_dir)
        else:
            os.makedirs(dest_dir, exist_ok=True)
            shutil.move(d_file_path, dest_dir)
            logger.info("Moved %s to %s/", d_file_path, dest_dir)

        # Delete the .Q file
        q_file_path = sds_file.filepath
        if "dry_run" in options and options["dry_run"]:
            logger.info("Would remove %s", q_file_path)
        else:
            os.remove(q_file_path)
            logger.info("Removed %s", q_file_path)

        # TODO: Report

    except (KeyError, shutil.Error, PermissionError) as ex:
        raise ExitPipelineException(True, str(ex))

    raise ExitPipelineException(False, "File quarantined")


def test_print(options, sds_file):
    """Prints the filename."""
    logger.info(sds_file.filename)
