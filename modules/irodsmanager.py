"""
This module implements an iRODS session manager as a _fake singleton_.

Instead of calling the IRODSManager() constructor, use the
irods_session variable, that is already created and connected to iRODS
when the module is loaded.

Example
-------

```
from irodsmanager import irods_session
irods_session.create_collection(sds_file.irods_directory)
irods_session.create_data_object(sds_file)
```
"""

import os
import logging

# Lots of iRODS imports
from irods.session import iRODSSession
from irods.models import Collection, DataObject, DataObjectMeta
from irods.exception import DataObjectDoesNotExist, CollectionDoesNotExist, MultipleResultsFound
from irods.rule import Rule
import irods.keywords as kw

from configuration import config


class IRODSManager():
    """
    Class IRODSManager
    Manages session to iRODS
    """

    root = config["IRODS_ROOT"]

    def __init__(self):

        # Initialize logger
        self.logger = logging.getLogger("RuleManager")
        self.logger.debug("Initializing a new iRODS Session.")

        self.session = None
        self.connect()

    def connect(self):
        """
        def IRODSManager::connect
        Creates a iRODSSession to connect to iRODS
        """

        if self.session is not None:
            return

        # Open a session
        self.session = iRODSSession(
            host=config["IRODS"]["HOST"],
            port=config["IRODS"]["PORT"],
            user=config["IRODS"]["USER"],
            password=config["IRODS"]["PASS"],
            zone=config["IRODS"]["ZONE"]
        )

    def disconnect(self):
        """
        def IRODSManager::disconnect
        The iRODSSession class is a context manager and calls this function during __exit__
        See: https://github.com/irods/python-irodsclient/blob/master/irods/session.py
        """

        self.logger.debug("Disconnecting the iRODS Session.")

        if self.session is None:
            return

        self.session.cleanup()

    def get_collection(self, path):
        """Returns the collection named by `path`."""
        return self.session.collections.get(path)

    def create_collection(self, collection):
        """Creates a collection in the iRODS catalog. Does nothing if it is already there."""
        self.session.collections.create(collection)

    def get_data_objects(self, path):
        return self.get_collection(path).data_objects

    def execute_rule(self, rule_path, input_parameters):

        """
        Executes a rule from given file path with input parameters
        """

        rule = Rule(self.session, rule_path, params=input_parameters, output="ruleExecOut")

        output = rule.execute()

        # This is insane but the output of writeLine is here
        return output.MsParam_PI[0].inOutStruct.stdoutBuf.buf.decode("utf-8").strip("\x00")

    def assign_pid(self, sds_file):
        """Assigns a persistent identifier to a SDSFile.

        Returns
        -------
        is_new : `bool` or `None`
            `True` if a new PID was assigned to the file, `False` file had a PID already,
            and `None` in case of an error.
        pid : `str`
            The PID assigned to the object.
        """

        if self.get_pid(sds_file):
            self.logger.error("File %s already has a PID registered in iRODS" % sds_file.filename)
            return

        # Path to the rule
        RULE_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), "..",
                                 "irods", "rules", "pid.r")

        input_parameters = {
            "*path": "'%s'" % sds_file.irods_path,
            "*parent_pid": "'None'",
            "*ror": "'None'",
            "*fio": "'None'",
            "*fixed": "'false'"
        }

        response_str = self.execute_rule(RULE_PATH, input_parameters).strip()
        [status, pid] = response_str.split()

        is_new = None
        if status == "PID-new:":
            is_new = True
        elif status == "PID-existing:":
            is_new = False
        else:
            self.logger.error("Unknown response: %s" % response_str)

        return is_new, pid

    def eudat_replication(self, sds_file, replication_root):
        """Execute a replication using EUDAT rules.

        Parameters
        ----------
        sds_file : `SDSFile`
            The file to replicate.
        replication_root : `str`
            Root replication collection.

        Returns
        -------
        success : `bool`
            `True` if the replication was successful.
        response : `str`
            The message returned by the rule. Useful in case of failure.
        """

        # Path to the rule
        RULE_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), "..",
                                 "irods", "rules", "replicate.r")

        input_parameters = {
            "*source": "'%s'" % sds_file.irods_path,
            "*destination": "'%s'" % sds_file.custom_path(replication_root)
        }

        # Create the collection if it does not exist
        self.create_collection(sds_file.custom_directory(replication_root))

        response_str = self.execute_rule(RULE_PATH, input_parameters).strip()

        status = response_str.split()[0]
        response = response_str[len(status): + 1]

        success = False
        if status == "Success:":
            success = True

        # Trim cache
        data_object = self.get_data_object(sds_file)
        options = {kw.REPL_NUM_KW: str(data_object.replicas[-1].number)}
        data_object.unlink(**options)

        return success, response

    def create_data_object(self, sds_file,
                           resc_name="demoResc",
                           purge_cache=False,
                           register_checksum=False):
        """Insert an SDS data object into iRODS at a collection given by
        `sds_file.irods_directory`.

        Parameters
        ----------
        sds_file : `SDSFile`
            An SDS file descriptor.
        resc_name : `str`, optional
            Name of the resource to save the data object.
        purge_cache : `bool`, optional
            Whether or not to purge the cache, in case the resource is compound.
        register_checksum : `bool`, optional
            Whether or not to register the SHA256 checksum of the object in iCAT.
        """

        # Create the collection if it does not exist
        self.create_collection(sds_file.irods_directory)

        # Attempt to get the data object
        data_object = self.get_data_object(sds_file)
        if data_object is not None:

            # Checksum of file did not change vs. iRODS checksum
            if data_object.checksum == sds_file.checksum:
                self.logger.debug("File already registered, cancelling ingestion.")
                return

        # Some put options
        options = {
            kw.RESC_NAME_KW: resc_name,
            kw.PURGE_CACHE_KW: purge_cache,
            kw.REG_CHKSUM_KW: register_checksum
        }

        # Add the data object
        self.session.data_objects.put(sds_file.filepath, sds_file.irods_path, **options)

    def delete_data_object(self, sds_file, force=False):
        """Delete an SDS data object from iRODS at a collection given by
        `sds_file.irods_directory`.

        Parameters
        ----------
        sds_file : `SDSFile`
            An SDS file descriptor.
        force : `bool`
            Whether to force the deletion.
        """

        # Attempt to get the data object
        data_object = self.get_data_object(sds_file)
        if data_object is None:
            self.logger.debug("File not registered, cancelling deletion.")
            return

        # Unlink the data object
        data_object.unlink(force=force)

    def remote_put(self, sds_file, root_collection,
                   resc_name="demoResc",
                   purge_cache=False,
                   register_checksum=False):
        """Insert an SDS data object into iRODS at a collection rooted at `root_collection`.

        Parameters
        ----------
        sds_file : `SDSFile`
            An SDS file descriptor.
        root_collection : `str`
            The root collection to save the data object.
        resc_name : `str`, optional
            Name of the resource to save the data object.
        purge_cache : `bool`, optional
            Whether or not to purge the cache, in case the resource is compound.
        register_checksum : `bool`, optional
            Whether or not to register the SHA256 checksum of the object in iCAT.
        """

        # Create the collection if it does not exist
        self.create_collection(sds_file.custom_directory(root_collection))

        # Some put options
        options = {
            kw.RESC_NAME_KW: resc_name,
            kw.PURGE_CACHE_KW: purge_cache,
            kw.REG_CHKSUM_KW: register_checksum
        }

        # Add the data object
        self.session.data_objects.put(sds_file.filepath,
                                      sds_file.custom_path(root_collection),
                                      **options)

    def get_federated_data_object(self, sds_file, root_collection):
        """Retrieves a data object from a federated iRODS and returns None if it does not exist.

        The irods-pythonclient library does not support a get() for a
        federated data object, only for collections. See
        <https://github.com/irods/python-irodsclient/issues/163>.

        Parameters
        ----------
        sds_file : `SDSFile`
            File to search.
        root_collection : `str`
            The archive's root collection.
        """

        # Get collection
        try:
            fed_col = self.get_collection(sds_file.custom_directory(root_collection))
        except CollectionDoesNotExist:
            return None

        # Iterate over the collection objects looking for the right file
        for obj in fed_col.data_objects:
            if sds_file.filename == obj.name:
                return obj

        return None

    def federated_exists(self, sds_file, root_collection):
        """Check whether a data object is present in a federated iRODS zone with the same checksum.

        Parameters
        ----------
        sds_file : `SDSFile`
            File to search.
        root_collection : `str`
            The archive's root collection.

        Raises
        ------
        MultipleResultsFound
            Raised if more than one different versions of the file exist in remote location.
        """
        # Query iRODS
        q = (irods_session.session.query(Collection.name, DataObject.name, DataObject.checksum)
             .filter(Collection.name == sds_file.custom_directory(root_collection))
             .filter(DataObject.name == sds_file.filename))
        results = q.all()

        # No file found
        if len(results) == 0:
            self.logger.debug("File %s does not exist in root collection %s."
                              % (sds_file.filename, root_collection))
            return False

        # Read checksum(s) into a set to eliminate repeats
        checksum_set = {r[DataObject.checksum] for r in results}
        if len(checksum_set) > 1:
            raise MultipleResultsFound("File %s has more than one different version."
                                       % sds_file.custom_path(root_collection))
        remote_checksum = checksum_set.pop()

        # Compare checksums
        if sds_file.checksum == remote_checksum:
            self.logger.debug("File %s does exist in iRODS, with same checksum (%s)."
                              % (sds_file.filename, sds_file.checksum))
            return True

        self.logger.debug(
            "File %s does exist in iRODS, but with a different checksum (%s vs %s)."
            % (sds_file.filename, remote_checksum, sds_file.checksum))
        return False

    def get_federated_pid(self, sds_file, root_collection):
        """Get the PID of a data object in a federated iRODS.

        Parameters
        ----------
        sds_file : `SDSFile`
            File to search.
        root_collection : `str`
            The archive's root collection.

        Returns
        -------
        pid : `str`
            The PID is the file has one, or None if the file does not exist or does not have a PID.

        Raises
        ------
        MultipleResultsFound
            Raised if file has more than one different PID assigned to it.
        """
        # Query iRODS
        q = (irods_session.session
             .query(Collection.name, DataObject.name, DataObjectMeta.value)
             .filter(Collection.name == sds_file.custom_directory(root_collection))
             .filter(DataObject.name == sds_file.filename)
             .filter(DataObjectMeta.name == "PID"))
        results = q.all()

        # No file or PID found
        if len(results) == 0:
            self.logger.debug("File %s does not exist or does not have a PID registered."
                              % sds_file.filename)
            return None

        # Read PID(s) into a set to eliminate repeats
        pid_set = {r[DataObjectMeta.value] for r in results}
        if len(pid_set) > 1:
            raise MultipleResultsFound("File %s has more than one PID."
                                       % sds_file.custom_path(root_collection))

        # Return the PID
        pid = pid_set.pop()
        self.logger.debug("File %s has PID %s." % (sds_file.filename, pid))
        return pid

    def get_data_object(self, sds_file, root_collection=None):
        """
        Retrieves a data object from iRODS and returns None if it does not exist.

        Parameters
        ----------
        sds_file : `SDSFile`
            File to search.
        root_collection : `str`, optional
            The archive's root collection.
        """

        # Attempt to get the file from iRODS
        # If it does not exists an exception is raised and we return None
        try:
            if root_collection is None:
                return self.session.data_objects.get(sds_file.irods_path)
            return self.get_federated_data_object(sds_file, root_collection)
        except (DataObjectDoesNotExist, CollectionDoesNotExist):
            return None

    def exists(self, sds_file, root_collection=None):
        """Check whether the file, with the same checksum, is registered in iRODS.

        Parameters
        ----------
        sds_file : `SDSFile`
            The file we want to check.
        root_collection : `str`, optional
            The archive's root collection, to search in a non-default collection,
            e.g., a replication site.
        """

        # Attempt to get the data object
        data_object = self.get_data_object(sds_file, root_collection=root_collection)
        if data_object is None:
            self.logger.debug("File %s does not exist in iRODS." % sds_file.filename)
            return False
        else:
            # Compare checksum
            if data_object.checksum == sds_file.checksum:
                self.logger.debug("File %s does exist in iRODS, with same checksum (%s)." % (
                    sds_file.filename, sds_file.checksum))
                return True
            else:
                self.logger.debug(
                    "File %s does exist in iRODS, but with a different checksum (%s vs %s)."
                    % (sds_file.filename, data_object.checksum, sds_file.checksum))
                return False

    def get_pid(self, sds_file, root_collection=None):
        """Get the PID assigned to the file, or None if the file has no PID.

        Parameters
        ----------
        sds_file : `SDSFile`
            The file we want to check.
        root_collection : `str`, optional
            The archive's root collection, to search in a non-default collection,
            e.g., a replication site.
        """

        # Attempt to get the data object
        data_object = self.get_data_object(sds_file, root_collection=root_collection)
        if data_object is None:
            self.logger.error("File %s does not exist in iRODS." % sds_file.filename)
            return None

        pid_query = data_object.metadata.get_all("PID")
        if len(pid_query) == 0:
            return None
        elif len(pid_query) > 1:
            self.logger.error("File %s has more than one PID assigned to it." % sds_file.filename)
            return None

        return pid_query[0].value


irods_session = IRODSManager()
irods_session.connect()
