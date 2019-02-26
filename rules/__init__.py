from datetime import datetime, timedelta
import logging

from irodsmanager import irodsSession
from mongomanager import mongoSession
from wfcatalog import collector
from dublincore import dublinCore
from psdcollector import psdCollector

logger = logging.getLogger(__name__)


class RuleFunctions():

    """
    Class RuleFunctions
    Container for configured rule functions
    """

    def __init__(self):
        pass

    def purge(self, options, SDSFile):
        """
        Function RuleFunctions::purge
        Handler for the temporary archive purge rule.

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

    def psdMetadata(self, options, SDSFile):
        """
        Function RuleFunctions::wfcatalog
        Handler for the WFCatalog metadata rule
        TODO XXX

        Parameters
        ----------
        options : `dict`
            The rule's options.
        SDSFile : `SDSFile`
            The file to be processed.
        """

        print(psdCollector.process(SDSFile))

    def waveformMetadata(self, options, SDSFile):
        """
        Function RuleFunctions::wfcatalog
        Handler for the WFCatalog metadata rule
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

    def ingestion(self, options, SDSFile):
        """
        Function RuleFunctions::ingestion
        Handler for the ingestion rule

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

        # Check the modification time of the file
        if SDSFile.modified < (datetime.now() - timedelta(days=100)):
            return

        # The file was already ingested by iRODS
        if self.isIngested(SDSFile):
            return

        # A prune is requested
        if options["prune"]:
            logger.info("Prune is requested.")

        # Attempt to ingest to iRODS
        irodsSession.createDataObject(SDSFile, rescName="compResc", registerChecksum=True)

        # Check if checksum is saved
        logger.info(irodsSession.getDataObject(SDSFile).checksum)

    def isIngested(self, SDSFile):
        """
        Stateless check to see if the file exists in iRODS?
        TODO XXX
        """

        return False

    def dublinCore(self, options, sdsFile):
        """Process and save Dublin Core metadata of an SDS file.

        Parameters
        ----------
        options : `dict`
            The rule's options.
        SDSFile : `SDSFile`
            The file to be processed.
        """

        if dublinCore.getDCMetadata(sdsFile) is not None:
            logger.info("DC metadata already exists for " + sdsFile.filename)
            return

        document = DublinCore.extractDCMetadata(sdsFile)

        # Save to the database
        if document:
            mongoSession.saveDCDocument(document)
            logger.info("Saved DC metadata for " + sdsFile.filename)
