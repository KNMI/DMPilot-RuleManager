from datetime import datetime, timedelta
import logging

from irodsmanager import irodsSession
from mongomanager import mongoSession
from wfcatalog import collector
from dublincore import DublinCore

logger = logging.getLogger(__name__)


class RuleFunctions():

    """
    Class RuleFunctions
    Container for configured rule functions
    """

    def __init__(self):
        pass

    def purge(self, options, SDSFile):

        # We can check time modified etc etc..
        if SDSFile.created > (datetime.now() - timedelta(days=7)):
            return

        # Some other configurable rules
        print(irodsSession.purgeTemporaryFile(SDSFile))

    def waveformMetadata(self, options, SDSFile):
        """
        Function RuleFunctions::wfcatalog
        Handler for the WFCatalog metadata rule
        TODO XXX
        """
        if mongoSession.getMetadataDocument(SDSFile) is not None:
            return

        #print(collector.getMetadata(SDSFile))

    def ingestion(self, options, SDSFile):
        """
        Function RuleFunctions::ingestion
        Handler for the ingestion rule
        """

        # Check the modification time of the file
        if SDSFile.modified < (datetime.now() - timedelta(days=100)):
            return

        # The file was already ingested by iRODS
        if self.isIngested(SDSFile):
            return

        # A prune is requested
        if options["prune"]:
            print("Prune is requested.")

        # Attempt to ingest to iRODS
        irodsSession.createDataObject(SDSFile, rescName="compResc", registerChecksum=True)

        # Check if checksum is saved
        print(irodsSession.getDataObject(SDSFile).checksum)

    def isIngested(self, SDSFile):
        """
        Stateless check to see if the file exists in iRODS?
        TODO XXX
        """

        return False

    def dublinCore(self, options, sdsFile):
        """Process and save Dublin Core metadata of an SDS file."""

        if DublinCore.getDCMetadata(sdsFile) is not None:
            logger.info("DC metadata already exists for " + sdsFile.filename)
            return

        document = DublinCore.extractDCMetadata(sdsFile)

        # Save to the database
        if document:
            mongoSession.saveDCDocument(document)
            logger.info("Saved DC metadata for " + sdsFile.filename)
