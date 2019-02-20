from datetime import datetime, timedelta
from irodsmanager import irodsSession


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
        irodsSession.createDataObject(SDSFile)

        # Check if checksum is saved
        print(irodsSession.getDataObject(SDSFile).checksum)

    def isIngested(self, SDSFile):
        """
        Stateless check to see if the file exists in iRODS?
        TODO XXX
        """

        return False
