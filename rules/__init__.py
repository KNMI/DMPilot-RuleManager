import logging

logger = logging.getLogger(__name__)

class RuleFunctions():

    """
    Class RuleFunctions
    Container for configured rule functions
    """

    def __init__(self):
        pass

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

    def ingestionMetadata(self, options, metadataFile):
        """
        Ingest metadata record to the database (new collection)
        To keep file hash, etc
        """

        return True

    def psdMetadataUpdate(self, options, metadataFile):
        """
        Check if metadata file was updated (hash change)
        Then re-do all PSD calculations
        """

        return True
