from datetime import datetime, timedelta


class RuleFunctions():

    """
    Class RuleFunctions
    Container for configured rule functions
    """

    def __init__(self):
        pass

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
            print "Prune is requested."

        print "Setting record size to %i" % options["repackRecordSize"]

        print(SDSFile.filename)

    def isIngested(self, SDSFile):
        """
        Stateless check to see if the file exists in iRODS?
        TODO XXX
        """

        return False
