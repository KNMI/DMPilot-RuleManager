from datetime import datetime, timedelta
from irodsmanager import irodsSession
from mongomanager import mongoSession
from wfcatalog import collector


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
        if mongoSession.getMetadataDocument(SDSFile) is None:
            return

        print(collector.getMetadata(SDSFile))

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

        nowTime = datetime.now()

        # Determine coordinates
        location = sdsFile.location
        if location is None:
            print('Impossible to locate ' + sdsFile.filename)
            return
        lon = location['longitude']
        lat = location['latitude']
        ele = location['elevation']

        # Build the document that will be saved
        document = {
            '_cls': 'eudat.models.mongo.wf_do',
            'fileId': sdsFile.filename,
            'dc_identifier': 'TODO_PID',
            'dc_title': 'INGV_Repository',
            'dc_subject': 'mSEED, waveform, quality',
            'dc_creator': 'EIDA NODE (TODO)',
            'dc_contributor': 'network operator',
            'dc_publisher': 'EIDA NODE (TODO)',
            'dc_type': 'seismic waveform',
            'dc_format': 'MSEED',
            'dc_date': nowTime,
            'dc_coverage_x': lat,
            'dc_coverage_y': lon,
            'dc_coverage_z': ele,
            'dc_coverage_t_min': sdsFile.sampleStart,
            'dc_coverage_t_max': sdsFile.sampleEnd,
            'dcterms_available': nowTime,
            'dcterms_dateAccepted': nowTime,
            'dc_rights': 'open access',
            'dcterms_isPartOf': 'wfmetadata_catalog',
            'irods_path': sdsFile.irodsPath
        }

        # Save to the database
        mongoSession.save('wf_do', document)
