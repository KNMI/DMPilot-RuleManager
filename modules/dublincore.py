from datetime import datetime
import logging

from modules.mongomanager import mongoSession

logger = logging.getLogger(__name__)


class DublinCore:
    """Methods for processing Dublin Core metadata."""

    def getDCMetadata(self, SDSFile):
        """Returns the stored metadata for the file described by
        `SDSFile`. Returns None if no document is found corresponding
        to the filename of the given file."""

        return mongoSession.getDCDocument(SDSFile)

    def extractDCMetadata(self, SDSFile):
        """Computes Dublin Core metadata for the file described by
        `SDSFile`."""

        nowTime = datetime.now()

        # Determine coordinates
        location = SDSFile.location
        if location is None:
            logger.info("Impossible to locate " + SDSFile.filename)
            return None
        lon = location["longitude"]
        lat = location["latitude"]
        ele = location["elevation"]

        # Build the document that will be saved
        document = {
            "_cls": "eudat.models.mongo.wf_do",
            "fileId": SDSFile.filename,
            "dc_identifier": "TODO_PID",
            "dc_title": "INGV_Repository",
            "dc_subject": "mSEED, waveform, quality",
            "dc_creator": "EIDA NODE (TODO)",
            "dc_contributor": "network operator",
            "dc_publisher": "EIDA NODE (TODO)",
            "dc_type": "seismic waveform",
            "dc_format": "MSEED",
            "dc_date": nowTime,
            "dc_coverage_x": lat,
            "dc_coverage_y": lon,
            "dc_coverage_z": ele,
            "dc_coverage_t_min": SDSFile.sampleStart,
            "dc_coverage_t_max": SDSFile.sampleEnd,
            "dcterms_available": nowTime,
            "dcterms_dateAccepted": nowTime,
            "dc_rights": "open access",
            "dcterms_isPartOf": "wfmetadata_catalog",
            "irods_path": SDSFile.irodsPath
        }

        return document

dublinCore = DublinCore()