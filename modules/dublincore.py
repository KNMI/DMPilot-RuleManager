"""Functions for processing Dublin Core metadata."""

from datetime import datetime


def extractDCMetadata(SDSFile, pid):
    """Computes Dublin Core metadata for the file described by
    `SDSFile`."""

    nowTime = datetime.now()

    # Determine coordinates
    location = SDSFile.location
    if location is None:
        raise ValueError("Impossible to locate %s." % SDSFile.filename)

    lon = location["longitude"]
    lat = location["latitude"]
    ele = location["elevation"]

    # Build the document that will be saved
    document = {
        "_cls": "eudat.models.mongo.wf_do",
        "fileId": SDSFile.filename,
        "checksum": SDSFile.checksum,
        "dc_identifier": pid,
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
