"""Functions for processing Dublin Core metadata."""

from datetime import datetime


def extract_dc_metadata(sds_file, pid):
    """Computes Dublin Core metadata for the file described by `sds_file`."""

    now_time = datetime.now()

    # Determine coordinates
    location = sds_file.location
    if location is None:
        raise ValueError("Impossible to locate %s." % sds_file.filename)

    lon = location["longitude"]
    lat = location["latitude"]
    ele = location["elevation"]

    # Build the document that will be saved
    document = {
        "_cls": "eudat.models.mongo.wf_do",
        "fileId": sds_file.filename,
        "checksum": sds_file.checksum,
        "dc_identifier": pid,
        "dc_title": "INGV_Repository",
        "dc_subject": "mSEED, waveform, quality",
        "dc_creator": "EIDA NODE (TODO)",
        "dc_contributor": "network operator",
        "dc_publisher": "EIDA NODE (TODO)",
        "dc_type": "seismic waveform",
        "dc_format": "MSEED",
        "dc_date": now_time,
        "dc_coverage_x": lat,
        "dc_coverage_y": lon,
        "dc_coverage_z": ele,
        "dc_coverage_t_min": sds_file.sample_start,
        "dc_coverage_t_max": sds_file.sample_end,
        "dcterms_available": now_time,
        "dcterms_dateAccepted": now_time,
        "dc_rights": "open access",
        "dcterms_isPartOf": "wfmetadata_catalog",
        "irods_path": sds_file.irods_path
    }

    return document
