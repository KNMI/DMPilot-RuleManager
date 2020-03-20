import warnings
from datetime import datetime

from obspy.signal.quality_control import MSEEDMetadata

# Version of the WFCatalog collector (saved in the DB)
WCATALOG_COLLECTOR_VERSION = "1.0.0"

def getWFMetadata(SDSFile):
    """
        Calls the ObsPy metadata quality class to get metrics.
        Returns a tuple with the daily document and the documents of the
        continuous segments if trace is not continuous
    """

    with warnings.catch_warnings(record=True) as w:

        # Catch warnings raised by ObsPy
        warnings.simplefilter("always")

        metadata = MSEEDMetadata([sdsfile.filepath for sdsfile in SDSFile.neighbours],
                                 starttime=SDSFile.start,
                                 endtime=SDSFile.end,
                                 add_flags=True,
                                 add_c_segments=True)

        # Mark documents with data warnings
        metadata.meta.update({"warnings": len(w) > 0})

    return (extractDailyDocument(SDSFile, metadata.meta),
            extractSegmentDocuments(SDSFile, metadata.meta))


def extractDailyDocument(SDSFile, trace):
    """Document parser for daily granule."""

    # Determine the number of continuous segments
    nSegments = len(trace.get("c_segments") or [])

    # Source document for granules
    source = {
        "created": datetime.now(),
        "checksum": SDSFile.checksum,
        "checksum_prev": SDSFile.previous.checksum,
        "collector": WCATALOG_COLLECTOR_VERSION,
        "warnings": trace["warnings"],
        "status": "open",
        "format": "mSEED",
        "fileId": SDSFile.filename,
        "type": ("seismic" if not SDSFile.isPressureChannel else "acoustic"),
        "nseg": nSegments,
        "continuous": trace["num_gaps"] == 0,
        "network": trace["network"],
        "station": trace["station"],
        "channel": trace["channel"],
        "location": trace["location"],
        "quality": trace["quality"],
        "start_time": trace["start_time"].datetime,
        "end_time": trace["end_time"].datetime,
        "encoding": trace["encoding"],
        "sample_rate": trace["sample_rate"],
        "record_length": trace["record_length"],
        "num_records": int(trace["num_records"]) if trace["num_records"] is not None else None,
        "num_samples": int(trace["num_samples"]),
        "sample_min": int(trace["sample_min"]),
        "sample_max": int(trace["sample_max"]),
        "sample_mean": float(trace["sample_mean"]),
        "sample_median": float(trace["sample_median"]),
        "sample_upper_quartile": float(trace["sample_upper_quartile"]),
        "sample_lower_quartile": float(trace["sample_lower_quartile"]),
        "sample_rms": float(trace["sample_rms"]),
        "sample_stdev": float(trace["sample_stdev"]),
        "num_gaps": int(trace["num_gaps"]),
        "sum_gaps": float(trace["sum_gaps"]),
        "num_overlaps": int(trace["num_overlaps"]),
        "sum_overlaps": float(trace["sum_overlaps"]),
        "max_gap": float(trace["max_gap"]) if trace["max_gap"] is not None else None,
        "max_overlap": float(trace["max_overlap"]) if trace["max_overlap"] is not None else None,
        "percent_availability": float(trace["percent_availability"]),
        "start_gap": trace["start_gap"] is not None,
        "end_gap": trace["end_gap"] is not None
    }

    # Add timing qualities
    source.update(extractTimingQuality(trace))

    # Add mSEED header flags
    source.update(extractHeaderFlags(trace))

    return source


def extractTimingQuality(trace):
    """Writes timing quality parameters and correction to source document."""

    def __floatOrNone(value):
        """
        Collector.extractTimingQuality::__floatOrNone
        Returns None or value to float
        """

        if value is None:
            return None

        return float(value)

    trace = trace["miniseed_header_percentages"]

    # Add the timing correction
    return {
        "timing_correction": __floatOrNone(trace["timing_correction"]),
        "timing_quality_min": __floatOrNone(trace["timing_quality_min"]),
        "timing_quality_max": __floatOrNone(trace["timing_quality_max"]),
        "timing_quality_mean": __floatOrNone(trace["timing_quality_mean"]),
        "timing_quality_median": __floatOrNone(trace["timing_quality_median"]),
        "timing_quality_upper_quartile": __floatOrNone(trace["timing_quality_upper_quartile"]),
        "timing_quality_lower_quartile": __floatOrNone(trace["timing_quality_lower_quartile"])
    }


def extractHeaderFlags(trace):
    """Writes mSEED header flag percentages to source document."""

    header = trace["miniseed_header_percentages"]

    return {
      "io_and_clock_flags": mapHeaderFlags(header, "io_and_clock_flags"),
      "data_quality_flags": mapHeaderFlags(header, "data_quality_flags"),
      "activity_flags": mapHeaderFlags(header, "activity_flags")
    }


def mapHeaderFlags(trace, flag_type):
    """Returns MongoDB document structure for miniseed header percentages."""

    trace = trace[flag_type]

    if flag_type == "activity_flags":

        source = {
            "calibration_signal": trace["calibration_signal"],
            "time_correction_applied": trace["time_correction_applied"],
            "event_begin": trace["event_begin"],
            "event_end": trace["event_end"],
            "event_in_progress": trace["event_in_progress"],
            "positive_leap": trace["positive_leap"],
            "negative_leap": trace["negative_leap"]
        }

    elif flag_type == "data_quality_flags":

        source = {
            "amplifier_saturation": trace["amplifier_saturation"],
            "digitizer_clipping": trace["digitizer_clipping"],
            "spikes": trace["spikes"],
            "glitches": trace["glitches"],
            "missing_padded_data": trace["missing_padded_data"],
            "telemetry_sync_error": trace["telemetry_sync_error"],
            "digital_filter_charging": trace["digital_filter_charging"],
            "suspect_time_tag": trace["suspect_time_tag"]
        }

    elif flag_type == "io_and_clock_flags":

        source = {
            "station_volume": trace["station_volume"],
            "long_record_read": trace["long_record_read"],
            "short_record_read": trace["short_record_read"],
            "start_time_series": trace["start_time_series"],
            "end_time_series": trace["end_time_series"],
            "clock_locked": trace["clock_locked"]
        }

    # Make sure that all the flags are floats
    for flag in source:
        source[flag] = float(source[flag])

    return source


def extractSegmentDocuments(SDSFile, trace):
    """Return documents for continuous segments if trace is not continuous."""

    # If trace is continuous (no gaps), return None
    if trace["num_gaps"] == 0:
        return None

    # Loop continuous segments
    segment_docs = []
    for segment in trace["c_segments"]:
        segment_docs.append(parseSegment(segment, SDSFile.filename))

    return segment_docs


def parseSegment(segment, file_id):
    """Document parser for 1 continuous segment."""

    # Source document for continuous segment
    source = {
      'fileId': file_id,
      'sample_min': int(segment['sample_min']),
      'sample_max': int(segment['sample_max']),
      'sample_mean': float(segment['sample_mean']),
      'sample_median': float(segment['sample_median']),
      'sample_stdev': float(segment['sample_stdev']),
      'sample_rms': float(segment['sample_rms']),
      'sample_upper_quartile': float(segment['sample_upper_quartile']),
      'sample_lower_quartile': float(segment['sample_lower_quartile']),
      'num_samples': int(segment['num_samples']),
      'sample_rate': float(segment['sample_rate']),
      'start_time': segment['start_time'].datetime,
      'end_time': segment['end_time'].datetime,
      'segment_length': float(segment['segment_length'])
    }

    return source
