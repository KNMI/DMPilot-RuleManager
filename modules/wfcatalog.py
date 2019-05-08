import logging
import warnings
from datetime import datetime

from obspy.signal.quality_control import MSEEDMetadata

class Collector():
    """
    Class Collector
    Container for the WFCatalog collector responsble
    for the extraction of mSEED metadata
    """

    def __init__(self):

        # Initialize logger
        self.logger = logging.getLogger(__name__)
        self.logger.info("Initializing WFCatalog Collector.")

    def getMetadata(self, SDSFile):
        """
        Collector.getMetadata
        Calls the ObsPy metadata quality class to get metrics
        """

        try:
            with warnings.catch_warnings(record=True) as w:

                # Catch warnings raised by ObsPy
                warnings.simplefilter("always")

                metadata = MSEEDMetadata([SDSFile.filepath],
                                         starttime=SDSFile.start,
                                         endtime=SDSFile.end,
                                         add_flags=True,
                                         add_c_segments=True)

                # Mark documents with data warnings
                metadata.meta.update({"warnings": len(w) > 0})

        except Exception as Ex:
            return None

        return self.extractDatabaseDocument(SDSFile, metadata.meta)

    def extractDatabaseDocument(self, SDSFile, trace):
        """
        Collect.extractDatabaseDocument
        Document parser for daily and hourly granules
        """

        # Determine the number of continuous segments
        nSegments = len(trace.get("c_segments") or [])

        # Source document for granules
        source = {
            "created": datetime.now(),
            "checksum": SDSFile.checksum,
            "collector": "XXXTODO",
            "warnings": trace["warnings"],
            "status": "open",
            "format": "mSEED",
            "fileId": SDSFile.filename,
            "type": "seismic",
            "nseg": nSegments,
            "cont": trace["num_gaps"] == 0,
            "net": trace["network"],
            "sta": trace["station"],
            "cha": trace["channel"],
            "loc": trace["location"],
            "qlt": trace["quality"],
            "ts": trace["start_time"].datetime,
            "te": trace["end_time"].datetime,
            "enc": trace["encoding"],
            "srate": trace["sample_rate"],
            "rlen": trace["record_length"],
            "nrec": int(trace["num_records"]) if trace["num_records"] is not None else None,
            "nsam": int(trace["num_samples"]),
            "smin": int(trace["sample_min"]),
            "smax": int(trace["sample_max"]),
            "smean": float(trace["sample_mean"]),
            "smedian": float(trace["sample_median"]),
            "supper": float(trace["sample_upper_quartile"]),
            "slower": float(trace["sample_lower_quartile"]),
            "rms": float(trace["sample_rms"]),
            "stdev": float(trace["sample_stdev"]),
            "ngaps": int(trace["num_gaps"]),
            "glen": float(trace["sum_gaps"]),
            "nover": int(trace["num_overlaps"]),
            "olen": float(trace["sum_overlaps"]),
            "gmax": float(trace["max_gap"]) if trace["max_gap"] is not None else None,
            "omax": float(trace["max_overlap"]) if trace["max_overlap"] is not None else None,
            "avail": float(trace["percent_availability"]),
            "sgap": trace["start_gap"] is not None,
            "egap": trace["end_gap"] is not None
        }

        # Add timing qualities
        source.update(self.extractTimingQuality(trace))

        # Add mSEED header flags
        source.update(self.extractHeaderFlags(trace))

        return source

    def extractTimingQuality(self, trace):
        """
        Collector.extractTimingQuality
        Writes timing quality parameters and correction to source document
        """

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
            "tcorr": __floatOrNone(trace["timing_correction"]),
            "tqmin": __floatOrNone(trace["timing_quality_min"]),
            "tqmax": __floatOrNone(trace["timing_quality_max"]),
            "tqmean": __floatOrNone(trace["timing_quality_mean"]),
            "tqmedian": __floatOrNone(trace["timing_quality_median"]),
            "tqupper": __floatOrNone(trace["timing_quality_upper_quartile"]),
            "tqlower": __floatOrNone(trace["timing_quality_lower_quartile"])
        }

    def extractHeaderFlags(self, trace):
        """
        Collector.extractHeaderFlags
        writes mSEED header flag percentages to source document
        """

        header = trace["miniseed_header_percentages"]

        return {
          "io_flags": self.mapHeaderFlags(header, "io_and_clock_flags"),
          "dq_flags": self.mapHeaderFlags(header, "data_quality_flags"),
          "ac_flags": self.mapHeaderFlags(header, "activity_flags")
        }

    def mapHeaderFlags(self, trace, flag_type):
        """
        Collector.mapHeaderFlags
        Returns MongoDB document structure for miniseed header percentages
        """

        trace = trace[flag_type]

        if flag_type == "activity_flags":

            source = {
                "cas": trace["calibration_signal"],
                "tca": trace["time_correction_applied"],
                "evb": trace["event_begin"],
                "eve": trace["event_end"],
                "eip": trace["event_in_progress"],
                "pol": trace["positive_leap"],
                "nel": trace["negative_leap"]
            }

        elif flag_type == "data_quality_flags":

            source = {
                "asa": trace["amplifier_saturation"],
                "dic": trace["digitizer_clipping"],
                "spi": trace["spikes"],
                "gli": trace["glitches"],
                "mpd": trace["missing_padded_data"],
                "tse": trace["telemetry_sync_error"],
                "dfc": trace["digital_filter_charging"],
                "stt": trace["suspect_time_tag"]
            }

        elif flag_type == "io_and_clock_flags":

            source = {
                "svo": trace["station_volume"],
                "lrr": trace["long_record_read"],
                "srr": trace["short_record_read"],
                "sts": trace["start_time_series"],
                "ets": trace["end_time_series"],
                "clo": trace["clock_locked"]
            }

        # Make sure that all the flags are floats
        for flag in source:
            source[flag] = float(source[flag])

        return source


collector = Collector()
