import warnings
from obspy.signal.quality_control import MSEEDMetadata

class Collector():
    """
    Class Collector
    Container for the WFCatalog collector responsble
    for the extraction of mSEED metadata
    """

    def __init__(self):
        pass

    def __signalHandler(self):
        """
        Collector.__signalHandler
        Raise an exception when a signal SIGALRM was received
        """

        raise TimeoutError("Metric calculation has timed out.")

    def getMetadata(self, SDSFile):
        """
        Collector.getMetadata
        Calls the ObsPy metadata quality class to get metrics
        """

        # Set a signal to timeout metrics that take too long
        signal.signal(signal.SIGALRM, self.__signalHandler)
        signal.alarm(120)

        # The ObsPy method may raise various Exceptions
        try:
            with warnings.catch_warnings(record=True) as w:
 
                # Catch warnings raised by ObsPy
                warnings.simplefilter("always")

                metadata = MSEEDMetadata([SDSFile.filepath],
                                         starttime=SDSfile.start,
                                         endtime=SDSFile.end,
                                         add_flags=True,
                                         add_c_segments=True)

                # Mark documents with data warnings
                metadata.meta.update({"warnings": len(w) > 0})

        except Exception:
            return None

        # Reset the alarm
        finally:
            signal.alarm(0)

        return metadata.meta

collector = Collector()
