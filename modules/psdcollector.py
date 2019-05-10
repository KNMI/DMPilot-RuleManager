import base64
import logging

from bson.binary import Binary
from datetime import timedelta
from struct import pack
from hashlib import sha256

# ObsPy imports
from obspy.signal import PPSD
from obspy import read, read_inventory, Stream, UTCDateTime

class PSDCollector():

    """
    Class PSDCollector
    Uses ObsPy PPSD to extract PSD information

    Additional information:
    This needs to be installed under ObsPy 1.2.0 for some fixes see:

      https://github.com/obspy/obspy/pull/2040/files

    This has been modified /usr/local/lib/python3.5/dist-packages/obspy/signal/spectral_estimation.py

    We use the default ObsPy options to calculate PSDs

    We use 50% overlap and hourly segments. This means we get 48 segments per day!

             END
              | 
            [~~~] (NOT IN "DAY" BUT REQUIRED FOR OVERLAP)
          [~~~] (1 HOUR SEGMENT)
        ... (46 SEGMENTS NOT SHOWN)
      [~~~] (1 HOUR SEGMENT)
    [~~~] (NOT IN "DAY" BUT REQUIRED FOR OVERLAP)
      |     
    START

    To get the segment for a single day, we need access to the "before" and "after" file
    to guarantee the overlap is present. Otherwise we will introduce boundary effects when
    calculating the PSDs. A PSD will start at 00:00 and end at 00:30.. take overlap through following day.

    Gaps are filled with 0's to make sure that we always have a full day of data with no gaps.. which makes
    the calculation more consistent.

    Database storage:

    The PSD is continuous across the frequency spectrum and generally has a value between 0 and -255 dB
    We represent the dB value as a single 8-bit integer and save the "offset" on the frequency axis.
    Low sampling rates do not have data for high frequencies but we wish to represent the PSD data
    as single 8-bit array of values. This offset is stored as another 8-bit integer at index 0.

    e.g.  [57, 120, 120, 120, .., 120] 

    means a frequency offset of 57 steps. The steps are defined by ObsPy giving the period limit tuple
    0.01 - 1000 and are always the same starting at 0.01. Increase is per 1/8th octave (factor of 0.125).
    The current database runs from 0.001 to 1000 but that's probably not necessary.

    e.g.

    [0] = 0.01s
    [1] = 0.01090507732s (0.01 * 2 ** 0.125)
    [2] = 0.01189207114s (0.01 * (2 ** 0.125) ** 2)
    [~] = ...
    [133] = 1010.70328654s (0.01 * (2 ** 0.125) ** 133)

    It stops after going over 1000 as we configured.

    From this we can reconstruct the period (frequency) the first value belongs to.
    Then we proceed continuously across the spectrum (add a negative sign to the power) in the webservice.
    """

    # Period limits 100Hz should be enough and 0.001Hz a low enough frequency
    # In case of 200Hz sampled data this is the nyquist frequency
    PERIOD_LIMIT_TUPLE = (0.01, 1000)

    def __init__(self):

        # Initialize logger
        self.logger = logging.getLogger(__name__)
        self.logger.debug("Initializing PSD Collector.")

    def __getResponseChecksum(self, inventory):

        """
        def PSDCollector::__getResponseChecksum
        Returns SHA256 hash of the instrument response
        """

        # Get the ObsPy response object and channel sampling rate
        response = inventory[0][0][0].response
        samplingRate = inventory[0][0][0].sample_rate

        # Evaluate the response
        resp, _ = response.get_evalresp_response(
            1.0 / samplingRate,
            1E3 * samplingRate
        )

        # Create checksum from the array of response values
        checksum = sha256()
        checksum.update(resp)

        # Return in the same format as the SDSFile.checksum (compatible with iRODS)
        return "sha2:" + base64.b64encode(checksum.digest()).decode()

    def __prepareData(self, SDSFile):

        """
        def PSDCollector::__prepareData
        Prepares the correct data for plotting
        """

        # Create an empty stream to fill
        ObspyStream = Stream()

        # Read neighbouring files is necessary to get the boundary overlapping data
        # Of the next day.. the previous day was already "done" with the previous file
        for neighbour in SDSFile.neighbours:
            st = read(neighbour.filepath,
                      starttime=UTCDateTime(SDSFile.start),
                      endtime=UTCDateTime(SDSFile.next.end),
                      nearest_sample=False,
                      format="MSEED")

            # Concatenate all traces
            for tr in st:
                if tr.stats.npts != 0:
                    ObspyStream.extend([tr])

        # No data found
        if not ObspyStream:
            return None

        # Concatenate all traces with a fill of zeros. Otherwise some segments may be skipped
        # Data from multiple files will be in two different traces by default
        ObspyStream.merge(0, fill_value=0)

        # More than a single stream cannot be handled.. should not be the case after merging
        # but there may be some crappy mixed data in the archive:
        #
        # msi -T /data/storage/orfeus/SDS/2013/II/AAK/BHZ.D/II.AAK.00.BHZ.D.2013.249
        #
        # ...
        # II_AAK_00_BHZ     2013,249,23:59:46.719500 2013,250,00:00:07.569500 20  418
        # II_AAK_10_BHZ     2013,249,00:05:16.069500 2013,249,00:24:14.319500 40  45531
        # ...
        #
        # How 'bout dat?
        if len(ObspyStream) > 1:
            return None

        # Cut to the appropriate end and start (start & end are both inclusive).
        # Therefore subtract 1E-6 from the endtime
        # Here is some code to illustrate:

        # from obspy import Trace, UTCDateTime
        # import numpy as np
        # 
        # tr = Trace(data=np.arange(0, 86401))
        # tr.trim(starttime=UTCDateTime("1970-01-01"), endtime=UTCDateTime("1970-01-02"), nearest_sample=False)
        # >>> ... | 1970-01-01T00:00:00.000000Z - 1970-01-02T00:00:00.000000Z | 1.0 Hz, 86401 samples
        # tr.trim(starttime=UTCDateTime("1970-01-01"), endtime=UTCDateTime("1970-01-02") - 1E-6, nearest_sample=False)
        # >>> ... | 1970-01-01T00:00:00.000000Z - 1970-01-01T23:59:59.000000Z | 1.0 Hz, 86400 samples

        # Pad start & end with zeros if necessary and fill with zeros in case we do not cover the full day range
        ObspyStream.trim(starttime=UTCDateTime(SDSFile.start),
                         endtime=UTCDateTime(SDSFile.end + timedelta(minutes=30)) - 1E-6,
                         pad=True,
                         fill_value=0,
                         nearest_sample=False)

        return ObspyStream

    def __toByteArray(self, array):

        """
        def PSDCollector::__toByteArray
        Converts values to single byte array we pack the values to a string of single bytes
        """

        return Binary(b"".join([pack("B", b) for b in array]))

    def __getFrequencyOffset(self, segment, mask, isPressureChannel):

        """
        def __getFrequencyOffset
        Detects the first frequency and uses this offset
        The passed property "mask" is a boolean mask that marks which values are "valid" e.g. exist
        and are above the nyquist frequency. We check the first occurence of True: that means the frequency offset
        """

        # Determine the first occurrence of True
        # from the Boolean mask, this will be the offset
        counter = 0
        for boolean in mask:
            if not boolean:
                counter += 1
            else:
                return [counter] + [self.__reduce(x, isPressureChannel) for x in segment]

    def __reduce(self, x, isPressureChannel):

        """
        def PSDCollector::__reduce
        Keeps values within single byte bounds (ObsPy PSD values are negative)
        """

        # Infrasound is shifted downward by 100dB
        # They use a normalized pressure of 20 micropascals which shifts our PSD out of range
        if isPressureChannel:
            x = int(x) - 100
        else:
            x = int(x)

        # Value is bound within one byte
        if x < -255:
            return 0
        if x > 0:
            return 255
        else:
            return x + 255

    def process(self, SDSFile):

        """
        def PSDCollector::process
        Processes a single SDSFile to extract PSDs and store them in the DB
        """

        # Create an empty spectra list that can be preemptively
        # returned to the calling procedure
        spectra = list()

        inventory = SDSFile.inventory

        # Inventory could not be read: return empty spectra
        if inventory is None:
            return spectra

        # And the prepared data
        data = self.__prepareData(SDSFile)

        # Data could not be read: return empty spectra
        if data is None:
            return spectra

        # Try creating the PPSD
        try:

            # Set handling to hydrophone if using pressure data
            # This is a bit hacky but the process should be the same for infrasound data
            handling = "hydrophone" if SDSFile.isPressureChannel else None
            
            ppsd = PPSD(data[0].stats,
                        inventory,
                        period_limits=self.PERIOD_LIMIT_TUPLE,
                        special_handling=handling)

            # Add the waveform
            ppsd.add(data)

        except Exception as ex:
            return spectra

        for segment, time in zip(ppsd._binned_psds, SDSFile.psdBins):

            # XXX NOTE:
            # Modified /home/ubuntu/.local/lib/python2.7/site-packages/obspy/signal/spectral_estimation.py
            # And /usr/local/lib/python3.5/dist-packages/obspy/signal/spectral_estimation.py
            # To set ppsd.valid as a public attribute! We need this to determine the offset on the frequency axis
            try:
                psd_array = self.__getFrequencyOffset(segment, ppsd.valid, SDSFile.isPressureChannel)
                byteAmplitudes = self.__toByteArray(psd_array)
            # This may fail in multiple ways.. try the next segment
            except Exception as ex:
                continue

            # Add hash of the data & metadata (first 8 hex digits)
            # Saving 64 bytes * 2 makes (checksums) our database pretty big and this should be sufficient to 
            # detect changes
            psdObject = {
                "checksum": SDSFile.checksum[:13],
                "checksumInventory": self.__getResponseChecksum(inventory)[:13],
                "net": SDSFile.net,
                "sta": SDSFile.sta,
                "loc": SDSFile.loc,
                "cha": SDSFile.cha,
                "quality": SDSFile.quality,
                "ts": time.datetime,
                "te": (time + timedelta(minutes=60)).datetime,
                "bin": byteAmplitudes
            }

            spectra.append(psdObject)

        return spectra

psdCollector = PSDCollector()
