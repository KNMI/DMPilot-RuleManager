from bson.binary import Binary
from datetime import timedelta
from struct import pack
from hashlib import sha256

import base64

# ObsPy imports
from obspy.signal import PPSD
from obspy import read, read_inventory, Stream, UTCDateTime

class PSDCollector():

    """
    Class PSDCollector
    Uses ObsPy PPSD to extract PSD information
    XXX THIS SHOULD BE REVIEWED!
    """

    # Period limits
    # XXX TO FIX
    PERIOD_LIMIT_TUPLE = (0.01, 1000)

    def __init__(self):
        pass

    def __getResponseHash(self, inventory):
        """
        def PSDCollector::__getResponseHash
        Returns SHA256 hash for instrument response
        """

        response = inventory[0][0][0].response

        samplingRate = self.__getSamplingRate(response.response_stages)

        # Evaluate the response
        resp, _ = response.get_evalresp_response(
            1.0 / samplingRate,
            1E3 * samplingRate
        )

        # Create checksum from the response
        checksum = sha256()
        checksum.update(resp)
        return "sha2:" + base64.b64encode(checksum.digest()).decode()

    def __prepareData(self, SDSFile):
        """
        def PSDCollector::__prepareData
        Prepares the correct data for plotting
        """

        # Create an empty stream
        ObspyStream = Stream()

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

        # No data
        if not ObspyStream:
            return None

        # Concatenate all traces with a fill of zeros
        ObspyStream.merge(0, fill_value=0)

        if len(ObspyStream) > 1:
            return None

        # Cut to the appropriate end and start
        ObspyStream.trim(starttime=UTCDateTime(SDSFile.start),
                         endtime=UTCDateTime(SDSFile.end + timedelta(minutes=30)),
                         pad=True,
                         fill_value=0,
                         nearest_sample=False)

        return ObspyStream

    def __toByteArray(self, array):
        """
        def PSDCollector::__toByteArray
        Converts values to single byte array
        """

        return Binary(b"".join([pack("B", b) for b in array]))

    def __getFrequencyOffset(self, segment, mask, isInfrasound):
        """
        def __getFrequencyOffset
        Detects the first frequency and uses this offset
        """
        # Determine the first occurrence of True
        # from the Boolean mask, this will be the offset
        counter = 0
        for boolean in mask:
            if not boolean:
                counter += 1
            else:
                return [counter] + [self.__reduce(x, isInfrasound) for x in segment]

    def __reduce(self, x, isInfrasound):
        """
        def PSDCollector::__reduce
        Keeps values within single byte bounds (ObsPy PSD values are negative)
        """

        # Infrasound is shifted downward by value 100
        if isInfrasound:
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

    def __getSamplingRate(self, stages):

        for stage in stages[::-1]:
            if stage.decimation_input_sample_rate is not None and stage.decimation_factor is not None:
                return stage.decimation_input_sample_rate / stage.decimation_factor

        return None

    def process(self, SDSFile):

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
            handling = "hydrophone" if SDSFile.isInfrasound else None
            
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
            # To set ppsd.valid as a public attribute!
            try:
                psd_array = self.__getFrequencyOffset(segment, ppsd.valid, SDSFile.isInfrasound)
                byteAmplitudes = self.__toByteArray(psd_array)
            except Exception as ex:
                continue

            # XXX TODO:
            # Add hash of the metadata? This is a hard problem to solve
            psdObject = {
                "hash": SDSFile.checksum,
                "hashI": self.__getResponseHash(inventory),
                "net": SDSFile.net,
                "file": SDSFile.filename,
                "sta": SDSFile.sta,
                "loc": SDSFile.loc,
                "cha": SDSFile.cha,
                "ts": SDSFile.start,
                "te": SDSFile.start + timedelta(minutes=60),
                "bin": byteAmplitudes
            }

            spectra.append(psdObject)

        return spectra


psdCollector = PSDCollector()
