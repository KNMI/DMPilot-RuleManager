from bson.binary import Binary
from datetime import timedelta
from struct import pack 

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

	if len(ObspyStream) > 1:
	    return None

	# Concatenate all traces with a fill of zeros
	ObspyStream.merge(0, fill_value=0)

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

	return Binary("".join([pack("B", b) for b in array]))

    def __getFrequencyOffset(self, segment, mask):
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
		return [counter - 1] + [self.__reduce(int(x)) for x in segment]

    def __reduce(self, x):

      if -255 <= x and x <= 0:
	return x + 255
      else:
	return 255


    def process(self, SDSFile):

        # Create an empty spectra list that can be preemptively
        # returned to the calling procedure
        spectra = list()

        inventory = SDSFile.inventory

        # Could not be read
	if inventory is None:
	    return spectra

	# And the prepared data
	data = self.__prepareData(SDSFile)

	if data is None:
	    return spectra

	# Try creating the PPSD
	try:

	  ppsd = PPSD(data[0].stats,
		      inventory,
		      period_limits=self.PERIOD_LIMIT_TUPLE)

          # Add the waveform
	  ppsd.add(data)

	except Exception as ex:
	  return spectra

	for segment, time in zip(ppsd._binned_psds, SDSFile.psdBins):

	    # XXX NOTE:
	    # Modified /home/ubuntu/.local/lib/python2.7/site-packages/obspy/signal/spectral_estimation.py
	    # To set ppsd.valid as a public attribute!
	    try:
		psd_array = self.__getFrequencyOffset(segment, ppsd.valid)
		byteAmplitudes = self.__toByteArray(psd_array)
	    except Exception as ex:
		print(ex)
		continue

	    psdObject = {
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
