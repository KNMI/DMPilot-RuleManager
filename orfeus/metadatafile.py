import xml.etree.ElementTree as ET
from hashlib import sha256
import dateutil.parser

class FDSNXMLFile():

    NAMESPACE = "http://www.fdsn.org/xml/station/1"

    def __init__(self, filepath):

        self.filepath = filepath
        self.channels = list()

        # Extract
        self.parseChannels()
        print(self.channels)

    def parseChannels(self):

        self.channels = list()  

        tree = ET.parse(self.filepath)
        root = tree.getroot()

        # Extract all channels 
        for network in root.findall("{%s}Network" % self.NAMESPACE):
            for station in network.findall("{%s}Station" % self.NAMESPACE):
                for channel in station.findall("{%s}Channel" % self.NAMESPACE):

                     networkCode = network.get("code")
                     stationCode = station.get("code")
                     channelCode = channel.get("code")

                     channelStart = dateutil.parser.parse(channel.get("startDate"))
                     channelEnd = channel.get("endDate")

                     # End may be none
                     if channelEnd is not None:
                         channelEnd = dateutil.parser.parse(channelEnd)

                     # Get the hash of the XML string per channel
                     # The XML should be canonicalized
                     channelHash = sha256(ET.tostring(channel)).hexdigest()

                     self.channels.append({
                       "net": networkCode,
                       "sta": stationCode,
                       "cha": channelCode,
                       "start": channelStart,
                       "end": channelEnd,
                       "hash": channelHash
                     })


# Example
FDSNXMLFile("/data/temp_archive/metadata/NL.HGN.xml")
