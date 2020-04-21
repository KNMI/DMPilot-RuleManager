import xml.etree.ElementTree as ET
from hashlib import sha256
import dateutil.parser


class FDSNXMLFile():
    NAMESPACE = "http://www.fdsn.org/xml/station/1"

    def __init__(self, filepath):
        """def FDSNXMLFile::__init__ Initializes a FDSNXMLFile instance from a
        StationXML file."""

        self.filepath = filepath
        self.channels = list()

        # Extract all available channels
        self.parse_channels()

    def parse_channels(self):

        tree = ET.parse(self.filepath)
        root = tree.getroot()

        # Extract all channels
        for network in root.findall("{%s}Network" % self.NAMESPACE):
            for station in network.findall("{%s}Station" % self.NAMESPACE):
                for channel in station.findall("{%s}Channel" % self.NAMESPACE):

                    network_code = network.get("code")
                    station_code = station.get("code")
                    channel_code = channel.get("code")

                    channel_start = dateutil.parser.parse(channel.get("startDate"))
                    channel_end = channel.get("endDate")

                    # End may be none
                    if channel_end is not None:
                        channel_end = dateutil.parser.parse(channel_end)

                    # Maybe some sanity rules
                    channel_latitude = float(
                        channel.find("{%s}Latitude" % self.NAMESPACE).text)
                    channel_longitude = float(
                        channel.find("{%s}Longitude" % self.NAMESPACE).text)
                    channel_elevation = float(
                        channel.find("{%s}Elevation" % self.NAMESPACE).text)
                    channel_sample_rate = float(
                        channel.find("{%s}SampleRate" % self.NAMESPACE).text)

                    # Get the hash of the XML string per channel
                    # The XML should be canonicalized
                    channel_hash = sha256(ET.tostring(channel)).hexdigest()

                    self.channels.append({
                        "net": network_code,
                        "sta": station_code,
                        "cha": channel_code,
                        "start": channel_start,
                        "end": channel_end,
                        "lat": channel_latitude,
                        "lng": channel_longitude,
                        "elev": channel_elevation,
                        "rate": channel_sample_rate,
                        "hash": channel_hash
                    })


# Example
FDSNXMLFile("/data/temp_archive/metadata/NL.HGN.xml")
