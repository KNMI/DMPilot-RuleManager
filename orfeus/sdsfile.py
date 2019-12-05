"""

  orfeus/sdsfile class for handling SDS type files.

  Author: Mathijs Koymans, 2017
  Copyright: ORFEUS Data Center, 2017
  Modified: 2019

"""

import os
import requests
import subprocess
import base64
import logging

from datetime import datetime, timedelta
from hashlib import sha256

from obspy import read_inventory, UTCDateTime
from configuration import config


class SDSFile():

    """
    Public Class SDSFile
    Class for handling files in SDS structure.

    Attributes
    ----------
    filename : `str`
        Name of file.
    net : `str`
        Network code.
    sta : `str`
        Station code.
    loc : `str`
        Location code.
    cha : `str`
        Channel code.
    quality : `str`
        Quality parameter.
    year : `str`
        Year in YYYY format.
    day : `str`
        Day of the year, in DDD format (i.e., it goes from "001" to "366").
    """

    # Save some configuration to the class
    irodsRoot = config["IRODS_ROOT"]
    fdsnws = config["FDSNWS_ADDRESS"]
    s3Prefix = config["S3"]["PREFIX"]

    def __init__(self, filename, archiveRoot):
        """
        Create a filestream from a given filename
        """

        try:
            # Extract stream identification
            (self.net,
             self.sta,
             self.loc,
             self.cha,
             self.quality,
             self.year,
             self.day) = filename.split(".")
        except ValueError:
            raise ValueError("Invalid SDS file submitted.")

        self.archiveRoot = archiveRoot

        # Initialize logger
        self.logger = logging.getLogger('RuleManager')

    # Returns the filename
    @property
    def filename(self):
        return ".".join([self.net,
                         self.sta,
                         self.loc,
                         self.cha,
                         self.quality,
                         self.year,
                         self.day])

    # Returns custom filepath for a given file
    def customPath(self, root):
        return os.path.join(self.customDirectory(root), self.filename)

    # Returns filepath for a given file
    @property
    def filepath(self):
        return self.customPath(self.archiveRoot)

    # Returns iRODS filepath for a given file
    @property
    def irodsPath(self):
        return self.customPath(self.irodsRoot)

    # Returns the S3 key for a given file
    @property
    def s3Key(self):
        return os.path.join(
            self.s3Prefix,
            self.subDirectory,
            self.filename
        )

    # Returns the stream identifier
    @property
    def id(self):
        return ".".join([
            self.net,
            self.sta,
            self.loc,
            self.cha
        ])

    # Returns the subdirectory
    @property
    def subDirectory(self):
        return os.path.join(
            self.year,
            self.net,
            self.sta,
            self.channelDirectory
        )

    def customDirectory(self, root):
        return os.path.join(
            root,
            self.subDirectory
        )

    @property
    def irodsDirectory(self):
        return self.customDirectory(self.irodsRoot)

    # Returns the file directory based on SDS structure
    @property
    def directory(self):
        return self.customDirectory(self.archiveRoot)

    # Returns channel directory
    @property
    def channelDirectory(self):
        return ".".join([self.cha, self.quality])

    # Returns next file in stream
    @property
    def next(self):
        return self.__getAdjacentFile(1)

    # Returns previous file in stream
    @property
    def previous(self):
        return self.__getAdjacentFile(-1)

    # Returns start time of file
    @property
    def start(self):
        return datetime.strptime(self.year + " " + self.day, "%Y %j")

    # Returns end time of file
    @property
    def end(self):
        return self.start + timedelta(days=1)

    # Start for dataselect pruning (start is INCLUSIVE)
    @property
    def sampleStart(self):
        return self.start.strftime("%Y,%j,00,00,00.000000")

    # End for dataselect pruning (end is INCLUSIVE)
    @property
    def sampleEnd(self):
        return self.start.strftime("%Y,%j,23,59,59.999999")

    # Returns list of files neighbouring a file
    @property
    def neighbours(self):
        return filter(lambda x: os.path.isfile(x.filepath), [self.previous, self, self.next])

    @property
    def stats(self):
        try:
            return os.stat(self.filepath)
        except FileNotFoundError:
            return None

    def getStat(self, enum):

        # Check if none and propagate
        if self.stats is None:
            return None

        if enum == "size":
            return self.stats.st_size
        elif enum == "created":
            return datetime.fromtimestamp(self.stats.st_ctime)
        elif enum == "modified":
            return datetime.fromtimestamp(self.stats.st_mtime)

    @property
    def size(self):
        return self.getStat("size")

    @property
    def created(self):
        return self.getStat("created")

    @property
    def modified(self):
        return self.getStat("modified")

    @property
    def checksumTruncated(self):
        """
        def SDSFile::checksumTrunc
        Returns truncated checksum value (8 base-64 characters)
        """
        return self.checksum[5:13]

    @property
    def checksum(self):
        """
        def SDSFile::checksum
        Calculates the SHA256 checksum for a given file prepended with sha2:
        """

        if self.stats is None:
            return None

        checksum = sha256()
        with open(self.filepath, "rb") as f:
            for block in iter(lambda: f.read(0x10000), b""):
                checksum.update(block)
        return "sha2:" + base64.b64encode(checksum.digest()).decode()

    @property
    def queryStringTXT(self):

        return self.queryString + "&" + "&".join([
            "format=text",
            "level=channel"
        ])

    @property
    def queryStringXML(self):

        return self.queryString + "&" + "&".join([
            "format=fdsnxml",
            "level=response"
        ])

    @property
    def queryString(self):
        """
        def SDSFile::queryString
        Returns the query string for a particular SDS file
        """

        return "?" + "&".join([
            "start=%s" % self.start.isoformat(),
            "end=%s" % self.end.isoformat(),
            "network=%s" % self.net,
            "station=%s" % self.sta,
            "location=%s" % self.loc,
            "channel=%s" % self.cha
        ])

    @property
    def samples(self):
        """
        def SDSFile::samples
        Returns number of samples in the SDS file
        """

        return sum(map(lambda x: x["samples"], self.traces))

    @property
    def continuous(self):
        """
        def SDSFile::continuous
        Returns True when a SDS file is considered continuous and the
        record start time & end time come before and after the file ending respectively
        """

        return len(
            self.traces) == 1 and (
            self.traces[0]["start"] <= self.start) and (
            self.traces[0]["end"] >= self.end)

    @property
    def traces(self):
        """
        def SDSFile::traces
        Returns a list of traces
        """

        def parseMSIOutput(line):
            """
            def SDSFile::traces::parseMSIOutput
            Parses the MSI output
            """

            # Format for seed dates e.g. 2005,068,00:00:01.000000
            SEED_DATE_FMT = "%Y,%j,%H:%M:%S.%f"

            (stream, start, end, rate, samples) = map(lambda x: x.decode("ascii"), line.split())

            # Return a simple dict with some information
            return {
                "samples": int(samples),
                "rate": float(rate),
                "start": datetime.strptime(start, SEED_DATE_FMT),
                "end": datetime.strptime(end, SEED_DATE_FMT)
            }

        # Cut to day boundary on sample level
        dataselect = subprocess.Popen([
            "dataselect",
            "-ts", self.sampleStart,
            "-te", self.sampleEnd,
            "-Ps",
            "-szs",
            "-o", "-",
        ] + list(map(lambda x: x.filepath, self.neighbours)), stdout=subprocess.PIPE)

        lines = subprocess.check_output([
            "msi", 
            "-ts", self.sampleStart,
            "-te", self.sampleEnd,
            "-T",
            "-"
        ], stdin=dataselect.stdout, stderr=subprocess.DEVNULL).splitlines()

        # Not sure why we need this
        dataselect.stdout.close()

        # Avoid warning when status code for child process is not read (for Python 3.6):
        # Introduced in https://github.com/python/cpython/commit/5a48e21ff163107a6f1788050b2f1ffc8bce2b6d#diff-cc136486b4a8e112e64b57436a0619eb
        dataselect.wait()

        # Skip first header & final line
        return list(map(parseMSIOutput, lines[1:-1]))

    @property
    def isPressureChannel(self):
        """
        def SDSFile::isPressureChannel
        Returns true when the channel is an infrasound channel
        """

        return self.cha.endswith("DF")

    @property
    def inventory(self):
        """
        def SDSFile::inventory
        Returns the FDSNWSXML inventory
        """

        # Query our FDSNWS Webservice for the station location
        request = os.path.join(self.fdsnws, self.queryStringXML)

        try:
            return read_inventory(request)
        except Exception as ex:
            return None

    @property
    def location(self):
        """
        def SDSFile::location
        Returns the geographical location of the stream
        """

        # Query our FDSNWS Webservice for the station location
        try:
            request = requests.get(os.path.join(self.fdsnws, self.queryStringTXT))
        except Exception as ex:
            return None

        # Any error just ignore
        if request.status_code != 200:
            return None

        lines = request.text.split("\n")

        # Multiple lines means that the location is somehow ambiguous
        if len(lines) != 3:
            return None

        # Some magic parsing: fields 4, 5, 6 on the 2nd line
        (latitude, longitude, elevation) = lines[1].split("|")[4:7]

        return {
            "longitude": float(longitude),
            "latitude": float(latitude),
            "elevation": float(elevation)
        }

    @property
    def psdBins(self):
        """
        def SDSFile::psdBins
        Returns 48 times starting at the start of the SDSFile
        with 30 minute increments
        """

        return map(UTCDateTime, map(lambda x: self.start + timedelta(minutes=(30 * x)), range(48)))

    def prune(self, recordLength=4096, removeOverlap=True):
        """
        def SDSFile::prune
        Uses IRIS dataselect to prune a file on the sample level
        and sets the quality indicator to Q

        QUALITIES:
        D - The state of quality control of the data is indeterminate
        R - Raw Waveform Data with no Quality Control
        Q - Quality Controlled Data, some processes have been applied to the data.
        M - Data center modified, time-series values have not been changed.
        """

        # Record length within some bounds
        if recordLength < 512 and recordLength > 65536:
            raise ValueError("Record length is invalid")

        # Confirm record length is power of two
        if recordLength & (recordLength - 1) != 0:
            raise ValueError("Record length is not is a power of two")

        # Create a phantom SDSFile with a different quality idenfier
        qualityFile = SDSFile(self.filename, self.archiveRoot)
        qualityFile.quality = "Q"

        # Create directories for the pruned file (quality Q)
        if not os.path.exists(qualityFile.directory):
            os.makedirs(qualityFile.directory)

        # Get neighbours
        neighbours = list(map(lambda x: x.filepath, self.neighbours))

        # Check if overlap needs to be removed
        if removeOverlap:
            pruneFlag = "-Ps"
        else:
            pruneFlag = "-Pe"

        # Create a dataselect process
        # -Ps prunes to sample level
        # -Q set quality indicator to Q
        # -ts, -te are start & end time of sample respectively (INCLUSIVE)
        # -szs remove records with 0 samples (may result in empty pruned files)
        # -o - write to stdout
        dataselect = subprocess.Popen([
            "dataselect",
            pruneFlag,
            "-Q", "Q",
            "-ts", self.sampleStart,
            "-te", self.sampleEnd,
            "-szs",
            "-o", "-",
        ] + neighbours, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)

        # Open a msrepack process and connect stdin to dataselect stdout
        # -R repack record size to recordLength
        # -o output file for pruned data
        # - read from STDIN
        msrepack = subprocess.Popen([
           "msrepack",
           "-R", str(recordLength),
           "-o", qualityFile.filepath,
           "-"
        ], stdin=dataselect.stdout, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

        # Not sure why we need this
        dataselect.stdout.close()

        # Wait for child processes to terminate
        if msrepack.wait() != 0 or dataselect.wait() != 0:
            raise Exception("Unable to prune file (dataselect returned %s, msrepack returned %s)" % (
                            str(dataselect.returncode), str(msrepack.returncode)))

        # Check that quality file has been created
        if os.path.exists(qualityFile.filepath):
            self.logger.debug("Created pruned file %s" % qualityFile.filename)
        else:
            raise Exception("Pruned file %s has not been created!" % qualityFile.filename)

    def __getAdjacentFile(self, direction):
        """
        def SDSFile::__getAdjacentFile
        Private function that returns adjacent SDSFile based on direction
        """

        newDate = self.start + timedelta(days=direction)

        # The year and day may change
        newYear = newDate.strftime("%Y")
        newDay = newDate.strftime("%j")

        newFilename = ".".join([
            self.net,
            self.sta,
            self.loc,
            self.cha,
            self.quality,
            newYear,
            newDay
        ])

        return SDSFile(newFilename, self.archiveRoot)
