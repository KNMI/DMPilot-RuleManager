"""

  orfeus/filecollector.py
  Used for collecting files from an SDS archive based on dates

  Author: Mathijs Koymans, 2019
  Copyright: ORFEUS Data Center, 2019

"""

import logging
from fnmatch import fnmatch
from datetime import date, datetime, timedelta
from itertools import repeat
import dateutil.parser as parser

from orfeus.sdsfile import SDSFile
from orfeus.filecollector import FileCollector


class SDSFileCollector(FileCollector):

    """
    Class SDSFileCollector
    Used for collecting files from an SDS archive based on time and/or filename
    """

    files = []

    def __init__(self, archiveDir):
        """
        def fileCollector.__init__
        Initializes a file collector class
        """

        # Initialize logger
        self.logger = logging.getLogger('RuleManager')

        # Collects all filenames
        super().__init__(archiveDir)

        # Process filenames
        sds_files = []
        for filename in self.files:
            try:
                sds_files.append(SDSFile(filename, self.archiveDir))
            except Exception as e:
                self.logger.debug("Unable to parse file '%s' as SDSFile: '%s'" % (filename,
                                                                                  str(e)))
        self.files = sds_files

    def _collectFromDate(self, iDate, mode="file_name"):
        """
        Collects SDS files for a particular date, based on file's name or on
        file's modification time
        """

        # Parse provided date if necessary
        if not isinstance(iDate, datetime) and not isinstance(iDate, date):
            iDate = parser.parse(iDate)

        if mode == "file_name":

            # Extract the julian day and year
            day = iDate.strftime("%j")
            year = iDate.strftime("%Y")
            self.logger.debug("Searching files whose name's date is '%s.%s'" % (year, day))

            # Filter by day and year
            files = list(filter(lambda x: (x.day == day and x.year == year), self.files))

        elif mode == "mod_time":

            # Extract start and end of the date
            date_start = datetime(iDate.year, iDate.month, iDate.day)
            date_end = date_start + timedelta(days=1)
            self.logger.debug("Searching files modified between '%s' and '%s'" % (date_start, date_end))

            # Filter by modification time
            files = list(filter(lambda x: (x.modified >= date_start and x.modified < date_end),
                                self.files))

        else:
            raise ValueError("Unsupported mode %s requested to find files." % mode)

        self.logger.debug("Found %d files using '%s' mode." % (len(files), mode))
        return files

    def _collectFromWildcards(self, filename):
        """Collects SDS files based on a filename that allows wildcards."""

        # Check if an SDS file was specified
        if len(filename.split(".")) != 7:
            raise ValueError("An invalid expression was submitted: %s" % filename)

        self.logger.debug("Searching files whose name fits in '%s'" % filename)

        # Take the basename and map to SDSFile
        files = list(filter(lambda x: fnmatch(x.filename, filename), self.files))

        self.logger.debug("Found %d files for this filename/wildcard." % len(files))
        return files

    def filterFromWildcardsArray(self, wildcards_array):
        """Filters SDS files based on an array of filenames that allow
        wildcards. Accepts all files that match at least one of the
        wildcards."""

        self.logger.debug("Searching files for a list of %d filenames/wildcards" % len(wildcards_array))

        # Aggregate files found for each wildcards combination
        collectedFiles = list()
        for wildcards in wildcards_array:
            collectedFiles += self._collectFromWildcards(wildcards)
        self.files = list(set(collectedFiles))

        self.logger.debug("Found %d unique files in total." % len(self.files))

    def filterFinishedFiles(self, tolerance):
        """Filters all SDS files with modification timestamp older than last
        midnight + tolerance minutes."""

        # Compute maximum allowed time
        timestamp = (datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
                     + timedelta(minutes=tolerance))
        self.logger.debug("Searching for files modified before %s" % timestamp.isoformat())

        # Select files by modification date
        self.files = list(filter(lambda f: f.modified < timestamp, self.files))
        self.logger.debug("Found %d files." % len(self.files))

    def filterFromDateRange(self, iDate, days, mode="file_name"):
        """Filters files from a range of dates;
            if days > 0: [date, date + N - 1]
            if days == 0: nothing
            if days < 0: [date - N, date - 1]
        """

        # Parse provided date if necessary
        if not isinstance(iDate, datetime) and not isinstance(iDate, date):
            iDate = parser.parse(iDate)

        collectedFiles = list()

        # Go over every day in increasing order, skipping "date" if days is negative
        if days > 0:
            start = 0
            stop = start + days
        else:
            start = days
            stop = 0
        for day in range(start, stop):
            collectedFiles += self._collectFromDate(iDate + timedelta(days=day), mode=mode)

        self.files = collectedFiles

    def filterFromPastDays(self, days, mode='file_name'):
        """Filters files from N days in the past: [today - N, yesterday]"""

        # Negative days, skipping today
        self.filterFromDateRange(datetime.now(), -days, mode=mode)

    def filterFromFileList(self, file_list):
        """Filter files that are in a list of filenames."""

        self.files = list(filter(lambda x: x.filename in file_list, self.files))

    def sortFiles(self, order):
        """Sort files by filename."""
        self.logger.debug("Sorting files by filename (%s)" % order)
        self.files = sorted(self.files, key=lambda sdsfile: sdsfile.filename,
                            reverse=(order == "desc"))
        self.logger.debug("Sorting finished")
