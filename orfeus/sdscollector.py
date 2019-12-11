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
    Used for collecting files from an SDS archive based on time
    """

    files = []

    def __init__(self, archiveDir):
        """
        def fileCollector.__init__
        Initializes a file collector class
        """

        # Initialize logger
        self.logger = logging.getLogger('RuleManager')

        super().__init__(archiveDir)
        self.files = self.collectAll()

    def collectAll(self):
        """
        def fileCollector.collectAll
        Returns all files in the SDS archive
        """
        files = []
        for f in self.files:
            try:
                files.append(SDSFile(f, self.archiveDir))
            except Exception as e:
                self.logger.debug("Unable to parse file '%s' as SDSFile: '%s'" % (f, str(e)))
        return files

    def collectFromDate(self, iDate, mode="file_name"):
        """
        def fileCollector.collectFromDate
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
            files = list(filter(lambda x: (x.modified >= date_start and x.modified < date_end), self.files))

        else:
            raise ValueError("Unsupported mode %s requested to find files." % mode)

        self.logger.debug("Found %d files using '%s' mode." % (len(files), mode))

        return files

    def collectFromWildcards(self, filename):
        """
        def fileCollector.collectFromWildcards
        Collects SDS files based on a filename that allows wildcards
        """

        # Check if an SDS file was specified
        if len(filename.split(".")) != 7:
            raise ValueError("An invalid expression was submitted: %s" % filename)

        self.logger.debug("Searching files whose name fits in '%s'" % filename)

        # Take the basename and map to SDSFile
        files = list(filter(lambda x: fnmatch(x.filename, filename), self.files))

        self.logger.debug("Found %d files." % len(files))
        return files

    def collectFromDateRange(self, iDate, days, mode="file_name"):
        """
        def collectFromDateRange
        Collects files from a range of dates;
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
            stop =  0
        for day in range(start, stop):
            collectedFiles += self.collectFromDate(iDate + timedelta(days=day), mode=mode)

        return collectedFiles

    def collectFromPastDays(self, days, mode='file_name'):
        """
        def collectFromPastDays
        Collects files from N days in the past: [today - N, yesterday]
        """

        # Negative days, skipping today
        return self.collectFromDateRange(datetime.now(), -days, mode=mode)

    def collectFromFileList(self, file_list):
        """Collect files from a list of filenames."""

        return list(filter(lambda x: x.filename in file_list,
                           self.files))
