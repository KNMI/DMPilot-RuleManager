"""

  orfeus/filecollector.py
  Used for collecting files from an SDS archive based on dates

  Author: Mathijs Koymans, 2019
  Copyright: ORFEUS Data Center, 2019

"""

from fnmatch import fnmatch
from datetime import datetime, timedelta
from itertools import repeat
import dateutil.parser as parser

from orfeus.sdsfile import SDSFile
from orfeus.filecollector import FileCollector


class SDSFileCollector(FileCollector):

    """
    Class SDSFileCollector
    Used for collecting files from an SDS archive based on time
    """

    def __init__(self, archiveDir):
        """
        def fileCollector.__init__
        Initializes a file collector class
        """

        super().__init__(archiveDir)
        self.files = self.collectAll()

    def collectAll(self):
        """
        def fileCollector.collectAll
        Returns all files in the SDS archive
        """

        return list(map(SDSFile, self.files, repeat(self.archiveDir)))

    def collectFromDate(self, date, mode='file_name'):
        """
        def fileCollector.collectFromDate
        Collects SDS files for a particular date, based on file's name or on
        file's modification time
        """

        # Parse provided date if necessary
        if not isinstance(date, datetime):
            date = parser.parse(date)

        if mode == 'file_name':

            # Extract the julian day and year
            day = date.strftime("%j")
            year = date.strftime("%Y")
            self.logger.debug("Searching files whose name's date is '%s.%s'" % (year, day))

            # Filter by day and year
            files = list(filter(lambda x: (x.day == day and x.year == year), self.files))

        elif mode == 'mod_time':

            # Extract start and end of the date
            date_start = datetime(date.year, date.month, date.day)
            date_end = date_start + timedelta(days=1)
            self.logger.debug("Searching files modified between '%s' and '%s'" % (date_start, date_end))

            # Filter by modification time
            files = list(filter(lambda x: (x.modified >= date_start and x.modified < date_end), self.files))

        self.logger.debug("Found %d files." % len(files))
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

    def collectFromDateRange(self, date, days, mode='file_name'):
        """
        def collectFromPast
        Collects files from N days in the past
        """

        collectedFiles = list()

        # Go over every day (skip today)
        for day in range(1, abs(days)):

            if(days > 0):
                collectedFiles += self.collectFromDate(date + timedelta(days=day), mode)
            else:
                collectedFiles += self.collectFromDate(date - timedelta(days=day), mode)

        return collectedFiles

    def collectFromPastDays(self, days, mode='file_name'):
        """
        def collectFromPast
        Collects files from N days in the past
        """

        # Negative days
        return self.collectFromDateRange(datetime.now(), -days, mode)
