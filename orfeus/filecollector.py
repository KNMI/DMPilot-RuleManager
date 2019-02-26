"""

  orfeus/filecollector.py
  Used for collecting files from an SDS archive based on dates

  Author: Mathijs Koymans, 2019
  Copyright: ORFEUS Data Center, 2019

"""

import os
from fnmatch import fnmatch

from datetime import datetime, timedelta
from orfeus.sdsfile import SDSFile

from configuration import config


class SDSFileCollector():

    """
    Class SDSFileCollector
    Used for collecting files from an SDS archive based on time
    """

    archive = config["ARCHIVE_ROOT"]

    def __init__(self):
        """
        def fileCollector.__init__
        Initializes a file collector class
        """

        self._initialized = datetime.now()

        # During initialization collect all files in the archive
        # This may be optimized when required
        self.files = self.collectAll()

    def collectAll(self):
        """
        def fileCollector.collectAll
        Returns all files in the SDS archive
        """

        collectedFiles = list()

        # Walk over the yearly directory and find all files with the particular julian date
        for subdir, dirs, files in os.walk(self.archive):
            for file in files:
                try:
                    collectedFiles.append(file)
                except ValueError:
                    pass

        return map(SDSFile, collectedFiles)

    def collectFromDate(self, date):
        """
        def fileCollector.collectFromDate
        Collects SDS files for a particular date
        """

        # Extract the julian day and year
        day = date.strftime("%j")
        year = date.strftime("%Y")

        # Filter by day and year
        return list(filter(lambda x: (x.day == day and x.year == year), self.files))

    def collectFromWildcards(self, filename):
        """
        def fileCollector.collectFromWildcards
        Collects SDS files based on a filename that allows wildcards
        """

        # Check if an SDS file was specified
        if len(filename.split(".")) != 7:
            raise ValueError("An invalid expression was submitted: %s" % filename)

        # Take the basename and map to SDSFile
        return list(filter(lambda x: fnmatch(x.filename, filename), self.files))

    def collectFromDateRange(self, date, days):
        """
        def collectFromPast
        Collects files from N days in the past
        """

        collectedFiles = list()

        # Go over every day (skip today)
        for day in range(1, abs(days)):

            if(days > 0):
                collectedFiles += self.collectFromDate(date + timedelta(days=day))
            else:
                collectedFiles += self.collectFromDate(date - timedelta(days=day))

        return collectedFiles

    def collectFromPastDays(self, days):
        """
        def collectFromPast
        Collects files from N days in the past
        """

        # Negative days
        return self.collectFromDateRange(datetime.now(), -days)
