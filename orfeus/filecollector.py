"""
"""

import os
from datetime import datetime


class FileCollector:

    """
    Class FileCollector
    Used for collecting files from a directory
    """

    def __init__(self, archiveDir):
        """
        def fileCollector.__init__
        Initializes a file collector class
        """

        self._initialized = datetime.now()
        self.archiveDir = archiveDir

        # During initialization collect all files in the archive
        # This may be optimized when required
        self.files = self.collectAllFiles()

    def collectAllFiles(self):
        """
        def fileCollector.collectAll
        Stores all files in the directory
        """

        collectedFiles = list()

        # Walk over the directory and find all files
        for subdir, dirs, files in os.walk(self.archiveDir):
            for file in files:
                try:
                    collectedFiles.append(file)
                except ValueError:
                    pass

        return collectedFiles
