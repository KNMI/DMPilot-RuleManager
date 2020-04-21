"""
"""

import os
import logging
from datetime import datetime


class FileCollector:

    """
    Class FileCollector
    Used for collecting files from a directory
    """

    def __init__(self, archive_dir):
        """Initialize a file collector class."""

        # Initialize logger
        self.logger = logging.getLogger("RuleManager")
        self.logger.debug("Initializing the %s." % self.__class__.__name__)

        self._initialized = datetime.now()
        self.archive_dir = archive_dir

        # During initialization collect all files in the archive
        # This may be optimized when required
        self.files = self.collect_all_files()

    def collect_all_files(self):
        """Store all files in the directory."""

        collected_files = list()

        # Walk over the directory and find all files
        for subdir, dirs, files in os.walk(self.archive_dir):
            for file in files:
                try:
                    collected_files.append(file)
                except ValueError:
                    pass

        return collected_files
