#!/usr/bin/env python3

"""
Script that collects files and outputs their names.
"""

import sys
import logging
import argparse

import core.logger
from orfeus.sdscollector import SDSFileCollector

from configuration import config


def main():
    try:
        # Initialize logger
        logger = logging.getLogger('RuleManager')
        logger.info("Running SDS File Collector.")

        # Parse command line arguments
        parser = argparse.ArgumentParser()
        parser.add_argument("--dir",
                            help=("directory containing the files to collect"
                                  "(defaults to the value in configuration.py)"),
                            default=config["DATA_DIR"])
        parser.add_argument("--collect_wildcards",
                            help=("files to collect, defined by a wildcards string "
                                  "(within single quotes!)"))
        parser.add_argument("--collect_finished",
                            help=("collect all files with modification date older that last "
                                  "midnight plus the given number of minutes"),
                            type=int)
        parser.add_argument("-o", "--output",
                            help="output, a file name or stdout if not provided",
                            type=argparse.FileType('w'), default=sys.stdout)
        parsedargs = parser.parse_args()

        # Check collection parameters
        if (parsedargs.collect_wildcards is None and parsedargs.collect_finished is None):
            return print("Files to collect need to be specified using "
                         "--collect_wildcards or --collect_finished")

        # Collect files
        fileCollector = SDSFileCollector(parsedargs.dir)

        if parsedargs.collect_wildcards is not None:
            fileCollector.filterFromWildcards(parsedargs.collect_wildcards)
        if parsedargs.collect_finished is not None:
            fileCollector.filterFinishedFiles(parsedargs.collect_finished)

        # Write to output (file or stdout)
        with parsedargs.output as list_file:
            for sds_file in fileCollector.files:
                list_file.write(sds_file.filename + "\n")
        logger.info("Finished SDS File Collector execution. Output to '%s'.", parsedargs.output.name)

    except Exception as e:
        logger.error('General error!: "%s"' % e, exc_info=True)


if __name__ == "__main__":
    main()
