#!/usr/bin/env python3

"""
Script that collects files and outputs their names.
"""

import logging
import argparse

import core.logger
from orfeus.sdscollector import SDSFileCollector


def main():
    try:
        # Initialize logger
        logger = logging.getLogger(__name__)
        logger.info("Running SDS File Collector.")

        # Parse command line arguments
        parser = argparse.ArgumentParser()
        parser.add_argument("-o", "--output", help="output file")
        parser.add_argument("--dir", help="directory containing the files to collect")
        parser.add_argument("--collect_wildcards", help="files to collect, defined by a wildcards string (within single quotes!)")
        parsedargs = parser.parse_args()

        # Check parameters
        if parsedargs.dir is None:
            return print("A directory needs to be specified using --dir")
        if parsedargs.collect_wildcards is None:
            return print("Files to collect need to be specified using --collect_wildcards")

        # Collect files
        fileCollector = SDSFileCollector(parsedargs.dir)
        files = fileCollector.collectFromWildcards(parsedargs.collect_wildcards)

        if parsedargs.output is None:
            for sds_file in files:
                print(sds_file.filename)
            logger.info("Finished SDS File Collector execution.")

        else:
            with open(parsedargs.output, "w") as list_file:
                for sds_file in files:
                    list_file.write(sds_file.filename + "\n")
            logger.info("Finished SDS File Collector execution. Saved file %s.", parsedargs.output)

    except Exception as e:
        logger.error('General error!: "%s"' % e, exc_info=True)


if __name__ == "__main__":
    main()
