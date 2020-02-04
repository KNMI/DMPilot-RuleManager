#!/usr/bin/env python3

"""
Script that runs the rules for managing the SDS archive.
"""

import logging
import argparse

import core.logger
from core.rulemanager import RuleManager
from orfeus.sdscollector import SDSFileCollector
import rules.sdsrules as sdsrules
import conditions.sdsconditions as sdsconditions

from configuration import config


def main():
    try:
        # Initialize logger
        logger = logging.getLogger('RuleManager')
        logger.info("Running SDS Manager.")

        # Parse command line arguments
        parser = argparse.ArgumentParser()
        parser.add_argument("--dir",
                            help=("directory containing the files to process "
                                  "(defaults to the value in configuration.py)"),
                            default=config["DATA_DIR"])
        parser.add_argument("--ruleseq", help="rule sequence file", required=True)
        parser.add_argument("--collect_wildcards", nargs='+',
                            help=("files to collect, defined by a wildcards string "
                                  "(within single quotes!)"))
        parser.add_argument("--from_file",
                            help="files to collect, listed in a text file or stdin '-'",
                            type=argparse.FileType('r'))
        parser.add_argument("--collect_finished",
                            help=("collect all files with modification date older that last "
                                  "midnight plus the given number of minutes"),
                            type=int)
        parsedargs = vars(parser.parse_args())

        # Check collection parameters
        if (parsedargs["collect_wildcards"] is None
            and parsedargs["from_file"] is None
            and parsedargs["collect_finished"] is None):
            return print("Files to collect need to be specified using "
                         "--collect_wildcards, --from_file, or --collect_finished")

        # Set up rules
        RM = RuleManager()
        RM.loadRules(sdsrules, sdsconditions, parsedargs["ruleseq"])

        # Collect files
        fileCollector = SDSFileCollector(parsedargs["dir"])

        if parsedargs["collect_wildcards"] is not None:
            fileCollector.filterFromWildcardsArray(parsedargs["collect_wildcards"])
        if parsedargs["from_file"] is not None:
            filename_list = []
            with parsedargs["from_file"] as list_file:
                for line in list_file:
                    filename_list.append(line.strip())
            fileCollector.filterFromFileList(filename_list)
        if parsedargs["collect_finished"] is not None:
            fileCollector.filterFinishedFiles(parsedargs["collect_finished"])

        # Apply the sequence of rules on files
        RM.sequence(fileCollector.files)

        logger.info("Finished SDS Manager execution.")

    except Exception as e:
        logger.error('General error!: "%s"' % e, exc_info=True)


if __name__ == "__main__":
    main()
