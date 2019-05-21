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


def main():
    try:
        # Initialize logger
        logger = logging.getLogger(__name__)
        logger.info("Running SDS Manager.")

        # Parse command line arguments
        parser = argparse.ArgumentParser()
        parser.add_argument("--dir", help="directory containing the files to process", required=True)
        parser.add_argument("--rulemap", help="rule map file", required=True)
        parser.add_argument("--ruleseq", help="rule sequence file", required=True)
        parser.add_argument("--collect_wildcards", help="files to collect, defined by a wildcards string (within single quotes!)")
        parser.add_argument("--from_file", help="files to collect, listed in a text file or stdin '-'", type=argparse.FileType('r'))
        parsedargs = vars(parser.parse_args())

        # Check collection parameters
        if parsedargs["collect_wildcards"] is None and parsedargs["from_file"] is None:
            return print("Files to collect need to be specified using --collect_wildcards or --from_file")

        # Set up rules
        RM = RuleManager()
        RM.loadRules(sdsrules, sdsconditions, parsedargs["rulemap"], parsedargs["ruleseq"])

        # Collect files
        fileCollector = SDSFileCollector(parsedargs["dir"])

        files = None
        if parsedargs["collect_wildcards"] is not None:
            files = fileCollector.collectFromWildcards(parsedargs["collect_wildcards"])
        elif parsedargs["from_file"] is not None:
            filename_list = []
            with parsedargs["from_file"] as list_file:
                for line in list_file:
                    filename_list.append(line.strip())
            files = fileCollector.collectFromFileList(filename_list)

        # Apply the sequence of rules on files
        RM.sequence(files)

        logger.info("Finished SDS Manager execution.")

    except Exception as e:
        logger.error('General error!: "%s"' % e, exc_info=True)

if __name__ == "__main__":
    main()
