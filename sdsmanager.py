#!/usr/bin/env python3

"""
Script that runs the rules for managing the SDS archive.
"""

import logging
import argparse

import core.logger
from core.rulemanager import RuleManager
from sds.sdscollector import SDSFileCollector
import rules.sdsrules as sdsrules
import conditions.sdsconditions as sdsconditions

from configuration import config


def main():
    try:
        # Initialize logger
        logger = logging.getLogger("RuleManager")
        logger.info("Running SDS Manager.")

        # Parse command line arguments
        parser = argparse.ArgumentParser()
        parser.add_argument("--dir",
                            help=("directory containing the files to process "
                                  "(defaults to the value in configuration.py)"),
                            default=config["DATA_DIR"])
        parser.add_argument("--ruleseq", help="rule sequence file", required=True)
        parser.add_argument("--collect_wildcards", nargs="+",
                            help=("files to collect, defined by one or more wildcard string(s) "
                                  "within quotes (in the case of more than one string, any file "
                                  "that matches at least one of them is collected)"))
        parser.add_argument("--from_file",
                            help="files to collect, listed in a text file or stdin '-'",
                            type=argparse.FileType("r"))
        parser.add_argument("--collect_finished",
                            help=("collect all files with modification date older that last "
                                  "midnight plus the given number of minutes"),
                            type=int)
        parser.add_argument("--sort",
                            help=("whether (and how) to sort collected files "
                                  "by name before processing them "
                                  "(defaults to none)"),
                            choices=["none", "asc", "desc"],
                            default="none")
        parsedargs = vars(parser.parse_args())

        # Check collection parameters
        if (parsedargs["collect_wildcards"] is None
            and parsedargs["from_file"] is None
            and parsedargs["collect_finished"] is None):
            return print("Files to collect need to be specified using "
                         "--collect_wildcards, --from_file, and/or --collect_finished")

        # Set up rules
        RM = RuleManager()
        RM.load_rules(sdsrules, sdsconditions, parsedargs["ruleseq"])

        # Collect files
        file_collector = SDSFileCollector(parsedargs["dir"])

        if parsedargs["collect_wildcards"] is not None:
            file_collector.filter_from_wildcards_array(parsedargs["collect_wildcards"])
        if parsedargs["from_file"] is not None:
            filename_list = []
            with parsedargs["from_file"] as list_file:
                for line in list_file:
                    filename_list.append(line.strip())
            file_collector.filter_from_file_list(filename_list)
        if parsedargs["collect_finished"] is not None:
            file_collector.filter_finished_files(parsedargs["collect_finished"])

        # Sort files alphabetically
        if parsedargs["sort"] != "none":
            file_collector.sort_files(parsedargs["sort"])

        # Apply the sequence of rules on files
        RM.sequence(file_collector.files)

        logger.info("Finished SDS Manager execution.")

    except Exception as e:
        logger.error("General error!: '%s'" % e, exc_info=True)


if __name__ == "__main__":
    main()
