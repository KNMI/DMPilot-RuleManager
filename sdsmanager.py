#!/usr/bin/env python3

"""
Script that runs the rules for managing the SDS archive.
"""

import logging
import argparse

import core.logging
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
        parser.add_argument("--dir", help="directory containing the files to process")
        parser.add_argument("--rulemap", help="set custom rule map file")
        parser.add_argument("--ruleseq", help="set custom rule sequence file")
        parsedargs = vars(parser.parse_args())

        # Check parameters
        if parsedargs["dir"] is None:
            return print("A directory needs to be specified using --dir")
        if parsedargs["rulemap"] is None:
            return print("A rulemap needs to be specified using --rulemap")
        if parsedargs["ruleseq"] is None:
            return print("A rule sequence needs to be specified using --ruleseq")

        # Set up rules
        RM = RuleManager()
        RM.loadRules(sdsrules, sdsconditions, parsedargs["rulemap"], parsedargs["ruleseq"])

        # Collect files
        fileCollector = SDSFileCollector(parsedargs["dir"])
        files = fileCollector.collectFromWildcards("*.*.*.BHZ.*.*.022")

        # Apply the sequence of rules on files
        RM.sequence(files)

        logger.info("Finished SDS Manager execution.")

    except Exception as e:
        logger.error('General error!: "%s"' % e, exc_info=True)

if __name__ == "__main__":
    main()
