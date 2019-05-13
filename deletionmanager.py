#!/usr/bin/env python3

"""
Script that runs the rules for file deletion.
"""

import logging
import argparse

import core.logging
from core.rulemanager import RuleManager
from orfeus.sdsfile import SDSFile
import rules.sdsrules as sdsrules
import conditions.sdsconditions as sdsconditions


def main():
    try:
        # Initialize logger
        logger = logging.getLogger(__name__)
        logger.info("Running deletion Manager.")

        # Parse command line arguments
        parser = argparse.ArgumentParser()
        parser.add_argument("--rulemap", help="set custom rule map file")
        parser.add_argument("--ruleseq", help="set custom rule sequence file")
        parser.add_argument("--delfile", help="list of files to be deleted")
        parsedargs = vars(parser.parse_args())

        # Check parameters
        if parsedargs["delfile"] is None:
            return print("An input file list needs to be specified using --delfile")
        if parsedargs["rulemap"] is None:
            return print("A rulemap needs to be specified using --rulemap")
        if parsedargs["ruleseq"] is None:
            return print("A rule sequence needs to be specified using --ruleseq")

        # Set up rules
        RM = RuleManager()
        RM.loadRules(sdsrules, sdsconditions, parsedargs["rulemap"], parsedargs["ruleseq"])

        # Collect files
        files = []
        with open(parsedargs["delfile"]) as del_list:
            for line in del_list:
                files.append(SDSFile(line.strip(), None))

        # Apply the sequence of rules on files
        RM.sequence(files)

        logger.info("Finished Deletion Manager execution.")

    except Exception as e:
        logger.error('General error!: "%s"' % e, exc_info=True)

if __name__ == "__main__":
    main()
