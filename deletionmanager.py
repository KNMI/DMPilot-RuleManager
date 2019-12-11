#!/usr/bin/env python3

"""
Script that runs the rules for file deletion.
"""

import logging
import argparse

import core.logger
from core.rulemanager import RuleManager
from orfeus.sdsfile import SDSFile
from core.database import deletion_database
import rules.sdsrules as sdsrules
import conditions.sdsconditions as sdsconditions


def main():
    try:
        # Initialize logger
        logger = logging.getLogger('RuleManager')
        logger.info("Running deletion Manager.")

        # Parse command line arguments
        parser = argparse.ArgumentParser()
        parser.add_argument("--dir", help="temporary archive directory", required=True)
        parser.add_argument("--rulemap", help="rule map file", required=True)
        parser.add_argument("--ruleseq", help="rule sequence file", required=True)
        parser.add_argument("--from_file", help="files to delete, listed in a text file or stdin '-'", type=argparse.FileType('r'), required=True)
        parsedargs = vars(parser.parse_args())

        # Set up rules
        RM = RuleManager()
        RM.loadRules(sdsrules, sdsconditions, parsedargs["rulemap"], parsedargs["ruleseq"])

        # Collect new files to delete
        with parsedargs["from_file"] as del_list:
            for line in del_list:
                deletion_database.add_filename(line.strip())

        # Get all files from database
        files = [SDSFile(filename, parsedargs["dir"]) for filename in deletion_database.get_all_filenames()]
        logger.debug("Collected %d files for deletion" % len(files))

        # Apply the sequence of rules on files
        RM.sequence(files)

        logger.info("Finished Deletion Manager execution.")

    except Exception as e:
        logger.error('General error!: "%s"' % e, exc_info=True)

if __name__ == "__main__":
    main()
