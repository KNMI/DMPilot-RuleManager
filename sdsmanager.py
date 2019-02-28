#!/usr/bin/env python3

"""
Script that runs the rules for managing the SDS archive.
"""

import argparse

from core.rulemanager import RuleManager
from orfeus.sdscollector import SDSFileCollector
import rules.sdsrules as sdsrules

if __name__ == "__main__":
    # Parse command line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("--dir", help="directory containing the files to process")
    parser.add_argument("--rulemap", help="set custom rule map file")
    parsedargs = vars(parser.parse_args())

    # Set up rules
    RM = RuleManager()
    RM.loadRules(sdsrules, parsedargs["rulemap"])

    # Collect files
    fileCollector = SDSFileCollector(parsedargs["dir"])
    files = fileCollector.collectFromWildcards("*.*.*.*.*.*.*")

    # Apply the sequence of rules on files
    RM.sequence(files)
