#!/usr/bin/env python3

"""
Script that runs the rules for managing the SDS archive.
"""

import argparse

from core.rulemanager import RuleManager
from orfeus.sdscollector import SDSFileCollector
import rules.sdsrules as sdsrules
import policies.sdspolicies as sdspolicies

from modules.psdcollector import psdCollector

def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("--dir", help="directory containing the files to process")
    parser.add_argument("--rulemap", help="set custom rule map file")
    parsedargs = vars(parser.parse_args())

    # Check parameters
    if parsedargs["dir"] is None:
        return print("A directory needs to be specified using --dir")
    if parsedargs["rulemap"] is None:
        return print("A rulemap needs to be specified using --rulemap")

    # Set up rules
    RM = RuleManager()
    RM.loadRules(sdsrules, sdspolicies, parsedargs["rulemap"])

    # Collect files
    fileCollector = SDSFileCollector(parsedargs["dir"])
    files = fileCollector.collectFromWildcards("*.*.*.BHZ.*.*.022")

    # Apply the sequence of rules on files
    RM.sequence(files)

if __name__ == "__main__":
    main()
