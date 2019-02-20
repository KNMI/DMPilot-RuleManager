#!/usr/bin/env python3

"""
Main script for running the rule manager.

Usage: First, edit configuration.py and rules.json. The first file
defines the options for running the script, and the second defines
which rules are executed on each file, their options, and their order.
After that, just run this file as a script, with no arguments.
"""

import logging
import json
import sys

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(name)s %(levelname)s %(message)s"
)

from rules import RuleFunctions
from orfeus.filecollector import SDSFileCollector
from functools import partial

logger = logging.getLogger(__name__)


class RuleManager():

    """
    Class RuleManager
    Main manager class for rule functions
    """

    def __init__(self):

        logger.info("Initializing the Rule Manager.")

        # Load the configured sequence of rules
        with open("rules.json") as infile:
            self.ruleSequence = json.load(infile)

        # Load the Python scripted rules
        self.rules = RuleFunctions()

        # Check if the rules are valid
        self.__checkRuleSequence()


    def __checkRuleSequence(self):
        """
        Def RuleManager.__checkRuleSequence
        Checks validity of the configured rule sequence
        """

        # Check each rule that it exists & is a callable Python function
        for item in self.ruleSequence:

            # Check if the rule exists
            try:
                rule = self.getRule(item)
            except AttributeError:
                raise AttributeError(
                    "Python rule for configured sequence item %s does not exist." %
                    item)

            # The rule must be callable (function) too
            if not callable(rule):
                raise ValueError(
                    "Python rule for configured sequence item %s is not callable." %
                    item)


    def getRule(self, rule):
        """
        Def RuleManager.getRule
        Returns specific rule from name
        """

        # Bind the rule options to the function call
        return partial(getattr(self.rules, rule["name"]), rule["options"])


    def sequence(self, files):
        """
        Def RuleManager.sequence
        """

        # Get the sequence of rules to be applied
        for ruleCall in map(self.getRule, self.ruleSequence):
            
            for SDSFile in files:

                # Rule options are bound to the call
                ruleCall(SDSFile)


    def initialize(self):
        """
        Def RuleManager.initialize
        Initializes the rule manager
        """

        # Open a file collector instance
        self.fileCollector = SDSFileCollector()

        # EXAMPLE XXX Collect a bunch of files
        files = self.fileCollector.collectFromWildcards("*.*.*.*.*.*.*")
        files = [files[4]]

        # Apply the sequence of rules
        self.sequence(files)


if __name__ == "__main__":

    RM = RuleManager()
    RM.initialize()
