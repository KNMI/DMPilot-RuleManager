import logging
import signal
import json

from functools import partial

class RuleManager():

    """
    Class RuleManager
    Main manager class for rule functions
    """

    RULE_TIMEOUT_SECONDS = 1

    def __init__(self):

        # Initialize logger
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s %(name)s %(levelname)s %(message)s"
        )
        self.logger = logging.getLogger(__name__)
        self.logger.info("Initializing the Rule Manager.")

        self.rules = None
        self.ruleSequence = None

    def __signalHandler(self, signum, frame):
        """
        Collector.__signalHandler
        Raise an exception when a signal SIGALRM was received
        """

        raise TimeoutError("Metric calculation has timed out.")

    def loadRules(self, ruleModule, ruleMapFile):
        """Loads the rules.

        Parameters
        ----------
        ruleModule : module
            A module containing all the rule handling functions.
        ruleMapFile : `str`
            The path for a JSON file defining which rules to run, their order, and their options.
        """

        # Load the Python scripted rules
        self.rules = ruleModule

        # Load the configured sequence of rules
        with open(ruleMapFile) as infile:
            self.ruleSequence = json.load(infile)

        # Check if the rules are valid
        self.__checkRuleSequence(self.ruleSequence)

    def __checkRuleSequence(self, sequence):
        """
        Def RuleManager.__checkRuleSequence
        Checks validity of the configured rule sequence
        """

        # Check each rule that it exists & is a callable Python function
        for item in sequence:

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

    def sequence(self, items):
        """
        Def RuleManager.sequence
        Runs the sequence of rules on the given file list.

        Parameters
        ----------
        items
            An iterable collection of objects that can be processed by the loaded rules.
        """

        total = len(items)
        
        # Items can be SDSFiles or metadata (XML) files
        for i, item in enumerate(items):
            self.logger.info("Processing item %d/%d.", i, total)
            # Get the sequence of rules to be applied
            for ruleCall in map(self.getRule, self.ruleSequence):

                # Set a signal (hardcoded at 2min for now)
                signal.signal(signal.SIGALRM, self.__signalHandler)
                signal.alarm(self.RULE_TIMEOUT_SECONDS)

                # Rule options are bound to the call
                # Capture exceptions (e.g. TimeoutError)
                try:
                    ruleCall(item)
                except TimeoutError:
                    logging.info("Timeout calling rule %s." % ruleCall.func.__name__)
                except Exception as Ex:
                    logging.error(Ex)
                finally:
                    signal.alarm(0)

