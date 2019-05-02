import logging
import signal
import json

from functools import partial, wraps
from core.rule import Rule


class RuleManager():

    """
    Class RuleManager
    Main manager class for rule functions
    """

    def __init__(self):

        # Initialize logger
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s %(name)s %(levelname)s %(message)s"
        )
        self.logger = logging.getLogger(__name__)
        self.logger.info("Initializing the Rule Manager.")

        self.rules = None
        self.policies = None
        self.ruleSequence = None

    def __signalHandler(self, signum, frame):
        """
        Collector.__signalHandler
        Raise an exception when a signal SIGALRM was received
        """

        raise TimeoutError("Metric calculation has timed out.")

    def loadRules(self, ruleModule, policyModule, ruleMapFile, ruleSequenceFile):
        """Loads the rules.

        Parameters
        ----------
        ruleModule : module
            A module containing all the rule handling functions.
        ruleMapFile : `str`
            The path for a JSON file defining which rules to run, their order, and their options.
        """

        # Load the Python scripted rules and policies
        self.rules = ruleModule
        self.policies = policyModule

        # Load the configured sequence of rules
        rule_desc = None    # Rule configuration
        rule_seq = None     # Rule order

        try:
            with open(ruleMapFile) as rule_file:
                rule_desc = json.load(rule_file)
        except IOError:
            raise IOError("The rulemap %s could not be found." % ruleMapFile)

        try:
            with open(ruleSequenceFile) as order_file:
                rule_seq = json.load(order_file)
        except IOError:
            raise IOError("The rule sequence file %s could not be found." % ruleSequenceFile)

        # Get the rule from the map
        self.ruleSequence = map(lambda x: rule_desc.get(x), rule_seq)

        if None in self.ruleSequence:
            raise ValueError("A rule sequence parameter could not be found in the rule map.")

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
                raise NotImplementedError(
                    "Python rule for configured sequence item %s does not exist." %
                    item)

            # TODO check policies

            # The rule must be callable (function) too
            if not callable(rule.call):
                raise ValueError(
                    "Python rule for configured sequence item %s is not callable." %
                    item)

    def bindOptions(self, definitions, item):
        """
        Def RuleManager.bindOptions
        Binds options to a function call
        """

        def invert(f):
            @wraps(f)
            def g(*args, **kwargs):
                return not f(*args, **kwargs)
            return g

        # Invert the boolean result from the policy
        if (definitions == self.policies) and item["name"].startswith("!"):
            return partial(invert(getattr(definitions, item["name"][1:])), item["options"])
        else:
            return partial(getattr(definitions, item["name"]), item["options"])

    def getRule(self, rule):
        """
        Def RuleManager.getRule
        Returns specific rule from name
        """

        # Bind the rule options to the function call
        # There may be multiple policies defined per rule
        return Rule(
            self.bindOptions(self.rules, rule),
            map(lambda x: self.bindOptions(self.policies, x), rule["policies"])
        )

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

            self.logger.info("Processing item %s (%d/%d)." % (item.filename, i, total))

            # Get the sequence of rules to be applied
            for rule in map(self.getRule, self.ruleSequence):

                # Set a signal (hardcoded at 2min for now)
                signal.signal(signal.SIGALRM, self.__signalHandler)
                signal.alarm(rule.TIMEOUT_SECONDS)

                # Rule options are bound to the call
                try:
                    rule.apply(item)

                # The rule was timed out
                except TimeoutError:
                    logging.info("Timeout calling rule %s." % rule.call.func.__name__)

                # Policy assertion errors
                except AssertionError as e:
                    logging.info("Not executing rule %s. Rule did not pass policy %s." % (rule.call.func.__name__, e))

                # Other exceptions
                except Exception as e:
                    logging.error("Rule execution %s failed: %s" % (rule.call.func.__name__, e))

                # Disable the alarm
                finally:
                    signal.alarm(0)
