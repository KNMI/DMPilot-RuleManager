import logging


class Rule():
    """
    Class Rule
    Container for a single rule with a rule and conditions to be asserted
    """

    def __init__(self, call, conditions, name=None):
        """
        Rule.__init__
        Initializes a rule with a rule and condition
        """
        self.call = call
        self.conditions = conditions
        self.name = name

        # Initialize logger
        self.logger = logging.getLogger("RuleManager")

    def apply(self, SDSFile):
        """
        Rule.apply
        Applies a given rule and conditions to a file
        """

        # Assert the conditions
        self.assert_policies(SDSFile)

        # Call the rule
        self.call(SDSFile)

    def assert_policies(self, sds_file):
        """Assert whether all conditions evaluate to True."""

        # Go over each configured condition and assert the condition evaluates to True
        for condition in self.conditions:
            self.logger.debug("%s: Asserting condition '%s'." % (sds_file.filename,
                                                                 condition.func.__name__))
            if not condition(sds_file):

                # If a __wrapped__ attribute exists the function was inverted
                if "__wrapped__" in dir(condition.func):
                    raise AssertionError("!%s" % condition.func.__name__)
                else:
                    raise AssertionError(condition.func.__name__)
