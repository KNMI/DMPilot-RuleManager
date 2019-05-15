import logging


class Rule():
    """
    Class Rule
    Container for a single rule with a rule and conditions to be asserted
    """

    def __init__(self, call, conditions):
        """
        Rule.__init__ 
        Initializes a rule with a rule and condition
        """
        self.call = call
        self.conditions = conditions

        # Initialize logger
        self.logger = logging.getLogger(__name__)

    def apply(self, SDSFile):
        """
        Rule.apply
        Applies a given rule and conditions to a file
        """

        # Assert the conditions
        self.assertPolicies(SDSFile)

        # Call the rule
        self.call(SDSFile)

    def assertPolicies(self, SDSFile):
        """
        Rule.assertPolicies
        Asserts whether all conditions evaluate to True
        """

        # Go over each configured condition and assert the condition evaluates to True
        for condition in self.conditions:
            self.logger.debug("%s: Asserting condition '%s'." % (SDSFile.filename, condition.func.__name__))
            if not condition(SDSFile):

                # If a __wrapped__ attribute exists the function was inverted
                if "__wrapped__" in dir(condition.func):
                  raise AssertionError("!%s" % condition.func.__name__)
                else:
                  raise AssertionError(condition.func.__name__)
