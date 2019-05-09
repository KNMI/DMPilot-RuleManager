class Rule():
    """
    Class Rule
    Container for a single rule with a rule and conditions to be asserted
    """

    def __init__(self, call, conditions):
        """
        Rule.__init__ 
        Initializes a rule with a rule and policy
        """
        self.call = call
        self.conditions = conditions

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

        # Go over each configured policy and assert the policy evaluates to True
        for policy in self.conditions:
            if not policy(SDSFile):

                # If a __wrapped__ attribut exists the function was inverted
                if "__wrapped__" in dir(policy.func):
                  raise AssertionError("!%s" % policy.func.__name__)
                else:
                  raise AssertionError(policy.func.__name__)
