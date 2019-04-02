class Rule():
    """
    Class Rule
    Container for a single rule with a rule and policies to be asserted
    """

    TIMEOUT_SECONDS = 10

    def __init__(self, call, policies):
        """
        Rule.__init__ 
        Initializes a rule with a rule and policy
        """
        self.call = call
        self.policies = policies

    def apply(self, SDSFile):
        """
        Rule.apply
        Applies a given rule and policies to a file
        """

        # Assert the policies
        self.assertPolicies(SDSFile)

        # Call the rule
        self.call(SDSFile)

    def assertPolicies(self, SDSFile):
        """
        Rule.assertPolicies
        Asserts whether all policies evaluate to True
        """

        # Go over each configured policy and assert the policy evaluates to True
        for policy in self.policies:
            if not policy(SDSFile):
                raise AssertionError(policy.func.__name__)
