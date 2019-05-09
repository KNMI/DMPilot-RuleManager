import unittest
import sys
import testconditions, testrules

# Patch to parent folder to import code
sys.path.append("..")

from orfeus.sdsfile import SDSFile
from core.rulemanager import RuleManager

class TestRuleManager(unittest.TestCase):

    # Create a mock SDSFile
    SDSFILE = SDSFile("NL.HGN.02.BHZ.D.1970.001", "/data/temp_archive/SDS/")

    def setUp(self):

        # Create a rule manager class
        self.RM = RuleManager()

        # Load the 
        self.RM.loadRules(testrules, testconditions, "rules.json", "rule_seq.json")

    def test_sdsfile_invalid(self):

        """
        def test_sdsfile_invalid
        expects exception to be raised when an invalid SDS filename is submitted
        """
        
        # Assert that missing day is invalid
        with self.assertRaises(ValueError) as ex:
            SDSFile("NL.HGN.02.BHZ.D.1970", "/data/temp_archive/SDS/")
        
        # Assert the exception
        self.assertEqual("Invalid SDS file submitted.", str(ex.exception.args[0]))
 

    def test_rule_timeout(self):

        """
        def test_rule_timeout
        test mock timeout rule that should raise after 1 second
        """

        # Capture the log
        with self.assertLogs("core.rulemanager", level="WARNING") as cm:
            self.RM.sequence([self.SDSFILE])

        # Assert timeout message in log
        self.assertEqual(cm.output, ["WARNING:core.rulemanager:NL.HGN.02.BHZ.D.1970.001: Timeout calling rule 'timeoutRule'."])


if __name__ == '__main__':
    unittest.main()
