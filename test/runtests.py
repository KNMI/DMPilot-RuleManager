import os
import sys
import unittest

from datetime import datetime

# Patch to parent folder to import code
sys.path.append("..")

from orfeus.sdsfile import SDSFile
from core.rulemanager import RuleManager

class TestRuleManager(unittest.TestCase):

    # Create a mock SDSFile
    SDSFILE = SDSFile("NL.HGN.02.BHZ.D.1970.001", "/data/temp_archive/SDS/")
    
    def loadSequence(self, sequence):

        """
        def loadSequence
        Wrapper function to load a rule sequence to the Rule Manager instance
        """

        import testconditions
        import testrules

        sequence = os.path.join("sequences", sequence)

        self.RM.loadRules(testrules, testconditions, "rules.json", sequence)

    def setUp(self):

        """
        def setUp
        Sets up the TestRuleManager test suite
        """

        # Create a rule manager class
        self.RM = RuleManager()

    def test_sdsfile_class(self):

        """
        def test_sdsfile
        tests the SDS file class
        """

        # Assert identifiers are OK
        self.assertEqual(self.SDSFILE.net, "NL")
        self.assertEqual(self.SDSFILE.sta, "HGN")
        self.assertEqual(self.SDSFILE.loc, "02")
        self.assertEqual(self.SDSFILE.cha, "BHZ")
        self.assertEqual(self.SDSFILE.quality, "D")
        self.assertEqual(self.SDSFILE.year, "1970")
        self.assertEqual(self.SDSFILE.day, "001")

        # Assert neighbouring files are OK
        self.assertEqual(self.SDSFILE.filename, "NL.HGN.02.BHZ.D.1970.001")
        self.assertEqual(self.SDSFILE.next.filename, "NL.HGN.02.BHZ.D.1970.002")
        self.assertEqual(self.SDSFILE.previous.filename, "NL.HGN.02.BHZ.D.1969.365")

        # Confirm FDSNWS query string for this file
        self.assertEqual(self.SDSFILE.queryString, "?start=1970-01-01T00:00:00&end=1970-01-02T00:00:00&network=NL&station=HGN&location=02&channel=BHZ")

        # Not an infrasound channel
        self.assertFalse(self.SDSFILE.isPressureChannel)

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
 
    def test_rule_exception(self):

        """
        def test_rule_timeout
        test mock timeout rule that raises an exception
        """

        # Load an exception sequence
        self.loadSequence("rule_seq_exception.json")

        # Capture the log
        with self.assertLogs("core.rulemanager", level="ERROR") as cm:
            self.RM.sequence([self.SDSFILE])

        # Assert timeout message in log
        self.assertEqual(cm.output, ["ERROR:core.rulemanager:NL.HGN.02.BHZ.D.1970.001: Rule execution 'exceptionRule' failed: Oops!"])

    def test_rule_conditions(self):

        """
        def test_rule_conditions
        Tests two rule conditions: one that passes and one that fails
        """

        # Load the timeout sequence
        self.loadSequence("rule_seq_conditions.json")

        # Capture the log
        with self.assertLogs("core.rulemanager", level="INFO") as cm:
            self.RM.sequence([self.SDSFILE])

        # Expected log messages
        # First sequence should pass on condition (trueCondition) and execute rule
        # Second sequence should fail on condition (falseCondition)
        expected = ["INFO:core.rulemanager:NL.HGN.02.BHZ.D.1970.001: Successfully executed rule 'passRule'.",
                    "INFO:core.rulemanager:NL.HGN.02.BHZ.D.1970.001: Not executing rule 'passRule'. Rule did not pass policy 'falseCondition'."]

        # Assert log messages equal but skip first processing
        for a, b in zip(cm.output[1:], expected):
            self.assertEqual(a, b);

    def test_rule_condition_exception(self):

        """
        def test_rule_condition_exception
        Rule that raises an exception during execution of condition
        """

        self.loadSequence("rule_seq_condition_exception.json")

        with self.assertLogs("core.rulemanager", level="ERROR") as cm:
            self.RM.sequence([self.SDSFILE])

        self.assertEqual(cm.output, ["ERROR:core.rulemanager:NL.HGN.02.BHZ.D.1970.001: Rule execution 'passRule' failed: Oops!"])

    def test_rule_conditions_options(self):

        """
        def test_rule_conditions_options
        Tests whether options are properly passed to the conditions
        """

        # Load the timeout sequence
        self.loadSequence("rule_seq_condition_options.json")

        # Will raise an exception if options are not properly passed
        with self.assertLogs("core.rulemanager", level="INFO") as cm:
            self.RM.sequence([self.SDSFILE])

        self.assertEqual(cm.output[1:], ["INFO:core.rulemanager:NL.HGN.02.BHZ.D.1970.001: Successfully executed rule 'passRule'."])

    def test_rule_options(self):

        """
        def test_rule_options
        Tests whether options are properly passed to the rule call
        """

        # Load the timeout sequence
        self.loadSequence("rule_seq_options.json")

        # Will raise an exception if options are not properly passed
        with self.assertLogs("core.rulemanager", level="INFO") as cm:
            self.RM.sequence([self.SDSFILE])

        self.assertEqual(cm.output[1:], ["INFO:core.rulemanager:NL.HGN.02.BHZ.D.1970.001: Successfully executed rule 'optionRule'."])

    def test_rule_timeout(self):

        """
        def test_rule_timeout
        test mock timeout rule that should raise after 1 second
        """

        # Load the timeout sequence
        self.loadSequence("rule_seq_timeout.json")

        start = datetime.now()

        # Capture the log
        with self.assertLogs("core.rulemanager", level="WARNING") as cm:
            self.RM.sequence([self.SDSFILE])

        # Assert that the timeout took roughly 1s
        self.assertAlmostEqual(1.0, (datetime.now() - start).total_seconds(), places=2)

        # Assert timeout message in log
        self.assertEqual(cm.output, ["WARNING:core.rulemanager:NL.HGN.02.BHZ.D.1970.001: Timeout calling rule 'timeoutRule'."])


if __name__ == '__main__':
    unittest.main()
