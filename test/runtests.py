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

        """
        def setUp
        Sets up the TestRuleManager test suite
        """

        # Create a rule manager class
        self.RM = RuleManager()

        # Load the 
        self.RM.loadRules(testrules, testconditions, "rules.json", "rule_seq.json")

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
