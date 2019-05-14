#!/usr/bin/env python3

import os
import sys
import unittest

from datetime import datetime, timedelta
from unittest.mock import patch, PropertyMock
from obspy import read_inventory

CWD = os.path.abspath(os.path.dirname(__file__))

# Patch to parent folder to import some code
sys.path.append(os.path.dirname(CWD))
from core.rulemanager import RuleManager

from orfeus.sdscollector import SDSFileCollector

# Modules
from modules.psdcollector import psdCollector
from orfeus.sdsfile import SDSFile

# Cleanup
sys.path.pop()

class TestRuleManager(unittest.TestCase):

    """
    Class TestRuleManager
    Test suite for the RDSA Rule Manager
    """

    @classmethod
    def setUpClass(cls):

        """
        def setUpClass
        Sets up the TestRuleManager test suite
        """

        print("Setting up Rule Manager for test suite.")

        # Create mock and real SDSFiles
        cls.SDSMock = cls.createSDSFile(cls, "NL.HGN.02.BHZ.D.1970.001")
        cls.SDSReal = cls.createSDSFile(cls, "NL.HGN.02.BHZ.D.2019.022")

        # Create a rule manager class
        cls.RM = RuleManager()

        # Point file collector to the temporary archive
        cls.FC = SDSFileCollector(os.path.join(CWD, "data", "SDS"))

    def createSDSFile(self, filename):

        return SDSFile(filename, os.path.join(CWD, "data", "SDS"))

    def loadSequence(self, sequence):

        """
        def loadSequence
        Wrapper function to load a rule sequence to the Rule Manager instance
        """

        import conditions.testconditions as testconditions
        import rules.testrules as testrules

        # Load sequence and rules
        self.RM.loadRules(
            testrules,
            testconditions,
            os.path.join(CWD, "rules.json"),
            os.path.join(CWD, "sequences", sequence)
        )

    def test_sdsfile_class(self):

        """
        def test_sdsfile
        tests the SDS file class
        """

        # Assert identifiers are OK
        self.assertEqual(self.SDSMock.net, "NL")
        self.assertEqual(self.SDSMock.sta, "HGN")
        self.assertEqual(self.SDSMock.loc, "02")
        self.assertEqual(self.SDSMock.cha, "BHZ")
        self.assertEqual(self.SDSMock.quality, "D")
        self.assertEqual(self.SDSMock.year, "1970")
        self.assertEqual(self.SDSMock.day, "001")

        # Assert neighbouring files are OK
        self.assertEqual(self.SDSMock.filename, "NL.HGN.02.BHZ.D.1970.001")
        self.assertEqual(self.SDSMock.next.filename, "NL.HGN.02.BHZ.D.1970.002")
        self.assertEqual(self.SDSMock.previous.filename, "NL.HGN.02.BHZ.D.1969.365")

        # Confirm FDSNWS query string for this file
        self.assertEqual(self.SDSMock.queryString, "?start=1970-01-01T00:00:00&end=1970-01-02T00:00:00&network=NL&station=HGN&location=02&channel=BHZ")

        # Not an infrasound channel
        self.assertFalse(self.SDSMock.isPressureChannel)

        # File does not exist
        self.assertEqual(self.SDSMock.created, None)
        self.assertEqual(self.SDSMock.modified, None)
        self.assertEqual(self.SDSMock.size, None)
        self.assertEqual(self.SDSMock.checksum, None)

        # File is real does exist in data test archive
        self.assertIsNotNone(self.SDSReal.created)
        self.assertIsNotNone(self.SDSReal.modified)
        self.assertEqual(self.SDSReal.size, 4571648)
        self.assertEqual(self.SDSReal.checksum, "sha2:yQQ9przMS2Pav5kyTsAtpF2F7aU/TgyRDZa2kTxg6DA=")

        # Confirm dataselect trimming of file and number of samples is expected @ 40Hz
        self.assertEqual(self.SDSReal.samples, 40 * 86400)

        # First sample after 2019-01-22T00:00:00 and end before 2019-01-23T00:00:00
        self.assertTrue(self.SDSReal.traces[0]["start"] > datetime(2019, 1, 22, 0, 0, 0, 0))
        self.assertTrue(self.SDSReal.traces[0]["end"] < datetime(2019, 1, 23, 0, 0, 0, 0))

    def test_sdsfile_invalid(self):

        """
        def test_sdsfile_invalid
        expects exception to be raised when an invalid SDS filename is submitted
        """
        
        # Assert that missing day is invalid
        with self.assertRaises(ValueError) as ex:
            self.createSDSFile("NL.HGN.02.BHZ.D.1970")
        
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
            self.RM.sequence([self.SDSMock])

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
            self.RM.sequence([self.SDSMock])

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
            self.RM.sequence([self.SDSMock])

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
            self.RM.sequence([self.SDSMock])

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
            self.RM.sequence([self.SDSMock])

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
            self.RM.sequence([self.SDSMock])

        # Assert that the timeout took roughly 1s
        self.assertAlmostEqual(1.0, (datetime.now() - start).total_seconds(), places=2)

        # Assert timeout message in log
        self.assertEqual(cm.output, ["WARNING:core.rulemanager:NL.HGN.02.BHZ.D.1970.001: Timeout calling rule 'timeoutRule'."])

    def test_collect_files_days_future_range(self):

        now = datetime.now().date()

        # Create a fake SDSFile for today
        todayFile = self.createSDSFile(now.strftime("NL.HGN.02.BHZ.D.%Y.%j"))

        # Mock files returned by the file collector
        with patch('orfeus.sdscollector.SDSFileCollector.files', new_callable=PropertyMock) as mock_files:

            # Fake SDS file from today, yesterday, and day before
            mock_files.return_value = [
                todayFile.next.next.next,
                todayFile.next.next,
                todayFile.next,
                todayFile
            ]

            # Attempt to collect from the past 3 days
            files = self.FC.collectFromDateRange(now, 3)

        # Check if the start times of the collected files match
        for i, file in enumerate(sorted(files, key=lambda x: x.start)):
            self.assertEqual(file.start.date(), now + timedelta(days=i))

    def test_collect_files_from_wildcards(self):

        """
        def test_collect_files_from_wildcards
        Tests collection of SDSFiles from wildcards
        """

        with patch('orfeus.sdscollector.SDSFileCollector.files', new_callable=PropertyMock) as mock_files:

            # Fake SDS file from today, yesterday, and day before
            mock_files.return_value = [
                # Different channels
                self.createSDSFile("NL.HGN.02.LHZ.D.2019.001"),
                self.createSDSFile("NL.HGN.02.BHZ.D.2019.001"),
                self.createSDSFile("NL.HGN.02.HHZ.D.2019.001"),
                # Different days
                self.createSDSFile("NL.OPLO.02.HHZ.D.2019.001"),
                self.createSDSFile("NL.OPLO.02.HHZ.D.2019.010"),
                self.createSDSFile("NL.OPLO.02.HHZ.D.2019.100"),
                self.createSDSFile("NL.OPLO.02.HHZ.D.2019.365"),
                # Multiple stations
                self.createSDSFile("NL.G010..HGZ.D.2019.001"),
                self.createSDSFile("NL.G011..HGZ.D.2019.001"),
                self.createSDSFile("NL.G012..HGZ.D.2019.001"),
                self.createSDSFile("NL.G013..HGZ.D.2019.001"),
                self.createSDSFile("NL.G014..HGZ.D.2019.001"),
            ]

            filesStations = self.FC.collectFromWildcards("NL.G0*..HGZ.D.2019.001")
            filesChannels = self.FC.collectFromWildcards("NL.HGN.02.?HZ.D.2019.001")
            filesDays = self.FC.collectFromWildcards("NL.OPLO.02.HHZ.D.2019.*")

        # Assert files are properly collected
        self.assertEqual(len(filesStations), 5)
        self.assertEqual(len(filesDays), 4)
        self.assertEqual(len(filesChannels), 3)

    def test_collect_files_days_past_range(self):

        """
        def test_collect_files_days_past
        Tests collection of files from past days
        """

        # Get the current date for testing
        now = datetime.now().date()

        # Create a fake SDSFile for today
        todayFile = self.createSDSFile(now.strftime("NL.HGN.02.BHZ.D.%Y.%j"))

        # Mock files returned by the file collector
        with patch('orfeus.sdscollector.SDSFileCollector.files', new_callable=PropertyMock) as mock_files:

            # Fake SDS file from today, yesterday, and day before
            mock_files.return_value = [
                todayFile,
                todayFile.previous,
                todayFile.previous.previous,
                todayFile.previous.previous.previous
            ]

            # Attempt to collect from the past 3 days
            files = self.FC.collectFromPastDays(3)

        # Check if the start times of the collected files match
        for i, file in enumerate(sorted(files, key=lambda x: x.start, reverse=True)):
            self.assertEqual(file.start.date(), now - timedelta(days=(i + 1)))

    def test_collect_files_filename_date(self):

        """
        def test_collect_files_filename_date
        Tests the SDSFileCollector collecting files by date in filename
        """

        # Single file
        collectedFile = self.FC.collectFromDate("2019-01-22").pop()

        # Assert properties
        self.assertEqual(collectedFile.net, "NL")
        self.assertEqual(collectedFile.sta, "HGN")
        self.assertEqual(collectedFile.loc, "02")
        self.assertEqual(collectedFile.cha, "BHZ")

        self.assertEqual(collectedFile.start, datetime(2019, 1, 22))
        self.assertEqual(collectedFile.end, datetime(2019, 1, 23))

    def test_PSD_Module(self):

        """
        def test_PSD_Module
        Tests the PSD module
        """
        return
        def testSegment(i, segment):

            """
            def test_PSD_Module::testSegment
            Test results for a single PSD segment
            """

            start = datetime(2019, 1, 22)

            # Segment start & end
            self.assertEqual(segment["ts"], start + timedelta(minutes=(30 * i)))
            self.assertEqual(segment["te"], start + timedelta(minutes=(30 * i + 60)))

            # Confirm seed parameters
            self.assertEqual(segment["net"], "NL")
            self.assertEqual(segment["sta"], "HGN")
            self.assertEqual(segment["loc"], "02")
            self.assertEqual(segment["cha"], "BHZ")

        # Mock the inventory property to avoid HTTP request
        with patch('orfeus.sdsfile.SDSFile.inventory', new_callable=PropertyMock) as mock_inventory:

            # Read a static response file
            mock_inventory.return_value = read_inventory(os.path.join(CWD, "data/inventory.xml"))

            # Call module with mocked function
            result = psdCollector.process(self.SDSReal)

        # Should return 48 segments
        self.assertEqual(len(result), 48)

        map(testSegment, enumerate(result))

if __name__ == '__main__':
    unittest.main()
