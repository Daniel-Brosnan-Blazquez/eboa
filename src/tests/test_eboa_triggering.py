"""
Automated tests for the eboa_triggering module

Written by DEIMOS Space S.L. (dibb)

module eboa
"""
# Import python utilities
import datetime
import unittest
import json
import before_after
import tempfile
import os
import shutil

# Import eboa_ingestion module
from eboa.triggering import eboa_triggering

# Import engine
import eboa.engine.engine as eboa_engine
from eboa.engine.engine import Engine
from eboa.engine.query import Query

class TestEboaIngestion(unittest.TestCase):

    def setUp(self):

        self.engine = Engine()
        self.query = Query()
        # Clear DDBB
        self.query.clear_db()

    def tearDown(self):
        try:
            os.rename("/resources_path/triggering_bak.xml", "/resources_path/triggering.xml")
        except FileNotFoundError:
            pass
        # end try
        # Close query session
        self.query.close_session()
        self.engine.close_session()

    def test_trigger_file_does_not_exist(self):

        exit_code = "0"
        try:
            eboa_triggering.main("FILE_DOES_NOT_EXIST", test = True)
        except SystemExit as e:
            exit_code = str(e)
        # end try

        assert exit_code == "-1"
        
        # Check the logging of the file
        sources = self.query.get_sources()

        assert len(sources) == 1
        
        sources = self.query.get_sources(names = {"filter": "FILE_DOES_NOT_EXIST", "op": "=="})

        assert len(sources) == 1

        assert len(sources[0].alerts) == 1

        assert sources[0].alerts[0].alertDefinition.name == "PENDING_INGESTION_OF_SOURCE"

    def test_trigger_file_exists(self):

        # Move test configuration for triggering
        os.rename("/resources_path/triggering.xml", "/resources_path/triggering_bak.xml")
        shutil.copyfile(os.path.dirname(os.path.abspath(__file__)) + "/xml_inputs/triggering.xml", "/resources_path/triggering.xml")

        filename = "S2_OPER_DEC_F_RECV_ALL_CASES.xml"
        file_path = os.path.dirname(os.path.abspath(__file__)) + "/xml_inputs/" + filename
        
        try:
            eboa_triggering.main(file_path, test = True)
        except SystemExit as e:
            exit_code = str(e)
        # end try

        assert exit_code == "0"

        # Check inserted data
        sources = self.query.get_sources()

        assert len(sources) == 2

        sources = self.query.get_sources(names = {"filter": "S2_OPER_DEC_F_RECV_ALL_CASES.xml", "op": "=="})

        assert len(sources) == 1

        sources = self.query.get_sources(names = {"filter": "matching_source", "op": "=="})

        assert len(sources) == 1

        shutil.copyfile("/resources_path/triggering_bak.xml", "/resources_path/triggering.xml")

    def test_trigger_skip_rule(self):

        # Move test configuration for triggering
        os.rename("/resources_path/triggering.xml", "/resources_path/triggering_bak.xml")
        shutil.copyfile(os.path.dirname(os.path.abspath(__file__)) + "/xml_inputs/triggering.xml", "/resources_path/triggering.xml")

        filename = "FILE_NOT_TO_PROCESS.xml"
        file_path = os.path.dirname(os.path.abspath(__file__)) + "/xml_inputs/" + filename
        
        try:
            eboa_triggering.main(file_path, test = True)
        except SystemExit as e:
            exit_code = str(e)
        # end try

        assert exit_code == "0"

        # Check inserted data
        sources = self.query.get_sources()

        assert len(sources) == 1

        sources = self.query.get_sources(names = {"filter": "FILE_NOT_TO_PROCESS.xml", "op": "=="},
                                         dim_signatures = {"filter": "SOURCES_NOT_PROCESSED", "op": "=="})

        assert len(sources) == 1

        shutil.copyfile("/resources_path/triggering_bak.xml", "/resources_path/triggering.xml")
        
    def test_trigger_file_exists_passing_schema(self):

        # Move test configuration for triggering
        os.rename("/resources_path/triggering.xml", "/resources_path/triggering_bak.xml")
        shutil.copyfile(os.path.dirname(os.path.abspath(__file__)) + "/xml_inputs/triggering.xml", "/resources_path/triggering.xml")

        filename = "DEC_F_RECS_ALL_CASES.xml"
        file_path = os.path.dirname(os.path.abspath(__file__)) + "/xml_inputs/" + filename
        
        try:
            eboa_triggering.main(file_path, test = True)
        except SystemExit as e:
            exit_code = str(e)
        # end try

        assert exit_code == "0"

        # Check inserted data
        sources = self.query.get_sources()

        assert len(sources) == 2

        sources = self.query.get_sources(names = {"filter": "DEC_F_RECS_ALL_CASES.xml", "op": "=="})

        assert len(sources) == 1

        sources = self.query.get_sources(names = {"filter": "matching_source", "op": "=="})

        assert len(sources) == 1

        shutil.copyfile("/resources_path/triggering_bak.xml", "/resources_path/triggering.xml")
        
    def test_trigger_file_defining_events_out_of_validity(self):

        # Move test configuration for triggering
        os.rename("/resources_path/triggering.xml", "/resources_path/triggering_bak.xml")
        shutil.copyfile(os.path.dirname(os.path.abspath(__file__)) + "/xml_inputs/triggering.xml", "/resources_path/triggering.xml")

        filename = "DEC_F_RECS_NOT_VALID_VALIDITY.xml"
        file_path = os.path.dirname(os.path.abspath(__file__)) + "/xml_inputs/" + filename

        try:
            eboa_triggering.main(file_path, test = True)
        except SystemExit as e:
            exit_code = str(e)
        # end try

        assert exit_code == "-1"

        # Check inserted data
        sources = self.query.get_sources()

        assert len(sources) == 3

        sources = self.query.get_sources(names = {"filter": "DEC_F_RECS_NOT_VALID_VALIDITY.xml", "op": "=="},
                                         dim_signatures = {"filter": "PENDING_SOURCES", "op": "=="})

        assert len(sources) == 1

        relevant_source_status = [status for status in sources[0].statuses if status.log == "The triggered command associated to the source file with name DEC_F_RECS_NOT_VALID_VALIDITY.xml ended unexpectedly with the following errors:\nThe ingestion of the file DEC_F_RECS_NOT_VALID_VALIDITY.xml has failed for the DIM signature RECEIVED_FILES_BY_DEC using the processor ingestion_dec.py with status: WRONG_SOURCE_PERIOD\n"]

        assert len(relevant_source_status) == 1

        sources = self.query.get_sources(names = {"filter": "DEC_F_RECS_NOT_VALID_VALIDITY.xml", "op": "=="},
                                         dim_signatures = {"filter": "RECEIVED_FILES_BY_DEC", "op": "=="})

        assert len(sources) == 1

        relevant_source_status = [status for status in sources[0].statuses if status.log == "The source file with name DEC_F_RECS_NOT_VALID_VALIDITY.xml associated to the DIM signature RECEIVED_FILES_BY_DEC and DIM processing ingestion_dec.py with version 1.0 has a validity period which its stop (2020-01-30T16:31:29.000) is lower than its start (2021-01-30T16:31:29.000)"]
        assert len(relevant_source_status) == 1

        shutil.copyfile("/resources_path/triggering_bak.xml", "/resources_path/triggering.xml")
