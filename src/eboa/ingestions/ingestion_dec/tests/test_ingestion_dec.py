"""
Automated tests for the ingestion of the DEC_F_RECV files

Written by DEIMOS Space S.L. (femd)

module ingestions
"""
# Import python utilities
import os
import sys
import unittest
import datetime
from dateutil import parser
import shutil
import before_after

# Import engine of the DDBB
import eboa.engine.engine as eboa_engine
from eboa.engine.engine import Engine
from eboa.engine.query import Query

# Import ingestion
import eboa.ingestion.eboa_ingestion as ingestion

class TestDec(unittest.TestCase):
    def setUp(self):
        # Create the engine to manage the data
        self.engine_eboa = Engine()
        self.query_eboa = Query()

        # Clear all tables before executing the test
        self.query_eboa.clear_db()

    def tearDown(self):
        os.rename("/resources_path/triggering_bak.xml", "/resources_path/triggering.xml")
        # Close connections to the DDBB
        self.engine_eboa.close_session()
        self.query_eboa.close_session()

    def test_insertion_of_alert(self):
        
        filename = "S2_OPER_DEC_F_RECV_ALL_CASES.xml"
        file_path = os.path.dirname(os.path.abspath(__file__)) + "/inputs/" + filename

        # Move test configuration for triggering
        os.rename("/resources_path/triggering.xml", "/resources_path/triggering_bak.xml")
        shutil.copyfile(os.path.dirname(os.path.abspath(__file__)) + "/inputs/triggering.xml", "/resources_path/triggering.xml")

        returned_value = ingestion.command_process_file("eboa.ingestions.ingestion_dec.ingestion_dec", file_path, "2018-01-01T00:00:00")

        assert returned_value[0]["status"] == eboa_engine.exit_codes["OK"]["status"]

        sources = self.query_eboa.get_sources()

        assert len(sources) == 2

        dec_sources = self.query_eboa.get_sources(names = {"filter": filename, "op": "=="}, dim_signatures = {"filter": "RECEIVED_FILES_BY_DEC", "op": "=="})

        assert len(dec_sources) == 1

        received_sources_to_triggered = self.query_eboa.get_sources(names = {"filter": filename, "op": "!="}, dim_signatures = {"filter": "PENDING_RECEIVED_SOURCES_BY_DEC", "op": "=="})

        assert len(received_sources_to_triggered) == 1

        shutil.copyfile("/resources_path/triggering_bak.xml", "/resources_path/triggering.xml")

    def test_not_insertion_of_alert(self):

        # Move test configuration for triggering
        os.rename("/resources_path/triggering.xml", "/resources_path/triggering_bak.xml")
        shutil.copyfile(os.path.dirname(os.path.abspath(__file__)) + "/inputs/triggering.xml", "/resources_path/triggering.xml")

        data = {"operations": [{
                "mode": "insert",
                "dim_signature": {"name": "PROCESSING",
                                  "exec": "",
                                  "version": ""},
                "source": {"name": "matching_source",
                           "reception_time": datetime.datetime.now().isoformat(),
                           "generation_time": datetime.datetime.now().isoformat(),
                           "validity_start": datetime.datetime.now().isoformat(),
                           "validity_stop": datetime.datetime.now().isoformat(),
                           "ingested": "true"}
            }]
        }

        returned_value = self.engine_eboa.treat_data(data)

        assert returned_value[0]["status"] == eboa_engine.exit_codes["OK"]["status"]

        filename = "S2_OPER_DEC_F_RECV_ALL_CASES.xml"
        file_path = os.path.dirname(os.path.abspath(__file__)) + "/inputs/" + filename

        returned_value = ingestion.command_process_file("eboa.ingestions.ingestion_dec.ingestion_dec", file_path, "2018-01-01T00:00:00")

        assert returned_value[0]["status"] == eboa_engine.exit_codes["OK"]["status"]

        sources = self.query_eboa.get_sources()

        assert len(sources) == 2

        dec_sources = self.query_eboa.get_sources(names = {"filter": filename, "op": "=="}, dim_signatures = {"filter": "RECEIVED_FILES_BY_DEC", "op": "=="})

        assert len(dec_sources) == 1

        processed_sources_received_by_dec = self.query_eboa.get_sources(names = {"filter": "matching_source", "op": "=="}, dim_signatures = {"filter": "PROCESSING", "op": "=="})

        assert len(processed_sources_received_by_dec) == 1
        
        received_sources_to_trigger = self.query_eboa.get_sources(names = {"filter": "matching_source", "op": "=="}, dim_signatures = {"filter": "PENDING_RECEIVED_SOURCES_BY_DEC", "op": "=="})

        assert len(received_sources_to_trigger) == 0

        shutil.copyfile("/resources_path/triggering_bak.xml", "/resources_path/triggering.xml")

    def test_insertion_of_alert_and_removal(self):
        
        filename = "S2_OPER_DEC_F_RECV_ALL_CASES.xml"
        file_path = os.path.dirname(os.path.abspath(__file__)) + "/inputs/" + filename

        # Move test configuration for triggering
        os.rename("/resources_path/triggering.xml", "/resources_path/triggering_bak.xml")
        shutil.copyfile(os.path.dirname(os.path.abspath(__file__)) + "/inputs/triggering.xml", "/resources_path/triggering.xml")

        def insert_dec_file():
            returned_value = ingestion.command_process_file("eboa.ingestions.ingestion_dec.ingestion_dec", file_path, "2018-01-01T00:00:00")
            
            assert returned_value[0]["status"] == eboa_engine.exit_codes["OK"]["status"]
        # end def

        def insert_file_in_boa():
            data = {"operations": [{
                    "mode": "insert",
                    "dim_signature": {"name": "PROCESSING",
                                      "exec": "",
                                      "version": ""},
                    "source": {"name": "matching_source",
                               "reception_time": datetime.datetime.now().isoformat(),
                               "generation_time": datetime.datetime.now().isoformat(),
                               "validity_start": datetime.datetime.now().isoformat(),
                               "validity_stop": datetime.datetime.now().isoformat(),
                               "ingested": "true"}
                }]
            }

            returned_value = self.engine_eboa.treat_data(data)

            assert returned_value[0]["status"] == eboa_engine.exit_codes["OK"]["status"]
        # end def

        with before_after.before("eboa.ingestions.ingestion_dec.ingestion_dec.race_condition", insert_file_in_boa):
            insert_dec_file()
        # end with

        sources = self.query_eboa.get_sources()

        assert len(sources) == 2

        dec_sources = self.query_eboa.get_sources(names = {"filter": filename, "op": "=="}, dim_signatures = {"filter": "RECEIVED_FILES_BY_DEC", "op": "=="})

        assert len(dec_sources) == 1

        processed_sources_received_by_dec = self.query_eboa.get_sources(names = {"filter": "matching_source", "op": "=="}, dim_signatures = {"filter": "PROCESSING", "op": "=="})

        assert len(processed_sources_received_by_dec) == 1
        
        received_sources_to_trigger = self.query_eboa.get_sources(names = {"filter": "matching_source", "op": "=="}, dim_signatures = {"filter": "PENDING_RECEIVED_SOURCES_BY_DEC", "op": "=="})

        assert len(received_sources_to_trigger) == 0

        shutil.copyfile("/resources_path/triggering_bak.xml", "/resources_path/triggering.xml")
