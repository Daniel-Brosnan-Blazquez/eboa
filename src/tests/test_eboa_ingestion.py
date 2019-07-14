"""
Automated tests for the eboa_ingestion module

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

# Import eboa_ingestion module
from eboa.ingestion import eboa_ingestion

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

        # Close query session
        self.query.close_session()
        self.engine.close_session()

    def test_insert_data(self):

        data = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source1.json",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2018-07-05T02:07:03",
                       "validity_start": "2018-06-05T02:07:03",
                       "validity_stop": "2018-06-05T08:07:36"},
            "events": [{
                "key": "EVENT_KEY",
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "SIMPLE_UPDATE"},
                "start": "2018-06-05T02:07:03",
                "stop": "2018-06-05T08:07:36"
            }],

        }]
        }
        returned_value = self.engine.treat_data(data)[0]["status"]

        assert returned_value == eboa_engine.exit_codes["OK"]["status"]

        events = self.query.get_events()

        assert len(events) == 1
        
        data1 = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source2.json",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2019-07-05T02:07:03",
                       "validity_start": "2018-06-05T02:07:03",
                       "validity_stop": "2018-06-05T08:07:36"},
            "events": [{
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "SIMPLE_UPDATE"},
                "start": "2018-06-05T02:07:03",
                "stop": "2018-06-05T08:07:36",
                "links": [{
                    "link": str(events[0].event_uuid),
                    "link_mode": "by_uuid",
                    "name": "EVENT_LINK_NAME"
                }]
            }],

        }]
        }

        data2 = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source3.json",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2019-07-05T02:07:03",
                       "validity_start": "2018-06-05T02:07:03",
                       "validity_stop": "2018-06-05T08:07:36"},
            "events": [{
                "key": "EVENT_KEY",
                "gauge": {"name": "GAUGE_NAME2",
                          "system": "GAUGE_SYSTEM2",
                          "insertion_type": "EVENT_KEYS"},
                "start": "2018-06-05T02:07:03",
                "stop": "2018-06-05T08:07:36"
            }],

        }]
        }

        new_file = tempfile.NamedTemporaryFile()
        new_file_path = new_file.name

        with open(new_file_path, "w") as write_file:
            json.dump(data1, write_file, indent=4)
        # end with

        data = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "PENDING_SOURCES",
                              "exec": "",
                              "version": ""},
            "source": {"name": os.path.basename(new_file_path),
                       "reception_time": datetime.datetime.now().isoformat(),
                       "generation_time": datetime.datetime.now().isoformat(),
                       "validity_start": datetime.datetime.now().isoformat(),
                       "validity_stop": datetime.datetime.now().isoformat(),
                       "ingested": "false"}
        }]
        }

        returned_value = self.engine.treat_data(data)[0]["status"]

        assert returned_value == eboa_engine.exit_codes["OK"]["status"]
        
        new_file2 = tempfile.NamedTemporaryFile()
        new_file_path2 = new_file2.name

        with open(new_file_path2, "w") as write_file:
            json.dump(data2, write_file, indent=4)
        # end with

        def insert_event_keys():
            eboa_ingestion.command_process_file("eboa.processors.eboa_processor", new_file_path2, datetime.datetime.now().isoformat())
        # end def

        def insert_event_links():
            eboa_ingestion.command_process_file("eboa.processors.eboa_processor", new_file_path, datetime.datetime.now().isoformat())
        # end def

        with before_after.before("eboa.engine.engine.race_condition2", insert_event_keys):
            insert_event_links()
        # end with
        
        new_file.close()
        new_file2.close()

        events = self.query.get_events()

        assert len(events) == 1

        assert events[0].gauge.name == "GAUGE_NAME2"

        not_ingested_sources = self.query.get_sources(names = {"filter": os.path.basename(new_file_path), "op": "like"})

        assert len(not_ingested_sources) == 1

        assert not_ingested_sources[0].ingestion_error == True

        assert len([status for status in not_ingested_sources[0].statuses if status.status == eboa_engine.exit_codes["INGESTION_ENDED_UNEXPECTEDLY"]["status"]]) == 1

    def test_wrong_processor_name(self):

        new_file = tempfile.NamedTemporaryFile()
        new_file_path = new_file.name

        data = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "PENDING_SOURCES",
                              "exec": "",
                              "version": ""},
            "source": {"name": os.path.basename(new_file_path),
                       "reception_time": datetime.datetime.now().isoformat(),
                       "generation_time": datetime.datetime.now().isoformat(),
                       "validity_start": datetime.datetime.now().isoformat(),
                       "validity_stop": datetime.datetime.now().isoformat(),
                       "ingested": "false"}
        }]
        }

        returned_value = self.engine.treat_data(data)[0]["status"]

        assert returned_value == eboa_engine.exit_codes["OK"]["status"]

        eboa_ingestion.command_process_file("processor_does_not_exist", new_file_path, datetime.datetime.now().isoformat())

        not_ingested_sources = self.query.get_sources(names = {"filter": os.path.basename(new_file_path), "op": "like"})

        assert len(not_ingested_sources) == 1

        assert not_ingested_sources[0].ingestion_error == True

        assert len([status for status in not_ingested_sources[0].statuses if status.status == eboa_engine.exit_codes["PROCESSOR_DOES_NOT_EXIST"]["status"]]) == 1

    def test_wrong_processor_functioning(self):

        new_file = tempfile.NamedTemporaryFile()
        new_file_path = new_file.name

        data = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "PENDING_SOURCES",
                              "exec": "",
                              "version": ""},
            "source": {"name": os.path.basename(new_file_path),
                       "reception_time": datetime.datetime.now().isoformat(),
                       "generation_time": datetime.datetime.now().isoformat(),
                       "validity_start": datetime.datetime.now().isoformat(),
                       "validity_stop": datetime.datetime.now().isoformat(),
                       "ingested": "false"}
        }]
        }

        returned_value = self.engine.treat_data(data)[0]["status"]

        assert returned_value == eboa_engine.exit_codes["OK"]["status"]

        data1 = {}

        with open(new_file_path, "w") as write_file:
            json.dump(data1, write_file, indent=4)
        # end with

        eboa_ingestion.command_process_file("eboa.processors.failing_processor", new_file_path, datetime.datetime.now().isoformat())

        not_ingested_sources = self.query.get_sources(names = {"filter": os.path.basename(new_file_path), "op": "like"})

        assert len(not_ingested_sources) == 1

        assert not_ingested_sources[0].ingestion_error == True

        assert len([status for status in not_ingested_sources[0].statuses if status.status == eboa_engine.exit_codes["PROCESSING_ENDED_UNEXPECTEDLY"]["status"]]) == 1


    def test_input_file_does_not_exist(self):

        data = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "PENDING_SOURCES",
                              "exec": "",
                              "version": ""},
            "source": {"name": "file_does_not_exist",
                       "reception_time": datetime.datetime.now().isoformat(),
                       "generation_time": datetime.datetime.now().isoformat(),
                       "validity_start": datetime.datetime.now().isoformat(),
                       "validity_stop": datetime.datetime.now().isoformat(),
                       "ingested": "false"}
        }]
        }

        returned_value = self.engine.treat_data(data)[0]["status"]

        assert returned_value == eboa_engine.exit_codes["OK"]["status"]
        
        eboa_ingestion.main("file_does_not_exist", "eboa.processors.eboa_processor")

        not_ingested_sources = self.query.get_sources(names = {"filter": os.path.basename("file_does_not_exist"), "op": "like"})

        assert len(not_ingested_sources) == 1

        assert not_ingested_sources[0].ingestion_error == True

        assert len([status for status in not_ingested_sources[0].statuses if status.status == eboa_engine.exit_codes["FILE_DOES_NOT_EXIST"]["status"]]) == 1

    def test_insert_wrong_data_in_one_operation(self):

        data = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source1.json",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2018-07-05T02:07:03",
                       "validity_start": "2018-06-05T02:07:03",
                       "validity_stop": "2018-06-05T08:07:36"},
            "events": [{
                "key": "EVENT_KEY",
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "SIMPLE_UPDATE"},
                "start": "2018-06-05T02:07:03",
                "stop": "2018-06-05T08:07:36"
            }],

        },{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source2.json",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2018-07-05T02:07:03",
                       "validity_start": "2018-06-05T02:07:03",
                       "validity_stop": "2018-06-05T08:07:36"},
            "events": [{
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "SIMPLE_UPDATE"},
                "start": "2018-06-05T02:07:03",
                "stop": "2019-06-05T08:07:36"
            }],

        }]
        }

        new_file = tempfile.NamedTemporaryFile()
        new_file_path = new_file.name

        with open(new_file_path, "w") as write_file:
            json.dump(data, write_file, indent=4)
        # end with

        data = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "PENDING_SOURCES",
                              "exec": "",
                              "version": ""},
            "source": {"name": os.path.basename(new_file_path),
                       "reception_time": datetime.datetime.now().isoformat(),
                       "generation_time": datetime.datetime.now().isoformat(),
                       "validity_start": datetime.datetime.now().isoformat(),
                       "validity_stop": datetime.datetime.now().isoformat(),
                       "ingested": "false"}
        }]
        }

        returned_value = self.engine.treat_data(data)[0]["status"]

        assert returned_value == eboa_engine.exit_codes["OK"]["status"]
        
        returned_statuses = eboa_ingestion.command_process_file("eboa.processors.eboa_processor", new_file_path, datetime.datetime.now().isoformat())

        # Check 1 success
        assert len([returned_status for returned_status in returned_statuses if returned_status["status"] in [eboa_engine.exit_codes["OK"]["status"], eboa_engine.exit_codes["SOURCE_ALREADY_INGESTED"]["status"]]]) == 1

        # Check 1 failure
        assert len([returned_status for returned_status in returned_statuses if not returned_status["status"] in [eboa_engine.exit_codes["OK"]["status"], eboa_engine.exit_codes["SOURCE_ALREADY_INGESTED"]["status"]]]) == 1
