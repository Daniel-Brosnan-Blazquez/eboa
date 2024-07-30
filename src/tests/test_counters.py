"""
Automated tests for the engine submodule (event insertion types related to counters)

Written by DEIMOS Space S.L. (dibb)

module eboa
"""
# Import python utilities
import os
import sys
import unittest
import datetime
import uuid
import random
import before_after
import shutil
from multiprocessing import get_context

# Import eboa_ingestion module
from eboa.triggering import eboa_triggering

# Import engine of the DDBB
import eboa.engine.engine as eboa_engine
from eboa.engine.engine import Engine
from eboa.engine.query import Query
from eboa.datamodel.base import Session, engine, Base

def execute_eboa_triggering(file_path):
    try:
        eboa_triggering.main(file_path, test = True)
    except SystemExit as e:
        exit_code = str(e)
    # end try

    assert exit_code == "0"

class TestCounters(unittest.TestCase):
    def setUp(self):
        # Create the engine to manage the data
        self.engine_eboa = Engine()
        self.engine_eboa_race_conditions = Engine()
        self.query_eboa = Query()

        # Create session to connect to the database
        self.session = Session()

        # Clear all tables before executing the test
        self.query_eboa.clear_db()

    def tearDown(self):
        try:
            os.rename("/resources_path/triggering_bak.xml", "/resources_path/triggering.xml")
        except FileNotFoundError:
            pass
        # end try
        # Close connections to the DDBB
        self.engine_eboa.close_session()
        self.engine_eboa_race_conditions.close_session()
        self.query_eboa.close_session()
        self.session.close()

    def test_update_counter(self):

        data = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source1.json",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2016-07-04T02:07:03",
                       "validity_start": "2018-06-05T04:07:03",
                       "validity_stop": "2018-06-05T06:07:03"},
            "events": [{
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "UPDATE_COUNTER"},
                "start": "2018-06-05T04:07:03",
                "stop": "2018-06-05T04:07:03",
                "values": [{"type": "double",
                            "name": "value",
                            "value": "10"}]
            }]
        }]
        }
        
        exit_status = self.engine_eboa.treat_data(data)

        assert len([item for item in exit_status if item["status"] != eboa_engine.exit_codes["OK"]["status"]]) == 0

        events = self.query_eboa.get_events()

        assert len(events) == 1

        events = self.query_eboa.get_events(gauge_names = {"filter": "GAUGE_NAME", "op": "=="},
                                            gauge_systems = {"filter": "GAUGE_SYSTEM", "op": "=="},
                                            start_filters = [{"date": "2018-06-05T04:07:03", "op": "=="}],
                                            stop_filters = [{"date": "2018-06-05T04:07:03", "op": "=="}],
                                            value_filters = [{"name": {"filter": "value", "op": "=="}, "type": "double", "value": {"op": "==", "filter": "10"}}]
        )

        assert len(events) == 1

    def test_update_counters_different_periods(self):

        data = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source1.json",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2016-07-04T02:07:03",
                       "validity_start": "2018-06-05T04:07:03",
                       "validity_stop": "2018-06-05T06:07:03"},
            "events": [{
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "UPDATE_COUNTER"},
                "start": "2018-06-05T04:07:03",
                "stop": "2018-06-05T04:07:03",
                "values": [{"type": "double",
                            "name": "value",
                            "value": "10"}]
            },{
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "UPDATE_COUNTER"},
                "start": "2018-06-05T05:07:03",
                "stop": "2018-06-05T06:07:03",
                "values": [{"type": "double",
                            "name": "value",
                            "value": "10"}]
            }]
        }]
        }
        
        exit_status = self.engine_eboa.treat_data(data)

        assert len([item for item in exit_status if item["status"] != eboa_engine.exit_codes["OK"]["status"]]) == 0

        events = self.query_eboa.get_events()

        assert len(events) == 2

        events = self.query_eboa.get_events(gauge_names = {"filter": "GAUGE_NAME", "op": "=="},
                                            gauge_systems = {"filter": "GAUGE_SYSTEM", "op": "=="},
                                            start_filters = [{"date": "2018-06-05T04:07:03", "op": "=="}],
                                            stop_filters = [{"date": "2018-06-05T04:07:03", "op": "=="}],
                                            value_filters = [{"name": {"filter": "value", "op": "=="}, "type": "double", "value": {"op": "==", "filter": "10"}}]
        )

        assert len(events) == 1

        events = self.query_eboa.get_events(gauge_names = {"filter": "GAUGE_NAME", "op": "=="},
                                            gauge_systems = {"filter": "GAUGE_SYSTEM", "op": "=="},
                                            start_filters = [{"date": "2018-06-05T05:07:03", "op": "=="}],
                                            stop_filters = [{"date": "2018-06-05T06:07:03", "op": "=="}],
                                            value_filters = [{"name": {"filter": "value", "op": "=="}, "type": "double", "value": {"op": "==", "filter": "10"}}]
        )

        assert len(events) == 1

    def test_several_update_counter_in_same_operation(self):

        data = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source1.json",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2016-07-04T02:07:03",
                       "validity_start": "2018-06-05T04:07:03",
                       "validity_stop": "2018-06-05T06:07:03"},
            "events": [{
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "UPDATE_COUNTER"},
                "start": "2018-06-05T04:07:03",
                "stop": "2018-06-05T06:07:03",
                "values": [{"type": "double",
                            "name": "value",
                            "value": "10"}]
            },{
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "UPDATE_COUNTER"},
                "start": "2018-06-05T04:07:03",
                "stop": "2018-06-05T06:07:03",
                "values": [{"type": "double",
                            "name": "value",
                            "value": "10"}]
            },{
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "UPDATE_COUNTER"},
                "start": "2018-06-05T04:07:03",
                "stop": "2018-06-05T06:07:03",
                "values": [{"type": "double",
                            "name": "value",
                            "value": "10"}]
            }]
        }]
        }
        
        exit_status = self.engine_eboa.treat_data(data)

        assert len([item for item in exit_status if item["status"] != eboa_engine.exit_codes["OK"]["status"]]) == 0

        events = self.query_eboa.get_events()

        assert len(events) == 1

        events = self.query_eboa.get_events(gauge_names = {"filter": "GAUGE_NAME", "op": "=="},
                                            gauge_systems = {"filter": "GAUGE_SYSTEM", "op": "=="},
                                            start_filters = [{"date": "2018-06-05T04:07:03", "op": "=="}],
                                            stop_filters = [{"date": "2018-06-05T06:07:03", "op": "=="}],
                                            value_filters = [{"name": {"filter": "value", "op": "=="}, "type": "double", "value": {"op": "==", "filter": "30"}}]
        )

        assert len(events) == 1

    def test_several_update_counter_in_different_operations(self):

        data = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source1.json",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2016-07-04T02:07:03",
                       "validity_start": "2018-06-05T04:07:03",
                       "validity_stop": "2018-06-05T06:07:03"},
            "events": [{
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "UPDATE_COUNTER"},
                "start": "2018-06-05T04:07:03",
                "stop": "2018-06-05T06:07:03",
                "values": [{"type": "double",
                            "name": "value",
                            "value": "10"}]
            }]
        }]
        }
        
        exit_status = self.engine_eboa.treat_data(data)

        assert len([item for item in exit_status if item["status"] != eboa_engine.exit_codes["OK"]["status"]]) == 0

        data2 = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source2.json",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2016-07-04T02:07:03",
                       "validity_start": "2018-06-05T04:07:03",
                       "validity_stop": "2018-06-05T06:07:03"},
            "events": [{
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "UPDATE_COUNTER"},
                "start": "2018-06-05T04:07:03",
                "stop": "2018-06-05T06:07:03",
                "values": [{"type": "double",
                            "name": "value",
                            "value": "10"}]
            },{
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "UPDATE_COUNTER"},
                "start": "2018-06-05T04:07:03",
                "stop": "2018-06-05T06:07:03",
                "values": [{"type": "double",
                            "name": "value",
                            "value": "10"}]
            }]
        }]
        }
        
        exit_status = self.engine_eboa.treat_data(data2)

        assert len([item for item in exit_status if item["status"] != eboa_engine.exit_codes["OK"]["status"]]) == 0

        events = self.query_eboa.get_events()

        assert len(events) == 1

        events = self.query_eboa.get_events(gauge_names = {"filter": "GAUGE_NAME", "op": "=="},
                                            gauge_systems = {"filter": "GAUGE_SYSTEM", "op": "=="},
                                            start_filters = [{"date": "2018-06-05T04:07:03", "op": "=="}],
                                            stop_filters = [{"date": "2018-06-05T06:07:03", "op": "=="}],
                                            value_filters = [{"name": {"filter": "value", "op": "=="}, "type": "double", "value": {"op": "==", "filter": "30"}}]
        )

        assert len(events) == 1

    def test_set_counter(self):

        data = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source1.json",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2016-07-04T02:07:03",
                       "validity_start": "2018-06-05T04:07:03",
                       "validity_stop": "2018-06-05T06:07:03"},
            "events": [{
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "SET_COUNTER"},
                "start": "2018-06-05T04:07:03",
                "stop": "2018-06-05T04:07:03",
                "values": [{"type": "double",
                            "name": "value",
                            "value": "10"}]
            }]
        }]
        }
        
        exit_status = self.engine_eboa.treat_data(data)

        assert len([item for item in exit_status if item["status"] != eboa_engine.exit_codes["OK"]["status"]]) == 0

        events = self.query_eboa.get_events()

        assert len(events) == 1

        events = self.query_eboa.get_events(gauge_names = {"filter": "GAUGE_NAME", "op": "=="},
                                            gauge_systems = {"filter": "GAUGE_SYSTEM", "op": "=="},
                                            start_filters = [{"date": "2018-06-05T04:07:03", "op": "=="}],
                                            stop_filters = [{"date": "2018-06-05T04:07:03", "op": "=="}],
                                            value_filters = [{"name": {"filter": "value", "op": "=="}, "type": "double", "value": {"op": "==", "filter": "10"}}]
        )

        assert len(events) == 1

    def test_set_counters_different_periods(self):

        data = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source1.json",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2016-07-04T02:07:03",
                       "validity_start": "2018-06-05T04:07:03",
                       "validity_stop": "2018-06-05T06:07:03"},
            "events": [{
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "SET_COUNTER"},
                "start": "2018-06-05T04:07:03",
                "stop": "2018-06-05T04:07:03",
                "values": [{"type": "double",
                            "name": "value",
                            "value": "10"}]
            },{
                "gauge": {"name": "GAUGE_NAME2",
                          "system": "GAUGE_SYSTEM2",
                          "insertion_type": "SET_COUNTER"},
                "start": "2018-06-05T05:07:03",
                "stop": "2018-06-05T06:07:03",
                "values": [{"type": "double",
                            "name": "value",
                            "value": "10"}]
            }]
        }]
        }
        
        exit_status = self.engine_eboa.treat_data(data)

        assert len([item for item in exit_status if item["status"] != eboa_engine.exit_codes["OK"]["status"]]) == 0

        events = self.query_eboa.get_events()

        assert len(events) == 2

        events = self.query_eboa.get_events(gauge_names = {"filter": "GAUGE_NAME", "op": "=="},
                                            gauge_systems = {"filter": "GAUGE_SYSTEM", "op": "=="},
                                            start_filters = [{"date": "2018-06-05T04:07:03", "op": "=="}],
                                            stop_filters = [{"date": "2018-06-05T04:07:03", "op": "=="}],
                                            value_filters = [{"name": {"filter": "value", "op": "=="}, "type": "double", "value": {"op": "==", "filter": "10"}}]
        )

        assert len(events) == 1

        events = self.query_eboa.get_events(gauge_names = {"filter": "GAUGE_NAME2", "op": "=="},
                                            gauge_systems = {"filter": "GAUGE_SYSTEM2", "op": "=="},
                                            start_filters = [{"date": "2018-06-05T05:07:03", "op": "=="}],
                                            stop_filters = [{"date": "2018-06-05T06:07:03", "op": "=="}],
                                            value_filters = [{"name": {"filter": "value", "op": "=="}, "type": "double", "value": {"op": "==", "filter": "10"}}]
        )

        assert len(events) == 1

    def test_several_set_counter_in_same_operation(self):

        data = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source1.json",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2016-07-04T02:07:03",
                       "validity_start": "2018-06-05T04:07:03",
                       "validity_stop": "2018-06-05T06:07:03"},
            "events": [{
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "SET_COUNTER"},
                "start": "2018-06-05T04:07:03",
                "stop": "2018-06-05T06:07:03",
                "values": [{"type": "double",
                            "name": "value",
                            "value": "10"}]
            },{
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "SET_COUNTER"},
                "start": "2018-06-05T04:07:03",
                "stop": "2018-06-05T06:07:03",
                "values": [{"type": "double",
                            "name": "value",
                            "value": "11"}]
            },{
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "SET_COUNTER"},
                "start": "2018-06-05T04:07:03",
                "stop": "2018-06-05T06:07:03",
                "values": [{"type": "double",
                            "name": "value",
                            "value": "12"}]
            }]
        }]
        }
        
        exit_status = self.engine_eboa.treat_data(data)

        assert len([item for item in exit_status if item["status"] == eboa_engine.exit_codes["DUPLICATED_SET_COUNTER"]["status"]]) == 1

        events = self.query_eboa.get_events()

        assert len(events) == 0

    def test_set_counter_in_different_operations(self):

        data = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source1.json",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2016-07-04T02:07:03",
                       "validity_start": "2018-06-05T04:07:03",
                       "validity_stop": "2018-06-05T06:07:03"},
            "events": [{
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "SET_COUNTER"},
                "start": "2018-06-05T04:07:03",
                "stop": "2018-06-05T05:07:03",
                "values": [{"type": "double",
                            "name": "value",
                            "value": "10"}]
            }]
        }]
        }
        
        exit_status = self.engine_eboa.treat_data(data)

        assert len([item for item in exit_status if item["status"] != eboa_engine.exit_codes["OK"]["status"]]) == 0

        data2 = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source2.json",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2016-07-04T02:07:03",
                       "validity_start": "2018-06-05T04:07:03",
                       "validity_stop": "2018-06-05T06:07:03"},
            "events": [{
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "SET_COUNTER"},
                "start": "2018-06-05T04:07:03",
                "stop": "2018-06-05T05:07:03",
                "values": [{"type": "double",
                            "name": "value",
                            "value": "30"}]
            }]
        }]
        }
        
        exit_status = self.engine_eboa.treat_data(data2)

        assert len([item for item in exit_status if item["status"] != eboa_engine.exit_codes["OK"]["status"]]) == 0

        events = self.query_eboa.get_events()

        assert len(events) == 1

        events = self.query_eboa.get_events(gauge_names = {"filter": "GAUGE_NAME", "op": "=="},
                                            gauge_systems = {"filter": "GAUGE_SYSTEM", "op": "=="},
                                            start_filters = [{"date": "2018-06-05T04:07:03", "op": "=="}],
                                            stop_filters = [{"date": "2018-06-05T05:07:03", "op": "=="}],
                                            value_filters = [{"name": {"filter": "value", "op": "=="}, "type": "double", "value": {"op": "==", "filter": "30"}}]
        )

        assert len(events) == 1

    def test_update_counter_more_than_one_value(self):

        data = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source1.json",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2016-07-04T02:07:03",
                       "validity_start": "2018-06-05T04:07:03",
                       "validity_stop": "2018-06-05T06:07:03"},
            "events": [{
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "UPDATE_COUNTER"},
                "start": "2018-06-05T04:07:03",
                "stop": "2018-06-05T04:07:03",
                "values": [{"type": "double",
                            "name": "value",
                            "value": "10"},
                           {"type": "double",
                            "name": "value",
                            "value": "10"}]
            }]
        }]
        }
        
        exit_status = self.engine_eboa.treat_data(data)

        assert len([item for item in exit_status if item["status"] == eboa_engine.exit_codes["FILE_NOT_VALID"]["status"]]) == 1

        events = self.query_eboa.get_events()

        assert len(events) == 0

    def test_update_counter_value_type_text(self):

        data = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source1.json",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2016-07-04T02:07:03",
                       "validity_start": "2018-06-05T04:07:03",
                       "validity_stop": "2018-06-05T06:07:03"},
            "events": [{
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "UPDATE_COUNTER"},
                "start": "2018-06-05T04:07:03",
                "stop": "2018-06-05T04:07:03",
                "values": [{"type": "text",
                            "name": "value",
                            "value": "10"}]
            }]
        }]
        }
        
        exit_status = self.engine_eboa.treat_data(data)

        assert len([item for item in exit_status if item["status"] == eboa_engine.exit_codes["FILE_NOT_VALID"]["status"]]) == 1

        events = self.query_eboa.get_events()

        assert len(events) == 0

    def test_update_counter_value_name_not_value(self):

        data = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source1.json",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2016-07-04T02:07:03",
                       "validity_start": "2018-06-05T04:07:03",
                       "validity_stop": "2018-06-05T06:07:03"},
            "events": [{
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "UPDATE_COUNTER"},
                "start": "2018-06-05T04:07:03",
                "stop": "2018-06-05T04:07:03",
                "values": [{"type": "double",
                            "name": "name",
                            "value": "10"}]
            }]
        }]
        }
        
        exit_status = self.engine_eboa.treat_data(data)

        assert len([item for item in exit_status if item["status"] == eboa_engine.exit_codes["FILE_NOT_VALID"]["status"]]) == 1

        events = self.query_eboa.get_events()

        assert len(events) == 0

    def test_update_counter_value_not_covertible_to_float(self):

        data = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source1.json",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2016-07-04T02:07:03",
                       "validity_start": "2018-06-05T04:07:03",
                       "validity_stop": "2018-06-05T06:07:03"},
            "events": [{
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "UPDATE_COUNTER"},
                "start": "2018-06-05T04:07:03",
                "stop": "2018-06-05T04:07:03",
                "values": [{"type": "double",
                            "name": "value",
                            "value": "text"}]
            }]
        }]
        }
        
        exit_status = self.engine_eboa.treat_data(data)

        assert len([item for item in exit_status if item["status"] == eboa_engine.exit_codes["FILE_NOT_VALID"]["status"]]) == 1

        events = self.query_eboa.get_events()

        assert len(events) == 0

    def test_update_counter_with_explicit_reference(self):

        data = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source1.json",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2016-07-04T02:07:03",
                       "validity_start": "2018-06-05T04:07:03",
                       "validity_stop": "2018-06-05T06:07:03"},
            "events": [{
                "explicit_reference": "EXPLICIT_REFERENCE_EVENT",
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "UPDATE_COUNTER"},
                "start": "2018-06-05T04:07:03",
                "stop": "2018-06-05T04:07:03",
                "values": [{"type": "double",
                            "name": "value",
                            "value": "10"}]
            }]
        }]
        }
        
        exit_status = self.engine_eboa.treat_data(data)

        assert len([item for item in exit_status if item["status"] == eboa_engine.exit_codes["FILE_NOT_VALID"]["status"]]) == 1

        events = self.query_eboa.get_events()

        assert len(events) == 0

    def test_update_counter_with_key(self):

        data = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source1.json",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2016-07-04T02:07:03",
                       "validity_start": "2018-06-05T04:07:03",
                       "validity_stop": "2018-06-05T06:07:03"},
            "events": [{
                "key": "EVENT_KEY",
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "UPDATE_COUNTER"},
                "start": "2018-06-05T04:07:03",
                "stop": "2018-06-05T04:07:03",
                "values": [{"type": "double",
                            "name": "value",
                            "value": "10"}]
            }]
        }]
        }
        
        exit_status = self.engine_eboa.treat_data(data)

        assert len([item for item in exit_status if item["status"] == eboa_engine.exit_codes["FILE_NOT_VALID"]["status"]]) == 1

        events = self.query_eboa.get_events()

        assert len(events) == 0

    def test_update_counter_with_links(self):

        data = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source1.json",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2016-07-04T02:07:03",
                       "validity_start": "2018-06-05T04:07:03",
                       "validity_stop": "2018-06-05T06:07:03"},
            "events": [{
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "UPDATE_COUNTER"},
                "start": "2018-06-05T04:07:03",
                "stop": "2018-06-05T04:07:03",
                "values": [{"type": "double",
                            "name": "value",
                            "value": "10"}],
                "links": []
            }]
        }]
        }
        
        exit_status = self.engine_eboa.treat_data(data)

        assert len([item for item in exit_status if item["status"] == eboa_engine.exit_codes["FILE_NOT_VALID"]["status"]]) == 1

        events = self.query_eboa.get_events()

        assert len(events) == 0

    def test_update_counter_with_link_ref(self):

        data = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source1.json",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2016-07-04T02:07:03",
                       "validity_start": "2018-06-05T04:07:03",
                       "validity_stop": "2018-06-05T06:07:03"},
            "events": [{
                "link_ref": "LINK_REF",
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "UPDATE_COUNTER"},
                "start": "2018-06-05T04:07:03",
                "stop": "2018-06-05T04:07:03",
                "values": [{"type": "double",
                            "name": "value",
                            "value": "10"}]
            }]
        }]
        }
        
        exit_status = self.engine_eboa.treat_data(data)

        assert len([item for item in exit_status if item["status"] == eboa_engine.exit_codes["FILE_NOT_VALID"]["status"]]) == 1

        events = self.query_eboa.get_events()

        assert len(events) == 0

    def test_update_counter_with_alerts(self):

        data = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source1.json",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2016-07-04T02:07:03",
                       "validity_start": "2018-06-05T04:07:03",
                       "validity_stop": "2018-06-05T06:07:03"},
            "events": [{
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "UPDATE_COUNTER"},
                "start": "2018-06-05T04:07:03",
                "stop": "2018-06-05T04:07:03",
                "values": [{"type": "double",
                            "name": "value",
                            "value": "10"}],
                "alerts": []
            }]
        }]
        }
        
        exit_status = self.engine_eboa.treat_data(data)

        assert len([item for item in exit_status if item["status"] == eboa_engine.exit_codes["FILE_NOT_VALID"]["status"]]) == 1

        events = self.query_eboa.get_events()

        assert len(events) == 0

    def test_set_counter_more_than_one_value(self):

        data = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source1.json",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2016-07-04T02:07:03",
                       "validity_start": "2018-06-05T04:07:03",
                       "validity_stop": "2018-06-05T06:07:03"},
            "events": [{
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "SET_COUNTER"},
                "start": "2018-06-05T04:07:03",
                "stop": "2018-06-05T04:07:03",
                "values": [{"type": "double",
                            "name": "value",
                            "value": "10"},
                           {"type": "double",
                            "name": "value",
                            "value": "10"}]
            }]
        }]
        }
        
        exit_status = self.engine_eboa.treat_data(data)

        assert len([item for item in exit_status if item["status"] == eboa_engine.exit_codes["FILE_NOT_VALID"]["status"]]) == 1

        events = self.query_eboa.get_events()

        assert len(events) == 0

    def test_set_counter_value_type_text(self):

        data = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source1.json",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2016-07-04T02:07:03",
                       "validity_start": "2018-06-05T04:07:03",
                       "validity_stop": "2018-06-05T06:07:03"},
            "events": [{
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "SET_COUNTER"},
                "start": "2018-06-05T04:07:03",
                "stop": "2018-06-05T04:07:03",
                "values": [{"type": "text",
                            "name": "value",
                            "value": "10"}]
            }]
        }]
        }
        
        exit_status = self.engine_eboa.treat_data(data)

        assert len([item for item in exit_status if item["status"] == eboa_engine.exit_codes["FILE_NOT_VALID"]["status"]]) == 1

        events = self.query_eboa.get_events()

        assert len(events) == 0

    def test_set_counter_value_name_not_value(self):

        data = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source1.json",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2016-07-04T02:07:03",
                       "validity_start": "2018-06-05T04:07:03",
                       "validity_stop": "2018-06-05T06:07:03"},
            "events": [{
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "SET_COUNTER"},
                "start": "2018-06-05T04:07:03",
                "stop": "2018-06-05T04:07:03",
                "values": [{"type": "double",
                            "name": "name",
                            "value": "10"}]
            }]
        }]
        }
        
        exit_status = self.engine_eboa.treat_data(data)

        assert len([item for item in exit_status if item["status"] == eboa_engine.exit_codes["FILE_NOT_VALID"]["status"]]) == 1

        events = self.query_eboa.get_events()

        assert len(events) == 0

    def test_set_counter_value_not_covertible_to_float(self):

        data = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source1.json",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2016-07-04T02:07:03",
                       "validity_start": "2018-06-05T04:07:03",
                       "validity_stop": "2018-06-05T06:07:03"},
            "events": [{
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "SET_COUNTER"},
                "start": "2018-06-05T04:07:03",
                "stop": "2018-06-05T04:07:03",
                "values": [{"type": "double",
                            "name": "value",
                            "value": "text"}]
            }]
        }]
        }
        
        exit_status = self.engine_eboa.treat_data(data)

        assert len([item for item in exit_status if item["status"] == eboa_engine.exit_codes["FILE_NOT_VALID"]["status"]]) == 1

        events = self.query_eboa.get_events()

        assert len(events) == 0

    def test_set_counter_with_explicit_reference(self):

        data = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source1.json",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2016-07-04T02:07:03",
                       "validity_start": "2018-06-05T04:07:03",
                       "validity_stop": "2018-06-05T06:07:03"},
            "events": [{
                "explicit_reference": "EXPLICIT_REFERENCE_EVENT",
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "SET_COUNTER"},
                "start": "2018-06-05T04:07:03",
                "stop": "2018-06-05T04:07:03",
                "values": [{"type": "double",
                            "name": "value",
                            "value": "10"}]
            }]
        }]
        }
        
        exit_status = self.engine_eboa.treat_data(data)

        assert len([item for item in exit_status if item["status"] == eboa_engine.exit_codes["FILE_NOT_VALID"]["status"]]) == 1

        events = self.query_eboa.get_events()

        assert len(events) == 0

    def test_set_counter_with_key(self):

        data = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source1.json",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2016-07-04T02:07:03",
                       "validity_start": "2018-06-05T04:07:03",
                       "validity_stop": "2018-06-05T06:07:03"},
            "events": [{
                "key": "EVENT_KEY",
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "SET_COUNTER"},
                "start": "2018-06-05T04:07:03",
                "stop": "2018-06-05T04:07:03",
                "values": [{"type": "double",
                            "name": "value",
                            "value": "10"}]
            }]
        }]
        }
        
        exit_status = self.engine_eboa.treat_data(data)

        assert len([item for item in exit_status if item["status"] == eboa_engine.exit_codes["FILE_NOT_VALID"]["status"]]) == 1

        events = self.query_eboa.get_events()

        assert len(events) == 0

    def test_set_counter_with_links(self):

        data = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source1.json",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2016-07-04T02:07:03",
                       "validity_start": "2018-06-05T04:07:03",
                       "validity_stop": "2018-06-05T06:07:03"},
            "events": [{
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "SET_COUNTER"},
                "start": "2018-06-05T04:07:03",
                "stop": "2018-06-05T04:07:03",
                "values": [{"type": "double",
                            "name": "value",
                            "value": "10"}],
                "links": []
            }]
        }]
        }
        
        exit_status = self.engine_eboa.treat_data(data)

        assert len([item for item in exit_status if item["status"] == eboa_engine.exit_codes["FILE_NOT_VALID"]["status"]]) == 1

        events = self.query_eboa.get_events()

        assert len(events) == 0

    def test_set_counter_with_link_ref(self):

        data = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source1.json",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2016-07-04T02:07:03",
                       "validity_start": "2018-06-05T04:07:03",
                       "validity_stop": "2018-06-05T06:07:03"},
            "events": [{
                "link_ref": "LINK_REF",
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "SET_COUNTER"},
                "start": "2018-06-05T04:07:03",
                "stop": "2018-06-05T04:07:03",
                "values": [{"type": "double",
                            "name": "value",
                            "value": "10"}]
            }]
        }]
        }
        
        exit_status = self.engine_eboa.treat_data(data)

        assert len([item for item in exit_status if item["status"] == eboa_engine.exit_codes["FILE_NOT_VALID"]["status"]]) == 1

        events = self.query_eboa.get_events()

        assert len(events) == 0

    def test_set_counter_with_alerts(self):

        data = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source1.json",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2016-07-04T02:07:03",
                       "validity_start": "2018-06-05T04:07:03",
                       "validity_stop": "2018-06-05T06:07:03"},
            "events": [{
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "SET_COUNTER"},
                "start": "2018-06-05T04:07:03",
                "stop": "2018-06-05T04:07:03",
                "values": [{"type": "double",
                            "name": "value",
                            "value": "10"}],
                "alerts": []
            }]
        }]
        }
        
        exit_status = self.engine_eboa.treat_data(data)

        assert len([item for item in exit_status if item["status"] == eboa_engine.exit_codes["FILE_NOT_VALID"]["status"]]) == 1

        events = self.query_eboa.get_events()

        assert len(events) == 0

    def test_update_counter_by_3_ingestions_in_parallel(self):

        # Move test configuration for triggering
        os.rename("/resources_path/triggering.xml", "/resources_path/triggering_bak.xml")
        shutil.copyfile(os.path.dirname(os.path.abspath(__file__)) + "/xml_inputs/triggering_one_rule.xml", "/resources_path/triggering.xml")

        filename = "source1_counters.json"
        file1_path = os.path.dirname(os.path.abspath(__file__)) + "/json_inputs/" + filename

        filename = "source2_counters.json"
        file2_path = os.path.dirname(os.path.abspath(__file__)) + "/json_inputs/" + filename

        filename = "source3_counters.json"
        file3_path = os.path.dirname(os.path.abspath(__file__)) + "/json_inputs/" + filename

        with get_context("spawn").Pool() as pool:
            pool.map(execute_eboa_triggering, [file1_path, file2_path, file3_path])
        # end with
        
        # Check inserted data
        sources = self.query_eboa.get_sources()

        assert len(sources) == 3

        sources = self.query_eboa.get_sources(names = {"filter": "source1_counters.json", "op": "=="})

        assert len(sources) == 1

        assert len([status for status in sources[0].statuses if status.status == eboa_engine.exit_codes["OK"]["status"]]) > 0

        sources = self.query_eboa.get_sources(names = {"filter": "source2_counters.json", "op": "=="})

        assert len(sources) == 1

        assert len([status for status in sources[0].statuses if status.status == eboa_engine.exit_codes["OK"]["status"]]) > 0

        sources = self.query_eboa.get_sources(names = {"filter": "source3_counters.json", "op": "=="})

        assert len(sources) == 1

        assert len([status for status in sources[0].statuses if status.status == eboa_engine.exit_codes["OK"]["status"]]) > 0

        events = self.query_eboa.get_events()

        assert len(events) == 1

        events = self.query_eboa.get_events(gauge_names = {"filter": "GAUGE_NAME", "op": "=="},
                                            gauge_systems = {"filter": "GAUGE_SYSTEM", "op": "=="},
                                            start_filters = [{"date": "2018-06-05T04:07:03", "op": "=="}],
                                            stop_filters = [{"date": "2018-06-05T06:07:03", "op": "=="}],
                                            value_filters = [{"name": {"filter": "value", "op": "=="}, "type": "double", "value": {"op": "==", "filter": "55"}}]
        )

        assert len(events) == 1

        shutil.copyfile("/resources_path/triggering_bak.xml", "/resources_path/triggering.xml")
