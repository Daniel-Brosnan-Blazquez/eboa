"""
Automated tests for the engine submodule

Written by DEIMOS Space S.L. (dibb)

module sboa
"""
# Import python utilities
import os
import sys
import unittest
import datetime
import uuid
import random
import before_after
from dateutil import parser
import time

# Import engine of the DDBB
import sboa.engine.engine as sboa_engine
from sboa.engine.engine import Engine
from sboa.engine.query import Query
from sboa.datamodel.base import Session, engine, Base

# Import logging
from eboa.logging import Log

# Import scheduler
import sboa.scheduler.boa_scheduler as scheduler
import sboa.scheduler.boa_execute_triggering as execute

class TestEngine(unittest.TestCase):
    def setUp(self):
        # Create the engine to manage the data
        self.engine_sboa = Engine()
        self.query_sboa = Query()

        # Create session to connect to the database
        self.session = Session()

        # Clear all tables before executing the test
        self.query_sboa.clear_db()

    def tearDown(self):
        # Close connections to the DDBB
        self.engine_sboa.close_session()
        self.query_sboa.close_session()
        self.session.close()

    def test_execute_triggering(self):

        filename = "test_general_scheduler.xml"
        path_to_scheduler = os.path.dirname(os.path.abspath(__file__)) + "/xml_inputs/" + filename
        t0 = parser.parse("2019-12-09")
        returned_value = self.engine_sboa.insert_configuration(t0, path_to_scheduler)["status"]

        assert returned_value == sboa_engine.exit_codes["OK"]["status"]

        rules = self.query_sboa.get_rules()

        assert len(rules) == 3

        tasks = self.query_sboa.get_tasks(names = {"filter": "ECHO_3_1_ECHO_3", "op": "=="})

        assert len(tasks) == 1

        date = datetime.datetime.now()
        triggering_info = self.engine_sboa.insert_triggering(date, tasks[0].task_uuid)
        stop = tasks[0].triggering_time - datetime.timedelta(days=float(tasks[0].rule.window_delay))
        start = stop - datetime.timedelta(days=float(tasks[0].rule.window_size))            

        execute.execute_triggering(triggering_info["triggering_uuid"], start.isoformat(), stop.isoformat())

        triggerings = self.query_sboa.get_triggerings(triggered = True)

        assert len(triggerings) == 1

        self.query_sboa.session.expunge_all()
        tasks = self.query_sboa.get_tasks(names = {"filter": "ECHO_3_1_ECHO_3", "op": "=="})

        assert len(tasks) == 1

        assert tasks[0].triggering_time.isoformat() == "2019-12-01T10:00:00"

    def test_query_and_execute_task(self):

        filename = "test_general_scheduler.xml"
        path_to_scheduler = os.path.dirname(os.path.abspath(__file__)) + "/xml_inputs/" + filename
        t0 = parser.parse("2019-12-09")
        returned_value = self.engine_sboa.insert_configuration(t0, path_to_scheduler)["status"]

        assert returned_value == sboa_engine.exit_codes["OK"]["status"]

        rules = self.query_sboa.get_rules()

        assert len(rules) == 3

        tasks = self.query_sboa.get_tasks()

        assert len(tasks) == 4

        scheduler.query_and_execute_tasks()

        time.sleep(10)

        triggerings = self.query_sboa.get_triggerings(task_names = {"filter": ["ECHO_3_1_ECHO_3", "ECHO_3_2_ECHO_3"], "op": "in"}, triggered = True)

        assert len(triggerings) == 2

        self.query_sboa.session.expunge_all()
        
        tasks = self.query_sboa.get_tasks(names = {"filter": "ECHO_3_2_ECHO_3", "op": "=="})

        assert len(tasks) == 1

        assert tasks[0].triggering_time.isoformat() == "2019-12-02T10:00:00"
        
        tasks = self.query_sboa.get_tasks(names = {"filter": "ECHO_3_1_ECHO_3", "op": "=="})

        assert len(tasks) == 1

        assert tasks[0].triggering_time.isoformat() == "2019-12-02T10:00:00"

    def test_execute_10_tasks_2_times(self):

        filename = "test_10_tasks.xml"
        path_to_scheduler = os.path.dirname(os.path.abspath(__file__)) + "/xml_inputs/" + filename
        t0 = parser.parse("2019-12-09")
        returned_value = self.engine_sboa.insert_configuration(t0, path_to_scheduler)["status"]

        assert returned_value == sboa_engine.exit_codes["OK"]["status"]

        rules = self.query_sboa.get_rules()

        assert len(rules) == 1

        tasks = self.query_sboa.get_tasks()

        assert len(tasks) == 10

        scheduler.create_pid_files_folder()
        scheduler.query_and_execute_tasks()
        scheduler.query_and_execute_tasks()

        time.sleep(40)
        
        triggerings = self.query_sboa.get_triggerings(triggered = True)

        assert len(triggerings) == 20

    # Comment test due to the difficulty to have always the same results
    # def test_execute_20_tasks_2_times_one_skip_in_the_middle(self):

    #     filename = "test_20_tasks.xml"
    #     path_to_scheduler = os.path.dirname(os.path.abspath(__file__)) + "/xml_inputs/" + filename
    #     t0 = parser.parse("2019-12-09")
    #     returned_value = self.engine_sboa.insert_configuration(t0, path_to_scheduler)["status"]

    #     assert returned_value == sboa_engine.exit_codes["OK"]["status"]

    #     rules = self.query_sboa.get_rules()

    #     assert len(rules) == 1

    #     tasks = self.query_sboa.get_tasks()

    #     assert len(tasks) == 20

    #     scheduler.query_and_execute_tasks()

    #     time.sleep(3)        

    #     scheduler.query_and_execute_tasks()

    #     time.sleep(6)

    #     scheduler.query_and_execute_tasks()

    #     time.sleep(6)
        
    #     triggerings = self.query_sboa.get_triggerings(triggered = True)

    #     assert len(triggerings) == 40
