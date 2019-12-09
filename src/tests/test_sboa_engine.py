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

# Import engine of the DDBB
import sboa.engine.engine as sboa_engine
from sboa.engine.engine import Engine
from sboa.engine.query import Query
from sboa.datamodel.base import Session, engine, Base

# Import logging
from eboa.logging import Log

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

    def test_insert_general_scheduler(self):

        filename = "test_general_scheduler.xml"
        path_to_scheduler = os.path.dirname(os.path.abspath(__file__)) + "/xml_inputs/" + filename
        t0 = parser.parse("2019-12-09")
        returned_value = self.engine_sboa.insert_configuration(t0, path_to_scheduler)

        assert returned_value == sboa_engine.exit_codes["OK"]["status"]

        rules = self.query_sboa.get_rules()

        assert len(rules) == 3

        rules = self.query_sboa.get_rules(names={"filter": "ECHO_1", "op": "=="}, window_size_filters=[{"float": 1, "op": "=="}])

        assert len(rules) == 1

        rules = self.query_sboa.get_rules(names={"filter": "ECHO_2", "op": "=="}, window_size_filters=[{"float": 1, "op": "=="}])

        assert len(rules) == 1

        rules = self.query_sboa.get_rules(names={"filter": "ECHO_3", "op": "=="}, window_size_filters=[{"float": 1, "op": "=="}])

        assert len(rules) == 1

        tasks = self.query_sboa.get_tasks()

        assert len(tasks) == 4        

        tasks = self.query_sboa.get_tasks(names={"filter": "ECHO_1", "op": "=="})

        assert len(tasks) == 1

        tasks = self.query_sboa.get_tasks(names={"filter": "ECHO_2", "op": "=="})

        assert len(tasks) == 1

        tasks = self.query_sboa.get_tasks(names={"filter": "ECHO_3_1", "op": "=="})

        assert len(tasks) == 1

        tasks = self.query_sboa.get_tasks(names={"filter": "ECHO_3_2", "op": "=="})

        assert len(tasks) == 1
