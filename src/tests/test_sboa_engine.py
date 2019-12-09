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
        returned_value = self.engine_sboa.insert_configuration(t0, path_to_scheduler)["status"]

        assert returned_value == sboa_engine.exit_codes["OK"]["status"]

        rules = self.query_sboa.get_rules()

        assert len(rules) == 3

        weekday_number = 3
        current_weekday = t0.isoweekday()
        if current_weekday > weekday_number:
            sum_days = 7 + weekday_number - current_weekday
        else:
            sum_days = weekday_number - current_weekday
        # end if
        triggering_date = datetime.datetime(t0.year, t0.month, t0.day).date() + datetime.timedelta(days=sum_days)
        triggering_time = triggering_date.isoformat() + "T10:00:00"

        rules = self.query_sboa.get_rules(names={"filter": "ECHO_1", "op": "=="}, window_size_filters=[{"float": 7, "op": "=="}], triggering_time_filters=[{"date": triggering_time, "op": "=="}])

        assert len(rules) == 1

        rules = self.query_sboa.get_rules(names={"filter": "ECHO_2", "op": "=="}, window_size_filters=[{"float": 1, "op": "=="}], triggering_time_filters=[{"date": "2019-12-09T10:00:00", "op": "=="}])

        assert len(rules) == 1

        rules = self.query_sboa.get_rules(names={"filter": "ECHO_3", "op": "=="}, window_size_filters=[{"float": 1, "op": "=="}], triggering_time_filters=[{"date": "2019-12-10T10:00:00", "op": "=="}])

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
