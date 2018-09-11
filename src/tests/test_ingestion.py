"""
Automated tests for the parsing submodule

Written by DEIMOS Space S.L. (dibb)

module eboa
"""
# Import python utilities
import unittest

# Import parsing
import eboa.engine.ingestion as ingestion

class TestParsing(unittest.TestCase):

    def test_event_out_of_source_validity_period(self):
        """
        """
        list_of_events = []
        event = {
            "start": "2018-06-05T02:07:03",
            "stop": "2018-06-05T08:07:03",
        }
        source = {
            "validity_start": "2018-06-06T02:07:03",
            "validity_stop": "2018-06-06T08:07:03",
        }

        ingestion.insert_event_for_ingestion(event, source, list_of_events)

        assert len(list_of_events) == 0

    def test_event_start_less_than_source_validity_start(self):
        """
        """
        list_of_events = []
        event = {
            "start": "2018-06-05T02:07:03",
            "stop": "2018-06-05T08:07:03",
        }
        source = {
            "validity_start": "2018-06-05T04:07:03",
            "validity_stop": "2018-06-05T08:07:03",
        }

        ingestion.insert_event_for_ingestion(event, source, list_of_events)

        assert len(list_of_events) == 1
        
        assert list_of_events[0]["start"] == "2018-06-05T04:07:03"

    def test_event_stop_greater_than_source_validity_stop(self):
        """
        """
        list_of_events = []
        event = {
            "start": "2018-06-05T02:07:03",
            "stop": "2018-06-05T08:07:03",
        }
        source = {
            "validity_start": "2018-06-05T02:07:03",
            "validity_stop": "2018-06-05T06:07:03",
        }

        ingestion.insert_event_for_ingestion(event, source, list_of_events)

        assert len(list_of_events) == 1
        
        assert list_of_events[0]["stop"] == "2018-06-05T06:07:03"

    def test_event_start_less_than_source_validity_start_stop_greater_than_source_validity_stop(self):
        """
        """
        list_of_events = []
        event = {
            "start": "2018-06-05T02:07:03",
            "stop": "2018-06-05T08:07:03",
        }
        source = {
            "validity_start": "2018-06-05T04:07:03",
            "validity_stop": "2018-06-05T06:07:03",
        }

        ingestion.insert_event_for_ingestion(event, source, list_of_events)

        assert len(list_of_events) == 1
        
        assert list_of_events[0]["start"] == "2018-06-05T04:07:03"
        assert list_of_events[0]["stop"] == "2018-06-05T06:07:03"

    def test_event_inside_source_period(self):
        """
        """
        list_of_events = []
        event = {
            "start": "2018-06-05T04:07:03",
            "stop": "2018-06-05T06:07:03",
        }
        source = {
            "validity_start": "2018-06-05T02:07:03",
            "validity_stop": "2018-06-05T08:07:03",
        }

        ingestion.insert_event_for_ingestion(event, source, list_of_events)

        assert len(list_of_events) == 1
        
        assert list_of_events[0]["start"] == "2018-06-05T04:07:03"
        assert list_of_events[0]["stop"] == "2018-06-05T06:07:03"
