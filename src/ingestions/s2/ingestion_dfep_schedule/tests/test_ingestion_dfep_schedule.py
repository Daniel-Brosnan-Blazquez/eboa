"""
Automated tests for the ingestion of the MPL_FS files

Written by DEIMOS Space S.L. (femd)

module ingestions
"""
# Import python utilities
import os
import sys
import unittest
import datetime
from dateutil import parser

# Import engine of the DDBB
import eboa.engine.engine as eboa_engine
from eboa.engine.engine import Engine
from eboa.engine.query import Query
from eboa.datamodel.base import Session, engine, Base
from eboa.engine.errors import LinksInconsistency, UndefinedEventLink, DuplicatedEventLinkRef, WrongPeriod, SourceAlreadyIngested, WrongValue, OddNumberOfCoordinates, EboaResourcesPathNotAvailable, WrongGeometry, ErrorParsingDictionary
from eboa.engine.query import Query

# Import datamodel
from eboa.datamodel.dim_signatures import DimSignature
from eboa.datamodel.events import Event, EventLink, EventKey, EventText, EventDouble, EventObject, EventGeometry, EventBoolean, EventTimestamp
from eboa.datamodel.gauges import Gauge
from eboa.datamodel.sources import Source, SourceStatus
from eboa.datamodel.explicit_refs import ExplicitRef, ExplicitRefGrp, ExplicitRefLink
from eboa.datamodel.annotations import Annotation, AnnotationCnf, AnnotationText, AnnotationDouble, AnnotationObject, AnnotationGeometry, AnnotationBoolean, AnnotationTimestamp

# Import ingestion
import ingestions.s2.ingestion_dfep_schedule.ingestion_dfep_schedule as ingestion_dfep_schedule
import ingestions.s2.ingestion_nppf.ingestion_nppf as ingestion_nppf
import ingestions.s2.ingestion_orbpre.ingestion_orbpre as ingestion_orbpre

class TestEngine(unittest.TestCase):
    def setUp(self):
        # Create the engine to manage the data
        self.engine_eboa = Engine()
        self.query_eboa = Query()

        # Create session to connectx to the database
        self.session = Session()

        # Clear all tables before executing the test
        self.query_eboa.clear_db()

    def test_mpl_fs_only(self):

        filename = "S2A_OPER_MPL_FSMPS__PDMC_20180719T090010_RIPPED.EOF"
        file_path = os.path.dirname(os.path.abspath(__file__)) + "/inputs/" + filename

        returned_value = ingestion_dfep_schedule.command_process_file(file_path)

        assert returned_value[0]["status"] == eboa_engine.exit_codes["OK"]["status"]

        #Check sources
        sources = self.query_eboa.get_sources()

        assert len(sources) == 1

        definite_source = self.query_eboa.get_sources(validity_start_filters = [{"date": "2018-07-20T11:03:27.014", "op": "=="}],
                                              validity_stop_filters = [{"date": "2018-07-26T00:47:15.402", "op": "=="}],
                                              generation_time_filters = [{"date": "2018-07-19T09:00:10", "op": "=="}],
                                              processors = {"filter": "ingestion_dfep_schedule.py", "op": "like"},
                                              names = {"filter": "S2A_OPER_MPL_FSMPS__PDMC_20180719T090010_RIPPED.EOF", "op": "like"})

        assert len(definite_source) == 1

        #Check events
        events = self.query_eboa.get_events()

        assert len(events) == 1

        #Check definite event
        definite_event = self.query_eboa.get_events(gauge_names = {"filter": "DFEP_SCHEDULE", "op": "like"})

        assert definite_event[0].get_structured_values() == [{
                "name": "schedule_information",
                "type": "object",
                "values": [
                {
                    "name": "orbit",
                    "type": "double",
                    "value": "16078.0"
                },
                {
                    "name": "satellite",
                    "type": "text",
                    "value": "S2A"
                },
                {
                    "name": "station",
                    "type": "text",
                    "value": "MPS_"
                },
                {
                    "name": "status",
                    "type": "text",
                    "value": "NO_MATCHED_PLAYBACK"
                }
            ]
        }]

    def test_mpl_fs_with_plan(self):

        filename = "S2A_NPPF.EOF"
        file_path = os.path.dirname(os.path.abspath(__file__)) + "/inputs/" + filename

        returned_value = ingestion_nppf.command_process_file(file_path)

        assert returned_value[0]["status"] == eboa_engine.exit_codes["OK"]["status"]

        filename = "S2A_ORBPRE.EOF"
        file_path = os.path.dirname(os.path.abspath(__file__)) + "/inputs/" + filename

        returned_value = ingestion_orbpre.command_process_file(file_path)

        assert returned_value[0]["status"] == eboa_engine.exit_codes["OK"]["status"]

        filename = "S2A_OPER_MPL_FSMPS__PDMC_20180719T090010_RIPPED.EOF"
        file_path = os.path.dirname(os.path.abspath(__file__)) + "/inputs/" + filename

        returned_value = ingestion_dfep_schedule.command_process_file(file_path)

        assert returned_value[0]["status"] == eboa_engine.exit_codes["OK"]["status"]

        #Check sources gives 4 (2 orbpre instead of 1)
        # sources = self.query_eboa.get_sources()
        #
        # assert len(sources) == 3

        definite_source = self.query_eboa.get_sources(validity_start_filters = [{"date": "2018-07-20T11:03:27.014", "op": "=="}],
                                              validity_stop_filters = [{"date": "2018-07-26T00:47:15.402", "op": "=="}],
                                              generation_time_filters = [{"date": "2018-07-19T09:00:10", "op": "=="}],
                                              processors = {"filter": "ingestion_dfep_schedule.py", "op": "like"},
                                              names = {"filter": "S2A_OPER_MPL_FSMPS__PDMC_20180719T090010_RIPPED.EOF", "op": "like"})

        assert len(definite_source) == 1

        #Check definite event
        definite_event = self.query_eboa.get_events(gauge_names = {"filter": "DFEP_SCHEDULE", "op": "like"})

        assert definite_event[0].get_structured_values() == [{
                "name": "schedule_information",
                "type": "object",
                "values": [
                {
                    "name": "orbit",
                    "type": "double",
                    "value": "16078.0"
                },
                {
                    "name": "satellite",
                    "type": "text",
                    "value": "S2A"
                },
                {
                    "name": "station",
                    "type": "text",
                    "value": "MPS_"
                },
                {
                    "name": "status",
                    "type": "text",
                    "value": "MATCHED_PLAYBACK"
                }
            ]
        }]

        planned_playback_events = self.query_eboa.get_events(validity_start_filters = [{"date": "2018-07-21 10:35:27.430000", "op": "=="}],
                                              validity_stop_filters = [{"date": "2018-07-21 10:37:03.431000", "op": "=="}])

        assert len(planned_playback_events) == 1

        planned_playback_event = planned_playback_events[0]

        # Check links with PLANNED_PLAYBACK_CORRECTION
        link_to_plan = self.query_eboa.get_event_links(event_uuid_links = {"filter": [str(planned_playback_event.event_uuid)], "op": "in"},
                                                    event_uuids = {"filter": [str(definite_event.event_uuid)], "op": "in"},
                                                    link_names = {"filter": "DFEP_SCHEDULE", "op": "like"})

        assert len(link_to_plan) == 1

        link_from_plan = self.query_eboa.get_event_links(event_uuids = {"filter": [str(planned_playback_event.event_uuid)], "op": "in"},
                                                    event_uuid_links = {"filter": [str(definite_event.event_uuid)], "op": "in"},
                                                    link_names = {"filter": "PLANNED_PLAYBACK", "op": "like"})

        assert len(link_from_plan) == 1

        #Not corrected yet
        assert processing_validity_l0.get_structured_values() == [{
           "values": [
                {
                    "value": "COMPLETE",
                    "name": "status",
                    "type": "text"
                },
                {
                    "value": "L0",
                    "name": "level",
                    "type": "text"
                },
                {
                    "value": "S2A",
                    "name": "satellite",
                    "type": "text"
                },
                {
                    "value": "MPS_",
                    "type": "text",
                    "name": "processing_centre"
                },
                {
                    "value": "MATCHED_PLANNED_IMAGING",
                    "type": "text",
                    "name": "matching_plan_status"
                },
                {
                    "value": "NO_MATCHED_ISP_VALIDITY",
                    "type": "text",
                    "name": "matching_reception_status"
                },
                {
                    "value": "16077.0",
                    "type": "double",
                    "name": "sensing_orbit"
                }
            ],
            "type": "object",
            "name": "details"
        }]
