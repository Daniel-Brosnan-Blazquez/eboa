"""
Automated tests for the ingestion of the SRA_EDRS files

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
import ingestions.s2.ingestion_slot_request_edrs.ingestion_slot_request_edrs as ingestion_slot_request_edrs
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

    def test_sra_edrs_only(self):

        filename = "S2__OPER_SRA_EDRS_A_PDMC_20180720T030000_RIPPED.EOF"
        file_path = os.path.dirname(os.path.abspath(__file__)) + "/inputs/" + filename

        returned_value = ingestion_slot_request_edrs.command_process_file(file_path)

        assert returned_value[0]["status"] == eboa_engine.exit_codes["OK"]["status"]

        #Check sources
        sources = self.query_eboa.get_sources()

        assert len(sources) == 1

        definite_source = self.query_eboa.get_sources(validity_start_filters = [{"date": "2018-07-20T03:00:00", "op": "=="}],
                                              validity_stop_filters = [{"date": "2018-08-31T23:32:57", "op": "=="}],
                                              generation_time_filters = [{"date": "2018-07-20T03:00:00", "op": "=="}],
                                              processors = {"filter": "ingestion_slot_request_edrs.py", "op": "like"},
                                              names = {"filter": "S2__OPER_SRA_EDRS_A_PDMC_20180720T030000_RIPPED.EOF", "op": "like"})

        assert len(definite_source) == 1

        #Check events
        events = self.query_eboa.get_events()

        assert len(events) == 1

        #Check definite event
        definite_event = self.query_eboa.get_events(gauge_names = {"filter": "SLOT_REQUEST_EDRS", "op": "like"})

        assert definite_event[0].get_structured_values() == [
            {
                "name": "slot_request_information",
                "type": "object",
                "values": [
                    {
                        "name": "session_id",
                        "type": "text",
                        "value": "L20180608110336202000109"
                    },{
                        "name": "edrs_unit",
                        "type": "text",
                        "value": "EDRS-A"
                    },{
                        "name": "orbit",
                        "type": "double",
                        "value": "16071.0"
                    },{
                        "name": "satellite",
                        "type": "text",
                        "value": "S2A"
                    },{
                        "name": "status",
                        "type": "text",
                        "value": "NO_MATCHED_PLAYBACK"
                    }
                ]
            }
        ]

        #Check explicit_refs
        explicit_refs = self.query_eboa.get_explicit_refs()

        assert len(explicit_refs) == 1

        #Check a definite explicit_ref
        definite_explicit_ref = self.query_eboa.get_explicit_refs(groups = {"filter": "EDRS_SESSION_IDs", "op": "like"},
                                                          explicit_refs = {"filter": "L20180608110336202000109", "op": "like"})

        assert len(definite_explicit_ref) == 1

    def test_mpl_fs_with_plan(self):

        filename = "S2A_NPPF.EOF"
        file_path = os.path.dirname(os.path.abspath(__file__)) + "/inputs/" + filename

        returned_value = ingestion_nppf.command_process_file(file_path)

        assert returned_value[0]["status"] == eboa_engine.exit_codes["OK"]["status"]

        filename = "S2A_ORBPRE.EOF"
        file_path = os.path.dirname(os.path.abspath(__file__)) + "/inputs/" + filename

        returned_value = ingestion_orbpre.command_process_file(file_path)

        assert returned_value[0]["status"] == eboa_engine.exit_codes["OK"]["status"]

        filename = "S2__OPER_SRA_EDRS_A_PDMC_20180720T030000_RIPPED.EOF"
        file_path = os.path.dirname(os.path.abspath(__file__)) + "/inputs/" + filename

        returned_value = ingestion_slot_request_edrs.command_process_file(file_path)

        assert returned_value[0]["status"] == eboa_engine.exit_codes["OK"]["status"]

        #Check sources
        sources = self.query_eboa.get_sources()

        assert len(sources) == 4

        definite_source = self.query_eboa.get_sources(validity_start_filters = [{"date": "2018-07-20T03:00:00", "op": "=="}],
                                              validity_stop_filters = [{"date": "2018-08-31T23:32:57", "op": "=="}],
                                              generation_time_filters = [{"date": "2018-07-20T03:00:00", "op": "=="}],
                                              processors = {"filter": "ingestion_slot_request_edrs.py", "op": "like"},
                                              names = {"filter": "S2__OPER_SRA_EDRS_A_PDMC_20180720T030000_RIPPED.EOF", "op": "like"})

        assert len(definite_source) == 1

        #Check definite event
        definite_event = self.query_eboa.get_events(gauge_names = {"filter": "SLOT_REQUEST_EDRS", "op": "like"})

        assert definite_event[0].get_structured_values() == [
            {
                "name": "slot_request_information",
                "type": "object",
                "values": [
                    {
                        "name": "session_id",
                        "type": "text",
                        "value": "L20180608110336202000111"
                    },{
                        "name": "edrs_unit",
                        "type": "text",
                        "value": "EDRS-A"
                    },{
                        "name": "orbit",
                        "type": "double",
                        "value": "16073.0"
                    },{
                        "name": "satellite",
                        "type": "text",
                        "value": "S2A"
                    },{
                        "name": "status",
                        "type": "text",
                        "value": "MATCHED_PLAYBACK"
                    }
                ]
            }
        ]

        #Check explicit_refs
        explicit_refs = self.query_eboa.get_explicit_refs()

        assert len(explicit_refs) == 1

        #Check a definite explicit_ref
        definite_explicit_ref = self.query_eboa.get_explicit_refs(groups = {"filter": "EDRS_SESSION_IDs", "op": "like"},
                                                          explicit_refs = {"filter": "L20180608110336202000111", "op": "like"})

        assert len(definite_explicit_ref) == 1

        planned_playback_events = self.query_eboa.get_events(start_filters = [{"date": "2018-07-21 02:48:22", "op": "=="}],
                                              stop_filters = [{"date": "2018-07-21 03:04:04", "op": "=="}])

        assert len(planned_playback_events) == 1

        planned_playback_correction_events = self.query_eboa.get_events(start_filters = [{"date": "2018-07-21 02:48:27.472821", "op": "=="}],
                                              stop_filters = [{"date": "2018-07-21 03:04:08.957963", "op": "=="}])

        assert len(planned_playback_correction_events) == 1

        planned_playback_event = planned_playback_events[0]

        planned_playback_correction_event = planned_playback_correction_events[0]

        # Check links with PLANNED_PLAYBACK_CORRECTION
        link_to_plan = self.query_eboa.get_event_links(event_uuid_links = {"filter": [str(planned_playback_event.event_uuid)], "op": "in"},
                                                    event_uuids = {"filter": [str(definite_event[0].event_uuid)], "op": "in"},
                                                    link_names = {"filter": "PLANNED_PLAYBACK", "op": "like"})

        assert len(link_to_plan) == 1

        link_from_plan = self.query_eboa.get_event_links(event_uuids = {"filter": [str(planned_playback_event.event_uuid)], "op": "in"},
                                                    event_uuid_links = {"filter": [str(definite_event[0].event_uuid)], "op": "in"},
                                                    link_names = {"filter": "SLOT_REQUEST_EDRS", "op": "like"})

        assert len(link_from_plan) == 1

        assert planned_playback_event.get_structured_values() == [{
            "name": "details", "type": "object",
            "values": [
                {
                    "name": "start_request",
                    "type": "text",
                    "value": "MPMMPNOM"
                },{
                    "name": "stop_request",
                     "type": "text",
                      "value": "MPMMPSTP"
                },{
                    "name": "start_orbit",
                    "type": "double",
                    "value": "16073.0"
                },{
                    "name": "start_angle",
                    "type": "double",
                    "value": "289.7725"
                },{
                    "name": "stop_orbit",
                    "type": "double",
                    "value": "16073.0"
                },{
                    "name": "stop_angle",
                    "type": "double",
                    "value": "345.8436"
                },{
                    "name": "satellite",
                    "type": "text",
                    "value": "S2A"
                },{
                    "name": "playback_mean",
                    "type": "text",
                    "value": "OCP"
                },{
                    "name": "playback_type",
                    "type": "text",
                    "value": "NOMINAL"
                },
                {
                    "name": "parameters",
                    "type": "object",
                    "values": [
                        {
                            "name": "MEM_FREE",
                            "type": "double",
                            "value": "1.0"
                        },{
                            "name": "SCN_DUP",
                            "type": "double",
                            "value": "0.0"
                        },{
                            "name": "SCN_RWD",
                        "type": "double",
                            "value": "1.0"
                        }
                    ]
                },
                {
                    "name": "station_schedule",
                    "type": "object",
                    "values": [{"name": "station",
                                "type": "text",
                                "value": "EDRS"}]},
                {
                    "name": "dfep_schedule",
                    "type": "object",
                    "values": [{"name": "station",
                                "type": "text",
                                "value": "EDRS"}]}
            ]
        }
    ]

        assert planned_playback_correction_event.get_structured_values() == [{
            "name": "details", "type": "object",
            "values": [
                {
                    "name": "start_request",
                    "type": "text",
                    "value": "MPMMPNOM"
                },{
                    "name": "stop_request",
                     "type": "text",
                      "value": "MPMMPSTP"
                },{
                    "name": "start_orbit",
                    "type": "double",
                    "value": "16073.0"
                },{
                    "name": "start_angle",
                    "type": "double",
                    "value": "289.7725"
                },{
                    "name": "stop_orbit",
                    "type": "double",
                    "value": "16073.0"
                },{
                    "name": "stop_angle",
                    "type": "double",
                    "value": "345.8436"
                },{
                    "name": "satellite",
                    "type": "text",
                    "value": "S2A"
                },{
                    "name": "playback_mean",
                    "type": "text",
                    "value": "OCP"
                },{
                    "name": "playback_type",
                    "type": "text",
                    "value": "NOMINAL"
                },
                {
                    "name": "parameters",
                    "type": "object",
                    "values": [
                        {
                            "name": "MEM_FREE",
                            "type": "double",
                            "value": "1.0"
                        },{
                            "name": "SCN_DUP",
                            "type": "double",
                            "value": "0.0"
                        },{
                            "name": "SCN_RWD",
                        "type": "double",
                            "value": "1.0"
                        }
                    ]
                },
                {
                    "name": "status_correction",
                    "type": "text",
                    "value": "TIME_CORRECTED"
                },
                {
                    "name": "delta_start",
                    "type": "double",
                    "value": "-5.472821"
                },
                {
                    "name": "delta_stop",
                    "type": "double",
                    "value": "-4.957963"
                },
                {
                    "name": "station_schedule",
                    "type": "object",
                    "values": [{"name": "station",
                                "type": "text",
                                "value": "EDRS"}]},
                {
                    "name": "dfep_schedule",
                    "type": "object",
                    "values": [{"name": "station",
                                "type": "text",
                                "value": "EDRS"}]}
            ]
        }
    ]
