"""
Automated tests for the ingestion of the REP_PASS_2|5 files

Written by DEIMOS Space S.L. (dibb)

module ingestions
"""
import pdb
# Import python utilities
import os
import sys
import unittest
import datetime

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
import ingestions.s2.ingestion_dfep_acquisition.ingestion_dfep_acquisition as ingestion

class TestEngine(unittest.TestCase):
    def setUp(self):
        # Create the engine to manage the data
        self.engine_eboa = Engine()
        self.query_eboa = Query()

        # Create session to connectx to the database
        self.session = Session()

        # Clear all tables before executing the test
        for table in reversed(Base.metadata.sorted_tables):
            engine.execute(table.delete())

    def test_insert_rep_pass(self):
        filename = "S2A_REP_PASS_CONTAINING_ALL_DATA_TO_BE_PROCESS.EOF"
        file_path = os.path.dirname(os.path.abspath(__file__)) + "/inputs/" + filename

        returned_value = ingestion.command_process_file(file_path)

        assert returned_value[0]["status"] == eboa_engine.exit_codes["OK"]["status"]

        # Check number of events generated
        events = self.session.query(Event).all()

        assert len(events) == 9

        # Check that the validity period of the input has taken into consideration the MSI sensing received
        source = self.session.query(Source).filter(Source.validity_start == "2018-07-20T22:00:12.859222",
                                                   Source.validity_stop == "2018-07-21T10:37:26.355940").all()

        assert len(source) == 1

        # Check ISP_VALIDITY events
        isp_validities = self.session.query(Event).join(Gauge).filter(Gauge.name == "ISP_VALIDITY").all()

        assert len(isp_validities) == 1

        # Check specific ISP_VALIDITY
        specific_isp_validity1 = self.session.query(Event).join(Gauge).filter(Gauge.name == "ISP_VALIDITY",
                                                                                 Event.start == "2018-07-21T08:52:29.993268",
                                                                                 Event.stop == "2018-07-21T08:54:18.226646").all()

        assert len(specific_isp_validity1) == 1

        assert specific_isp_validity1[0].get_structured_values() == [{
            "values": [
                {
                    "values": [
                        {
                            "value": "COMPLETE",
                            "name": "status",
                            "type": "text"
                        },
                        {
                            "value": "16078.0",
                            "name": "downlink_orbit",
                            "type": "double"
                        },
                        {
                            "value": "S2A",
                            "name": "satellite",
                            "type": "text"
                        },
                        {
                            "value": "MPS_",
                            "name": "reception_station",
                            "type": "text"
                        },
                        {
                            "value": "NOMINAL",
                            "name": "downlink_mode",
                            "type": "text"
                        },
                        {
                            "value": "NO_MATCHED_PLANNED_IMAGING",
                            "name": "matching_plan_status",
                            "type": "text"
                        },
                        {
                            "value": "",
                            "name": "sensing_orbit",
                            "type": "text"
                        }
                    ],
                    "name": "values",
                    "type": "object"
                }
            ]
        }]

        # Check PLANNED_IMAGING_ISP_COMPLETENESS_CHANNEL events
        isp_completeness_events = self.session.query(Event).join(Gauge).filter(Gauge.name.like("PLANNED_IMAGING_ISP_COMPLETENESS_CHANNEL%")).all()

        assert len(isp_completeness_events) == 2

        # Check specific ISP_VALIDITY
        isp_completeness_event1 = self.session.query(Event).join(Gauge).filter(Gauge.name == "PLANNED_IMAGING_ISP_COMPLETENESS_CHANNEL_1",
                                                                                 Event.start == "2018-07-21T08:52:29.993268",
                                                                                 Event.stop == "2018-07-21T08:54:18.226646").all()

        assert len(isp_completeness_event1) == 1

        assert isp_completeness_event1[0].get_structured_values() == [{
            "values": [
                {
                    "type": "object",
                    "values": [
                        {
                            "value": "RECEIVED",
                            "type": "text",
                            "name": "status"
                        },
                        {
                            "value": "16078.0",
                            "type": "double",
                            "name": "downlink_orbit"
                        },
                        {
                            "value": "S2A",
                            "type": "text",
                            "name": "satellite"
                        },
                        {
                            "value": "MPS_",
                            "type": "text",
                            "name": "reception_station"
                        },
                        {
                            "value": "NOMINAL",
                            "type": "text",
                            "name": "downlink_mode"
                        },
                        {
                            "value": "",
                            "type": "text",
                            "name": "sensing_orbit"
                        }
                    ],
                    "name": "details"
                }
            ]
        }]

        # Check specific ISP_VALIDITY
        isp_completeness_event2 = self.session.query(Event).join(Gauge).filter(Gauge.name == "PLANNED_IMAGING_ISP_COMPLETENESS_CHANNEL_2",
                                                                                 Event.start == "2018-07-21T08:52:29.993268",
                                                                                 Event.stop == "2018-07-21T08:54:18.226646").all()

        assert len(isp_completeness_event2) == 1

        # Check PLAYBACK_VALIDITY events
        playback_validity_events = self.session.query(Event).join(Gauge).filter(Gauge.name.like("PLAYBACK_VALIDITY_%")).all()

        assert len(playback_validity_events) == 3

        # Check specific PLAYBACK_VALIDITY
        playback_validity_event1 = self.session.query(Event).join(Gauge).filter(Gauge.name == "PLAYBACK_VALIDITY_4",
                                                                                 Event.start == "2018-07-21T10:35:33.728601",
                                                                                 Event.stop == "2018-07-21T10:37:14.719834").all()

        assert len(playback_validity_event1) == 1

        assert playback_validity_event1[0].get_structured_values() == [{
            "values": [
                {
                    "type": "object",
                    "values": [
                        {
                            "value": "COMPLETE",
                            "type": "text",
                            "name": "status"
                        },
                        {
                            "value": "16078.0",
                            "type": "double",
                            "name": "downlink_orbit"
                        },
                        {
                            "value": "S2A",
                            "type": "text",
                            "name": "satellite"
                        },
                        {
                            "value": "MPS_",
                            "type": "text",
                            "name": "reception_station"
                        },
                        {
                            "value": "1.0",
                            "type": "double",
                            "name": "channel"
                        },
                        {
                            "value": "4.0",
                            "type": "double",
                            "name": "vcid"
                        },
                        {
                            "value": "NOMINAL",
                            "type": "text",
                            "name": "downlink_mode"
                        },
                        {
                            "value": "NO_MATCHED_PLANNED_PLAYBACK",
                            "type": "text",
                            "name": "matching_plan_status"
                        }
                    ],
                    "name": "values"
                }
            ]
        }]

        playback_validity_event2 = self.session.query(Event).join(Gauge).filter(Gauge.name == "PLAYBACK_VALIDITY_20",
                                                                                 Event.start == "2018-07-21T10:35:33.760977",
                                                                                 Event.stop == "2018-07-21T10:37:14.753003").all()

        assert len(playback_validity_event2) == 1

        playback_validity_event3 = self.session.query(Event).join(Gauge).filter(Gauge.name == "PLAYBACK_VALIDITY_2",
                                                                                 Event.start == "2018-07-21T10:37:20.858708",
                                                                                 Event.stop == "2018-07-21T10:37:26.355940").all()

        assert len(playback_validity_event3) == 1

        # Check PLANNED_PLAYBACK_COMPLETENESS_CHANNEL events
        playback_completeness_events = self.session.query(Event).join(Gauge).filter(Gauge.name.like("PLANNED_PLAYBACK_COMPLETENESS_CHANNEL%")).all()

        assert len(playback_completeness_events) == 3

        # Check specific PLANNED_PLAYBACK_COMPLETENESS_CHANNEL
        playback_completeness_event1 = self.session.query(Event).join(Gauge).filter(Gauge.name == "PLANNED_PLAYBACK_COMPLETENESS_CHANNEL_1",
                                                                                 Event.start == "2018-07-21T10:35:33.728601",
                                                                                 Event.stop == "2018-07-21T10:37:14.719834").all()

        assert len(playback_completeness_event1) == 1

        assert playback_completeness_event1[0].get_structured_values() == [{
            "values": [
                {
                    "type": "object",
                    "values": [
                        {
                            "type": "text",
                            "value": "RECEIVED",
                            "name": "status"
                        },
                        {
                            "type": "double",
                            "value": "16078.0",
                            "name": "downlink_orbit"
                        },
                        {
                            "type": "text",
                            "value": "S2A",
                            "name": "satellite"
                        },
                        {
                            "type": "text",
                            "value": "MPS_",
                            "name": "reception_station"
                        },
                        {
                            "type": "text",
                            "value": "NOMINAL",
                            "name": "downlink_mode"
                        }
                    ],
                    "name": "details"
                }
            ]
        }]

        playback_completeness_event2 = self.session.query(Event).join(Gauge).filter(Gauge.name == "PLANNED_PLAYBACK_COMPLETENESS_CHANNEL_2",
                                                                                 Event.start == "2018-07-21T10:35:33.760977",
                                                                                 Event.stop == "2018-07-21T10:37:14.753003").all()

        assert len(playback_completeness_event2) == 1

        playback_completeness_event3 = self.session.query(Event).join(Gauge).filter(Gauge.name == "PLANNED_PLAYBACK_COMPLETENESS_CHANNEL_2",
                                                                                 Event.start == "2018-07-21T10:37:20.858708",
                                                                                 Event.stop == "2018-07-21T10:37:26.355940").all()

        assert len(playback_completeness_event3) == 1

