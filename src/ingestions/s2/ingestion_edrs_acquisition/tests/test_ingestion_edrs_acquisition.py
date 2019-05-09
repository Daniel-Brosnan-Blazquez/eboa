"""
Automated tests for the ingestion of the REP_PASS_E files

Written by DEIMOS Space S.L. (femd)

module ingestions
"""
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

# Import ingestion edrs
import ingestions.s2.ingestion_edrs_acquisition.ingestion_edrs_acquisition as ingestion_edrs

# Import ingestion nppf
import ingestions.s2.ingestion_nppf.ingestion_nppf as ingestion_nppf

# Import ingestion orbpre
import ingestions.s2.ingestion_orbpre.ingestion_orbpre as ingestion_orbpre

# Import ingestion sra
import ingestions.s2.ingestion_slot_request_edrs.ingestion_slot_request_edrs as ingestion_sra

class TestDfepIngestion(unittest.TestCase):
    def setUp(self):
        # Create the engine to manage the data
        self.engine_eboa = Engine()
        self.query_eboa = Query()

        # Create session to connectx to the database
        self.session = Session()

        # Clear all tables before executing the test
        self.query_eboa.clear_db()

    def tearDown(self):
        # Close connections to the DDBB
        self.engine_eboa.close_session()
        self.query_eboa.close_session()
        self.session.close()

    def test_insert_only_rep_pass(self):
        filename = "S2A_OPER_REP_PASS_E_CONTAINING_ALL_DATA_TO_BE_PROCESS.EOF"
        file_path = os.path.dirname(os.path.abspath(__file__)) + "/inputs/" + filename

        returned_value = ingestion_edrs.command_process_file(file_path)

        assert returned_value[0]["status"] == eboa_engine.exit_codes["OK"]["status"]

        # Check number of sources generated
        sources = self.session.query(Source).all()

        assert len(sources) == 1

        # Check that the validity period of the input has taken into consideration the MSI sensing received
        source = self.session.query(Source).filter(Source.validity_start == "2018-07-20 17:33:12.859268",
                                                   Source.validity_stop == "2018-07-21T07:37:55.121811").all()

        assert len(source) == 1

        dim_signatures = self.session.query(DimSignature).all()

        # Check number of dim_signatures generated
        assert len(dim_signatures) == 1

        dim_signature = self.session.query(DimSignature).filter(DimSignature.dim_signature == "RECEPTION_S2A").all()

        assert len(dim_signature) == 1

        # Check number of annotations generated
        annotations = self.session.query(Annotation).all()

        assert len(annotations) == 1

        # Check LINK_DETAILS annotations
        link_details = self.session.query(Annotation).join(AnnotationCnf,ExplicitRef).filter(AnnotationCnf.name == "LINK-DETAILS-CH2",
                                                                                             ExplicitRef.explicit_ref == "L20180608110336202000113").all()

        assert len(link_details) == 1

        assert link_details[0].get_structured_values() == [
            {
                    "name": "link_details",
                    "type": "object",
                    "values": [
                        {
                            "name": "session_id",
                            "type": "text",
                            "value": "L20180608110336202000113"
                        },
                        {
                            "name": "downlink_orbit",
                            "type": "double",
                            "value": "16076.0"
                        },
                        {
                            "name": "satellite",
                            "type": "text",
                            "value": "S2A"
                        },
                        {
                            "name": "reception_station",
                            "type": "text",
                            "value": "EDRS"
                        },
                        {
                            "name": "isp_status",
                            "type": "text",
                            "value": "COMPLETE"
                        },
                        {
                            "name": "acquisition_status",
                            "type": "text",
                            "value": "COMPLETE"
                        }
                    ]
                }
            ]

        # Check number of events generated
        events = self.session.query(Event).all()

        assert len(events) == 3

        # Check ISP_VALIDITY events
        playback_validity_events = self.session.query(Event).join(Gauge).filter(Gauge.name == "PLAYBACK_VALIDITY").all()

        assert len(playback_validity_events) == 2

        definite_playback_validity_event1 = self.session.query(Event).join(Gauge).filter(Gauge.name == "PLAYBACK_VALIDITY",
                                                                                   Event.start == "2018-07-21T07:28:23.002358",
                                                                                   Event.stop == "2018-07-21T07:37:40.689924").all()

        assert definite_playback_validity_event1[0].get_structured_values() == [
            {
                "name": "values",
                "type": "object",
                "values": [
                    {
                        "name": "downlink_orbit",
                        "type": "double",
                        "value": "16076.0"
                    },{
                        "name": "satellite",
                        "type": "text",
                        "value": "S2A"
                    },{
                        "name": "reception_station",
                        "type": "text",
                        "value": "EDRS"
                    },{
                        "name": "channel",
                        "type": "double",
                        "value": "2.0"
                    },{
                        "name": "vcid",
                        "type": "double",
                        "value": "20.0"
                    },{
                        "name": "downlink_mode",
                        "type": "text",
                        "value": "NOMINAL"
                    },{
                        "name": "matching_plan_status",
                        "type": "text",
                        "value": "NO_MATCHED_PLANNED_PLAYBACK"
                    },{
                        "name": "status",
                        "type": "text",
                        "value": "COMPLETE"
                    }
                ]
            }
        ]

        raw_isp_validity_events = self.session.query(Event).join(Gauge).filter(Gauge.name == "RAW_ISP_VALIDITY").all()

        assert len(raw_isp_validity_events) == 1

        assert raw_isp_validity_events[0].get_structured_values() == [
            {
                "name": "values",
                "type": "object",
                "values": [
                    {
                        "name": "status",
                        "type": "text",
                        "value": "COMPLETE"
                    },{
                        "name": "downlink_orbit",
                         "type": "double",
                         "value": "16076.0"
                    },{
                        "name": "satellite",
                        "type": "text",
                        "value": "S2A"
                    },{
                    "name": "reception_station",
                     "type": "text",
                     "value": "EDRS"
                    },{
                        "name": "channel",
                        "type": "double",
                        "value": "2.0"
                    },{
                        "name": "vcid",
                        "type": "double",
                        "value": "20.0"
                    },{
                        "name": "downlink_mode",
                        "type": "text",
                        "value": "NOMINAL"
                    },{
                        "name": "num_packets",
                        "type": "double",
                        "value": "2015280.0"
                    },{
                        "name": "num_frames",
                        "type": "double",
                        "value": "17927163.0"
                     },{
                        "name": "expected_num_packets",
                        "type": "double",
                        "value": "0.0"
                    },{
                        "name": "diff_expected_received",
                        "type": "double",
                        "value": "-2015280.0"
                    },{
                        "name": "packet_status",
                        "type": "text",
                        "value": "MISSING"
                    }
                ]
            }
        ]

        explicit_refs = self.session.query(ExplicitRef).all()

        assert len(explicit_refs) == 1

        definite_explicit_ref = self.session.query(ExplicitRef).join(ExplicitRefGrp).filter(ExplicitRef.explicit_ref == "L20180608110336202000113",
                                                                                    ExplicitRefGrp.name == "EDRS_LINK_SESSION_IDs").all()

        assert len(definite_explicit_ref) == 1

    #Issues to be fixed in the ingestion
    def test_insert_rep_pass_with_msi_gaps(self):
        filename = "S2A_REP_PASS_CONTAINING_MSI_GAPS.EOF"
        file_path = os.path.dirname(os.path.abspath(__file__)) + "/inputs/" + filename

        returned_value = ingestion_dfep.command_process_file(file_path)

        assert returned_value[0]["status"] == eboa_engine.exit_codes["OK"]["status"]

        # Check number of events generated
        events = self.session.query(Event).all()

        assert len(events) == 14

        # Check number of annotations generated
        annotations = self.session.query(Annotation).all()

        assert len(annotations) == 1

        # Check that the validity period of the input has taken into consideration the MSI sensing received
        source = self.session.query(Source).filter(Source.validity_start == "2018-07-20T22:00:12.859222",
                                                   Source.validity_stop == "2018-07-21T10:37:26.355940").all()

        assert len(source) == 1

        # Check LINK_DETAILS annotations
        link_details = self.session.query(Annotation).join(AnnotationCnf).filter(AnnotationCnf.name == "LINK_DETAILS").all()

        assert len(link_details) == 1

        assert link_details[0].get_structured_values() == [
            {
                "values": [
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
                        "value": "INCOMPLETE",
                        "type": "text",
                        "name": "isp_status"
                    },
                    {
                        "value": "COMPLETE",
                        "type": "text",
                        "name": "acquisition_status"
                    }
                ],
                "type": "object",
                "name": "link_details"
            }
        ]

        # Check ISP_VALIDITY events
        isp_validities = self.session.query(Event).join(Gauge).filter(Gauge.name == "ISP_VALIDITY").all()

        assert len(isp_validities) == 1

        # Check definite ISP_VALIDITY
        definite_isp_validity1 = self.session.query(Event).join(Gauge).filter(Gauge.name == "ISP_VALIDITY",
                                                                                 Event.start == "2018-07-21T08:52:29.993268",
                                                                                 Event.stop == "2018-07-21T08:54:18.226646").all()

        assert len(definite_isp_validity1) == 1

        assert definite_isp_validity1[0].get_structured_values() == [{
            "name": "values",
            "values": [
                {
                    "name": "status",
                    "value": "INCOMPLETE",
                    "type": "text"
                },
                {
                    "name": "downlink_orbit",
                    "value": "16078.0",
                    "type": "double"
                },
                {
                    "name": "satellite",
                    "value": "S2A",
                    "type": "text"
                },
                {
                    "name": "reception_station",
                    "value": "MPS_",
                    "type": "text"
                },
                {
                    "name": "downlink_mode",
                    "value": "NOMINAL",
                    "type": "text"
                },
                {
                    "name": "matching_plan_status",
                    "value": "NO_MATCHED_PLANNED_IMAGING",
                    "type": "text"
                },
                {
                    "name": "sensing_orbit",
                    "value": "",
                    "type": "text"
                }
            ],
            "type": "object"
        }]

        # Check PLANNED_IMAGING_ISP_COMPLETENESS_CHANNEL events
        isp_completeness_events = self.session.query(Event).join(Gauge).filter(Gauge.name.like("PLANNED_IMAGING_ISP_COMPLETENESS_CHANNEL%")).all()

        assert len(isp_completeness_events) == 2

        # Check definite ISP_VALIDITY
        isp_completeness_event1 = self.session.query(Event).join(Gauge).filter(Gauge.name == "PLANNED_IMAGING_ISP_COMPLETENESS_CHANNEL_1",
                                                                                 Event.start == "2018-07-21T08:52:29.993268",
                                                                                 Event.stop == "2018-07-21T08:54:18.226646").all()

        assert len(isp_completeness_event1) == 1

        assert isp_completeness_event1[0].get_structured_values() == [{
            "name": "details",
            "values": [
                {
                    "name": "status",
                    "value": "INCOMPLETE",
                    "type": "text"
                },
                {
                    "name": "downlink_orbit",
                    "value": "16078.0",
                    "type": "double"
                },
                {
                    "name": "satellite",
                    "value": "S2A",
                    "type": "text"
                },
                {
                    "name": "reception_station",
                    "value": "MPS_",
                    "type": "text"
                },
                {
                    "name": "downlink_mode",
                    "value": "NOMINAL",
                    "type": "text"
                },
                {
                    "name": "sensing_orbit",
                    "value": "",
                    "type": "text"
                }
            ],
            "type": "object"
        }]

        # Check definite ISP_VALIDITY
        isp_completeness_event2 = self.session.query(Event).join(Gauge).filter(Gauge.name == "PLANNED_IMAGING_ISP_COMPLETENESS_CHANNEL_2",
                                                                                 Event.start == "2018-07-21T08:52:29.993268",
                                                                                 Event.stop == "2018-07-21T08:54:18.226646").all()

        assert len(isp_completeness_event2) == 1

        assert isp_completeness_event2[0].get_structured_values() == [{
            "name": "details",
            "values": [
                {
                    "name": "status",
                    "value": "INCOMPLETE",
                    "type": "text"
                },
                {
                    "name": "downlink_orbit",
                    "value": "16078.0",
                    "type": "double"
                },
                {
                    "name": "satellite",
                    "value": "S2A",
                    "type": "text"
                },
                {
                    "name": "reception_station",
                    "value": "MPS_",
                    "type": "text"
                },
                {
                    "name": "downlink_mode",
                    "value": "NOMINAL",
                    "type": "text"
                },
                {
                    "name": "sensing_orbit",
                    "value": "",
                    "type": "text"
                }
            ],
            "type": "object"
        }]

        # Check PLAYBACK_VALIDITY events
        playback_validity_events = self.session.query(Event).join(Gauge).filter(Gauge.name.like("PLAYBACK_VALIDITY_%")).all()

        assert len(playback_validity_events) == 3

        # Check definite PLAYBACK_VALIDITY
        playback_validity_event1 = self.session.query(Event).join(Gauge).filter(Gauge.name == "PLAYBACK_VALIDITY_4",
                                                                                 Event.start == "2018-07-21T10:35:33.728601",
                                                                                 Event.stop == "2018-07-21T10:37:14.719834").all()

        assert len(playback_validity_event1) == 1

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

        # Check definite PLANNED_PLAYBACK_COMPLETENESS_CHANNEL
        playback_completeness_event1 = self.session.query(Event).join(Gauge).filter(Gauge.name == "PLANNED_PLAYBACK_COMPLETENESS_CHANNEL_1",
                                                                                 Event.start == "2018-07-21T10:35:33.728601",
                                                                                 Event.stop == "2018-07-21T10:37:14.719834").all()

        assert len(playback_completeness_event1) == 1

        playback_completeness_event2 = self.session.query(Event).join(Gauge).filter(Gauge.name == "PLANNED_PLAYBACK_COMPLETENESS_CHANNEL_2",
                                                                                 Event.start == "2018-07-21T10:35:33.760977",
                                                                                 Event.stop == "2018-07-21T10:37:14.753003").all()

        assert len(playback_completeness_event2) == 1

        playback_completeness_event3 = self.session.query(Event).join(Gauge).filter(Gauge.name == "PLANNED_PLAYBACK_COMPLETENESS_CHANNEL_2",
                                                                                 Event.start == "2018-07-21T10:37:20.858708",
                                                                                 Event.stop == "2018-07-21T10:37:26.355940").all()

        assert len(playback_completeness_event3) == 1

        # Check ISP_GAPs events
        isp_gap_events = self.session.query(Event).join(Gauge).filter(Gauge.name.like("ISP_GAP")).all()

        assert len(playback_completeness_events) == 3

        # Check definite ISP_GAP
        isp_gap_event1 = self.session.query(Event).join(Gauge).filter(Gauge.name == "ISP_GAP",
                                                                                 Event.start == "2018-07-21T08:52:29.993268",
                                                                                 Event.stop == "2018-07-21T08:54:18.226646").all()

        assert len(isp_gap_event1) == 1

        assert isp_gap_event1[0].get_structured_values() == [{
            "type": "object",
            "name": "values",
            "values": [
                {
                    "type": "text",
                    "name": "impact",
                    "value": "COMPLETE_SCENES_BAND"
                },
                {
                    "type": "text",
                    "name": "band",
                    "value": "2"
                },
                {
                    "type": "double",
                    "name": "detector",
                    "value": "12.0"
                },
                {
                    "type": "double",
                    "name": "downlink_orbit",
                    "value": "16078.0"
                },
                {
                    "type": "text",
                    "name": "satellite",
                    "value": "S2A"
                },
                {
                    "type": "text",
                    "name": "reception_station",
                    "value": "MPS_"
                },
                {
                    "type": "double",
                    "name": "vcid",
                    "value": "4.0"
                },
                {
                    "type": "text",
                    "name": "downlink_mode",
                    "value": "NOMINAL"
                },
                {
                    "type": "double",
                    "name": "apid",
                    "value": "1.0"
                }
            ]
        }]

        isp_gap_event2 = self.session.query(Event).join(Gauge).filter(Gauge.name == "ISP_GAP",
                                                                                 Event.start == "2018-07-21T08:52:29.993268",
                                                                                 Event.stop == "2018-07-21T08:52:31.254806").all()

        assert len(isp_gap_event2) == 1

        assert isp_gap_event2[0].get_structured_values() == [{
            "type": "object",
            "name": "values",
            "values": [
                {
                    "type": "text",
                    "name": "impact",
                    "value": "AT_BEGINNING"
                },
                {
                    "type": "text",
                    "name": "band",
                    "value": "3"
                },
                {
                    "type": "double",
                    "name": "detector",
                    "value": "12.0"
                },
                {
                    "type": "double",
                    "name": "downlink_orbit",
                    "value": "16078.0"
                },
                {
                    "type": "text",
                    "name": "satellite",
                    "value": "S2A"
                },
                {
                    "type": "text",
                    "name": "reception_station",
                    "value": "MPS_"
                },
                {
                    "type": "double",
                    "name": "vcid",
                    "value": "4.0"
                },
                {
                    "type": "text",
                    "name": "downlink_mode",
                    "value": "NOMINAL"
                },
                {
                    "type": "double",
                    "name": "apid",
                    "value": "2.0"
                },
                {
                    "type": "double",
                    "name": "missing_packets",
                    "value": "50.0"
                }
            ]
        }]


        isp_gap_event3 = self.session.query(Event).join(Gauge).filter(Gauge.name == "ISP_GAP",
                                                                                 Event.start == "2018-07-21T08:52:33.444170",
                                                                                 Event.stop == "2018-07-21T08:52:37.209040").all()

        assert len(isp_gap_event3) == 1

        assert isp_gap_event3[0].get_structured_values() == [{
            "type": "object",
            "name": "values",
            "values": [
                {
                    "type": "text",
                    "value": "SMALLER_THAN_A_SCENE",
                    "name": "impact"
                },
                {
                    "type": "text",
                    "value": "1",
                    "name": "band"
                },
                {
                    "type": "double",
                    "value": "12.0",
                    "name": "detector"
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
                    "type": "double",
                    "value": "4.0",
                    "name": "vcid"
                },
                {
                    "type": "text",
                    "value": "NOMINAL",
                    "name": "downlink_mode"
                },
                {
                    "type": "double",
                    "value": "0.0",
                    "name": "apid"
                },
                {
                    "type": "double",
                    "value": "23.0",
                    "name": "counter_start"
                },
                {
                    "type": "double",
                    "value": "23.0",
                    "name": "counter_stop"
                },
                {
                    "type": "double",
                    "value": "24.0",
                    "name": "missing_packets"
                }
            ]
        }]

        isp_gap_event4 = self.session.query(Event).join(Gauge).filter(Gauge.name == "ISP_GAP",
                                                                                 Event.start == "2018-07-21T08:52:37.051941",
                                                                                 Event.stop == "2018-07-21T08:52:38.777507").all()

        assert len(isp_gap_event4) == 1

        assert isp_gap_event4[0].get_structured_values() == [{
            "name": "values",
            "values": [
                {
                    "name": "impact",
                    "type": "text",
                    "value": "SMALLER_THAN_A_SCENE"
                },
                {
                    "name": "band",
                    "type": "text",
                    "value": "1"
                },
                {
                    "name": "detector",
                    "type": "double",
                    "value": "12.0"
                },
                {
                    "name": "downlink_orbit",
                    "type": "double",
                    "value": "16078.0"
                },
                {
                    "name": "satellite",
                    "type": "text",
                    "value": "S2A"
                },
                {
                    "name": "reception_station",
                    "type": "text",
                    "value": "MPS_"
                },
                {
                    "name": "vcid",
                    "type": "double",
                    "value": "4.0"
                },
                {
                    "name": "downlink_mode",
                    "type": "text",
                    "value": "NOMINAL"
                },
                {
                    "name": "apid",
                    "type": "double",
                    "value": "0.0"
                },
                {
                    "name": "counter_start",
                    "type": "double",
                    "value": "23.0"
                },
                {
                    "name": "counter_stop",
                    "type": "double",
                    "value": "10.0"
                },
                {
                    "name": "missing_packets",
                    "type": "double",
                    "value": "11.0"
                }
            ],
            "type": "object"
        }]

        isp_gap_event5 = self.session.query(Event).join(Gauge).filter(Gauge.name == "ISP_GAP",
                                                                                 Event.start == "2018-07-21T08:52:42.385279",
                                                                                 Event.stop == "2018-07-21T08:52:43.640235").all()

        assert len(isp_gap_event5) == 1

    def test_insert_rep_pass_with_two_datablocks(self):
        filename = "S2A_NPPF.EOF"
        file_path = os.path.dirname(os.path.abspath(__file__)) + "/inputs/" + filename

        returned_value = ingestion_nppf.command_process_file(file_path)

        assert returned_value[0]["status"] == eboa_engine.exit_codes["OK"]["status"]

        filename = "S2A_ORBPRE.EOF"
        file_path = os.path.dirname(os.path.abspath(__file__)) + "/inputs/" + filename

        returned_value = ingestion_orbpre.command_process_file(file_path)

        assert returned_value[0]["status"] == eboa_engine.exit_codes["OK"]["status"]

        filename = "S2__SRA.EOF"
        file_path = os.path.dirname(os.path.abspath(__file__)) + "/inputs/" + filename

        returned_value = ingestion_sra.command_process_file(file_path)

        assert returned_value[0]["status"] == eboa_engine.exit_codes["OK"]["status"]

        filename = "S2A_OPER_REP_PASS_E_CONTAINING_ALL_DATA_TO_BE_PROCESS.EOF"
        file_path = os.path.dirname(os.path.abspath(__file__)) + "/inputs/" + filename

        returned_value = ingestion_edrs.command_process_file(file_path)

        assert returned_value[0]["status"] == eboa_engine.exit_codes["OK"]["status"]

        # Check that the validity period of the input has taken into consideration the MSI sensing received
        source = self.session.query(Source).filter(Source.name == "S2A_OPER_REP_PASS_E_CONTAINING_ALL_DATA_TO_BE_PROCESS.EOF",
                                                   Source.generation_time == "2018-07-21T07:38:07",
                                                   Source.validity_start == "2018-07-21 05:16:48.773036",
                                                   Source.validity_stop == "2018-07-21 05:40:35.417601").all()

        assert len(source) == 1

        dim_signature = self.session.query(DimSignature).filter(DimSignature.dim_signature == "RECEPTION_S2A").all()

        assert len(dim_signature) == 1

        # definite_explicit_ref = self.session.query(ExplicitRef).join(ExplicitRefGrp).filter(ExplicitRef.explicit_ref == "L20180608110336202000113",
        #                                                                             ExplicitRefGrp.name == "EDRS_LINK_SESSION_IDs").all()
        #
        # assert len(definite_explicit_ref) == 1

        # Check number of annotations generated
        annotations = self.session.query(Annotation).join(Source).filter(Source.name == "S2A_OPER_REP_PASS_E_CONTAINING_ALL_DATA_TO_BE_PROCESS.EOF").all()

        assert len(annotations) == 1

        # Check LINK_DETAILS annotations
        link_details = self.session.query(Annotation).join(AnnotationCnf,ExplicitRef).filter(AnnotationCnf.name == "LINK-DETAILS-CH2",
                                                                                             ExplicitRef.explicit_ref == "L20180608110336202000113").all()

        assert len(link_details) == 1

        assert link_details[0].get_structured_values() == [
            {
                    "name": "link_details",
                    "type": "object",
                    "values": [
                        {
                            "name": "session_id",
                            "type": "text",
                            "value": "L20180608110336202000113"
                        },
                        {
                            "name": "downlink_orbit",
                            "type": "double",
                            "value": "16076.0"
                        },
                        {
                            "name": "satellite",
                            "type": "text",
                            "value": "S2A"
                        },
                        {
                            "name": "reception_station",
                            "type": "text",
                            "value": "EDRS"
                        },
                        {
                            "name": "isp_status",
                            "type": "text",
                            "value": "COMPLETE"
                        },
                        {
                            "name": "acquisition_status",
                            "type": "text",
                            "value": "COMPLETE"
                        }
                    ]
                }
            ]

        # Check number of events generated
        events = self.session.query(Event).join(Source).filter(Source.name == "S2A_OPER_REP_PASS_E_CONTAINING_ALL_DATA_TO_BE_PROCESS.EOF").all()

        assert len(events) == 9

        # Check ISP_VALIDITY events
        playback_validity_events = self.session.query(Event).join(Gauge).filter(Gauge.name == "PLAYBACK_VALIDITY").all()

        assert len(playback_validity_events) == 2

        definite_playback_validity_event = self.session.query(Event).join(Gauge).filter(Gauge.name == "PLAYBACK_VALIDITY",
                                                                                   Event.start == "2018-07-21 07:28:23.002358",
                                                                                   Event.stop == "2018-07-21 07:37:40.689924").all()

        assert definite_playback_validity_event[0].get_structured_values() == [
            {
                "name": "values",
                "type": "object",
                "values": [
                    {
                        "name": "downlink_orbit",
                        "type": "double",
                        "value": "16076.0"
                    },{
                        "name": "satellite",
                        "type": "text",
                        "value": "S2A"
                    },{
                        "name": "reception_station",
                        "type": "text",
                        "value": "EDRS"
                    },{
                        "name": "channel",
                        "type": "double",
                        "value": "2.0"
                    },{
                        "name": "vcid",
                        "type": "double",
                        "value": "20.0"
                    },{
                        "name": "downlink_mode",
                        "type": "text",
                        "value": "NOMINAL"
                    },{
                        "name": "matching_plan_status",
                        "type": "text",
                        "value": "MATCHED_PLANNED_PLAYBACK"
                    },{
                        "name": "status",
                        "type": "text",
                        "value": "COMPLETE"
                    }
                ]
            }
        ]

        raw_isp_validity_events = self.session.query(Event).join(Gauge).filter(Gauge.name == "RAW_ISP_VALIDITY").all()

        assert len(raw_isp_validity_events) == 1

        assert raw_isp_validity_events[0].get_structured_values() == [
            {
                "name": "values",
                "type": "object",
                "values": [
                    {
                        "name": "status",
                        "type": "text",
                        "value": "COMPLETE"
                    },{
                        "name": "downlink_orbit",
                        "type": "double",
                        "value": "16076.0"
                    },{
                        "name": "satellite",
                        "type": "text",
                        "value": "S2A"
                    },{
                        "name": "reception_station",
                        "type": "text",
                        "value": "EDRS"
                    },{
                        "name": "channel",
                        "type": "double",
                        "value": "2.0"
                    },{
                        "name": "vcid",
                        "type": "double",
                        "value": "20.0"
                    },{
                        "name": "downlink_mode",
                        "type": "text",
                        "value": "NOMINAL"
                    },{
                        "name": "num_packets",
                        "type": "double",
                        "value": "2015280.0"
                    },{
                        "name": "num_frames",
                        "type": "double",
                        "value": "17927163.0"
                    },{
                        "name": "expected_num_packets",
                        "type": "double",
                        "value": "1931040.0"
                    },{
                        "name": "diff_expected_received",
                        "type": "double",
                        "value": "-84240.0"
                    },{
                        "name": "packet_status",
                        "type": "text",
                        "value": "MISSING"
                    }
                ]
            }
        ]

        isp_validity_events = self.session.query(Event).join(Gauge).filter(Gauge.name == "ISP_VALIDITY").all()

        assert len(isp_validity_events) == 2

        definite_isp_validity_event = self.session.query(Event).join(Gauge).filter(Gauge.name == "ISP_VALIDITY",
                                                                                   Event.start == "2018-07-21 05:22:08.947423",
                                                                                   Event.stop == "2018-07-21 05:36:36.329510").all()

        assert len(definite_isp_validity_event) == 1

        definite_planned_cut_imaging_event = self.session.query(Event).join(Gauge).filter(Gauge.name == "PLANNED_CUT_IMAGING",
                                                                                          Event.start == "2018-07-21 05:16:43.408000",
                                                                                          Event.stop == "2018-07-21 05:36:31.207000").all()

        assert len(definite_planned_cut_imaging_event) == 1

        # Check link with imaging
        link_imaging = self.session.query(EventLink).filter(EventLink.name == "ISP-VALIDITY",
                                                              EventLink.event_uuid_link == definite_isp_validity_event[0].event_uuid,
                                                              EventLink.event_uuid == definite_planned_cut_imaging_event[0].event_uuid).all()

        assert len(link_imaging) == 1

        # Check link with raw_isp_validity
        link_raw_isp = self.session.query(EventLink).filter(EventLink.name == "ISP_VALIDITY",
                                                              EventLink.event_uuid_link == definite_isp_validity_event[0].event_uuid,
                                                              EventLink.event_uuid == raw_isp_validity_events[0].event_uuid).all()

        assert len(link_raw_isp) == 1

        # Check link with playback_validity
        link_playback = self.session.query(EventLink).filter(EventLink.name == "ISP_VALIDITY",
                                                              EventLink.event_uuid_link == definite_isp_validity_event[0].event_uuid,
                                                              EventLink.event_uuid == definite_playback_validity_event[0].event_uuid).all()

        assert len(link_playback) == 1

        assert definite_isp_validity_event[0].get_structured_values() == [
            {
                "name": "values",
                "type": "object",
                "values": [
                    {
                        "name": "status",
                        "type": "text",
                        "value": "COMPLETE"
                    },{
                        "name": "downlink_orbit",
                        "type": "double",
                        "value": "16076.0"
                    },{
                        "name": "satellite",
                        "type": "text",
                        "value": "S2A"
                    },{
                        "name": "reception_station",
                        "type": "text",
                        "value": "EDRS"
                    },{
                        "name": "channel",
                        "type": "double",
                        "value": "2.0"
                    },{
                        "name": "vcid",
                        "type": "double",
                        "value": "20.0"
                    },{
                        "name": "downlink_mode",
                        "type": "text",
                        "value": "NOMINAL"
                    },{
                        "name": "sensing_orbit",
                        "type": "text",
                        "value": "16075.0"
                    }
                ]
            }
        ]

        isp_completeness_events = self.session.query(Event).join(Gauge).filter(Gauge.name == "PLANNED_IMAGING_ISP_COMPLETENESS_CHANNEL_2").all()

        assert len(isp_completeness_events) == 2

        definite_isp_completeness_event = self.session.query(Event).join(Gauge).filter(Gauge.name == "PLANNED_IMAGING_ISP_COMPLETENESS_CHANNEL_2",
                                                                                   Event.start == "2018-07-21 05:16:48.773036",
                                                                                   Event.stop == "2018-07-21 05:36:36.329510").all()

        assert len(definite_isp_completeness_event) == 1

        # Check link with imaging
        link_imaging = self.session.query(EventLink).filter(EventLink.name == "ISP_COMPLETENESS",
                                                              EventLink.event_uuid_link == definite_isp_completeness_event[0].event_uuid,
                                                              EventLink.event_uuid == definite_planned_cut_imaging_event[0].event_uuid).all()

        assert len(link_imaging) == 1

        # Check link with sp_validity
        link_raw_isp = self.session.query(EventLink).filter(EventLink.name == "COMPLETENESS",
                                                              EventLink.event_uuid_link == definite_isp_completeness_event[0].event_uuid,
                                                              EventLink.event_uuid == definite_isp_validity_event[0].event_uuid).all()

        assert len(link_raw_isp) == 1

        assert definite_isp_validity_event[0].get_structured_values() == [
            {
                "name": "details",
                "type": "object",
                "values": [
                    {
                        "name": "status",
                        "type": "text",
                        "value": "RECEIVED"
                    },{
                        "name": "downlink_orbit",
                        "type": "double",
                        "value": "16076"
                    },{
                        "name": "satellite",
                        "type": "text",
                        "value": "S2A"
                    },{
                        "name": "reception_station",
                        "type": "text",
                        "value": "EDRS"
                    },{
                        "name": "channel",
                        "type": "double",
                        "value": "2.0"
                    },{
                        "name": "vcid",
                        "type": "double",
                        "value": "20.0"
                    },{
                        "name": "downlink_mode",
                        "type": "text",
                        "value": "NOMINAL"
                    }
                ]
            }
        ]

    def test_insert_rep_pass_no_data(self):
        filename = "S2A_REP_PASS_NO_DATA.EOF"
        file_path = os.path.dirname(os.path.abspath(__file__)) + "/inputs/" + filename

        returned_value = ingestion_dfep.command_process_file(file_path)

        assert returned_value[0]["status"] == eboa_engine.exit_codes["OK"]["status"]

        # Check number of events generated
        events = self.session.query(Event).all()

        assert len(events) == 0

        # Check number of annotations generated
        annotations = self.session.query(Annotation).all()

        assert len(annotations) == 1

        # Check that the validity period of the input has taken into consideration the MSI sensing received
        source = self.session.query(Source).filter(Source.validity_start == "2018-07-21T10:35:27",
                                                   Source.validity_stop == "2018-07-21T10:37:39").all()

        assert len(source) == 1

        # Check LINK_DETAILS annotations
        link_details = self.session.query(Annotation).join(AnnotationCnf).filter(AnnotationCnf.name == "LINK_DETAILS").all()

        assert len(link_details) == 1

        assert link_details[0].get_structured_values() == [
            {
                "values": [
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
                        "value": "COMPLETE",
                        "type": "text",
                        "name": "isp_status"
                    },
                    {
                        "value": "COMPLETE",
                        "type": "text",
                        "name": "acquisition_status"
                    }
                ],
                "type": "object",
                "name": "link_details"
            }
        ]

    def test_insert_rep_pass_with_playback_gaps(self):
        filename = "S2A_REP_PASS_CONTAINING_PLAYBACK_GAPS.EOF"
        file_path = os.path.dirname(os.path.abspath(__file__)) + "/inputs/" + filename

        returned_value = ingestion_dfep.command_process_file(file_path)

        assert returned_value[0]["status"] == eboa_engine.exit_codes["OK"]["status"]

        # Check number of events generated
        events = self.session.query(Event).all()

        assert len(events) == 10

        # Check number of annotations generated
        annotations = self.session.query(Annotation).all()

        assert len(annotations) == 1

        # Check that the validity period of the input has taken into consideration the MSI sensing received
        source = self.session.query(Source).filter(Source.validity_start == "2018-07-20T22:00:12.859222",
                                                   Source.validity_stop == "2018-07-21T10:37:26.355940").all()

        assert len(source) == 1

        # Check LINK_DETAILS annotations
        link_details = self.session.query(Annotation).join(AnnotationCnf).filter(AnnotationCnf.name == "LINK_DETAILS").all()

        assert len(link_details) == 1

        assert link_details[0].get_structured_values() == [
            {
                "values": [
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
                        "value": "COMPLETE",
                        "type": "text",
                        "name": "isp_status"
                    },
                    {
                        "value": "INCOMPLETE",
                        "type": "text",
                        "name": "acquisition_status"
                    }
                ],
                "type": "object",
                "name": "link_details"
            }
        ]

        # Check definite PLAYBACK_GAP
        playback_gap = self.session.query(Event).join(Gauge).filter(Gauge.name == "PLAYBACK_GAP",
                                                                                 Event.start == "2018-07-21T10:36:12.525910",
                                                                                 Event.stop == "2018-07-21T10:36:12.585768").all()

        assert len(playback_gap) == 1

        assert playback_gap[0].get_structured_values() == [{
            "type": "object",
            "name": "values",
            "values": [
                {
                    "type": "double",
                    "name": "downlink_orbit",
                    "value": "16078.0"
                },
                {
                    "type": "text",
                    "name": "satellite",
                    "value": "S2A"
                },
                {
                    "type": "text",
                    "name": "reception_station",
                    "value": "MPS_"
                },
                {
                    "type": "double",
                    "name": "channel",
                    "value": "1.0"
                },
                {
                    "type": "double",
                    "name": "vcid",
                    "value": "4.0"
                },
                {
                    "type": "text",
                    "name": "downlink_mode",
                    "value": "NOMINAL"
                },
                {
                    "type": "double",
                    "name": "estimated_lost",
                    "value": "1023.0"
                },
                {
                    "type": "double",
                    "name": "pre_counter",
                    "value": "6280314.0"
                },
                {
                    "type": "double",
                    "name": "post_counter",
                    "value": "6281338.0"
                }
            ]
        }]

    def test_insert_rep_pass_with_plan(self):

        filename = "S2A_NPPF.EOF"
        file_path = os.path.dirname(os.path.abspath(__file__)) + "/inputs/" + filename

        returned_value = ingestion_nppf.command_process_file(file_path)

        assert returned_value[0]["status"] == eboa_engine.exit_codes["OK"]["status"]

        filename = "S2A_ORBPRE.EOF"
        file_path = os.path.dirname(os.path.abspath(__file__)) + "/inputs/" + filename

        returned_value = ingestion_orbpre.command_process_file(file_path)

        assert returned_value[0]["status"] == eboa_engine.exit_codes["OK"]["status"]

        filename = "S2A_REP_PASS_CONTAINING_ALL_DATA_TO_BE_PROCESS.EOF"
        file_path = os.path.dirname(os.path.abspath(__file__)) + "/inputs/" + filename

        returned_value = ingestion_dfep.command_process_file(file_path)

        assert returned_value[0]["status"] == eboa_engine.exit_codes["OK"]["status"]

        # Check sources
        source = self.session.query(Source).filter(Source.validity_start == "2018-07-20T22:00:12.859222",
                                                   Source.validity_stop == "2018-07-21T10:37:26.355940",
                                                   Source.name == "S2A_REP_PASS_CONTAINING_ALL_DATA_TO_BE_PROCESS.EOF",
                                                   Source.processor == "ingestion_dfep_acquisition.py").all()

        assert len(source) == 1

        source = self.session.query(Source).filter(Source.validity_start == "2018-07-21T08:36:02.255634",
                                                   Source.validity_stop == "2018-07-21T09:08:56.195941",
                                                   Source.name == "S2A_REP_PASS_CONTAINING_ALL_DATA_TO_BE_PROCESS.EOF",
                                                   Source.processor == "isp_planning_completeness_ingestion_dfep_acquisition.py").all()

        assert len(source) == 1

        source = self.session.query(Source).filter(Source.validity_start == "2018-07-21T10:35:35.524661",
                                                   Source.validity_stop == "2018-07-21T10:37:24.534390",
                                                   Source.name == "S2A_REP_PASS_CONTAINING_ALL_DATA_TO_BE_PROCESS.EOF",
                                                   Source.processor == "playback_planning_completeness_ingestion_dfep_acquisition.py").all()

        assert len(source) == 1

        # Check number of events generated
        events = self.session.query(Event).join(Source).filter(Source.name == "S2A_REP_PASS_CONTAINING_ALL_DATA_TO_BE_PROCESS.EOF").all()

        assert len(events) == 13

        # Check PLANNED_IMAGING_ISP_COMPLETENESS_CHANNEL events
        isp_completeness_events = self.session.query(Event).join(Gauge).filter(Gauge.name.like("PLANNED_IMAGING_ISP_COMPLETENESS_CHANNEL%")).all()

        assert len(isp_completeness_events) == 6

        # Check definite ISP completeness
        isp_completeness_missing_left = self.session.query(Event).join(Gauge).filter(Gauge.name.like("PLANNED_IMAGING_ISP_COMPLETENESS_CHANNEL_%"),
                                                                                 Event.start == "2018-07-21T08:36:02.255634",
                                                                                 Event.stop == "2018-07-21T08:52:29.993268").all()

        assert len(isp_completeness_missing_left) == 2

        isp_completeness_statuses = [event for event in isp_completeness_missing_left if len([value for value in event.eventTexts if value.name == "status" and value.value == "MISSING"]) > 0]

        assert len(isp_completeness_statuses) == 2

        # Check number of annotations generated
        annotations = self.session.query(Annotation).join(Source).filter(Source.name == "S2A_REP_PASS_CONTAINING_ALL_DATA_TO_BE_PROCESS.EOF").all()

        assert len(annotations) == 1

    def test_insert_rep_pass_only_hktm_with_plan(self):

        filename = "S2A_NPPF_ONLY_HKTM.EOF"
        file_path = os.path.dirname(os.path.abspath(__file__)) + "/inputs/" + filename

        returned_value = ingestion_nppf.command_process_file(file_path)

        assert returned_value[0]["status"] == eboa_engine.exit_codes["OK"]["status"]

        filename = "S2A_ORBPRE_ONLY_HKTM.EOF"
        file_path = os.path.dirname(os.path.abspath(__file__)) + "/inputs/" + filename

        returned_value = ingestion_orbpre.command_process_file(file_path)

        assert returned_value[0]["status"] == eboa_engine.exit_codes["OK"]["status"]

        filename = "S2A_REP_PASS_ONLY_HKTM.EOF"
        file_path = os.path.dirname(os.path.abspath(__file__)) + "/inputs/" + filename

        returned_value = ingestion_dfep.command_process_file(file_path)

        assert returned_value[0]["status"] == eboa_engine.exit_codes["OK"]["status"]

        # Check sources
        source = self.session.query(Source).filter(Source.validity_start == "2018-07-21T01:47:42.854151",
                                                   Source.validity_stop == "2018-07-21T01:47:43.833085",
                                                   Source.name == "S2A_REP_PASS_ONLY_HKTM.EOF",
                                                   Source.processor == "ingestion_dfep_acquisition.py").all()

        assert len(source) == 1

        source = self.session.query(Source).filter(Source.validity_start == "2018-07-21T01:47:43.206477",
                                                   Source.validity_stop == "2018-07-21T01:47:43.206477",
                                                   Source.name == "S2A_REP_PASS_ONLY_HKTM.EOF",
                                                   Source.processor == "playback_planning_completeness_ingestion_dfep_acquisition.py").all()

        assert len(source) == 1

        # Check number of events generated
        events = self.session.query(Event).join(Source).filter(Source.name == "S2A_REP_PASS_ONLY_HKTM.EOF").all()

        assert len(events) == 2

        # Check number of annotations generated
        annotations = self.session.query(Annotation).join(Source).filter(Source.name == "S2A_REP_PASS_ONLY_HKTM.EOF").all()

        assert len(annotations) == 1

    def test_insert_rep_pass_playback_rt_with_plan(self):

        filename = "S2A_NPPF_PLAYBACK_RT.EOF"
        file_path = os.path.dirname(os.path.abspath(__file__)) + "/inputs/" + filename

        returned_value = ingestion_nppf.command_process_file(file_path)

        assert returned_value[0]["status"] == eboa_engine.exit_codes["OK"]["status"]

        filename = "S2A_ORBPRE.EOF"
        file_path = os.path.dirname(os.path.abspath(__file__)) + "/inputs/" + filename

        returned_value = ingestion_orbpre.command_process_file(file_path)

        assert returned_value[0]["status"] == eboa_engine.exit_codes["OK"]["status"]

        filename = "S2A_REP_PASS_PLAYBACK_RT.EOF"
        file_path = os.path.dirname(os.path.abspath(__file__)) + "/inputs/" + filename

        returned_value = ingestion_dfep.command_process_file(file_path)

        assert returned_value[0]["status"] == eboa_engine.exit_codes["OK"]["status"]

        # Check sources
        source = self.session.query(Source).filter(Source.validity_start == "2018-07-20T22:00:12.859222",
                                                   Source.validity_stop == "2018-07-21T10:37:26.355940",
                                                   Source.name == "S2A_REP_PASS_PLAYBACK_RT.EOF",
                                                   Source.processor == "ingestion_dfep_acquisition.py").all()

        assert len(source) == 1

        source = self.session.query(Source).filter(Source.validity_start == "2018-07-21T08:36:02.255634",
                                                   Source.validity_stop == "2018-07-21T09:08:56.195941",
                                                   Source.name == "S2A_REP_PASS_PLAYBACK_RT.EOF",
                                                   Source.processor == "isp_planning_completeness_ingestion_dfep_acquisition.py").all()

        assert len(source) == 1

        source = self.session.query(Source).filter(Source.validity_start == "2018-07-21T08:36:01.255634",
                                                   Source.validity_stop == "2018-07-21T10:37:24.534390",
                                                   Source.name == "S2A_REP_PASS_PLAYBACK_RT.EOF",
                                                   Source.processor == "playback_planning_completeness_ingestion_dfep_acquisition.py").all()

        assert len(source) == 1

        # Check number of events generated
        events = self.session.query(Event).join(Source).filter(Source.name == "S2A_REP_PASS_PLAYBACK_RT.EOF").all()

        assert len(events) == 15

        # Check number of annotations generated
        annotations = self.session.query(Annotation).join(Source).filter(Source.name == "S2A_REP_PASS_PLAYBACK_RT.EOF").all()

        assert len(annotations) == 1
