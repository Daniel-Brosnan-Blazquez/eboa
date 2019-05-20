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
import ingestions.s2.ingestion_edrs_acquisition.ingestion_edrs_acquisition as ingestion_edrs_acquisition

# Import ingestion nppf
import ingestions.s2.ingestion_nppf.ingestion_nppf as ingestion_nppf

# Import ingestion orbpre
import ingestions.s2.ingestion_orbpre.ingestion_orbpre as ingestion_orbpre

# Import ingestion sra
import ingestions.s2.ingestion_slot_request_edrs.ingestion_slot_request_edrs as ingestion_sra

class TestEdrsAcquisitionIngestion(unittest.TestCase):
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

        returned_value = ingestion_edrs_acquisition.command_process_file(file_path)

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
        link_details = self.session.query(Annotation).join(AnnotationCnf,ExplicitRef).filter(AnnotationCnf.name == "LINK_DETAILS_CH2",
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
                "name": "details",
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

        definite_playback_validity_event2 = self.session.query(Event).join(Gauge).filter(Gauge.name == "PLAYBACK_VALIDITY",
                                                                                   Event.start == "2018-07-21T07:37:41.747087",
                                                                                   Event.stop == "2018-07-21T07:37:55.121772").all()

        assert definite_playback_validity_event2[0].get_structured_values() == [
            {
                "name": "details",
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
                        "value": "2.0"
                    },{
                        "name": "downlink_mode",
                        "type": "text",
                        "value": "SAD"
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
                "name": "details",
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
                        "value": "1931040.0"
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
                        "value": "-1931040.0"
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

        filename = "S2A_OPER_REP_PASS_E_CONTAINING_GAPS.EOF"
        file_path = os.path.dirname(os.path.abspath(__file__)) + "/inputs/" + filename

        returned_value = ingestion_edrs_acquisition.command_process_file(file_path)

        assert returned_value[0]["status"] == eboa_engine.exit_codes["OK"]["status"]

        # Check number of events generated
        events = self.session.query(Event).join(Source).filter(Source.name == "S2A_OPER_REP_PASS_E_CONTAINING_GAPS.EOF").all()

        assert len(events) == 14

        # Check number of annotations generated
        annotations = self.session.query(Annotation).all()

        assert len(annotations) == 1

        # Check that the validity period of the input has taken into consideration the MSI sensing received
        source = self.session.query(Source).filter(Source.validity_start == "2018-07-20T17:33:12.859268",
                                                   Source.validity_stop == "2018-07-21T07:37:55.121811").all()

        assert len(source) == 1

        # Check LINK_DETAILS annotations
        link_details = self.session.query(Annotation).join(AnnotationCnf).filter(AnnotationCnf.name == "LINK_DETAILS_CH2").all()

        assert len(link_details) == 1

        assert link_details[0].get_structured_values() == [
            {
                "values": [
                    {
                        "value": "L20180608110336202000113",
                        "type": "text",
                        "name": "session_id"
                    },{
                        "value": "16076.0",
                        "type": "double",
                        "name": "downlink_orbit"
                    },
                    {
                        "value": "S2A",
                        "type": "text",
                        "name": "satellite"
                    },
                    {
                        "value": "EDRS",
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

        # Check RAW_ISP_VALIDITY events
        raw_isp_validities = self.session.query(Event).join(Gauge).filter(Gauge.name == "RAW_ISP_VALIDITY").all()

        assert len(raw_isp_validities) == 1

        # Check specific RAW_ISP_VALIDITY
        specific_raw_isp_validity1 = self.session.query(Event).join(Gauge).filter(Gauge.name == "RAW_ISP_VALIDITY",
                                                                                 Event.start == "2018-07-21T05:22:08.947423",
                                                                                 Event.stop == "2018-07-21T05:40:36").all()

        assert len(specific_raw_isp_validity1) == 1

        assert specific_raw_isp_validity1[0].get_structured_values() == [
            {
                "name": "details",
                "values": [
                    {
                        "name": "status",
                        "value": "INCOMPLETE",
                        "type": "text"
                    },
                    {
                        "name": "downlink_orbit",
                        "value": "16076.0",
                        "type": "double"
                    },
                    {
                        "name": "satellite",
                        "value": "S2A",
                        "type": "text"
                    },
                    {
                        "name": "reception_station",
                        "value": "EDRS",
                        "type": "text"
                    },
                    {
                        "name": "channel",
                        "value": "2.0",
                        "type": "double"
                    },
                    {
                        "name": "vcid",
                        "value": "20.0",
                        "type": "double"
                    },
                    {
                        "name": "downlink_mode",
                        "value": "NOMINAL",
                        "type": "text"
                    },
                    {
                        "name": "num_packets",
                        "value": "1930040.0",
                        "type": "double"
                    },
                    {
                        "name": "num_frames",
                        "value": "17927163.0",
                        "type": "double"
                    },
                    {
                        "name": "expected_num_packets",
                        "value": "1931040.0",
                        "type": "double"
                    },
                    {
                        "name": "diff_expected_received",
                        "value": "1000.0",
                        "type": "double"
                    },
                    {
                        "name": "packet_status",
                        "value": "MISSING",
                        "type": "text"
                    }
                ],
                "type": "object"
            }
        ]

        # Check ISP_GAPs events
        isp_gap_events = self.session.query(Event).join(Gauge).filter(Gauge.name.like("ISP_GAP")).all()

        assert len(isp_gap_events) == 3

        # Check definite ISP_GAP
        isp_gap_event1 = self.session.query(Event).join(Gauge).filter(Gauge.name == "ISP_GAP",
                                                                                 Event.start == "2018-07-21T05:22:08.947423",
                                                                                 Event.stop == "2018-07-21T05:40:36").all()

        assert len(isp_gap_event1) == 1

        assert isp_gap_event1[0].get_structured_values() == [
            {
                "name": "details",
                "values": [
                    {
                        "name": "impact",
                        "value": "COMPLETE",
                        "type": "text"
                    },
                    {
                        "name": "band",
                        "value": "1",
                        "type": "text"
                    },
                    {
                        "name": "detector",
                        "value": "6.0",
                        "type": "double"
                    },
                    {
                        "name": "downlink_orbit",
                        "value": "16076.0",
                        "type": "double"
                    },
                    {
                        "name": "satellite",
                        "value": "S2A",
                        "type": "text"
                    },
                    {
                        "name": "reception_station",
                        "value": "EDRS",
                        "type": "text"
                    },
                    {
                        "name": "channel",
                        "value": "2.0",
                        "type": "double"
                    },
                    {
                        "name": "vcid",
                        "value": "20.0",
                        "type": "double"
                    },
                    {
                        "name": "downlink_mode",
                        "value": "NOMINAL",
                        "type": "text"
                    },
                    {
                        "name": "apid",
                        "value": "256.0",
                        "type": "double"
                    }
                ],
                "type": "object"
            }
        ]
        
        isp_gap_event2 = self.session.query(Event).join(Gauge).filter(Gauge.name == "ISP_GAP",
                                                                                 Event.start == "2018-07-21T05:22:08.947423",
                                                                                 Event.stop == "2018-07-21T05:30:08.947423").all()

        assert len(isp_gap_event2) == 1

        assert isp_gap_event2[0].get_structured_values() == [
            {
                "name": "details",
                "values": [
                    {
                        "name": "impact",
                        "value": "AT_THE_BEGINNING",
                        "type": "text"
                    },
                    {
                        "name": "band",
                        "value": "3",
                        "type": "text"
                    },
                    {
                        "name": "detector",
                        "value": "6.0",
                        "type": "double"
                    },
                    {
                        "name": "downlink_orbit",
                        "value": "16076.0",
                        "type": "double"
                    },
                    {
                        "name": "satellite",
                        "value": "S2A",
                        "type": "text"
                    },
                    {
                        "name": "reception_station",
                        "value": "EDRS",
                        "type": "text"
                    },
                    {
                        "name": "channel",
                        "value": "2.0",
                        "type": "double"
                    },
                    {
                        "name": "vcid",
                        "value": "20.0",
                        "type": "double"
                    },
                    {
                        "name": "downlink_mode",
                        "value": "NOMINAL",
                        "type": "text"
                    },
                    {
                        "name": "apid",
                        "value": "258.0",
                        "type": "double"
                    }
                ],
                "type": "object"
            }   
        ]

        isp_gap_event3 = self.session.query(Event).join(Gauge).filter(Gauge.name == "ISP_GAP",
                                                                                 Event.start == "2018-07-21T05:35:36",
                                                                                 Event.stop == "2018-07-21T05:40:36").all()

        assert len(isp_gap_event3) == 1

        assert isp_gap_event3[0].get_structured_values() == [
            {
                "name": "details",
                "values": [
                    {
                        "name": "impact",
                        "value": "AT_THE_END",
                        "type": "text"
                    },
                    {
                        "name": "band",
                        "value": "4",
                        "type": "text"
                    },
                    {
                        "name": "detector",
                        "value": "6.0",
                        "type": "double"
                    },
                    {
                        "name": "downlink_orbit",
                        "value": "16076.0",
                        "type": "double"
                    },
                    {
                        "name": "satellite",
                        "value": "S2A",
                        "type": "text"
                    },
                    {
                        "name": "reception_station",
                        "value": "EDRS",
                        "type": "text"
                    },
                    {
                        "name": "channel",
                        "value": "2.0",
                        "type": "double"
                    },
                    {
                        "name": "vcid",
                        "value": "20.0",
                        "type": "double"
                    },
                    {
                        "name": "downlink_mode",
                        "value": "NOMINAL",
                        "type": "text"
                    },
                    {
                        "name": "apid",
                        "value": "259.0",
                        "type": "double"
                    }
                ],
                "type": "object"
            }
        ]

        # Check PLAYBACK_GAPs events
        playback_gap_events = self.session.query(Event).join(Gauge).filter(Gauge.name.like("PLAYBACK_GAP")).all()

        assert len(playback_gap_events) == 1

        # Check definite PLAYBACK_GAP
        playback_gap_event1 = self.session.query(Event).join(Gauge).filter(Gauge.name == "PLAYBACK_GAP",
                                                                                 Event.start == "2018-07-21T07:28:23.555000",
                                                                                 Event.stop == "2018-07-21T07:28:23.888999").all()

        assert len(playback_gap_event1) == 1

        assert playback_gap_event1[0].get_structured_values() == [
            {
                "type": "object",
                "values": [
                    {
                        "type": "double",
                        "name": "downlink_orbit",
                        "value": "16076.0"
                    },
                    {
                        "type": "text",
                        "name": "satellite",
                        "value": "S2A"
                    },
                    {
                        "type": "text",
                        "name": "reception_station",
                        "value": "EDRS"
                    },
                    {
                        "type": "double",
                        "name": "channel",
                        "value": "2.0"
                    },
                    {
                        "type": "double",
                        "name": "vcid",
                        "value": "20.0"
                    },
                    {
                        "type": "text",
                        "name": "downlink_mode",
                        "value": "NOMINAL"
                    },
                    {
                        "type": "double",
                        "name": "estimated_lost",
                        "value": "392733.0"
                    },
                    {
                        "type": "double",
                        "name": "pre_counter",
                        "value": "2399762.0"
                    },
                    {
                        "type": "double",
                        "name": "post_counter",
                        "value": "2792496.0"
                    }
                ],
                "name": "details"
            }
        ]

    def test_insert_rep_pass_no_data(self):
        filename = "S2A_OPER_REP_PASS_E_NO_DATA.EOF"
        file_path = os.path.dirname(os.path.abspath(__file__)) + "/inputs/" + filename

        returned_value = ingestion_edrs_acquisition.command_process_file(file_path)

        assert returned_value[0]["status"] == eboa_engine.exit_codes["OK"]["status"]

        # Check number of events generated
        events = self.session.query(Event).all()

        assert len(events) == 0

        # Check number of annotations generated
        annotations = self.session.query(Annotation).all()

        assert len(annotations) == 0

        # Check that the validity period of the input has taken into consideration the MSI sensing received
        source = self.session.query(Source).filter(Source.validity_start == "2018-07-21T07:28:23",
                                                   Source.validity_stop == "2018-07-21T07:37:55").all()

        assert len(source) == 1

    def test_insert_rep_pass_with_plan(self):

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

        returned_value = ingestion_edrs_acquisition.command_process_file(file_path)

        assert returned_value[0]["status"] == eboa_engine.exit_codes["OK"]["status"]
        assert returned_value[1]["status"] == eboa_engine.exit_codes["OK"]["status"]

        # Check sources
        source = self.session.query(Source).filter(Source.validity_start == "2018-07-20T17:33:12.859268",
                                                   Source.validity_stop == "2018-07-21T07:37:55.121811",
                                                   Source.name == "S2A_OPER_REP_PASS_E_CONTAINING_ALL_DATA_TO_BE_PROCESS.EOF",
                                                   Source.processor == "ingestion_edrs_acquisition.py").all()

        assert len(source) == 1

        source = self.session.query(Source).filter(Source.validity_start == "2018-07-21T05:16:48.773036",
                                                   Source.validity_stop == "2018-07-21T05:40:35.417601",
                                                   Source.name == "S2A_OPER_REP_PASS_E_CONTAINING_ALL_DATA_TO_BE_PROCESS.EOF",
                                                   Source.processor == "isp_planning_completeness_ingestion_edrs_acquisition.py").all()

        assert len(source) == 1

        # Check number of events generated
        events = self.session.query(Event).join(Source).filter(Source.name == "S2A_OPER_REP_PASS_E_CONTAINING_ALL_DATA_TO_BE_PROCESS.EOF").all()

        assert len(events) == 10

        # Check PLANNED_IMAGING_ISP_COMPLETENESS_CHANNEL events
        isp_completeness_events = self.session.query(Event).join(Gauge).filter(Gauge.name.like("PLANNED_IMAGING_ISP_COMPLETENESS_CHANNEL%")).all()

        assert len(isp_completeness_events) == 3

        # Check definite ISP completeness
        isp_completeness_missing_left = self.session.query(Event).join(Gauge).filter(Gauge.name.like("PLANNED_IMAGING_ISP_COMPLETENESS_CHANNEL_%"),
                                                                                 Event.start == "2018-07-21T05:16:48.773036",
                                                                                 Event.stop == "2018-07-21T05:22:08.947423").all()

        assert len(isp_completeness_missing_left) == 1

        assert isp_completeness_missing_left[0].get_structured_values() == [
            {
                "type": "object",
                "values": [
                    {
                        "type": "text",
                        "value": "MPMSNOBS",
                        "name": "start_request"
                    },
                    {
                        "type": "text",
                        "value": "MPMSIMID",
                        "name": "stop_request"
                    },
                    {
                        "type": "double",
                        "value": "16075.0",
                        "name": "start_orbit"
                    },
                    {
                        "type": "double",
                        "value": "100.3083",
                        "name": "start_angle"
                    },
                    {
                        "type": "double",
                        "value": "16075.0",
                        "name": "stop_orbit"
                    },
                    {
                        "type": "double",
                        "value": "171.1847",
                        "name": "stop_angle"
                    },
                    {
                        "type": "text",
                        "value": "S2A",
                        "name": "satellite"
                    },
                    {
                        "type": "text",
                        "value": "NOMINAL",
                        "name": "record_type"
                    },
                    {
                        "type": "text",
                        "value": "NOMINAL",
                        "name": "imaging_mode"
                    },
                    {
                        "type": "object",
                        "values": [
                            {
                                "type": "double",
                                "value": "0.0",
                                "name": "start_scn_dup"
                            }
                        ],
                        "name": "parameters"
                    },
                    {
                        "type": "text",
                        "value": "TIME_CORRECTED",
                        "name": "status_correction"
                    },
                    {
                        "type": "double",
                        "value": "-5.365036",
                        "name": "delta_start"
                    },
                    {
                        "type": "double",
                        "value": "-5.12251",
                        "name": "delta_stop"
                    },
                    {
                        "type": "text",
                        "value": "MISSING",
                        "name": "status"
                    }
                ],
                "name": "details"
            }                    
        ]
        
        isp_completeness_statuses = [event for event in isp_completeness_missing_left if len([value for value in event.eventTexts if value.name == "status" and value.value == "MISSING"]) > 0]

        assert len(isp_completeness_statuses) == 1

        # Check number of annotations generated
        annotations = self.session.query(Annotation).join(Source).filter(Source.name == "S2A_OPER_REP_PASS_E_CONTAINING_ALL_DATA_TO_BE_PROCESS.EOF").all()

        assert len(annotations) == 1

        # Check specific ISP completeness
        isp_completeness_1 = self.session.query(Event).join(Gauge).filter(Gauge.name.like("PLANNED_IMAGING_ISP_COMPLETENESS_CHANNEL_%"),
                                                                                 Event.start == "2018-07-21T05:22:08.947423",
                                                                                 Event.stop == "2018-07-21T05:36:36.329510").all()

        assert len(isp_completeness_1) == 1

        assert isp_completeness_1[0].get_structured_values() == [
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
                        "value": "16076.0",
                        "name": "downlink_orbit"
                    },
                    {
                        "type": "text",
                        "value": "S2A",
                        "name": "satellite"
                    },
                    {
                        "type": "text",
                        "value": "EDRS",
                        "name": "reception_station"
                    },
                    {
                        "type": "double",
                        "value": "2.0",
                        "name": "channel"
                    },
                    {
                        "type": "double",
                        "value": "20.0",
                        "name": "vcid"
                    },
                    {
                        "type": "text",
                        "value": "NOMINAL",
                        "name": "downlink_mode"
                    },
                    {
                        "type": "text",
                        "value": "16075.0",
                        "name": "sensing_orbit"
                    }
                ],
                "name": "details"
            }
        ]

        isp_completeness_2 = self.session.query(Event).join(Gauge).filter(Gauge.name.like("PLANNED_IMAGING_ISP_COMPLETENESS_CHANNEL_%"),
                                                                                 Event.start == "2018-07-21T05:37:14.362833",
                                                                                 Event.stop == "2018-07-21T05:40:35.417601").all()

        assert len(isp_completeness_2) == 1

        # Check PLANNED_PLAYBACK_COMPLETENESS_CHANNEL events
        playback_completeness_events = self.session.query(Event).join(Gauge).filter(Gauge.name.like("PLANNED_PLAYBACK_COMPLETENESS_CHANNEL%")).all()

        assert len(playback_completeness_events) == 2

        # Check specific playback completeness
        playback_completeness_1 = self.session.query(Event).join(Gauge).filter(Gauge.name.like("PLANNED_PLAYBACK_COMPLETENESS_CHANNEL_%"),
                                                                                 Event.start == "2018-07-21T07:07:47",
                                                                                 Event.stop == "2018-07-21T07:25:07").all()

        assert len(playback_completeness_1) == 1

        assert playback_completeness_1[0].get_structured_values() == [
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
                        "value": "16076.0",
                        "name": "downlink_orbit"
                    },
                    {
                        "type": "text",
                        "value": "S2A",
                        "name": "satellite"
                    },
                    {
                        "type": "text",
                        "value": "EDRS",
                        "name": "reception_station"
                    },
                    {
                        "type": "double",
                        "value": "2.0",
                        "name": "channel"
                    },
                    {
                        "type": "double",
                        "value": "20.0",
                        "name": "vcid"
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

        # Check specific playback completeness
        playback_completeness_2 = self.session.query(Event).join(Gauge).filter(Gauge.name.like("PLANNED_PLAYBACK_COMPLETENESS_CHANNEL_%"),
                                                                                 Event.start == "2018-07-21T07:25:18",
                                                                                 Event.stop == "2018-07-21T07:25:18").all()

        assert len(playback_completeness_2) == 1

        assert playback_completeness_2[0].get_structured_values() == [
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
                        "value": "16076.0",
                        "name": "downlink_orbit"
                    },
                    {
                        "type": "text",
                        "value": "S2A",
                        "name": "satellite"
                    },
                    {
                        "type": "text",
                        "value": "EDRS",
                        "name": "reception_station"
                    },
                    {
                        "type": "double",
                        "value": "2.0",
                        "name": "channel"
                    },
                    {
                        "type": "double",
                        "value": "2.0",
                        "name": "vcid"
                    },
                    {
                        "type": "text",
                        "value": "SAD",
                        "name": "downlink_mode"
                    }
                ],
                "name": "details"
            }
        ]
        
    def test_insert_rep_pass_playback_rt_with_plan(self):

        filename = "S2A_NPPF_PLAYBACK_RT.EOF"
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


        filename = "S2A_OPER_REP_PASS_E_PLAYBACK_RT.EOF"
        file_path = os.path.dirname(os.path.abspath(__file__)) + "/inputs/" + filename

        returned_value = ingestion_edrs_acquisition.command_process_file(file_path)

        assert returned_value[0]["status"] == eboa_engine.exit_codes["OK"]["status"]
        assert returned_value[1]["status"] == eboa_engine.exit_codes["OK"]["status"]

        # Check sources
        source = self.session.query(Source).filter(Source.validity_start == "2018-07-20T17:33:12.859268",
                                                   Source.validity_stop == "2018-07-21T07:37:55.121811",
                                                   Source.name == "S2A_OPER_REP_PASS_E_PLAYBACK_RT.EOF",
                                                   Source.processor == "ingestion_edrs_acquisition.py").all()

        assert len(source) == 1

        source = self.session.query(Source).filter(Source.validity_start == "2018-07-21T05:16:48.773036",
                                                   Source.validity_stop == "2018-07-21T05:40:35.417601",
                                                   Source.name == "S2A_OPER_REP_PASS_E_PLAYBACK_RT.EOF",
                                                   Source.processor == "isp_planning_completeness_ingestion_edrs_acquisition.py").all()

        assert len(source) == 1

        # Check number of events generated
        events = self.session.query(Event).join(Source).filter(Source.name == "S2A_OPER_REP_PASS_E_PLAYBACK_RT.EOF").all()

        assert len(events) == 10

        # Check PLANNED_IMAGING_ISP_COMPLETENESS_CHANNEL events
        isp_completeness_events = self.session.query(Event).join(Gauge).filter(Gauge.name.like("PLANNED_IMAGING_ISP_COMPLETENESS_CHANNEL%")).all()

        assert len(isp_completeness_events) == 3

        # Check definite ISP completeness
        isp_completeness_missing_left = self.session.query(Event).join(Gauge).filter(Gauge.name.like("PLANNED_IMAGING_ISP_COMPLETENESS_CHANNEL_%"),
                                                                                 Event.start == "2018-07-21T05:16:48.773036",
                                                                                 Event.stop == "2018-07-21T05:22:08.947423").all()

        assert len(isp_completeness_missing_left) == 1

        assert isp_completeness_missing_left[0].get_structured_values() == [
            {
                "type": "object",
                "values": [
                    {
                        "type": "text",
                        "value": "MPMSNOBS",
                        "name": "start_request"
                    },
                    {
                        "type": "text",
                        "value": "MPMSIMID",
                        "name": "stop_request"
                    },
                    {
                        "type": "double",
                        "value": "16075.0",
                        "name": "start_orbit"
                    },
                    {
                        "type": "double",
                        "value": "100.3083",
                        "name": "start_angle"
                    },
                    {
                        "type": "double",
                        "value": "16075.0",
                        "name": "stop_orbit"
                    },
                    {
                        "type": "double",
                        "value": "171.1847",
                        "name": "stop_angle"
                    },
                    {
                        "type": "text",
                        "value": "S2A",
                        "name": "satellite"
                    },
                    {
                        "type": "text",
                        "value": "NOMINAL",
                        "name": "record_type"
                    },
                    {
                        "type": "text",
                        "value": "NOMINAL",
                        "name": "imaging_mode"
                    },
                    {
                        "type": "object",
                        "values": [
                            {
                                "type": "double",
                                "value": "0.0",
                                "name": "start_scn_dup"
                            }
                        ],
                        "name": "parameters"
                    },
                    {
                        "type": "text",
                        "value": "TIME_CORRECTED",
                        "name": "status_correction"
                    },
                    {
                        "type": "double",
                        "value": "-5.365036",
                        "name": "delta_start"
                    },
                    {
                        "type": "double",
                        "value": "-5.12251",
                        "name": "delta_stop"
                    },
                    {
                        "type": "text",
                        "value": "MISSING",
                        "name": "status"
                    }
                ],
                "name": "details"
            }                    
        ]
        
        isp_completeness_statuses = [event for event in isp_completeness_missing_left if len([value for value in event.eventTexts if value.name == "status" and value.value == "MISSING"]) > 0]

        assert len(isp_completeness_statuses) == 1

        # Check number of annotations generated
        annotations = self.session.query(Annotation).join(Source).filter(Source.name == "S2A_OPER_REP_PASS_E_PLAYBACK_RT.EOF").all()

        assert len(annotations) == 1

        # Check specific ISP completeness
        isp_completeness_1 = self.session.query(Event).join(Gauge).filter(Gauge.name.like("PLANNED_IMAGING_ISP_COMPLETENESS_CHANNEL_%"),
                                                                                 Event.start == "2018-07-21T05:22:08.947423",
                                                                                 Event.stop == "2018-07-21T05:36:36.329510").all()

        assert len(isp_completeness_1) == 1

        assert isp_completeness_1[0].get_structured_values() == [
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
                        "value": "16076.0",
                        "name": "downlink_orbit"
                    },
                    {
                        "type": "text",
                        "value": "S2A",
                        "name": "satellite"
                    },
                    {
                        "type": "text",
                        "value": "EDRS",
                        "name": "reception_station"
                    },
                    {
                        "type": "double",
                        "value": "2.0",
                        "name": "channel"
                    },
                    {
                        "type": "double",
                        "value": "22.0",
                        "name": "vcid"
                    },
                    {
                        "type": "text",
                        "value": "RT",
                        "name": "downlink_mode"
                    },
                    {
                        "type": "text",
                        "value": "16075.0",
                        "name": "sensing_orbit"
                    }
                ],
                "name": "details"
            }
        ]

        isp_completeness_2 = self.session.query(Event).join(Gauge).filter(Gauge.name.like("PLANNED_IMAGING_ISP_COMPLETENESS_CHANNEL_%"),
                                                                                 Event.start == "2018-07-21T05:37:14.362833",
                                                                                 Event.stop == "2018-07-21T05:40:35.417601").all()

        assert len(isp_completeness_2) == 1

        # Check PLANNED_PLAYBACK_COMPLETENESS_CHANNEL events
        playback_completeness_events = self.session.query(Event).join(Gauge).filter(Gauge.name.like("PLANNED_PLAYBACK_COMPLETENESS_CHANNEL%")).all()

        assert len(playback_completeness_events) == 2

        # Check PLANNED_PLAYBACK_COMPLETENESS_CHANNEL events
        playback_completeness_events = self.session.query(Event).join(Gauge).filter(Gauge.name.like("PLANNED_PLAYBACK_COMPLETENESS_CHANNEL%")).all()

        assert len(playback_completeness_events) == 2

        # Check specific playback completeness
        playback_completeness_1 = self.session.query(Event).join(Gauge).filter(Gauge.name.like("PLANNED_PLAYBACK_COMPLETENESS_CHANNEL_%"),
                                                                                 Event.start == "2018-07-21T07:07:47",
                                                                                 Event.stop == "2018-07-21T07:25:07").all()

        assert len(playback_completeness_1) == 1

        assert playback_completeness_1[0].get_structured_values() == [
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
                        "value": "16076.0",
                        "name": "downlink_orbit"
                    },
                    {
                        "type": "text",
                        "value": "S2A",
                        "name": "satellite"
                    },
                    {
                        "type": "text",
                        "value": "EDRS",
                        "name": "reception_station"
                    },
                    {
                        "type": "double",
                        "value": "2.0",
                        "name": "channel"
                    },
                    {
                        "type": "double",
                        "value": "22.0",
                        "name": "vcid"
                    },
                    {
                        "type": "text",
                        "value": "RT",
                        "name": "downlink_mode"
                    }
                ],
                "name": "details"
            }
        ]

    def test_insert_rep_pass_only_hktm(self):

        filename = "S2A_OPER_REP_PASS_E_ONLY_HKTM.EOF"
        file_path = os.path.dirname(os.path.abspath(__file__)) + "/inputs/" + filename

        returned_value = ingestion_edrs_acquisition.command_process_file(file_path)

        assert returned_value[0]["status"] == eboa_engine.exit_codes["OK"]["status"]

        # Check sources
        source = self.session.query(Source).filter(Source.validity_start == "2018-07-21T01:47:42.854151",
                                                   Source.validity_stop == "2018-07-21T01:47:43.833085",
                                                   Source.name == "S2A_OPER_REP_PASS_E_ONLY_HKTM.EOF",
                                                   Source.processor == "ingestion_edrs_acquisition.py").all()

        assert len(source) == 1

        # Check number of events generated
        events = self.session.query(Event).join(Source).filter(Source.name == "S2A_OPER_REP_PASS_E_ONLY_HKTM.EOF").all()

        assert len(events) == 1

