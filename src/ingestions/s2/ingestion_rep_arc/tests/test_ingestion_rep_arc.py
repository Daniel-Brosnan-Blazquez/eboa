"""
Automated tests for the ingestion of the DPC files

Written by DEIMOS Space S.L. (dibb)

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
import ingestions.s2.ingestion_dpc.ingestion_dpc as ingestion_dpc
import ingestions.s2.ingestion_orbpre.ingestion_orbpre as ingestion_orbpre
import ingestions.s2.ingestion_nppf.ingestion_nppf as ingestion_nppf
import ingestions.s2.ingestion_dfep_acquisition.ingestion_dfep_acquisition as ingestion_dfep
import ingestions.s2.ingestion_rep_arc.ingestion_rep_arc as ingestion_rep_arc

class TestEngine(unittest.TestCase):
    def setUp(self):
        # Create the engine to manage the data
        self.engine_eboa = Engine()
        self.query_eboa = Query()

        # Create session to connectx to the database
        self.session = Session()

        # Clear all tables before executing the test
        self.query_eboa.clear_db()

    def test_rep_arc_L0_only(self):

        filename = "S2A_OPER_REP_ARC_L0.EOF"
        file_path = os.path.dirname(os.path.abspath(__file__)) + "/inputs/" + filename

        returned_value = ingestion_rep_arc.command_process_file(file_path)

        assert returned_value[0]["status"] == eboa_engine.exit_codes["OK"]["status"]

        sources = self.query_eboa.get_sources()

        assert len(sources) == 1

        sources = self.query_eboa.get_sources(validity_start_filters = [{"date": "2018-07-21T08:52:29.000000", "op": "=="}],
                                             validity_stop_filters = [{"date": "2018-07-21T08:54:14.000618", "op": "=="}],
                                              generation_time_filters = [{"date": "2018-07-21T10:39:20.000000", "op": "=="}],
                                             processors = {"filter": "ingestion_rep_arc.py", "op": "like"},
                                             names = {"filter": "S2A_OPER_REP_ARC_L0.EOF", "op": "like"})

        assert len(sources) == 1
        #Check datatake
        datatake = self.query_eboa.get_annotations(explicit_refs = {"op": "like", "filter": "S2A_OPER_MSI_L0__DS_MPS__20180721T103920_S20180721T085229_N02.06"},
                                                   annotation_cnf_names = {"op": "like", "filter": "DATATAKE"})

        assert len(datatake) == 1

        #Check baseline
        baseline = self.query_eboa.get_annotations(explicit_refs = {"op": "like", "filter": "S2A_OPER_MSI_L0__DS_MPS__20180721T103920_S20180721T085229_N02.06"},
                                                   annotation_cnf_names = {"op": "like", "filter": "BASELINE"})

        assert len(baseline) == 1

        # Check footprints
        # footprints = self.query_eboa.get_annotations(explicit_refs = {"op": "like", "filter": "S2A_OPER_MSI_L0__DS_MPS__20180721T103920_S20180721T085229_N02.06"},
        #                                    annotation_cnf_names = {"op": "like", "filter": "FOOTPRINT"})
        #
        # assert len(footprints) == 361

        # Check data sizes
        data_sizes = self.query_eboa.get_annotations(annotation_cnf_names = {"op": "like", "filter": "SIZE"})

        assert len(data_sizes) == 361

        # Check cloud percentages
        cloud_percentages = self.query_eboa.get_annotations(annotation_cnf_names = {"op": "like", "filter": "CLOUD_PERCENTAGE"})

        assert len(cloud_percentages) == 361

        # Check physical url
        physical_urls = self.query_eboa.get_annotations(annotation_cnf_names = {"op": "like", "filter": "PHYSICAL_URL"})

        assert len(physical_urls) == 361

        # Check datastrip_sensing_explicit_ref
        datastrip_sensing_er = self.query_eboa.get_explicit_refs(explicit_refs = {"filter": "S2A_OPER_MSI_L0__DS_MPS__20180721T103920_S20180721T085229_N02.06", "op": "like"},
                                                                 groups =  {"op": "like", "filter": "L0_DS"})

        assert len(datastrip_sensing_er) == 1

        # Check datastrip_sensing_explicit_ref
        granule_er = self.query_eboa.get_explicit_refs(groups = {"op": "like", "filter": "L0_GR"})

        assert len(granule_er) == 360

        # Check processing validities
        processing_validities = self.query_eboa.get_events(explicit_refs = {"op": "like", "filter": "S2A_OPER_MSI_L0__DS_MPS__20180721T103920_S20180721T085229_N02.06"},
                                                 gauge_names = {"op": "like", "filter": "PROCESSING_VALIDITY"})

        assert len(processing_validities) == 0



    # def test_dpc_report_L0_plan(self):
    #
    #     filename = "S2A_NPPF.EOF"
    #     file_path = os.path.dirname(os.path.abspath(__file__)) + "/inputs/" + filename
    #
    #     returned_value = ingestion_nppf.command_process_file(file_path)
    #
    #     assert returned_value[0]["status"] == eboa_engine.exit_codes["OK"]["status"]
    #
    #     filename = "S2A_ORBPRE.EOF"
    #     file_path = os.path.dirname(os.path.abspath(__file__)) + "/inputs/" + filename
    #
    #     returned_value = ingestion_orbpre.command_process_file(file_path)
    #
    #     assert returned_value[0]["status"] == eboa_engine.exit_codes["OK"]["status"]
    #
    #     filename = "S2A_OPER_REP_OPDPC_L0U_L0.EOF"
    #     file_path = os.path.dirname(os.path.abspath(__file__)) + "/inputs/" + filename
    #
    #     returned_value = ingestion_dpc.command_process_file(file_path)
    #
    #     assert returned_value[0]["status"] == eboa_engine.exit_codes["OK"]["status"]
    #     # Check sources
    #     # L0
    #     sources = self.query_eboa.get_sources(validity_start_filters = [{"date": "2018-07-21T08:36:02.255634", "op": "=="}],
    #                                          validity_stop_filters = [{"date": "2018-07-21T09:08:56.195941", "op": "=="}],
    #                                           generation_time_filters = [{"date": "2018-07-16T11:41:50", "op": "=="}],
    #                                          processors = {"filter": "planning_processing_ingestion_dpc.py", "op": "like"},
    #                                          names = {"filter": "S2A_OPER_REP_OPDPC_L0U_L0.EOF", "op": "like"})
    #
    #     assert len(sources) == 1
    #
    #     sources = self.query_eboa.get_sources(validity_start_filters = [{"date": "2018-07-21T08:52:29", "op": "=="}],
    #                                          validity_stop_filters = [{"date": "2018-07-21T10:43:26.741000", "op": "=="}],
    #                                           generation_time_filters = [{"date": "2018-07-21T10:43:34", "op": "=="}],
    #                                          processors = {"filter": "ingestion_dpc.py", "op": "like"},
    #                                          names = {"filter": "S2A_OPER_REP_OPDPC_L0U_L0.EOF", "op": "like"})
    #
    #     assert len(sources) == 1
    #
    #     sources = self.query_eboa.get_sources(validity_start_filters = [{"date": "2000-12-10 23:51:34", "op": "=="}],
    #                                          validity_stop_filters = [{"date": "9999-01-01T00:00:00", "op": "=="}],
    #                                           generation_time_filters = [{"date": "2018-07-21T10:43:34", "op": "=="}],
    #                                          processors = {"filter": "configuration_ingestion_dpc.py", "op": "like"},
    #                                          names = {"filter": "S2A_OPER_REP_OPDPC_L0U_L0.EOF", "op": "like"})
    #
    #     assert len(sources) == 1
    #
    #     imaging_plan = self.query_eboa.get_events(gauge_names = {"filter": "PLANNED_CUT_IMAGING", "op": "like"})[0]
    #     # Check processing validities
    #     processing_validities = self.query_eboa.get_events(explicit_refs = {"filter": "S2A_OPER_MSI_L0__DS_MPS__20180721T103920_S20180721T085229_N02.06", "op": "like"},
    #                                                        gauge_names = {"filter": "PROCESSING_VALIDITY", "op": "like"},
    #                                                        start_filters = [{"date": "2018-07-21T08:52:29", "op": "=="}],
    #                                                        stop_filters = [{"date": "2018-07-21T08:54:19", "op": "=="}])
    #
    #     assert len(processing_validities) == 1
    #
    #     assert len(processing_validities) == 1
    #     processing_validity_l0 = processing_validities[0]
    #
    #     # Check links with PROCESSING VALIDITY
    #     link_to_plan = self.query_eboa.get_event_links(event_uuid_links = {"filter": [str(processing_validity_l0.event_uuid)], "op": "in"},
    #                                                 event_uuids = {"filter": [str(imaging_plan.event_uuid)], "op": "in"},
    #                                                 link_names = {"filter": "PROCESSING_VALIDITY", "op": "like"})
    #
    #     assert len(link_to_plan) == 1
    #
    #     link_from_plan = self.query_eboa.get_event_links(event_uuids = {"filter": [str(processing_validity_l0.event_uuid)], "op": "in"},
    #                                                 event_uuid_links = {"filter": [str(imaging_plan.event_uuid)], "op": "in"},
    #                                                 link_names = {"filter": "PLANNED_IMAGING", "op": "like"})
    #
    #     assert len(link_from_plan) == 1
    #
    #     assert processing_validity_l0.get_structured_values() == [{
    #        "values": [
    #             {
    #                 "value": "COMPLETE",
    #                 "name": "status",
    #                 "type": "text"
    #             },
    #             {
    #                 "value": "L0",
    #                 "name": "level",
    #                 "type": "text"
    #             },
    #             {
    #                 "value": "S2A",
    #                 "name": "satellite",
    #                 "type": "text"
    #             },
    #             {
    #                 "value": "MPS_",
    #                 "type": "text",
    #                 "name": "processing_centre"
    #             },
    #             {
    #                 "value": "MATCHED_PLANNED_IMAGING",
    #                 "type": "text",
    #                 "name": "matching_plan_status"
    #             },
    #             {
    #                 "value": "NO_MATCHED_ISP_VALIDITY",
    #                 "type": "text",
    #                 "name": "matching_reception_status"
    #             },
    #             {
    #                 "value": "16077.0",
    #                 "type": "double",
    #                 "name": "sensing_orbit"
    #             }
    #         ],
    #         "type": "object",
    #         "name": "details"
    #     }]
    #
    #     # Check planning completeness
    #     planning_completeness = self.query_eboa.get_events(explicit_refs = {"filter": "S2A_OPER_MSI_L0__DS_MPS__20180721T103920_S20180721T085229_N02.06", "op": "like"},
    #                                                        gauge_names = {"filter": "PLANNED_IMAGING_PROCESSING_COMPLETENESS_L0", "op": "like"},
    #                                                        start_filters = [{"date": "2018-07-21T08:52:29", "op": "=="}],
    #                                                        stop_filters = [{"date": "2018-07-21T08:54:19", "op": "=="}])
    #
    #     assert len(planning_completeness) == 1
    #     planning_completeness_l0 = planning_completeness[0]
    #
    #     assert planning_completeness_l0.get_structured_values() == [{
    #         "name": "details",
    #         "values": [
    #             {
    #                 "name": "status",
    #                 "value": "COMPLETE",
    #                 "type": "text"
    #             },
    #             {
    #                 "name": "level",
    #                 "value": "L0",
    #                 "type": "text"
    #             },
    #             {
    #                 "name": "satellite",
    #                 "value": "S2A",
    #                 "type": "text"
    #             },
    #             {
    #                 "name": "processing_centre",
    #                 "value": "MPS_",
    #                 "type": "text"
    #             },
    #             {
    #                 "name": "matching_plan_status",
    #                 "value": "MATCHED_PLANNED_IMAGING",
    #                 "type": "text"
    #             },
    #             {
    #                 "name": "matching_reception_status",
    #                 "value": "NO_MATCHED_ISP_VALIDITY",
    #                 "type": "text"
    #             },
    #             {
    #                 "name": "sensing_orbit",
    #                 "value": "16077.0",
    #                 "type": "double"
    #             }
    #         ],
    #         "type": "object"
    #     }]
    #
    #     missing_planning_completeness = self.query_eboa.get_events(gauge_names = {"filter": "PLANNED_IMAGING_PROCESSING_COMPLETENESS_L0", "op": "like"},
    #                                                        start_filters = [{"date": "2018-07-21T08:36:02.255634", "op": "=="}],
    #                                                        stop_filters = [{"date": "2018-07-21T08:52:29", "op": "=="}])
    #
    #     assert len(missing_planning_completeness) == 1
    #
    #     missing_planning_completeness_statuses = [event for event in missing_planning_completeness if len([value for value in event.eventTexts if value.name == "status" and value.value == "MISSING"]) > 0]
    #
    #     assert len(missing_planning_completeness_statuses) == 1
    #
    #     missing_planning_completeness = self.query_eboa.get_events(gauge_names = {"filter": "PLANNED_IMAGING_PROCESSING_COMPLETENESS_L0", "op": "like"},
    #                                                        start_filters = [{"date": "2018-07-21T08:54:19", "op": "=="}],
    #                                                        stop_filters = [{"date": "2018-07-21T09:08:56.195941", "op": "=="}])
    #
    #     assert len(missing_planning_completeness) == 1

    # def test_dpc_report_L0_with_rep_pass(self):
    #
    #     filename = "S2A_REP_PASS_NO_GAPS.EOF"
    #     file_path = os.path.dirname(os.path.abspath(__file__)) + "/inputs/" + filename
    #
    #     returned_value = ingestion_dfep.command_process_file(file_path)
    #
    #     assert returned_value[0]["status"] == eboa_engine.exit_codes["OK"]["status"]
    #
    #     filename = "S2A_OPER_REP_OPDPC_L0U_L0.EOF"
    #     file_path = os.path.dirname(os.path.abspath(__file__)) + "/inputs/" + filename
    #
    #     returned_value = ingestion_dpc.command_process_file(file_path)
    #
    #     assert returned_value[0]["status"] == eboa_engine.exit_codes["OK"]["status"]
    #
    #     # Check sources
    #     # L0
    #     sources = self.query_eboa.get_sources(validity_start_filters = [{"date": "2018-07-21T08:52:35.993268", "op": "=="}],
    #                                          validity_stop_filters = [{"date": "2018-07-21T08:54:12.226646", "op": "=="}],
    #                                           generation_time_filters = [{"date": "2018-07-21T10:40:55", "op": "=="}],
    #                                          processors = {"filter": "processing_received_ingestion_dpc.py", "op": "like"},
    #                                          names = {"filter": "S2A_OPER_REP_OPDPC_L0U_L0.EOF", "op": "like"})
    #
    #     assert len(sources) == 1
    #
    #     sources = self.query_eboa.get_sources(validity_start_filters = [{"date": "2018-07-21T08:52:29", "op": "=="}],
    #                                          validity_stop_filters = [{"date": "2018-07-21T10:43:26.741000", "op": "=="}],
    #                                           generation_time_filters = [{"date": "2018-07-21T10:43:34", "op": "=="}],
    #                                          processors = {"filter": "ingestion_dpc.py", "op": "like"},
    #                                          names = {"filter": "S2A_OPER_REP_OPDPC_L0U_L0.EOF", "op": "like"})
    #
    #     assert len(sources) == 1
    #
    #     sources = self.query_eboa.get_sources(validity_start_filters = [{"date": "2000-12-10 23:51:34", "op": "=="}],
    #                                          validity_stop_filters = [{"date": "9999-01-01T00:00:00", "op": "=="}],
    #                                           generation_time_filters = [{"date": "2018-07-21T10:43:34", "op": "=="}],
    #                                          processors = {"filter": "configuration_ingestion_dpc.py", "op": "like"},
    #                                          names = {"filter": "S2A_OPER_REP_OPDPC_L0U_L0.EOF", "op": "like"})
    #
    #     assert len(sources) == 1
    #
    #     isp_validity = self.query_eboa.get_events(gauge_names = {"filter": "ISP_VALIDITY", "op": "like"})[0]
    #
    #     # Check processing validities
    #     processing_validities = self.query_eboa.get_events(explicit_refs = {"filter": "S2A_OPER_MSI_L0__DS_MPS__20180721T103920_S20180721T085229_N02.06", "op": "like"},
    #                                                        gauge_names = {"filter": "PROCESSING_VALIDITY", "op": "like"},
    #                                                        start_filters = [{"date": "2018-07-21T08:52:29", "op": "=="}],
    #                                                        stop_filters = [{"date": "2018-07-21T08:54:19", "op": "=="}])
    #
    #     assert len(processing_validities) == 1
    #     processing_validity_l0 = processing_validities[0]
    #
    #     # Check links with PROCESSING VALIDITY
    #     link_to_isp_validity = self.query_eboa.get_event_links(event_uuid_links = {"filter": [str(processing_validity_l0.event_uuid)], "op": "in"},
    #                                                 event_uuids = {"filter": [str(isp_validity.event_uuid)], "op": "in"},
    #                                                 link_names = {"filter": "PROCESSING_VALIDITY", "op": "like"})
    #
    #     assert len(link_to_isp_validity) == 1
    #
    #     link_from_isp_validity = self.query_eboa.get_event_links(event_uuids = {"filter": [str(processing_validity_l0.event_uuid)], "op": "in"},
    #                                                 event_uuid_links = {"filter": [str(isp_validity.event_uuid)], "op": "in"},
    #                                                 link_names = {"filter": "ISP_VALIDITY", "op": "like"})
    #
    #     assert len(link_from_isp_validity) == 1
    #
    #     assert processing_validity_l0.get_structured_values() == [{
    #        "values": [
    #             {
    #                 "value": "COMPLETE",
    #                 "name": "status",
    #                 "type": "text"
    #             },
    #             {
    #                 "value": "L0",
    #                 "name": "level",
    #                 "type": "text"
    #             },
    #             {
    #                 "value": "S2A",
    #                 "name": "satellite",
    #                 "type": "text"
    #             },
    #             {
    #                 "value": "MPS_",
    #                 "type": "text",
    #                 "name": "processing_centre"
    #             },
    #             {
    #                 "value": "NO_MATCHED_PLANNED_IMAGING",
    #                 "type": "text",
    #                 "name": "matching_plan_status"
    #             },
    #             {
    #                 "value": "MATCHED_ISP_VALIDITY",
    #                 "type": "text",
    #                 "name": "matching_reception_status"
    #             },
    #             {
    #                 "value": "16078.0",
    #                 "type": "double",
    #                 "name": "downlink_orbit"
    #             }
    #         ],
    #         "type": "object",
    #         "name": "details"
    #     }]


    def test_dpc_report_L1C_only(self):

        filename = "S2A_OPER_REP_ARC_L1C.EOF"
        file_path = os.path.dirname(os.path.abspath(__file__)) + "/inputs/" + filename

        returned_value = ingestion_rep_arc.command_process_file(file_path)

        assert returned_value[0]["status"] == eboa_engine.exit_codes["OK"]["status"]

        sources = self.query_eboa.get_sources()

        assert len(sources) == 1

        sources = self.query_eboa.get_sources(validity_start_filters = [{"date": "2018-07-21T08:52:29.000000", "op": "=="}],
                                             validity_stop_filters = [{"date": "2018-07-21T08:54:14.000618", "op": "=="}],
                                              generation_time_filters = [{"date": "2018-07-21 10:42:53", "op": "=="}],
                                             processors = {"filter": "ingestion_rep_arc.py", "op": "like"},
                                             names = {"filter": "S2A_OPER_REP_ARC_L1C.EOF", "op": "like"})

        assert len(sources) == 1

        processing_validities = self.query_eboa.get_events(explicit_refs = {"op": "like", "filter": "S2A_OPER_MSI_L1C_DS_MPS__20180721T104253_S20180721T085229_N02.06"},
                                                 gauge_names = {"op": "like", "filter": "PROCESSING_VALIDITY"})

        assert len(processing_validities) == 0

    def test_dpc_report_L1C_with_L0(self):

        filename = "S2A_OPER_REP_ARC_L0.EOF"
        file_path = os.path.dirname(os.path.abspath(__file__)) + "/inputs/" + filename

        returned_value = ingestion_rep_arc.command_process_file(file_path)

        assert returned_value[0]["status"] == eboa_engine.exit_codes["OK"]["status"]

        filename = "S2A_OPER_REP_ARC_L1C.EOF"
        file_path = os.path.dirname(os.path.abspath(__file__)) + "/inputs/" + filename

        returned_value = ingestion_rep_arc.command_process_file(file_path)

        assert returned_value[0]["status"] == eboa_engine.exit_codes["OK"]["status"]

        # Check sources
        # L0
        sources = self.query_eboa.get_sources(validity_start_filters = [{"date": "2018-07-21T08:52:29.000000", "op": "=="}],
                                                 validity_stop_filters = [{"date": "2018-07-21T08:54:14.000618", "op": "=="}],
                                                  generation_time_filters = [{"date": "2018-07-21T10:39:20.000000", "op": "=="}],
                                                 processors = {"filter": "ingestion_rep_arc.py", "op": "like"},
                                                 names = {"filter": "S2A_OPER_REP_ARC_L0.EOF", "op": "like"})

        assert len(sources) == 1

        #L1C
        sources = self.query_eboa.get_sources(validity_start_filters = [{"date": "2018-07-21T08:52:29.000000", "op": "=="}],
                                         validity_stop_filters = [{"date": "2018-07-21T08:54:14.000618", "op": "=="}],
                                          generation_time_filters = [{"date": "2018-07-21 10:42:53", "op": "=="}],
                                         processors = {"filter": "ingestion_rep_arc.py", "op": "like"},
                                         names = {"filter": "S2A_OPER_REP_ARC_L1C.EOF", "op": "like"})

        assert len(sources) == 1

        processing_validities = self.query_eboa.get_events(explicit_refs = {"op": "like", "filter": "S2A_OPER_MSI_L1C_DS_MPS__20180721T104253_S20180721T085229_N02.06"},
                                                 gauge_names = {"op": "like", "filter": "PROCESSING_VALIDITY"})

        assert len(processing_validities) == 0
        #processing_validity_l1c = processing_validities[0]

        # assert processing_validity_l1c.get_structured_values() == [{
        #     "values": [
        #         {
        #             "value": "COMPLETE",
        #             "name": "status",
        #             "type": "text"
        #         },
        #         {
        #             "value": "L1C",
        #             "name": "level",
        #             "type": "text"
        #         },
        #         {
        #             "value": "S2A",
        #             "name": "satellite",
        #             "type": "text"
        #         },
        #         {
        #             "value": "MPS_",
        #             "name": "processing_centre",
        #             "type": "text"
        #         },
        #         {
        #             "value": "NO_MATCHED_PLANNED_IMAGING",
        #             "name": "matching_plan_status",
        #             "type": "text"
        #         },
        #         {
        #             "value": "NO_MATCHED_ISP_VALIDITY",
        #             "name": "matching_reception_status",
        #             "type": "text"
        #         }
        #     ],
        #     "name": "details",
        #     "type": "object"
        # }]
        #
        # # Check planning completeness
        # planning_completeness = self.query_eboa.get_events(explicit_refs = {"filter": "S2A_OPER_MSI_L1C_DS_MPS__20180721T104253_S20180721T085229_N02.06", "op": "like"},
        #                                                    gauge_names = {"filter": "PLANNED_IMAGING_PROCESSING_COMPLETENESS_L1C", "op": "like"},
        #                                      start_filters = [{"date": "2018-07-21T08:52:29", "op": "=="}],
        #                                      stop_filters = [{"date": "2018-07-21T08:54:19", "op": "=="}])
        #
        # assert len(planning_completeness) == 1
        # planning_completeness_l1c = planning_completeness[0]
        #
        # assert planning_completeness_l1c.get_structured_values() == [{
        #     "name": "details",
        #     "values": [
        #         {
        #             "name": "status",
        #             "value": "COMPLETE",
        #             "type": "text"
        #         },
        #         {
        #             "name": "level",
        #             "value": "L1C",
        #             "type": "text"
        #         },
        #         {
        #             "name": "satellite",
        #             "value": "S2A",
        #             "type": "text"
        #         },
        #         {
        #             "name": "processing_centre",
        #             "value": "MPS_",
        #             "type": "text"
        #         },
        #         {
        #             "name": "matching_plan_status",
        #             "value": "NO_MATCHED_PLANNED_IMAGING",
        #             "type": "text"
        #         },
        #         {
        #             "name": "matching_reception_status",
        #             "value": "NO_MATCHED_ISP_VALIDITY",
        #             "type": "text"
        #         }
        #     ],
        #     "type": "object"
        # }]

    def test_dpc_report_L1C_with_L0_plan(self):
        filename = "S2A_NPPF.EOF"
        file_path = os.path.dirname(os.path.abspath(__file__)) + "/inputs/" + filename

        returned_value = ingestion_nppf.command_process_file(file_path)

        assert returned_value[0]["status"] == eboa_engine.exit_codes["OK"]["status"]

        filename = "S2A_ORBPRE.EOF"
        file_path = os.path.dirname(os.path.abspath(__file__)) + "/inputs/" + filename

        returned_value = ingestion_orbpre.command_process_file(file_path)

        assert returned_value[0]["status"] == eboa_engine.exit_codes["OK"]["status"]

        filename = "S2A_OPER_REP_ARC_L0.EOF"
        file_path = os.path.dirname(os.path.abspath(__file__)) + "/inputs/" + filename

        returned_value = ingestion_rep_arc.command_process_file(file_path)

        assert returned_value[0]["status"] == eboa_engine.exit_codes["OK"]["status"]

        filename = "S2A_OPER_REP_ARC_L1C.EOF"
        file_path = os.path.dirname(os.path.abspath(__file__)) + "/inputs/" + filename

        returned_value = ingestion_rep_arc.command_process_file(file_path)

        assert returned_value[0]["status"] == eboa_engine.exit_codes["OK"]["status"]

        # Check sources
        #L0
        sources = self.query_eboa.get_sources(validity_start_filters = [{"date": "2018-07-21T08:52:29.000000", "op": "=="}],
                                                 validity_stop_filters = [{"date": "2018-07-21T08:54:14.000618", "op": "=="}],
                                                  generation_time_filters = [{"date": "2018-07-21T10:39:20.000000", "op": "=="}],
                                                 processors = {"filter": "ingestion_rep_arc.py", "op": "like"},
                                                 names = {"filter": "S2A_OPER_REP_ARC_L0.EOF", "op": "like"})

        assert len(sources) == 1

        #L1C
        sources = self.query_eboa.get_sources(validity_start_filters = [{"date": "2018-07-21T08:52:29.000000", "op": "=="}],
                                         validity_stop_filters = [{"date": "2018-07-21T08:54:14.000618", "op": "=="}],
                                          generation_time_filters = [{"date": "2018-07-21 10:42:53", "op": "=="}],
                                         processors = {"filter": "ingestion_rep_arc.py", "op": "like"},
                                         names = {"filter": "S2A_OPER_REP_ARC_L1C.EOF", "op": "like"})

        assert len(sources) == 1

        imaging_plan = self.query_eboa.get_events(gauge_names = {"filter": "PLANNED_CUT_IMAGING", "op": "like"})[0]

        processing_validities = self.query_eboa.get_events(explicit_refs = {"op": "like", "filter": "S2A_OPER_MSI_L1C_DS_MPS__20180721T104253_S20180721T085229_N02.06"},
                                                 gauge_names = {"op": "like", "filter": "PROCESSING_VALIDITY"})

        assert len(processing_validities) == 0

        #processing_validity_l1c = processing_validities[0]

        # # Check links with PROCESSING VALIDITY
        # link_to_plan = self.query_eboa.get_event_links(event_uuid_links = {"filter": [str(processing_validity_l1c.event_uuid)], "op": "in"},
        #                                             event_uuids = {"filter": [str(imaging_plan.event_uuid)], "op": "in"},
        #                                             link_names = {"filter": "PROCESSING_VALIDITY", "op": "like"})
        #
        # assert len(link_to_plan) == 1
        #
        # link_from_plan = self.query_eboa.get_event_links(event_uuids = {"filter": [str(processing_validity_l1c.event_uuid)], "op": "in"},
        #                                             event_uuid_links = {"filter": [str(imaging_plan.event_uuid)], "op": "in"},
        #                                             link_names = {"filter": "PLANNED_IMAGING", "op": "like"})
        #
        # assert len(link_from_plan) == 1
        #
        # assert processing_validity_l1c.get_structured_values() == [{
        #     "values": [
        #         {
        #             "value": "COMPLETE",
        #             "name": "status",
        #             "type": "text"
        #         },
        #         {
        #             "value": "L1C",
        #             "name": "level",
        #             "type": "text"
        #         },
        #         {
        #             "value": "S2A",
        #             "name": "satellite",
        #             "type": "text"
        #         },
        #         {
        #             "value": "MPS_",
        #             "name": "processing_centre",
        #             "type": "text"
        #         },
        #         {
        #             "value": "MATCHED_PLANNED_IMAGING",
        #             "name": "matching_plan_status",
        #             "type": "text"
        #         },
        #         {
        #             "value": "NO_MATCHED_ISP_VALIDITY",
        #             "name": "matching_reception_status",
        #             "type": "text"
        #         },
        #         {
        #             "value": "16077.0",
        #             "name": "sensing_orbit",
        #             "type": "double"
        #         }
        #     ],
        #     "name": "details",
        #     "type": "object"
        # }]
        #
        # # Check planning completeness
        # planning_completeness = self.query_eboa.get_events(explicit_refs = {"filter": "S2A_OPER_MSI_L1C_DS_MPS__20180721T104253_S20180721T085229_N02.06", "op": "like"},
        #                                                    gauge_names = {"filter": "PLANNED_IMAGING_PROCESSING_COMPLETENESS_L1C", "op": "like"},
        #                                      start_filters = [{"date": "2018-07-21T08:52:29", "op": "=="}],
        #                                      stop_filters = [{"date": "2018-07-21T08:54:19", "op": "=="}])
        #
        # assert len(planning_completeness) == 1
        # planning_completeness_l1c = planning_completeness[0]
        #
        # assert planning_completeness_l1c.get_structured_values() == [{
        #     "name": "details",
        #     "values": [
        #         {
        #             "name": "status",
        #             "value": "COMPLETE",
        #             "type": "text"
        #         },
        #         {
        #             "name": "level",
        #             "value": "L1C",
        #             "type": "text"
        #         },
        #         {
        #             "name": "satellite",
        #             "value": "S2A",
        #             "type": "text"
        #         },
        #         {
        #             "name": "processing_centre",
        #             "value": "MPS_",
        #             "type": "text"
        #         },
        #         {
        #             "name": "matching_plan_status",
        #             "value": "MATCHED_PLANNED_IMAGING",
        #             "type": "text"
        #         },
        #         {
        #             "name": "matching_reception_status",
        #             "value": "NO_MATCHED_ISP_VALIDITY",
        #             "type": "text"
        #         },
        #         {
        #             "name": "sensing_orbit",
        #             "value": "16077.0",
        #             "type": "double"
        #         }
        #     ],
        #     "type": "object"
        # }]
        #
        # missing_planning_completeness = self.query_eboa.get_events(gauge_names = {"filter": "PLANNED_IMAGING_PROCESSING_COMPLETENESS_L1C", "op": "like"},
        #                                                    start_filters = [{"date": "2018-07-21T08:36:02.255634", "op": "=="}],
        #                                                    stop_filters = [{"date": "2018-07-21T08:52:29", "op": "=="}])
        #
        # assert len(missing_planning_completeness) == 1
        #
        # missing_planning_completeness_statuses = [event for event in missing_planning_completeness if len([value for value in event.eventTexts if value.name == "status" and value.value == "MISSING"]) > 0]
        #
        # assert len(missing_planning_completeness_statuses) == 1
        #
        # missing_planning_completeness = self.query_eboa.get_events(gauge_names = {"filter": "PLANNED_IMAGING_PROCESSING_COMPLETENESS_L1C", "op": "like"},
        #                                                    start_filters = [{"date": "2018-07-21T08:54:19", "op": "=="}],
        #                                                    stop_filters = [{"date": "2018-07-21T09:08:56.195941", "op": "=="}])
        #
        # assert len(missing_planning_completeness) == 1


    # def test_dpc_report_L1C_with_L0_plan_rep_pass(self):
    #
    #     filename = "S2A_NPPF.EOF"
    #     file_path = os.path.dirname(os.path.abspath(__file__)) + "/inputs/" + filename
    #
    #     returned_value = ingestion_nppf.command_process_file(file_path)
    #
    #     assert returned_value[0]["status"] == eboa_engine.exit_codes["OK"]["status"]
    #
    #     filename = "S2A_ORBPRE.EOF"
    #     file_path = os.path.dirname(os.path.abspath(__file__)) + "/inputs/" + filename
    #
    #     returned_value = ingestion_orbpre.command_process_file(file_path)
    #
    #     assert returned_value[0]["status"] == eboa_engine.exit_codes["OK"]["status"]
    #
    #     filename = "S2A_REP_PASS_NO_GAPS.EOF"
    #     file_path = os.path.dirname(os.path.abspath(__file__)) + "/inputs/" + filename
    #
    #     returned_value = ingestion_dfep.command_process_file(file_path)
    #
    #     assert returned_value[0]["status"] == eboa_engine.exit_codes["OK"]["status"]
    #
    #     filename = "S2A_OPER_REP_OPDPC_L0U_L0.EOF"
    #     file_path = os.path.dirname(os.path.abspath(__file__)) + "/inputs/" + filename
    #
    #     returned_value = ingestion_dpc.command_process_file(file_path)
    #
    #     assert returned_value[0]["status"] == eboa_engine.exit_codes["OK"]["status"]
    #
    #     filename = "S2A_OPER_REP_OPDPC_L1B_L1C.EOF"
    #     file_path = os.path.dirname(os.path.abspath(__file__)) + "/inputs/" + filename
    #
    #     returned_value = ingestion_dpc.command_process_file(file_path)
    #
    #     assert returned_value[0]["status"] == eboa_engine.exit_codes["OK"]["status"]
    #
    #     # Check sources
    #     # L0
    #     sources = self.query_eboa.get_sources(validity_start_filters = [{"date": "2018-07-21T08:36:02.255634", "op": "=="}],
    #                                          validity_stop_filters = [{"date": "2018-07-21T09:08:56.195941", "op": "=="}],
    #                                           generation_time_filters = [{"date": "2018-07-16T11:41:50", "op": "=="}],
    #                                          processors = {"filter": "planning_processing_ingestion_dpc.py", "op": "like"},
    #                                          names = {"filter": "S2A_OPER_REP_OPDPC_L0U_L0.EOF", "op": "like"})
    #
    #     assert len(sources) == 1
    #
    #     sources = self.query_eboa.get_sources(validity_start_filters = [{"date": "2018-07-21T08:52:35.993268", "op": "=="}],
    #                                          validity_stop_filters = [{"date": "2018-07-21T08:54:12.226646", "op": "=="}],
    #                                           generation_time_filters = [{"date": "2018-07-21T10:40:55", "op": "=="}],
    #                                          processors = {"filter": "processing_received_ingestion_dpc.py", "op": "like"},
    #                                          names = {"filter": "S2A_OPER_REP_OPDPC_L0U_L0.EOF", "op": "like"})
    #
    #     assert len(sources) == 1
    #
    #     sources = self.query_eboa.get_sources(validity_start_filters = [{"date": "2018-07-21T08:52:29", "op": "=="}],
    #                                          validity_stop_filters = [{"date": "2018-07-21T10:43:26.741000", "op": "=="}],
    #                                           generation_time_filters = [{"date": "2018-07-21T10:43:34", "op": "=="}],
    #                                          processors = {"filter": "ingestion_dpc.py", "op": "like"},
    #                                          names = {"filter": "S2A_OPER_REP_OPDPC_L0U_L0.EOF", "op": "like"})
    #
    #     assert len(sources) == 1
    #
    #     sources = self.query_eboa.get_sources(validity_start_filters = [{"date": "2000-12-10 23:51:34", "op": "=="}],
    #                                          validity_stop_filters = [{"date": "9999-01-01T00:00:00", "op": "=="}],
    #                                           generation_time_filters = [{"date": "2018-07-21T10:43:34", "op": "=="}],
    #                                          processors = {"filter": "configuration_ingestion_dpc.py", "op": "like"},
    #                                          names = {"filter": "S2A_OPER_REP_OPDPC_L0U_L0.EOF", "op": "like"})
    #
    #     assert len(sources) == 1
    #
    #     # L1C
    #     sources = self.query_eboa.get_sources(validity_start_filters = [{"date": "2018-07-21T08:36:02.255634", "op": "=="}],
    #                                          validity_stop_filters = [{"date": "2018-07-21T09:08:56.195941", "op": "=="}],
    #                                           generation_time_filters = [{"date": "2018-07-16T11:41:50", "op": "=="}],
    #                                          processors = {"filter": "planning_processing_ingestion_dpc.py", "op": "like"},
    #                                          names = {"filter": "S2A_OPER_REP_OPDPC_L1B_L1C.EOF", "op": "like"})
    #
    #     assert len(sources) == 1
    #
    #     sources = self.query_eboa.get_sources(validity_start_filters = [{"date": "2018-07-21T08:52:35.993268", "op": "=="}],
    #                                          validity_stop_filters = [{"date": "2018-07-21T08:54:12.226646", "op": "=="}],
    #                                           generation_time_filters = [{"date": "2018-07-21T10:40:55", "op": "=="}],
    #                                          processors = {"filter": "processing_received_ingestion_dpc.py", "op": "like"},
    #                                          names = {"filter": "S2A_OPER_REP_OPDPC_L1B_L1C.EOF", "op": "like"})
    #
    #     assert len(sources) == 1
    #
    #     sources = self.query_eboa.get_sources(validity_start_filters = [{"date": "2018-07-21T08:52:29", "op": "=="}],
    #                                          validity_stop_filters = [{"date": "2018-07-21T11:00:38.066000", "op": "=="}],
    #                                           generation_time_filters = [{"date": "2018-07-21T11:00:45", "op": "=="}],
    #                                          processors = {"filter": "ingestion_dpc.py", "op": "like"},
    #                                          names = {"filter": "S2A_OPER_REP_OPDPC_L1B_L1C.EOF", "op": "like"})
    #
    #     assert len(sources) == 1
    #
    #     sources = self.query_eboa.get_sources(validity_start_filters = [{"date": "1983-01-01T00:00:00", "op": "=="}],
    #                                          validity_stop_filters = [{"date": "9999-01-01T00:00:00", "op": "=="}],
    #                                           generation_time_filters = [{"date": "2018-07-21T11:00:45", "op": "=="}],
    #                                          processors = {"filter": "configuration_ingestion_dpc.py", "op": "like"},
    #                                          names = {"filter": "S2A_OPER_REP_OPDPC_L1B_L1C.EOF", "op": "like"})
    #
    #     assert len(sources) == 1
    #
    #     imaging_plan = self.query_eboa.get_events(gauge_names = {"filter": "PLANNED_CUT_IMAGING", "op": "like"})[0]
    #     isp_validity = self.query_eboa.get_events(gauge_names = {"filter": "ISP_VALIDITY", "op": "like"})[0]
    #
    #     processing_validities = self.query_eboa.get_events(explicit_refs = {"filter": "S2A_OPER_MSI_L1C_DS_MPS__20180721T104253_S20180721T085229_N02.06", "op": "like"},
    #                                                        gauge_names = {"filter": "PROCESSING_VALIDITY", "op": "like"},
    #                                          start_filters = [{"date": "2018-07-21T08:52:29", "op": "=="}],
    #                                          stop_filters = [{"date": "2018-07-21T08:54:19", "op": "=="}])
    #
    #     assert len(processing_validities) == 1
    #     processing_validity_l1c = processing_validities[0]
    #
    #     # Check links with PROCESSING VALIDITY
    #     link_to_plan = self.query_eboa.get_event_links(event_uuid_links = {"filter": [str(processing_validity_l1c.event_uuid)], "op": "in"},
    #                                                 event_uuids = {"filter": [str(imaging_plan.event_uuid)], "op": "in"},
    #                                                 link_names = {"filter": "PROCESSING_VALIDITY", "op": "like"})
    #
    #     assert len(link_to_plan) == 1
    #
    #     link_from_plan = self.query_eboa.get_event_links(event_uuids = {"filter": [str(processing_validity_l1c.event_uuid)], "op": "in"},
    #                                                 event_uuid_links = {"filter": [str(imaging_plan.event_uuid)], "op": "in"},
    #                                                 link_names = {"filter": "PLANNED_IMAGING", "op": "like"})
    #
    #     assert len(link_from_plan) == 1
    #
    #     # Check links with PROCESSING VALIDITY
    #     link_to_isp_validity = self.query_eboa.get_event_links(event_uuid_links = {"filter": [str(processing_validity_l1c.event_uuid)], "op": "in"},
    #                                                 event_uuids = {"filter": [str(isp_validity.event_uuid)], "op": "in"},
    #                                                 link_names = {"filter": "PROCESSING_VALIDITY", "op": "like"})
    #
    #     assert len(link_to_isp_validity) == 1
    #
    #     link_from_isp_validity = self.query_eboa.get_event_links(event_uuids = {"filter": [str(processing_validity_l1c.event_uuid)], "op": "in"},
    #                                                 event_uuid_links = {"filter": [str(isp_validity.event_uuid)], "op": "in"},
    #                                                 link_names = {"filter": "ISP_VALIDITY", "op": "like"})
    #
    #     assert len(link_from_isp_validity) == 1
    #
    #     assert processing_validity_l1c.get_structured_values() == [{
    #         "values": [
    #             {
    #                 "value": "COMPLETE",
    #                 "name": "status",
    #                 "type": "text"
    #             },
    #             {
    #                 "value": "L1C",
    #                 "name": "level",
    #                 "type": "text"
    #             },
    #             {
    #                 "value": "S2A",
    #                 "name": "satellite",
    #                 "type": "text"
    #             },
    #             {
    #                 "value": "MPS_",
    #                 "name": "processing_centre",
    #                 "type": "text"
    #             },
    #             {
    #                 "value": "MATCHED_PLANNED_IMAGING",
    #                 "name": "matching_plan_status",
    #                 "type": "text"
    #             },
    #             {
    #                 "value": "MATCHED_ISP_VALIDITY",
    #                 "name": "matching_reception_status",
    #                 "type": "text"
    #             },
    #             {
    #                 "value": "16077.0",
    #                 "name": "sensing_orbit",
    #                 "type": "double"
    #             },
    #             {
    #                 "value": "16078.0",
    #                 "name": "downlink_orbit",
    #                 "type": "double"
    #             }
    #         ],
    #         "name": "details",
    #         "type": "object"
    #     }]
    #
    #
    # def test_insert_dpc_L0_L1B_L1C_with_plan_and_rep_pass(self):
    #
    #     filename = "S2A_NPPF.EOF"
    #     file_path = os.path.dirname(os.path.abspath(__file__)) + "/inputs/" + filename
    #
    #     returned_value = ingestion_nppf.command_process_file(file_path)
    #
    #     assert returned_value[0]["status"] == eboa_engine.exit_codes["OK"]["status"]
    #
    #     filename = "S2A_ORBPRE.EOF"
    #     file_path = os.path.dirname(os.path.abspath(__file__)) + "/inputs/" + filename
    #
    #     returned_value = ingestion_orbpre.command_process_file(file_path)
    #
    #     assert returned_value[0]["status"] == eboa_engine.exit_codes["OK"]["status"]
    #
    #     filename = "S2A_REP_PASS_NO_GAPS.EOF"
    #     file_path = os.path.dirname(os.path.abspath(__file__)) + "/inputs/" + filename
    #
    #     returned_value = ingestion_dfep.command_process_file(file_path)
    #
    #     assert returned_value[0]["status"] == eboa_engine.exit_codes["OK"]["status"]
    #
    #     filename = "S2A_OPER_REP_OPDPC_L0U_L0.EOF"
    #     file_path = os.path.dirname(os.path.abspath(__file__)) + "/inputs/" + filename
    #
    #     returned_value = ingestion_dpc.command_process_file(file_path)
    #
    #     assert returned_value[0]["status"] == eboa_engine.exit_codes["OK"]["status"]
    #
    #     filename = "S2A_OPER_REP_OPDPC_L0_L1B.EOF"
    #     file_path = os.path.dirname(os.path.abspath(__file__)) + "/inputs/" + filename
    #
    #     returned_value = ingestion_dpc.command_process_file(file_path)
    #
    #     assert returned_value[0]["status"] == eboa_engine.exit_codes["OK"]["status"]
    #
    #     filename = "S2A_OPER_REP_OPDPC_L1B_L1C.EOF"
    #     file_path = os.path.dirname(os.path.abspath(__file__)) + "/inputs/" + filename
    #
    #     returned_value = ingestion_dpc.command_process_file(file_path)
    #
    #     assert returned_value[0]["status"] == eboa_engine.exit_codes["OK"]["status"]
    #
    #     # Check sources
    #     # L0
    #     sources = self.query_eboa.get_sources(validity_start_filters = [{"date": "2018-07-21T08:36:02.255634", "op": "=="}],
    #                                          validity_stop_filters = [{"date": "2018-07-21T09:08:56.195941", "op": "=="}],
    #                                           generation_time_filters = [{"date": "2018-07-16T11:41:50", "op": "=="}],
    #                                          processors = {"filter": "planning_processing_ingestion_dpc.py", "op": "like"},
    #                                          names = {"filter": "S2A_OPER_REP_OPDPC_L0U_L0.EOF", "op": "like"})
    #
    #     assert len(sources) == 1
    #
    #     sources = self.query_eboa.get_sources(validity_start_filters = [{"date": "2018-07-21T08:52:35.993268", "op": "=="}],
    #                                          validity_stop_filters = [{"date": "2018-07-21T08:54:12.226646", "op": "=="}],
    #                                           generation_time_filters = [{"date": "2018-07-21T10:40:55", "op": "=="}],
    #                                          processors = {"filter": "processing_received_ingestion_dpc.py", "op": "like"},
    #                                          names = {"filter": "S2A_OPER_REP_OPDPC_L0U_L0.EOF", "op": "like"})
    #
    #     assert len(sources) == 1
    #
    #     sources = self.query_eboa.get_sources(validity_start_filters = [{"date": "2018-07-21T08:52:29", "op": "=="}],
    #                                          validity_stop_filters = [{"date": "2018-07-21T10:43:26.741000", "op": "=="}],
    #                                           generation_time_filters = [{"date": "2018-07-21T10:43:34", "op": "=="}],
    #                                          processors = {"filter": "ingestion_dpc.py", "op": "like"},
    #                                          names = {"filter": "S2A_OPER_REP_OPDPC_L0U_L0.EOF", "op": "like"})
    #
    #     assert len(sources) == 1
    #
    #     sources = self.query_eboa.get_sources(validity_start_filters = [{"date": "2000-12-10 23:51:34", "op": "=="}],
    #                                          validity_stop_filters = [{"date": "9999-01-01T00:00:00", "op": "=="}],
    #                                           generation_time_filters = [{"date": "2018-07-21T10:43:34", "op": "=="}],
    #                                          processors = {"filter": "configuration_ingestion_dpc.py", "op": "like"},
    #                                          names = {"filter": "S2A_OPER_REP_OPDPC_L0U_L0.EOF", "op": "like"})
    #
    #     assert len(sources) == 1
    #
    #     # L1B
    #     sources = self.query_eboa.get_sources(validity_start_filters = [{"date": "2018-07-21T08:36:02.255634", "op": "=="}],
    #                                          validity_stop_filters = [{"date": "2018-07-21T09:08:56.195941", "op": "=="}],
    #                                           generation_time_filters = [{"date": "2018-07-16T11:41:50", "op": "=="}],
    #                                          processors = {"filter": "planning_processing_ingestion_dpc.py", "op": "like"},
    #                                          names = {"filter": "S2A_OPER_REP_OPDPC_L0_L1B.EOF", "op": "like"})
    #
    #     assert len(sources) == 1
    #
    #     sources = self.query_eboa.get_sources(validity_start_filters = [{"date": "2018-07-21T08:52:35.993268", "op": "=="}],
    #                                          validity_stop_filters = [{"date": "2018-07-21T08:54:12.226646", "op": "=="}],
    #                                           generation_time_filters = [{"date": "2018-07-21T10:40:55", "op": "=="}],
    #                                          processors = {"filter": "processing_received_ingestion_dpc.py", "op": "like"},
    #                                          names = {"filter": "S2A_OPER_REP_OPDPC_L0_L1B.EOF", "op": "like"})
    #
    #     assert len(sources) == 1
    #
    #     sources = self.query_eboa.get_sources(validity_start_filters = [{"date": "2018-07-21T08:52:29", "op": "=="}],
    #                                          validity_stop_filters = [{"date": "2018-07-21T10:55:38.778000", "op": "=="}],
    #                                           generation_time_filters = [{"date": "2018-07-21T10:55:47", "op": "=="}],
    #                                          processors = {"filter": "ingestion_dpc.py", "op": "like"},
    #                                          names = {"filter": "S2A_OPER_REP_OPDPC_L0_L1B.EOF", "op": "like"})
    #
    #     assert len(sources) == 1
    #
    #     sources = self.query_eboa.get_sources(validity_start_filters = [{"date": "1983-01-01T00:00:00", "op": "=="}],
    #                                          validity_stop_filters = [{"date": "9999-01-01T00:00:00", "op": "=="}],
    #                                           generation_time_filters = [{"date": "2018-07-21T10:55:47", "op": "=="}],
    #                                          processors = {"filter": "configuration_ingestion_dpc.py", "op": "like"},
    #                                          names = {"filter": "S2A_OPER_REP_OPDPC_L0_L1B.EOF", "op": "like"})
    #
    #     assert len(sources) == 1
    #
    #     # L1C
    #     sources = self.query_eboa.get_sources(validity_start_filters = [{"date": "2018-07-21T08:36:02.255634", "op": "=="}],
    #                                          validity_stop_filters = [{"date": "2018-07-21T09:08:56.195941", "op": "=="}],
    #                                           generation_time_filters = [{"date": "2018-07-16T11:41:50", "op": "=="}],
    #                                          processors = {"filter": "planning_processing_ingestion_dpc.py", "op": "like"},
    #                                          names = {"filter": "S2A_OPER_REP_OPDPC_L1B_L1C.EOF", "op": "like"})
    #
    #     assert len(sources) == 1
    #
    #     sources = self.query_eboa.get_sources(validity_start_filters = [{"date": "2018-07-21T08:52:35.993268", "op": "=="}],
    #                                          validity_stop_filters = [{"date": "2018-07-21T08:54:12.226646", "op": "=="}],
    #                                           generation_time_filters = [{"date": "2018-07-21T10:40:55", "op": "=="}],
    #                                          processors = {"filter": "processing_received_ingestion_dpc.py", "op": "like"},
    #                                          names = {"filter": "S2A_OPER_REP_OPDPC_L1B_L1C.EOF", "op": "like"})
    #
    #     assert len(sources) == 1
    #
    #     sources = self.query_eboa.get_sources(validity_start_filters = [{"date": "2018-07-21T08:52:29", "op": "=="}],
    #                                          validity_stop_filters = [{"date": "2018-07-21T11:00:38.066000", "op": "=="}],
    #                                           generation_time_filters = [{"date": "2018-07-21T11:00:45", "op": "=="}],
    #                                          processors = {"filter": "ingestion_dpc.py", "op": "like"},
    #                                          names = {"filter": "S2A_OPER_REP_OPDPC_L1B_L1C.EOF", "op": "like"})
    #
    #     assert len(sources) == 1
    #
    #     sources = self.query_eboa.get_sources(validity_start_filters = [{"date": "1983-01-01T00:00:00", "op": "=="}],
    #                                          validity_stop_filters = [{"date": "2100-01-01T00:00:00", "op": "=="}],
    #                                           generation_time_filters = [{"date": "2018-07-21T11:00:45", "op": "=="}],
    #                                          processors = {"filter": "configuration_ingestion_dpc.py", "op": "like"},
    #                                          names = {"filter": "S2A_OPER_REP_OPDPC_L1B_L1C.EOF", "op": "like"})
    #
    #     assert len(sources) == 1
    #
    #     imaging_plan = self.query_eboa.get_events(gauge_names = {"filter": "PLANNED_CUT_IMAGING", "op": "like"})[0]
    #     isp_validity = self.query_eboa.get_events(gauge_names = {"filter": "ISP_VALIDITY", "op": "like"})[0]
    #
    #
    #     # Check step infos
    #     step_infos = self.query_eboa.get_events(explicit_refs = {"filter": "S2A_OPER_MSI_L0__DS_MPS__20180721T103920_S20180721T085229_N02.06", "op": "like"},
    #                                                        gauge_names = {"filter": "STEP_INFO", "op": "like"})
    #
    #     assert len(step_infos) == 14
    #
    #     # Check mrf validities
    #     mrf_validities = self.query_eboa.get_events(sources = {"filter": "S2A_OPER_REP_OPDPC_L0U_L0.EOF", "op": "like"},
    #                                                        gauge_names = {"filter": "MRF_VALIDITY", "op": "like"})
    #
    #     assert len(mrf_validities) == 27
    #
    #     # Check processing validities
    #     processing_validities = self.query_eboa.get_events(explicit_refs = {"filter": "S2A_OPER_MSI_L0__DS_MPS__20180721T103920_S20180721T085229_N02.06", "op": "like"},
    #                                                        gauge_names = {"filter": "PROCESSING_VALIDITY", "op": "like"},
    #                                                        start_filters = [{"date": "2018-07-21T08:52:29", "op": "=="}],
    #                                                        stop_filters = [{"date": "2018-07-21T08:54:19", "op": "=="}])
    #
    #     assert len(processing_validities) == 1
    #     processing_validity_l0 = processing_validities[0]
    #
    #     # Check links with PROCESSING VALIDITY
    #     link_to_plan = self.query_eboa.get_event_links(event_uuid_links = {"filter": [str(processing_validity_l0.event_uuid)], "op": "in"},
    #                                                 event_uuids = {"filter": [str(imaging_plan.event_uuid)], "op": "in"},
    #                                                 link_names = {"filter": "PROCESSING_VALIDITY", "op": "like"})
    #
    #     assert len(link_to_plan) == 1
    #
    #     link_from_plan = self.query_eboa.get_event_links(event_uuids = {"filter": [str(processing_validity_l0.event_uuid)], "op": "in"},
    #                                                 event_uuid_links = {"filter": [str(imaging_plan.event_uuid)], "op": "in"},
    #                                                 link_names = {"filter": "PLANNED_IMAGING", "op": "like"})
    #
    #     assert len(link_from_plan) == 1
    #
    #     link_to_isp_validity = self.query_eboa.get_event_links(event_uuid_links = {"filter": [str(processing_validity_l0.event_uuid)], "op": "in"},
    #                                                 event_uuids = {"filter": [str(isp_validity.event_uuid)], "op": "in"},
    #                                                 link_names = {"filter": "PROCESSING_VALIDITY", "op": "like"})
    #
    #     assert len(link_to_isp_validity) == 1
    #
    #     link_from_isp_validity = self.query_eboa.get_event_links(event_uuids = {"filter": [str(processing_validity_l0.event_uuid)], "op": "in"},
    #                                                 event_uuid_links = {"filter": [str(isp_validity.event_uuid)], "op": "in"},
    #                                                 link_names = {"filter": "ISP_VALIDITY", "op": "like"})
    #
    #     assert len(link_from_isp_validity) == 1
    #
    #     assert processing_validity_l0.get_structured_values() == [{
    #        "values": [
    #             {
    #                 "value": "COMPLETE",
    #                 "name": "status",
    #                 "type": "text"
    #             },
    #             {
    #                 "value": "L0",
    #                 "name": "level",
    #                 "type": "text"
    #             },
    #             {
    #                 "value": "S2A",
    #                 "name": "satellite",
    #                 "type": "text"
    #             },
    #             {
    #                 "value": "MPS_",
    #                 "type": "text",
    #                 "name": "processing_centre"
    #             },
    #             {
    #                 "value": "MATCHED_PLANNED_IMAGING",
    #                 "type": "text",
    #                 "name": "matching_plan_status"
    #             },
    #             {
    #                 "value": "MATCHED_ISP_VALIDITY",
    #                 "type": "text",
    #                 "name": "matching_reception_status"
    #             },
    #             {
    #                 "value": "16077.0",
    #                 "type": "double",
    #                 "name": "sensing_orbit"
    #             },
    #             {
    #                 "value": "16078.0",
    #                 "type": "double",
    #                 "name": "downlink_orbit"
    #             }
    #         ],
    #         "type": "object",
    #         "name": "details"
    #     }]
    #
    #     processing_validities = self.query_eboa.get_events(explicit_refs = {"filter": "S2A_OPER_MSI_L1B_DS_MPS__20180721T104253_S20180721T085229_N02.06", "op": "like"},
    #                                                        gauge_names = {"filter": "PROCESSING_VALIDITY", "op": "like"},
    #                                          start_filters = [{"date": "2018-07-21T08:52:31", "op": "=="}],
    #                                          stop_filters = [{"date": "2018-07-21T08:54:14", "op": "=="}])
    #
    #     assert len(processing_validities) == 1
    #     processing_validity_l1b = processing_validities[0]
    #
    #     processing_validities = self.query_eboa.get_events(explicit_refs = {"filter": "S2A_OPER_MSI_L1C_DS_MPS__20180721T104253_S20180721T085229_N02.06", "op": "like"},
    #                                                        gauge_names = {"filter": "PROCESSING_VALIDITY", "op": "like"},
    #                                          start_filters = [{"date": "2018-07-21T08:52:31", "op": "=="}],
    #                                          stop_filters = [{"date": "2018-07-21T08:54:14", "op": "=="}])
    #
    #     assert len(processing_validities) == 1
    #     processing_validity_l1c = processing_validities[0]
    #
    #     assert processing_validity_l1c.get_structured_values() == [{
    #         "values": [
    #             {
    #                 "value": "COMPLETE",
    #                 "name": "status",
    #                 "type": "text"
    #             },
    #             {
    #                 "value": "L1C",
    #                 "name": "level",
    #                 "type": "text"
    #             },
    #             {
    #                 "value": "S2A",
    #                 "name": "satellite",
    #                 "type": "text"
    #             },
    #             {
    #                 "value": "MPS_",
    #                 "name": "processing_centre",
    #                 "type": "text"
    #             },
    #             {
    #                 "value": "MATCHED_PLANNED_IMAGING",
    #                 "name": "matching_plan_status",
    #                 "type": "text"
    #             },
    #             {
    #                 "value": "MATCHED_ISP_VALIDITY",
    #                 "name": "matching_reception_status",
    #                 "type": "text"
    #             },
    #             {
    #                 "value": "16077.0",
    #                 "name": "sensing_orbit",
    #                 "type": "double"
    #             },
    #             {
    #                 "value": "16078.0",
    #                 "name": "downlink_orbit",
    #                 "type": "double"
    #             }
    #         ],
    #         "name": "details",
    #         "type": "object"
    #     }]
    #
    #     # Check ISP validity completeness
    #     isp_completeness = self.query_eboa.get_events(explicit_refs = {"filter": "S2A_OPER_MSI_L0__DS_MPS__20180721T103920_S20180721T085229_N02.06", "op": "like"},
    #                                                        gauge_names = {"filter": "ISP_VALIDITY_PROCESSING_COMPLETENESS_L0", "op": "like"},
    #                                                        start_filters = [{"date": "2018-07-21T08:52:29", "op": "=="}],
    #                                                        stop_filters = [{"date": "2018-07-21T08:54:19", "op": "=="}])
    #
    #     assert len(isp_completeness) == 1
    #     isp_completeness_l0 = isp_completeness[0]
    #
    #     isp_completeness = self.query_eboa.get_events(explicit_refs = {"filter": "S2A_OPER_MSI_L1B_DS_MPS__20180721T104253_S20180721T085229_N02.06", "op": "like"},
    #                                                        gauge_names = {"filter": "ISP_VALIDITY_PROCESSING_COMPLETENESS_L1B", "op": "like"},
    #                                          start_filters = [{"date": "2018-07-21T08:52:31", "op": "=="}],
    #                                          stop_filters = [{"date": "2018-07-21T08:54:14", "op": "=="}])
    #
    #     assert len(isp_completeness) == 1
    #     isp_completeness_l1b = isp_completeness[0]
    #
    #     isp_completeness = self.query_eboa.get_events(explicit_refs = {"filter": "S2A_OPER_MSI_L1C_DS_MPS__20180721T104253_S20180721T085229_N02.06", "op": "like"},
    #                                                        gauge_names = {"filter": "ISP_VALIDITY_PROCESSING_COMPLETENESS_L1C", "op": "like"},
    #                                          start_filters = [{"date": "2018-07-21T08:52:31", "op": "=="}],
    #                                          stop_filters = [{"date": "2018-07-21T08:54:14", "op": "=="}])
    #
    #     assert len(isp_completeness) == 1
    #     isp_completeness_l1c = isp_completeness[0]
    #
    #     # Check planning completeness
    #     planning_completeness = self.query_eboa.get_events(explicit_refs = {"filter": "S2A_OPER_MSI_L0__DS_MPS__20180721T103920_S20180721T085229_N02.06", "op": "like"},
    #                                                        gauge_names = {"filter": "PLANNED_IMAGING_PROCESSING_COMPLETENESS_L0", "op": "like"},
    #                                                        start_filters = [{"date": "2018-07-21T08:52:29", "op": "=="}],
    #                                                        stop_filters = [{"date": "2018-07-21T08:54:19", "op": "=="}])
    #
    #     assert len(planning_completeness) == 1
    #     planning_completeness_l0 = planning_completeness[0]
    #
    #     assert planning_completeness_l0.get_structured_values() == [{
    #         "name": "details",
    #         "values": [
    #             {
    #                 "name": "status",
    #                 "value": "COMPLETE",
    #                 "type": "text"
    #             },
    #             {
    #                 "name": "level",
    #                 "value": "L0",
    #                 "type": "text"
    #             },
    #             {
    #                 "name": "satellite",
    #                 "value": "S2A",
    #                 "type": "text"
    #             },
    #             {
    #                 "name": "processing_centre",
    #                 "value": "MPS_",
    #                 "type": "text"
    #             },
    #             {
    #                 "name": "matching_plan_status",
    #                 "value": "MATCHED_PLANNED_IMAGING",
    #                 "type": "text"
    #             },
    #             {
    #                 "name": "matching_reception_status",
    #                 "value": "MATCHED_ISP_VALIDITY",
    #                 "type": "text"
    #             },
    #             {
    #                 "name": "sensing_orbit",
    #                 "value": "16077.0",
    #                 "type": "double"
    #             },
    #             {
    #                 "name": "downlink_orbit",
    #                 "value": "16078.0",
    #                 "type": "double"
    #             }
    #         ],
    #         "type": "object"
    #     }]
    #
    #     missing_planning_completeness = self.query_eboa.get_events(gauge_names = {"filter": "PLANNED_IMAGING_PROCESSING_COMPLETENESS_L0", "op": "like"},
    #                                                        start_filters = [{"date": "2018-07-21T08:36:02.255634", "op": "=="}],
    #                                                        stop_filters = [{"date": "2018-07-21T08:52:29", "op": "=="}])
    #
    #     assert len(missing_planning_completeness) == 1
    #
    #     missing_planning_completeness_statuses = [event for event in missing_planning_completeness if len([value for value in event.eventTexts if value.name == "status" and value.value == "MISSING"]) > 0]
    #
    #     assert len(missing_planning_completeness_statuses) == 1
    #
    #     missing_planning_completeness = self.query_eboa.get_events(gauge_names = {"filter": "PLANNED_IMAGING_PROCESSING_COMPLETENESS_L0", "op": "like"},
    #                                                        start_filters = [{"date": "2018-07-21T08:54:19", "op": "=="}],
    #                                                        stop_filters = [{"date": "2018-07-21T09:08:56.195941", "op": "=="}])
    #
    #     assert len(missing_planning_completeness) == 1
    #
    #     planning_completeness = self.query_eboa.get_events(explicit_refs = {"filter": "S2A_OPER_MSI_L1B_DS_MPS__20180721T104253_S20180721T085229_N02.06", "op": "like"},
    #                                                        gauge_names = {"filter": "PLANNED_IMAGING_PROCESSING_COMPLETENESS_L1B", "op": "like"},
    #                                          start_filters = [{"date": "2018-07-21T08:52:31", "op": "=="}],
    #                                          stop_filters = [{"date": "2018-07-21T08:54:14", "op": "=="}])
    #
    #     assert len(planning_completeness) == 1
    #     planning_completeness_l1b = planning_completeness[0]
    #
    #     missing_planning_completeness = self.query_eboa.get_events(gauge_names = {"filter": "PLANNED_IMAGING_PROCESSING_COMPLETENESS_L1B", "op": "like"},
    #                                                        start_filters = [{"date": "2018-07-21T08:36:02.255634", "op": "=="}],
    #                                                        stop_filters = [{"date": "2018-07-21T08:52:31", "op": "=="}])
    #
    #     assert len(missing_planning_completeness) == 1
    #
    #     missing_planning_completeness = self.query_eboa.get_events(gauge_names = {"filter": "PLANNED_IMAGING_PROCESSING_COMPLETENESS_L1B", "op": "like"},
    #                                                        start_filters = [{"date": "2018-07-21T08:54:14", "op": "=="}],
    #                                                        stop_filters = [{"date": "2018-07-21T09:08:56.195941", "op": "=="}])
    #
    #     assert len(missing_planning_completeness) == 1
    #
    #     planning_completeness = self.query_eboa.get_events(explicit_refs = {"filter": "S2A_OPER_MSI_L1C_DS_MPS__20180721T104253_S20180721T085229_N02.06", "op": "like"},
    #                                                        gauge_names = {"filter": "PLANNED_IMAGING_PROCESSING_COMPLETENESS_L1C", "op": "like"},
    #                                          start_filters = [{"date": "2018-07-21T08:52:31", "op": "=="}],
    #                                          stop_filters = [{"date": "2018-07-21T08:54:14", "op": "=="}])
    #
    #     assert len(planning_completeness) == 1
    #     planning_completeness_l1c = planning_completeness[0]
    #
    #     assert planning_completeness_l1c.get_structured_values() == [{
    #         "name": "details",
    #         "values": [
    #             {
    #                 "name": "status",
    #                 "value": "COMPLETE",
    #                 "type": "text"
    #             },
    #             {
    #                 "name": "level",
    #                 "value": "L1C",
    #                 "type": "text"
    #             },
    #             {
    #                 "name": "satellite",
    #                 "value": "S2A",
    #                 "type": "text"
    #             },
    #             {
    #                 "name": "processing_centre",
    #                 "value": "MPS_",
    #                 "type": "text"
    #             },
    #             {
    #                 "name": "matching_plan_status",
    #                 "value": "MATCHED_PLANNED_IMAGING",
    #                 "type": "text"
    #             },
    #             {
    #                 "name": "matching_reception_status",
    #                 "value": "MATCHED_ISP_VALIDITY",
    #                 "type": "text"
    #             },
    #             {
    #                 "name": "sensing_orbit",
    #                 "value": "16077.0",
    #                 "type": "double"
    #             },
    #             {
    #                 "name": "downlink_orbit",
    #                 "value": "16078.0",
    #                 "type": "double"
    #             }
    #         ],
    #         "type": "object"
    #     }]
    #
    #     missing_planning_completeness = self.query_eboa.get_events(gauge_names = {"filter": "PLANNED_IMAGING_PROCESSING_COMPLETENESS_L1C", "op": "like"},
    #                                                        start_filters = [{"date": "2018-07-21T08:36:02.255634", "op": "=="}],
    #                                                        stop_filters = [{"date": "2018-07-21T08:52:31", "op": "=="}])
    #
    #     assert len(missing_planning_completeness) == 1
    #
    #     missing_planning_completeness_statuses = [event for event in missing_planning_completeness if len([value for value in event.eventTexts if value.name == "status" and value.value == "MISSING"]) > 0]
    #
    #     assert len(missing_planning_completeness_statuses) == 1
    #
    #     missing_planning_completeness = self.query_eboa.get_events(gauge_names = {"filter": "PLANNED_IMAGING_PROCESSING_COMPLETENESS_L1C", "op": "like"},
    #                                                        start_filters = [{"date": "2018-07-21T08:54:14", "op": "=="}],
    #                                                        stop_filters = [{"date": "2018-07-21T09:08:56.195941", "op": "=="}])
    #
    #     assert len(missing_planning_completeness) == 1
    #
    #     # Check timeliness
    #     timeliness = self.query_eboa.get_events(explicit_refs = {"filter": "S2A_OPER_MSI_L0__DS_MPS__20180721T103920_S20180721T085229_N02.06", "op": "like"},
    #                                                        gauge_names = {"filter": "TIMELINESS", "op": "like"},
    #                                                        start_filters = [{"date": "2018-07-21T10:39:13.800000", "op": "=="}],
    #                                                        stop_filters = [{"date": "2018-07-21T10:43:26.741000", "op": "=="}])
    #
    #     assert len(timeliness) == 1
    #     timeliness_l0 = timeliness[0]
    #
    #     timeliness = self.query_eboa.get_events(explicit_refs = {"filter": "S2A_OPER_MSI_L1B_DS_MPS__20180721T104253_S20180721T085229_N02.06", "op": "like"},
    #                                                        gauge_names = {"filter": "TIMELINESS", "op": "like"},
    #                                          start_filters = [{"date": "2018-07-21T10:42:47.800000", "op": "=="}],
    #                                          stop_filters = [{"date": "2018-07-21T10:55:38.778000", "op": "=="}])
    #
    #     assert len(timeliness) == 1
    #     timeliness_l1b = timeliness[0]
    #
    #     timeliness = self.query_eboa.get_events(explicit_refs = {"filter": "S2A_OPER_MSI_L1C_DS_MPS__20180721T104253_S20180721T085229_N02.06", "op": "like"},
    #                                                        gauge_names = {"filter": "TIMELINESS", "op": "like"},
    #                                          start_filters = [{"date": "2018-07-21T10:50:39.900000", "op": "=="}],
    #                                          stop_filters = [{"date": "2018-07-21T11:00:38.066000", "op": "=="}])
    #
    #     assert len(timeliness) == 1
    #     timeliness_l1c = timeliness[0]
    #
    # def test_dpc_gaps_L1C_with_L0_plan_rep_pass(self):
    #
    #     filename = "S2A_NPPF.EOF"
    #     file_path = os.path.dirname(os.path.abspath(__file__)) + "/inputs/" + filename
    #
    #     returned_value = ingestion_nppf.command_process_file(file_path)
    #
    #     assert returned_value[0]["status"] == eboa_engine.exit_codes["OK"]["status"]
    #
    #     filename = "S2A_ORBPRE.EOF"
    #     file_path = os.path.dirname(os.path.abspath(__file__)) + "/inputs/" + filename
    #
    #     returned_value = ingestion_orbpre.command_process_file(file_path)
    #
    #     assert returned_value[0]["status"] == eboa_engine.exit_codes["OK"]["status"]
    #
    #     filename = "S2A_REP_PASS_WITH_GAPS.EOF"
    #     file_path = os.path.dirname(os.path.abspath(__file__)) + "/inputs/" + filename
    #
    #     returned_value = ingestion_dfep.command_process_file(file_path)
    #
    #     assert returned_value[0]["status"] == eboa_engine.exit_codes["OK"]["status"]
    #
    #     filename = "S2A_OPER_REP_OPDPC_L0U_L0_WITH_GAPS.EOF"
    #     file_path = os.path.dirname(os.path.abspath(__file__)) + "/inputs/" + filename
    #
    #     returned_value = ingestion_dpc.command_process_file(file_path)
    #
    #     assert returned_value[0]["status"] == eboa_engine.exit_codes["OK"]["status"]
    #
    #     filename = "S2A_OPER_REP_OPDPC_L1B_L1C.EOF"
    #     file_path = os.path.dirname(os.path.abspath(__file__)) + "/inputs/" + filename
    #
    #     returned_value = ingestion_dpc.command_process_file(file_path)
    #
    #     assert returned_value[0]["status"] == eboa_engine.exit_codes["OK"]["status"]
    #
    #     # Check processing validities
    #     processing_validities = self.query_eboa.get_events(explicit_refs = {"filter": "S2A_OPER_MSI_L0__DS_MPS__20180721T103920_S20180721T085229_N02.06", "op": "like"},
    #                                                        gauge_names = {"filter": "PROCESSING_VALIDITY", "op": "like"},
    #                                                        start_filters = [{"date": "2018-07-21T08:52:29", "op": "=="}],
    #                                                        stop_filters = [{"date": "2018-07-21T08:54:19", "op": "=="}])
    #
    #     assert len(processing_validities) == 1
    #     processing_validity_l0 = processing_validities[0]
    #
    #     assert processing_validity_l0.get_structured_values() == [{
    #        "values": [
    #             {
    #                 "value": "INCOMPLETE",
    #                 "name": "status",
    #                 "type": "text"
    #             },
    #             {
    #                 "value": "L0",
    #                 "name": "level",
    #                 "type": "text"
    #             },
    #             {
    #                 "value": "S2A",
    #                 "name": "satellite",
    #                 "type": "text"
    #             },
    #             {
    #                 "value": "MPS_",
    #                 "type": "text",
    #                 "name": "processing_centre"
    #             },
    #             {
    #                 "value": "MATCHED_PLANNED_IMAGING",
    #                 "type": "text",
    #                 "name": "matching_plan_status"
    #             },
    #             {
    #                 "value": "MATCHED_ISP_VALIDITY",
    #                 "type": "text",
    #                 "name": "matching_reception_status"
    #             },
    #             {
    #                 "value": "16077.0",
    #                 "type": "double",
    #                 "name": "sensing_orbit"
    #             },
    #             {
    #                 "value": "16078.0",
    #                 "type": "double",
    #                 "name": "downlink_orbit"
    #             }
    #         ],
    #         "type": "object",
    #         "name": "details"
    #     }]
    #
    #     processing_validities = self.query_eboa.get_events(explicit_refs = {"filter": "S2A_OPER_MSI_L1C_DS_MPS__20180721T104253_S20180721T085229_N02.06", "op": "like"},
    #                                                        gauge_names = {"filter": "PROCESSING_VALIDITY", "op": "like"},
    #                                          start_filters = [{"date": "2018-07-21T08:52:29", "op": "=="}],
    #                                          stop_filters = [{"date": "2018-07-21T08:54:19", "op": "=="}])
    #
    #     assert len(processing_validities) == 1
    #     processing_validity_l1c = processing_validities[0]
    #
    #     assert processing_validity_l1c.get_structured_values() == [{
    #         "values": [
    #             {
    #                 "value": "INCOMPLETE",
    #                 "name": "status",
    #                 "type": "text"
    #             },
    #             {
    #                 "value": "L1C",
    #                 "name": "level",
    #                 "type": "text"
    #             },
    #             {
    #                 "value": "S2A",
    #                 "name": "satellite",
    #                 "type": "text"
    #             },
    #             {
    #                 "value": "MPS_",
    #                 "name": "processing_centre",
    #                 "type": "text"
    #             },
    #             {
    #                 "value": "MATCHED_PLANNED_IMAGING",
    #                 "name": "matching_plan_status",
    #                 "type": "text"
    #             },
    #             {
    #                 "value": "MATCHED_ISP_VALIDITY",
    #                 "name": "matching_reception_status",
    #                 "type": "text"
    #             },
    #             {
    #                 "value": "16077.0",
    #                 "name": "sensing_orbit",
    #                 "type": "double"
    #             },
    #             {
    #                 "value": "16078.0",
    #                 "name": "downlink_orbit",
    #                 "type": "double"
    #             }
    #         ],
    #         "name": "details",
    #         "type": "object"
    #     }]
    #
    #     processing_gaps = self.query_eboa.get_events(explicit_refs = {"filter": "S2A_OPER_MSI_L0__DS_MPS__20180721T103920_S20180721T085229_N02.06", "op": "like"},
    #                                                        gauge_names = {"filter": "PROCESSING_GAP", "op": "like"},
    #                                          start_filters = [{"date": "2018-07-21T08:52:35", "op": "=="}],
    #                                          stop_filters = [{"date": "2018-07-21T08:52:37.209040", "op": "=="}])
    #
    #     assert len(processing_gaps) == 1
    #     first_processing_gap_l0 = processing_gaps[0]
    #
    #     # Check links with PROCESSING VALIDITY
    #     link_to_gap = self.query_eboa.get_event_links(event_uuid_links = {"filter": [str(processing_validity_l0.event_uuid)], "op": "in"},
    #                                                 event_uuids = {"filter": [str(first_processing_gap_l0.event_uuid)], "op": "in"},
    #                                                 link_names = {"filter": "PROCESSING_VALIDITY", "op": "like"})
    #
    #     assert len(link_to_gap) == 1
    #
    #     link_from_gap = self.query_eboa.get_event_links(event_uuids = {"filter": [str(processing_validity_l0.event_uuid)], "op": "in"},
    #                                                 event_uuid_links = {"filter": [str(first_processing_gap_l0.event_uuid)], "op": "in"},
    #                                                 link_names = {"filter": "PROCESSING_GAP", "op": "like"})
    #
    #     assert len(link_from_gap) == 1
    #
    #     assert first_processing_gap_l0.get_structured_values() == [{
    #         "values": [
    #             {
    #                 "value": "12.0",
    #                 "name": "detector",
    #                 "type": "double"
    #             },
    #             {
    #                 "value": "reception",
    #                 "name": "source",
    #                 "type": "text"
    #             },
    #             {
    #                 "value": "L0",
    #                 "name": "level",
    #                 "type": "text"
    #             },
    #             {
    #                 "value": "S2A",
    #                 "name": "satellite",
    #                 "type": "text"
    #             }
    #         ],
    #         "name": "details",
    #         "type": "object"
    #     }]
    #
    #     processing_gaps = self.query_eboa.get_events(explicit_refs = {"filter": "S2A_OPER_MSI_L0__DS_MPS__20180721T103920_S20180721T085229_N02.06", "op": "like"},
    #                                                        gauge_names = {"filter": "PROCESSING_GAP", "op": "like"},
    #                                          start_filters = [{"date": "2018-07-21T08:52:37.209040", "op": "=="}],
    #                                          stop_filters = [{"date": "2018-07-21T08:52:40", "op": "=="}])
    #
    #     assert len(processing_gaps) == 1
    #     second_processing_gap_l0 = processing_gaps[0]
    #
    #     # Check links with PROCESSING VALIDITY
    #     link_to_gap = self.query_eboa.get_event_links(event_uuid_links = {"filter": [str(processing_validity_l0.event_uuid)], "op": "in"},
    #                                                 event_uuids = {"filter": [str(second_processing_gap_l0.event_uuid)], "op": "in"},
    #                                                 link_names = {"filter": "PROCESSING_VALIDITY", "op": "like"})
    #
    #     assert len(link_to_gap) == 1
    #
    #     link_from_gap = self.query_eboa.get_event_links(event_uuids = {"filter": [str(processing_validity_l0.event_uuid)], "op": "in"},
    #                                                 event_uuid_links = {"filter": [str(second_processing_gap_l0.event_uuid)], "op": "in"},
    #                                                 link_names = {"filter": "PROCESSING_GAP", "op": "like"})
    #
    #     assert len(link_from_gap) == 1
    #
    #     assert second_processing_gap_l0.get_structured_values() == [{
    #         "values": [
    #             {
    #                 "value": "12.0",
    #                 "name": "detector",
    #                 "type": "double"
    #             },
    #             {
    #                 "value": "processing",
    #                 "name": "source",
    #                 "type": "text"
    #             },
    #             {
    #                 "value": "L0",
    #                 "name": "level",
    #                 "type": "text"
    #             },
    #             {
    #                 "value": "S2A",
    #                 "name": "satellite",
    #                 "type": "text"
    #             }
    #         ],
    #         "name": "details",
    #         "type": "object"
    #     }]
    #
    #
    #     processing_gaps = self.query_eboa.get_events(explicit_refs = {"filter": "S2A_OPER_MSI_L1C_DS_MPS__20180721T104253_S20180721T085229_N02.06", "op": "like"},
    #                                                        gauge_names = {"filter": "PROCESSING_GAP", "op": "like"},
    #                                          start_filters = [{"date": "2018-07-21T08:52:35", "op": "=="}],
    #                                          stop_filters = [{"date": "2018-07-21T08:52:37.209040", "op": "=="}])
    #
    #     assert len(processing_gaps) == 1
    #     first_processing_gap_l1c = processing_gaps[0]
    #
    #     # Check links with PROCESSING VALIDITY
    #     link_to_gap = self.query_eboa.get_event_links(event_uuid_links = {"filter": [str(processing_validity_l1c.event_uuid)], "op": "in"},
    #                                                 event_uuids = {"filter": [str(first_processing_gap_l1c.event_uuid)], "op": "in"},
    #                                                 link_names = {"filter": "PROCESSING_VALIDITY", "op": "like"})
    #
    #     assert len(link_to_gap) == 1
    #
    #     link_from_gap = self.query_eboa.get_event_links(event_uuids = {"filter": [str(processing_validity_l1c.event_uuid)], "op": "in"},
    #                                                 event_uuid_links = {"filter": [str(first_processing_gap_l1c.event_uuid)], "op": "in"},
    #                                                 link_names = {"filter": "PROCESSING_GAP", "op": "like"})
    #
    #     assert len(link_from_gap) == 1
    #
    #     assert first_processing_gap_l1c.get_structured_values() == [{
    #         "values": [
    #             {
    #                 "value": "12.0",
    #                 "name": "detector",
    #                 "type": "double"
    #             },
    #             {
    #                 "value": "reception",
    #                 "name": "source",
    #                 "type": "text"
    #             },
    #             {
    #                 "value": "L1C",
    #                 "name": "level",
    #                 "type": "text"
    #             },
    #             {
    #                 "value": "S2A",
    #                 "name": "satellite",
    #                 "type": "text"
    #             }
    #         ],
    #         "name": "details",
    #         "type": "object"
    #     }]
    #
    #     processing_gaps = self.query_eboa.get_events(explicit_refs = {"filter": "S2A_OPER_MSI_L1C_DS_MPS__20180721T104253_S20180721T085229_N02.06", "op": "like"},
    #                                                        gauge_names = {"filter": "PROCESSING_GAP", "op": "like"},
    #                                          start_filters = [{"date": "2018-07-21T08:52:37.209040", "op": "=="}],
    #                                          stop_filters = [{"date": "2018-07-21T08:52:40", "op": "=="}])
    #
    #     assert len(processing_gaps) == 1
    #     second_processing_gap_l1c = processing_gaps[0]
    #
    #     # Check links with PROCESSING VALIDITY
    #     link_to_gap = self.query_eboa.get_event_links(event_uuid_links = {"filter": [str(processing_validity_l1c.event_uuid)], "op": "in"},
    #                                                 event_uuids = {"filter": [str(second_processing_gap_l1c.event_uuid)], "op": "in"},
    #                                                 link_names = {"filter": "PROCESSING_VALIDITY", "op": "like"})
    #
    #     assert len(link_to_gap) == 1
    #
    #     link_from_gap = self.query_eboa.get_event_links(event_uuids = {"filter": [str(processing_validity_l1c.event_uuid)], "op": "in"},
    #                                                 event_uuid_links = {"filter": [str(second_processing_gap_l1c.event_uuid)], "op": "in"},
    #                                                 link_names = {"filter": "PROCESSING_GAP", "op": "like"})
    #
    #     assert second_processing_gap_l1c.get_structured_values() == [{
    #         "values": [
    #             {
    #                 "value": "12.0",
    #                 "name": "detector",
    #                 "type": "double"
    #             },
    #             {
    #                 "value": "processing",
    #                 "name": "source",
    #                 "type": "text"
    #             },
    #             {
    #                 "value": "L1C",
    #                 "name": "level",
    #                 "type": "text"
    #             },
    #             {
    #                 "value": "S2A",
    #                 "name": "satellite",
    #                 "type": "text"
    #             }
    #         ],
    #         "name": "details",
    #         "type": "object"
    #     }]
