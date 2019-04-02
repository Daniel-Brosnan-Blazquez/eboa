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


    def test_dpc_report_only(self):
        filename = "S2A_OPER_REP_OPDPC_INSERTION.EOF"
        file_path = os.path.dirname(os.path.abspath(__file__)) + "/inputs/" + filename
        ingestion_dpc.command_process_file(file_path)

        #Check a single dim_signature is correctly taken
        dim_signature = self.session.query(DimSignature).filter(DimSignature.dim_signature == "PROCESSING_S2A").all()

        assert len(dim_signature) == 1

        #Check baseline annotation configuration is correctly taken
        baseline_annotation_cnf = self.session.query(AnnotationCnf).filter(AnnotationCnf.name == "PRODUCTION_BASELINE",
                                                                                            AnnotationCnf.system == "EPAE").all()

        assert len(baseline_annotation_cnf) == 1
        #
        #Check baseline annotation value is correctly taken
        baseline_annotation_text = self.session.query(AnnotationText).filter(AnnotationText.name == "baseline",
                                                                                             AnnotationText.value == "N02.06").all()

        assert len(baseline_annotation_text) == 1

        #Check input_output explicit_ref is correctly taken
        definite_explicit_ref_1 = self.session.query(ExplicitRefGrp).join(ExplicitRef).filter(ExplicitRef.explicit_ref == "S2A_OPER_MSI_L1B_DS_EPAE_20180721T020145_S20180721T001610_N02.06",
                                                                                              ExplicitRefGrp.name == "L1B_DS").all()

        assert len(definite_explicit_ref_1) == 1

        #Check a single granule explicit_ref is correctly taken
        definite_explicit_ref_2  = self.session.query(ExplicitRefGrp).join(ExplicitRef).filter(ExplicitRef.explicit_ref == "S2A_OPER_MSI_L1B_GR_EPAE_20180721T020145_S20180721T001612_D02_N02.06",
                                                                                              ExplicitRefGrp.name == "L1B_GR").all()

        assert len(definite_explicit_ref_2) == 1

        # #Check a single mrf explicit_ref is correctly taken
        definite_explicit_ref_3 = self.session.query(ExplicitRefGrp).join(ExplicitRef).filter(ExplicitRef.explicit_ref == "S2A_OPER_GIP_PROBAS_MPC__20180716T000206_V20180718T000000_21000101T000000_B00",
                                                                                                ExplicitRefGrp.name == "MISSION_CONFIGURATION").all()

        assert len(definite_explicit_ref_3) == 1

        #Check timeliness event is correctly taken
        definite_timeliness_event = self.session.query(Event).join(Gauge).filter(Gauge.name == "TIMELINESS",
                                                                                 Gauge.system == "EPAE").all()

        assert len(definite_timeliness_event) == 1

        #Check all step events are correctly taken
        steps_events = self.session.query(Event).join(Gauge).filter(Gauge.name == "STEP_INFO",
                                                                    Gauge.system == "EPAE").all()

        assert len(steps_events) == 18

        #Check that the values from the steps are correctly taken
        definite_step_values_id = self.session.query(EventText).join(Event,Gauge).filter(Gauge.name == "STEP_INFO",
                                                                             EventText.name == "id",
                                                                             EventText.value == "step_Format_Image_L1B").all()

        assert len(definite_step_values_id) == 1

        definite_step_values_exec_mode = self.session.query(EventText).join(Event,Gauge).filter(Gauge.name == "STEP_INFO",
                                                                             EventText.name == "exec_mode",
                                                                             EventText.value == "nominal").all()

        assert len(definite_step_values_exec_mode) == 18

        #Check all mrf events are correctly taken
        mrfs_events = self.session.query(EventKey).join(Event,Gauge).filter(Gauge.name == "MRF_VALIDITY",
                                                                            Gauge.system == "EPAE").all()

        assert len(mrfs_events) == 131

        #Check a definite mrf event is correctly taken
        definite_mrf_event = self.session.query(EventKey).join(Event,Gauge).filter(Gauge.name == "MRF_VALIDITY",
                                                                         Gauge.system == "EPAE",
                                                                         EventKey.event_key == "S2A_OPER_GIP_PRDLOC_MPC__20180301T130000_V20180305T005000_21000101T000000_B00",
                                                                         Event.start == "2018-03-05T00:50:00.000",
                                                                         Event.stop == "2100-01-01 00:00:00").all()

        assert len(definite_mrf_event) == 1

        #Check that a definite MRF values are correctly taken
        definite_mrf_event_values = self.session.query(EventTimestamp).join(Event, Gauge).filter(Gauge.name == "MRF_VALIDITY",
                                                                         Gauge.system == "EPAE",
                                                                         Event.start == "2018-03-05T00:50:00.000",
                                                                         Event.stop == "2100-01-01 00:00:00",
                                                                         EventTimestamp.value == "20180301T130000").all()

        assert len(definite_mrf_event_values) == 1

        #Check that a definite MRF eventKey is correctly taken
        definite_mrf_event_key = self.session.query(EventKey).filter(EventKey.event_key == "S2A_OPER_GIP_PRDLOC_MPC__20180301T130000_V20180305T005000_21000101T000000_B00").all()

        assert len(definite_mrf_event_key) == 1

    def test_dpc_report_L0_L1B_with_plan(self):
        filename = "S2A_OPER_REP_OPDPC_INSERTION.EOF"
        orbpre_filename = "S2A_ORBPRE.EOF"
        nppf_filename = "S2A_NPPF.EOF"

        nppf_file_path = os.path.dirname(os.path.abspath(__file__)) + "/inputs/" + nppf_filename
        ingestion_nppf.command_process_file(nppf_file_path)

        orbpre_file_path = os.path.dirname(os.path.abspath(__file__)) + "/inputs/" + orbpre_filename
        ingestion_orbpre.command_process_file(orbpre_file_path)

        file_path = os.path.dirname(os.path.abspath(__file__)) + "/inputs/" + filename
        ingestion_dpc.command_process_file(file_path)

        #Check that the source validity times are correctly taken
        source = self.session.query(Source).filter(Source.name == 'S2A_OPER_REP_OPDPC_INSERTION.EOF',
                                                    Source.generation_time == '2018-07-21 02:21:01',
                                                    Source.validity_start == '2018-07-21 00:16:12',
                                                    Source.validity_stop == '2018-07-21 02:20:48.22').all()

        assert len(source) == 1

        #Check that the sources is correctly taken
        sources = self.session.query(Source).filter(Source.generation_time == "2018-07-21T02:21:01",
                                                            Source.name == "S2A_OPER_REP_OPDPC_INSERTION.EOF").all()

        assert len(sources) == 3

        #Check the missing segment is correctly taken
        missing_event = self.session.query(EventText).join(Event,Gauge).filter(Gauge.name == "PLANNED_IMAGING_L1B_COMPLETENESS",
                                                                               Event.start == "2018-07-21 00:16:02.341000",
                                                                               EventText.name == "status",
                                                                               EventText.value == "MISSING").all()

        assert len(missing_event) == 1

        #Check datablock_completeness_event is correctly taken
        datablock_event = self.session.query(EventText).join(Event,Gauge).filter(Gauge.name == "PLANNED_IMAGING_L1B_COMPLETENESS",
                                                                                 EventText.name == "status",
                                                                                 #Incomplete due to alignment not corrected
                                                                                 EventText.value == "INCOMPLETE").all()

        assert len(datablock_event) == 1

        datablock_event_links = self.session.query(EventLink).join(Event,EventText,Gauge).filter(Gauge.name == "PLANNED_IMAGING_L1B_COMPLETENESS",
                                                                                 Event.start == "2018-07-21 00:16:12",
                                                                                 EventText.name == "status",
                                                                                 #Incomplete due to alignment not corrected
                                                                                 EventText.value == "INCOMPLETE").all()

        assert len(datablock_event_links) == 2

    def test_dpc_report_L0_L1B_with_REP_PASS(self):
        filename = "S2A_OPER_REP_OPDPC_L0U_L0_2.EOF"
        rep_pass_filename = "S2A_REP_PASS_NO_GAPS.EOF"
        orbpre_filename = "S2A_ORBPRE_2.EOF"
        nppf_filename = "S2A_NPPF_2.EOF"

        nppf_file_path = os.path.dirname(os.path.abspath(__file__)) + "/inputs/" + nppf_filename
        ingestion_nppf.command_process_file(nppf_file_path)

        orbpre_file_path = os.path.dirname(os.path.abspath(__file__)) + "/inputs/" + orbpre_filename
        ingestion_orbpre.command_process_file(orbpre_file_path)

        rep_pass_file_path = os.path.dirname(os.path.abspath(__file__)) + "/inputs/" + rep_pass_filename
        ingestion_dfep.command_process_file(rep_pass_file_path)

        file_path = os.path.dirname(os.path.abspath(__file__)) + "/inputs/" + filename
        ingestion_dpc.command_process_file(file_path)

        #Check the received_imaging_completeness comprehends the same period as the processing_validity
        processing_validity_event = self.session.query(Event).join(Gauge).filter(Gauge.name == "PROCESSING_VALIDITY_L0").all()

        received_imaging_completeness = self.session.query(Event).join(Gauge).filter(Gauge.name == "RECEIVED_IMAGING_L0_COMPLETENESS").all()

        assert (processing_validity_event[0].start == received_imaging_completeness[0].start and processing_validity_event[0].stop == received_imaging_completeness[0].stop)

    def test_dpc_report_L1B_L1C_with_plan_no_previous_reports(self):
        filename = "S2A_OPER_REP_OPDPC_L1B_L1C.EOF"
        orbpre_filename = "S2A_ORBPRE.EOF"
        nppf_filename = "S2A_NPPF.EOF"

        nppf_file_path = os.path.dirname(os.path.abspath(__file__)) + "/inputs/" + nppf_filename
        ingestion_nppf.command_process_file(nppf_file_path)

        orbpre_file_path = os.path.dirname(os.path.abspath(__file__)) + "/inputs/" + orbpre_filename
        ingestion_orbpre.command_process_file(orbpre_file_path)

        file_path = os.path.dirname(os.path.abspath(__file__)) + "/inputs/" + filename
        ingestion_dpc.command_process_file(file_path)

        #Check the source is correctly taken
        source = self.session.query(Source).filter(Source.name == 'S2A_OPER_REP_OPDPC_L1B_L1C.EOF',
                                                    Source.generation_time == '2018-07-21T02:30:37',
                                                    Source.validity_start == '2018-07-21T00:16:10',
                                                    Source.validity_stop == '2018-07-21T02:26:47.244000').all()

        assert len(source) == 1

        #Check the validity event is not created
        validity = self.session.query(Event).join(Gauge).filter(Gauge.name == "PROCESSING_VALIDITY_L1C").all()

        assert len(validity) == 0

    def test_dpc_report_L1B_L1C_with_plan_and_previous_reports(self):
        filename_1 = "S2A_OPER_REP_OPDPC_L0_L1B.EOF"
        filename_2 = "S2A_OPER_REP_OPDPC_L1B_L1C.EOF"
        orbpre_filename = "S2A_ORBPRE.EOF"
        nppf_filename = "S2A_NPPF.EOF"

        nppf_file_path = os.path.dirname(os.path.abspath(__file__)) + "/inputs/" + nppf_filename
        ingestion_nppf.command_process_file(nppf_file_path)

        orbpre_file_path = os.path.dirname(os.path.abspath(__file__)) + "/inputs/" + orbpre_filename
        ingestion_orbpre.command_process_file(orbpre_file_path)

        file_path = os.path.dirname(os.path.abspath(__file__)) + "/inputs/" + filename_1
        ingestion_dpc.command_process_file(file_path)

        file_path = os.path.dirname(os.path.abspath(__file__)) + "/inputs/" + filename_2
        ingestion_dpc.command_process_file(file_path)

        #Check the source is correctly taken
        source = self.session.query(Source).filter(Source.name == 'S2A_OPER_REP_OPDPC_L1B_L1C.EOF',
                                                    Source.generation_time == '2018-07-21T02:30:37',
                                                    Source.validity_start == '2018-07-21 00:16:12',
                                                    Source.validity_stop == '2018-07-21T02:26:47.244000').all()

        assert len(source) == 1

        #Check that the sources is correctly taken
        sources = self.session.query(Source).filter(Source.generation_time == "2018-07-21T02:30:37",
                                                            Source.name == 'S2A_OPER_REP_OPDPC_L1B_L1C.EOF').all()

        assert len(sources) == 3

        #Check the validity event is correctly taken
        validity = self.session.query(Event).join(Gauge).filter(Gauge.name == "PROCESSING_VALIDITY_L1C").all()

        assert len(validity) == 1

        #Check the missing segment is correctly taken
        missing_event = self.session.query(EventText).join(Event,Gauge).filter(Gauge.name == "PLANNED_IMAGING_L1C_COMPLETENESS",
                                                                               Event.start == "2018-07-21 00:16:02.341000",
                                                                               EventText.name == "status",
                                                                               EventText.value == "MISSING").all()

        assert len(missing_event) == 1

        #Check completeness_event is correctly taken
        datablock_event = self.session.query(EventText).join(Event,Gauge).filter(Gauge.name == "PLANNED_IMAGING_L1C_COMPLETENESS",
                                                                                 EventText.name == "status",
                                                                                 #Incomplete due to alignment not corrected
                                                                                 EventText.value == "INCOMPLETE").all()

        assert len(datablock_event) == 1

        #Check the completeness links are correctly taken
        datablock_event_links = self.session.query(EventLink).join(Event,EventText,Gauge).filter(Gauge.name == "PLANNED_IMAGING_L1C_COMPLETENESS",
                                                                                 EventText.name == "status",
                                                                                 #Incomplete due to alignment not corrected
                                                                                 EventText.value == "INCOMPLETE").all()

        assert len(datablock_event_links) == 2

    def test_dpc_report_L1B_L1C_with_REP_PASS(self):
        filename = "S2A_OPER_REP_OPDPC_L0U_L0_2.EOF"
        filename_2 = "S2A_OPER_REP_OPDPC_L1B_L1C_2.EOF"
        rep_pass_filename = "S2A_REP_PASS_NO_GAPS.EOF"
        orbpre_filename = "S2A_ORBPRE_2.EOF"
        nppf_filename = "S2A_NPPF_2.EOF"

        nppf_file_path = os.path.dirname(os.path.abspath(__file__)) + "/inputs/" + nppf_filename
        ingestion_nppf.command_process_file(nppf_file_path)

        orbpre_file_path = os.path.dirname(os.path.abspath(__file__)) + "/inputs/" + orbpre_filename
        ingestion_orbpre.command_process_file(orbpre_file_path)

        rep_pass_file_path = os.path.dirname(os.path.abspath(__file__)) + "/inputs/" + rep_pass_filename
        ingestion_dfep.command_process_file(rep_pass_file_path)

        file_path = os.path.dirname(os.path.abspath(__file__)) + "/inputs/" + filename
        ingestion_dpc.command_process_file(file_path)

        file_path_2 = os.path.dirname(os.path.abspath(__file__)) + "/inputs/" + filename_2
        ingestion_dpc.command_process_file(file_path_2)

        #Check the received_imaging_completeness comprehends the same period as the processing_validity
        processing_validity_event = self.session.query(Event).join(Gauge).filter(Gauge.name == "PROCESSING_VALIDITY_L1C").all()

        received_imaging_completeness = self.session.query(Event).join(Gauge).filter(Gauge.name == "RECEIVED_IMAGING_L1C_COMPLETENESS").all()

        assert (processing_validity_event[0].start == received_imaging_completeness[0].start and processing_validity_event[0].stop == received_imaging_completeness[0].stop)

    def test_insert_dpc_report_aux(self):
        filename = "S2A_OPER_REP_OPDPC_SAD.EOF"
        file_path = os.path.dirname(os.path.abspath(__file__)) + "/inputs/" + filename
        ingestion_dpc.command_process_file(file_path)

        #Check a single dim_signature is correctly taken
        dim_signature = self.session.query(DimSignature).filter(DimSignature.dim_signature == "PROCESSING_S2A").all()

        assert len(dim_signature) == 1

    def test_insert_dpc_report_no_granules(self):
        filename = "S2A_OPER_REP_OPDPC_NO_GRANULES.EOF"
        file_path = os.path.dirname(os.path.abspath(__file__)) + "/inputs/" + filename
        ingestion_dpc.command_process_file(file_path)

        #Check input-output explicit_ref is correctly taken
        definite_explicit_ref_1 = self.session.query(ExplicitRefGrp).join(ExplicitRef).filter(ExplicitRef.explicit_ref == "S2A_OPER_MSI_L1B_DS_EPAE_20180721T020145_S20180721T001610_N02.06",
                                                                                              ExplicitRefGrp.name == "L1B_DS").all()

        assert len(definite_explicit_ref_1) == 1

        #Check a single granule explicit_ref is correctly taken
        definite_explicit_ref_2  = self.session.query(ExplicitRefGrp).join(ExplicitRef).filter(ExplicitRef.explicit_ref == "S2A_OPER_MSI_L1B_GR_EPAE_20180721T020145_S20180721T001612_D02_N02.06",
                                                                                              ExplicitRefGrp.name == "L1B_GR").all()

        assert len(definite_explicit_ref_2) == 0

    def test_insert_dpc_report_no_imagings(self):
        filename = "S2A_OPER_REP_OPDPC_NO_IMAGINGS.EOF"
        file_path = os.path.dirname(os.path.abspath(__file__)) + "/inputs/" + filename
        ingestion_dpc.command_process_file(file_path)

        #Check that the sources are taken
        source = self.session.query(Source).filter(Source.name == "S2A_OPER_REP_OPDPC_NO_IMAGINGS.EOF").all()

        assert len(source) == 2

        #Check the missing segment doesn't exist
        missing_event = self.session.query(EventText).join(Event,Gauge).filter(Gauge.name == "PLANNED_PROCESSING_COMPLETENESS",
                                                                               EventText.name == "status",
                                                                               EventText.value == "MISSING").all()

        assert len(missing_event) == 0

    def test_insert_dpc_report_steps_not_completed(self):
        filename = "S2A_OPER_REP_OPDPC_STEPS_NOT_COMPLETED.EOF"
        file_path = os.path.dirname(os.path.abspath(__file__)) + "/inputs/" + filename
        ingestion_dpc.command_process_file(file_path)

        #Check that the unfinished step is not taken
        definite_step_values_id = self.session.query(EventText).join(Event,Gauge).filter(Gauge.name == "STEP_INFO",
                                                                             EventText.name == "id",
                                                                             EventText.value == "step_Archive_Generic").all()

        assert len(definite_step_values_id) == 0

    def test_insert_dpc_report_mrfs_already_ingested(self):
        filename = "S2A_OPER_REP_OPDPC_MRFs.EOF"
        file_path = os.path.dirname(os.path.abspath(__file__)) + "/inputs/" + filename
        ingestion_dpc.command_process_file(file_path)

        #Check all mrf events are correctly taken
        mrfs_events_before = self.session.query(EventKey).join(Event,Gauge).filter(Gauge.name == "MRF_VALIDITY",
                                                                            Gauge.system == "EPAE").all()

        ingestion_dpc.command_process_file(file_path)

        mrfs_events_after = self.session.query(EventKey).join(Event,Gauge).filter(Gauge.name == "MRF_VALIDITY",
                                                                            Gauge.system == "EPAE").all()

        assert len(mrfs_events_before) == len(mrfs_events_after)

    def test_correct_validity(self):
        filename = "S2A_OPER_REP_OPDPC_WRONG_VALIDITY.EOF"
        file_path = os.path.dirname(os.path.abspath(__file__)) + "/inputs/" + filename
        ingestion_dpc.command_process_file(file_path)

        #Check that the sources have beem corrected
        source = self.session.query(Source).filter(Source.name == "S2A_OPER_REP_OPDPC_WRONG_VALIDITY.EOF",
                                                   Source.validity_start == "2018-07-21T02:01:38.200",
                                                   Source.validity_stop == "2018-07-21T02:02:06.244").all()

        assert len(source) == 1

    def test_gaps(self):
        filename = "S2A_OPER_REP_OPDPC_GAPS.EOF"
        orbpre_filename = "S2A_ORBPRE.EOF"
        nppf_filename = "S2A_NPPF.EOF"

        nppf_file_path = os.path.dirname(os.path.abspath(__file__)) + "/inputs/" + nppf_filename
        ingestion_nppf.command_process_file(nppf_file_path)

        orbpre_file_path = os.path.dirname(os.path.abspath(__file__)) + "/inputs/" + orbpre_filename
        ingestion_orbpre.command_process_file(orbpre_file_path)

        file_path = os.path.dirname(os.path.abspath(__file__)) + "/inputs/" + filename
        ingestion_dpc.command_process_file(file_path)

        gaps = self.session.query(Event).join(Gauge).filter(Gauge.name == "PROCESSING_GAP_L1B").all()

        assert len(gaps) == 16

    def test_insert_dpc_L0_L1B_L1C_with_plan_and_rep_pass(self):

        filename = "S2A_NPPF.EOF"
        file_path = os.path.dirname(os.path.abspath(__file__)) + "/inputs/" + filename

        returned_value = ingestion_nppf.command_process_file(file_path)

        assert returned_value[0]["status"] == eboa_engine.exit_codes["OK"]["status"]

        filename = "S2A_ORBPRE.EOF"
        file_path = os.path.dirname(os.path.abspath(__file__)) + "/inputs/" + filename

        returned_value = ingestion_orbpre.command_process_file(file_path)

        assert returned_value[0]["status"] == eboa_engine.exit_codes["OK"]["status"]

        filename = "S2A_REP_PASS_NO_GAPS.EOF"
        file_path = os.path.dirname(os.path.abspath(__file__)) + "/inputs/" + filename

        returned_value = ingestion_dfep.command_process_file(file_path)

        assert returned_value[0]["status"] == eboa_engine.exit_codes["OK"]["status"]

        filename = "S2A_OPER_REP_OPDPC_L0U_L0.EOF"
        file_path = os.path.dirname(os.path.abspath(__file__)) + "/inputs/" + filename

        returned_value = ingestion_dpc.command_process_file(file_path)

        assert returned_value[0]["status"] == eboa_engine.exit_codes["OK"]["status"]

        filename = "S2A_OPER_REP_OPDPC_L0_L1B.EOF"
        file_path = os.path.dirname(os.path.abspath(__file__)) + "/inputs/" + filename

        returned_value = ingestion_dpc.command_process_file(file_path)

        assert returned_value[0]["status"] == eboa_engine.exit_codes["OK"]["status"]

        filename = "S2A_OPER_REP_OPDPC_L1B_L1C.EOF"
        file_path = os.path.dirname(os.path.abspath(__file__)) + "/inputs/" + filename

        returned_value = ingestion_dpc.command_process_file(file_path)

        assert returned_value[0]["status"] == eboa_engine.exit_codes["OK"]["status"]

        # Check sources
        # L0
        sources = self.query_eboa.get_sources(validity_start_filters = [{"date": "2018-07-21T08:36:02.255634", "op": "=="}],
                                             validity_stop_filters = [{"date": "2018-07-21T09:08:56.195941", "op": "=="}],
                                              generation_time_filters = [{"date": "2018-07-16T11:41:50", "op": "=="}],
                                             processors = {"filter": "planning_processing_ingestion_dpc.py", "op": "like"},
                                             names = {"filter": "S2A_OPER_REP_OPDPC_L0U_L0.EOF", "op": "like"})

        assert len(sources) == 1

        sources = self.query_eboa.get_sources(validity_start_filters = [{"date": "2018-07-21T08:52:35.993268", "op": "=="}],
                                             validity_stop_filters = [{"date": "2018-07-21T08:54:12.226646", "op": "=="}],
                                              generation_time_filters = [{"date": "2018-07-21T10:40:55", "op": "=="}],
                                             processors = {"filter": "processing_received_ingestion_dpc.py", "op": "like"},
                                             names = {"filter": "S2A_OPER_REP_OPDPC_L0U_L0.EOF", "op": "like"})

        assert len(sources) == 1

        sources = self.query_eboa.get_sources(validity_start_filters = [{"date": "2018-07-21T08:52:29", "op": "=="}],
                                             validity_stop_filters = [{"date": "2018-07-21T10:43:26.741000", "op": "=="}],
                                              generation_time_filters = [{"date": "2018-07-21T10:43:34", "op": "=="}],
                                             processors = {"filter": "ingestion_dpc.py", "op": "like"},
                                             names = {"filter": "S2A_OPER_REP_OPDPC_L0U_L0.EOF", "op": "like"})

        assert len(sources) == 1

        sources = self.query_eboa.get_sources(validity_start_filters = [{"date": "2000-12-10 23:51:34", "op": "=="}],
                                             validity_stop_filters = [{"date": "9999-01-01T00:00:00", "op": "=="}],
                                              generation_time_filters = [{"date": "2018-07-21T10:43:34", "op": "=="}],
                                             processors = {"filter": "configuration_ingestion_dpc.py", "op": "like"},
                                             names = {"filter": "S2A_OPER_REP_OPDPC_L0U_L0.EOF", "op": "like"})

        assert len(sources) == 1

        # L1B
        sources = self.query_eboa.get_sources(validity_start_filters = [{"date": "2018-07-21T08:36:02.255634", "op": "=="}],
                                             validity_stop_filters = [{"date": "2018-07-21T09:08:56.195941", "op": "=="}],
                                              generation_time_filters = [{"date": "2018-07-16T11:41:50", "op": "=="}],
                                             processors = {"filter": "planning_processing_ingestion_dpc.py", "op": "like"},
                                             names = {"filter": "S2A_OPER_REP_OPDPC_L0_L1B.EOF", "op": "like"})

        assert len(sources) == 1

        sources = self.query_eboa.get_sources(validity_start_filters = [{"date": "2018-07-21T08:52:35.993268", "op": "=="}],
                                             validity_stop_filters = [{"date": "2018-07-21T08:54:12.226646", "op": "=="}],
                                              generation_time_filters = [{"date": "2018-07-21T10:40:55", "op": "=="}],
                                             processors = {"filter": "processing_received_ingestion_dpc.py", "op": "like"},
                                             names = {"filter": "S2A_OPER_REP_OPDPC_L0_L1B.EOF", "op": "like"})

        assert len(sources) == 1

        sources = self.query_eboa.get_sources(validity_start_filters = [{"date": "2018-07-21T08:52:29", "op": "=="}],
                                             validity_stop_filters = [{"date": "2018-07-21T10:55:38.778000", "op": "=="}],
                                              generation_time_filters = [{"date": "2018-07-21T10:55:47", "op": "=="}],
                                             processors = {"filter": "ingestion_dpc.py", "op": "like"},
                                             names = {"filter": "S2A_OPER_REP_OPDPC_L0_L1B.EOF", "op": "like"})

        assert len(sources) == 1

        sources = self.query_eboa.get_sources(validity_start_filters = [{"date": "1983-01-01T00:00:00", "op": "=="}],
                                             validity_stop_filters = [{"date": "9999-01-01T00:00:00", "op": "=="}],
                                              generation_time_filters = [{"date": "2018-07-21T10:55:47", "op": "=="}],
                                             processors = {"filter": "configuration_ingestion_dpc.py", "op": "like"},
                                             names = {"filter": "S2A_OPER_REP_OPDPC_L0_L1B.EOF", "op": "like"})

        assert len(sources) == 1

        # L1C
        sources = self.query_eboa.get_sources(validity_start_filters = [{"date": "2018-07-21T08:36:02.255634", "op": "=="}],
                                             validity_stop_filters = [{"date": "2018-07-21T09:08:56.195941", "op": "=="}],
                                              generation_time_filters = [{"date": "2018-07-16T11:41:50", "op": "=="}],
                                             processors = {"filter": "planning_processing_ingestion_dpc.py", "op": "like"},
                                             names = {"filter": "S2A_OPER_REP_OPDPC_L1B_L1C.EOF", "op": "like"})

        assert len(sources) == 1

        sources = self.query_eboa.get_sources(validity_start_filters = [{"date": "2018-07-21T08:52:35.993268", "op": "=="}],
                                             validity_stop_filters = [{"date": "2018-07-21T08:54:12.226646", "op": "=="}],
                                              generation_time_filters = [{"date": "2018-07-21T10:40:55", "op": "=="}],
                                             processors = {"filter": "processing_received_ingestion_dpc.py", "op": "like"},
                                             names = {"filter": "S2A_OPER_REP_OPDPC_L1B_L1C.EOF", "op": "like"})

        assert len(sources) == 1

        sources = self.query_eboa.get_sources(validity_start_filters = [{"date": "2018-07-21T08:52:29", "op": "=="}],
                                             validity_stop_filters = [{"date": "2018-07-21T11:00:38.066000", "op": "=="}],
                                              generation_time_filters = [{"date": "2018-07-21T11:00:45", "op": "=="}],
                                             processors = {"filter": "ingestion_dpc.py", "op": "like"},
                                             names = {"filter": "S2A_OPER_REP_OPDPC_L1B_L1C.EOF", "op": "like"})

        assert len(sources) == 1

        sources = self.query_eboa.get_sources(validity_start_filters = [{"date": "1983-01-01T00:00:00", "op": "=="}],
                                             validity_stop_filters = [{"date": "2100-01-01T00:00:00", "op": "=="}],
                                              generation_time_filters = [{"date": "2018-07-21T11:00:45", "op": "=="}],
                                             processors = {"filter": "configuration_ingestion_dpc.py", "op": "like"},
                                             names = {"filter": "S2A_OPER_REP_OPDPC_L1B_L1C.EOF", "op": "like"})

        assert len(sources) == 1
        
        imaging_plan = self.query_eboa.get_events(gauge_names = {"filter": "PLANNED_CUT_IMAGING", "op": "like"})[0]
        isp_validity = self.query_eboa.get_events(gauge_names = {"filter": "ISP_VALIDITY", "op": "like"})[0]

        
        # Check step infos
        step_infos = self.query_eboa.get_events(explicit_refs = {"filter": "S2A_OPER_MSI_L0__DS_MPS__20180721T103920_S20180721T085229_N02.06", "op": "like"},
                                                           gauge_names = {"filter": "STEP_INFO", "op": "like"})

        assert len(step_infos) == 14

        # Check mrf validities
        mrf_validities = self.query_eboa.get_events(sources = {"filter": "S2A_OPER_REP_OPDPC_L0U_L0.EOF", "op": "like"},
                                                           gauge_names = {"filter": "MRF_VALIDITY", "op": "like"})

        assert len(mrf_validities) == 27

        # Check processing validities
        processing_validities = self.query_eboa.get_events(explicit_refs = {"filter": "S2A_OPER_MSI_L0__DS_MPS__20180721T103920_S20180721T085229_N02.06", "op": "like"},
                                                           gauge_names = {"filter": "PROCESSING_VALIDITY", "op": "like"},
                                                           start_filters = [{"date": "2018-07-21T08:52:29", "op": "=="}],
                                                           stop_filters = [{"date": "2018-07-21T08:54:19", "op": "=="}])

        assert len(processing_validities) == 1
        processing_validity_l0 = processing_validities[0]

        # Check links with PROCESSING VALIDITY
        link_to_plan = self.query_eboa.get_event_links(event_uuid_links = {"filter": [str(processing_validity_l0.event_uuid)], "op": "in"},
                                                    event_uuids = {"filter": [str(imaging_plan.event_uuid)], "op": "in"},
                                                    link_names = {"filter": "PROCESSING_VALIDITY", "op": "like"})

        assert len(link_to_plan) == 1

        link_from_plan = self.query_eboa.get_event_links(event_uuids = {"filter": [str(processing_validity_l0.event_uuid)], "op": "in"},
                                                    event_uuid_links = {"filter": [str(imaging_plan.event_uuid)], "op": "in"},
                                                    link_names = {"filter": "PLANNED_IMAGING", "op": "like"})

        assert len(link_from_plan) == 1

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
                    "value": "16077.0",
                    "type": "double",
                    "name": "sensing_orbit"
                },
                {
                    "value": "16077.0",
                    "type": "double",
                    "name": "downlink_orbit"
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
                    "value": "MATCHED_ISP_VALIDITY",
                    "type": "text",
                    "name": "matching_reception_status"
                }
            ],
            "type": "object",
            "name": "details"
        }]

        processing_validities = self.query_eboa.get_events(explicit_refs = {"filter": "S2A_OPER_MSI_L1B_DS_MPS__20180721T104253_S20180721T085229_N02.06", "op": "like"},
                                                           gauge_names = {"filter": "PROCESSING_VALIDITY", "op": "like"},
                                             start_filters = [{"date": "2018-07-21T08:52:31", "op": "=="}],
                                             stop_filters = [{"date": "2018-07-21T08:54:14", "op": "=="}])

        assert len(processing_validities) == 1
        processing_validity_l1b = processing_validities[0]

        processing_validities = self.query_eboa.get_events(explicit_refs = {"filter": "S2A_OPER_MSI_L1C_DS_MPS__20180721T104253_S20180721T085229_N02.06", "op": "like"},
                                                           gauge_names = {"filter": "PROCESSING_VALIDITY", "op": "like"},
                                             start_filters = [{"date": "2018-07-21T08:52:31", "op": "=="}],
                                             stop_filters = [{"date": "2018-07-21T08:54:14", "op": "=="}])

        assert len(processing_validities) == 1
        processing_validity_l1c = processing_validities[0]

        assert processing_validity_l1c.get_structured_values() == [{
            "values": [
                {
                    "value": "COMPLETE",
                    "name": "status",
                    "type": "text"
                },
                {
                    "value": "L1C",
                    "name": "level",
                    "type": "text"
                },
                {
                    "value": "S2A",
                    "name": "satellite",
                    "type": "text"
                },
                {
                    "value": "16077.0",
                    "name": "sensing_orbit",
                    "type": "double"
                },
                {
                    "value": "16077.0",
                    "name": "downlink_orbit",
                    "type": "double"
                },
                {
                    "value": "MPS_",
                    "name": "processing_centre",
                    "type": "text"
                },
                {
                    "value": "MATCHED_PLANNED_IMAGING",
                    "name": "matching_plan_status",
                    "type": "text"
                },
                {
                    "value": "MATCHED_ISP_VALIDITY",
                    "name": "matching_reception_status",
                    "type": "text"
                }
            ],
            "name": "details",
            "type": "object"
        }]

        # Check ISP validity completeness
        isp_completeness = self.query_eboa.get_events(explicit_refs = {"filter": "S2A_OPER_MSI_L0__DS_MPS__20180721T103920_S20180721T085229_N02.06", "op": "like"},
                                                           gauge_names = {"filter": "ISP_VALIDITY_PROCESSING_COMPLETENESS_L0", "op": "like"},
                                                           start_filters = [{"date": "2018-07-21T08:52:29", "op": "=="}],
                                                           stop_filters = [{"date": "2018-07-21T08:54:19", "op": "=="}])

        assert len(isp_completeness) == 1
        isp_completeness_l0 = isp_completeness[0]

        isp_completeness = self.query_eboa.get_events(explicit_refs = {"filter": "S2A_OPER_MSI_L1B_DS_MPS__20180721T104253_S20180721T085229_N02.06", "op": "like"},
                                                           gauge_names = {"filter": "ISP_VALIDITY_PROCESSING_COMPLETENESS_L1B", "op": "like"},
                                             start_filters = [{"date": "2018-07-21T08:52:31", "op": "=="}],
                                             stop_filters = [{"date": "2018-07-21T08:54:14", "op": "=="}])

        assert len(isp_completeness) == 1
        isp_completeness_l1b = isp_completeness[0]

        isp_completeness = self.query_eboa.get_events(explicit_refs = {"filter": "S2A_OPER_MSI_L1C_DS_MPS__20180721T104253_S20180721T085229_N02.06", "op": "like"},
                                                           gauge_names = {"filter": "ISP_VALIDITY_PROCESSING_COMPLETENESS_L1C", "op": "like"},
                                             start_filters = [{"date": "2018-07-21T08:52:31", "op": "=="}],
                                             stop_filters = [{"date": "2018-07-21T08:54:14", "op": "=="}])

        assert len(isp_completeness) == 1
        isp_completeness_l1c = isp_completeness[0]
        
        # Check planning completeness
        planning_completeness = self.query_eboa.get_events(explicit_refs = {"filter": "S2A_OPER_MSI_L0__DS_MPS__20180721T103920_S20180721T085229_N02.06", "op": "like"},
                                                           gauge_names = {"filter": "PLANNED_IMAGING_PROCESSING_COMPLETENESS_L0", "op": "like"},
                                                           start_filters = [{"date": "2018-07-21T08:52:29", "op": "=="}],
                                                           stop_filters = [{"date": "2018-07-21T08:54:19", "op": "=="}])

        assert len(planning_completeness) == 1
        planning_completeness_l0 = planning_completeness[0]

        assert planning_completeness_l0.get_structured_values() == [{
            "name": "details",
            "values": [
                {
                    "name": "status",
                    "value": "COMPLETE",
                    "type": "text"
                },
                {
                    "name": "level",
                    "value": "L0",
                    "type": "text"
                },
                {
                    "name": "satellite",
                    "value": "S2A",
                    "type": "text"
                },
                {
                    "name": "sensing_orbit",
                    "value": "16077.0",
                    "type": "double"
                },
                {
                    "name": "downlink_orbit",
                    "value": "16077.0",
                    "type": "double"
                },
                {
                    "name": "processing_centre",
                    "value": "MPS_",
                    "type": "text"
                },
                {
                    "name": "matching_plan_status",
                    "value": "MATCHED_PLANNED_IMAGING",
                    "type": "text"
                },
                {
                    "name": "matching_reception_status",
                    "value": "MATCHED_ISP_VALIDITY",
                    "type": "text"
                }
            ],
            "type": "object"
        }]

        missing_planning_completeness = self.query_eboa.get_events(gauge_names = {"filter": "PLANNED_IMAGING_PROCESSING_COMPLETENESS_L0", "op": "like"},
                                                           start_filters = [{"date": "2018-07-21T08:36:02.255634", "op": "=="}],
                                                           stop_filters = [{"date": "2018-07-21T08:52:29", "op": "=="}])

        assert len(missing_planning_completeness) == 1

        missing_planning_completeness_statuses = [event for event in missing_planning_completeness if len([value for value in event.eventTexts if value.name == "status" and value.value == "MISSING"]) > 0]

        assert len(missing_planning_completeness_statuses) == 1

        missing_planning_completeness = self.query_eboa.get_events(gauge_names = {"filter": "PLANNED_IMAGING_PROCESSING_COMPLETENESS_L0", "op": "like"},
                                                           start_filters = [{"date": "2018-07-21T08:54:19", "op": "=="}],
                                                           stop_filters = [{"date": "2018-07-21T09:08:56.195941", "op": "=="}])

        assert len(missing_planning_completeness) == 1

        planning_completeness = self.query_eboa.get_events(explicit_refs = {"filter": "S2A_OPER_MSI_L1B_DS_MPS__20180721T104253_S20180721T085229_N02.06", "op": "like"},
                                                           gauge_names = {"filter": "PLANNED_IMAGING_PROCESSING_COMPLETENESS_L1B", "op": "like"},
                                             start_filters = [{"date": "2018-07-21T08:52:31", "op": "=="}],
                                             stop_filters = [{"date": "2018-07-21T08:54:14", "op": "=="}])

        assert len(planning_completeness) == 1
        planning_completeness_l1b = planning_completeness[0]

        missing_planning_completeness = self.query_eboa.get_events(gauge_names = {"filter": "PLANNED_IMAGING_PROCESSING_COMPLETENESS_L1B", "op": "like"},
                                                           start_filters = [{"date": "2018-07-21T08:36:02.255634", "op": "=="}],
                                                           stop_filters = [{"date": "2018-07-21T08:52:31", "op": "=="}])

        assert len(missing_planning_completeness) == 1

        missing_planning_completeness = self.query_eboa.get_events(gauge_names = {"filter": "PLANNED_IMAGING_PROCESSING_COMPLETENESS_L1B", "op": "like"},
                                                           start_filters = [{"date": "2018-07-21T08:54:14", "op": "=="}],
                                                           stop_filters = [{"date": "2018-07-21T09:08:56.195941", "op": "=="}])

        assert len(missing_planning_completeness) == 1

        planning_completeness = self.query_eboa.get_events(explicit_refs = {"filter": "S2A_OPER_MSI_L1C_DS_MPS__20180721T104253_S20180721T085229_N02.06", "op": "like"},
                                                           gauge_names = {"filter": "PLANNED_IMAGING_PROCESSING_COMPLETENESS_L1C", "op": "like"},
                                             start_filters = [{"date": "2018-07-21T08:52:31", "op": "=="}],
                                             stop_filters = [{"date": "2018-07-21T08:54:14", "op": "=="}])

        assert len(planning_completeness) == 1
        planning_completeness_l1c = planning_completeness[0]

        assert planning_completeness_l1c.get_structured_values() == [{
            "name": "details",
            "values": [
                {
                    "name": "status",
                    "value": "COMPLETE",
                    "type": "text"
                },
                {
                    "name": "level",
                    "value": "L1C",
                    "type": "text"
                },
                {
                    "name": "satellite",
                    "value": "S2A",
                    "type": "text"
                },
                {
                    "name": "sensing_orbit",
                    "value": "16077.0",
                    "type": "double"
                },
                {
                    "name": "downlink_orbit",
                    "value": "16077.0",
                    "type": "double"
                },
                {
                    "name": "processing_centre",
                    "value": "MPS_",
                    "type": "text"
                },
                {
                    "name": "matching_plan_status",
                    "value": "MATCHED_PLANNED_IMAGING",
                    "type": "text"
                },
                {
                    "name": "matching_reception_status",
                    "value": "MATCHED_ISP_VALIDITY",
                    "type": "text"
                }
            ],
            "type": "object"
        }]
        
        missing_planning_completeness = self.query_eboa.get_events(gauge_names = {"filter": "PLANNED_IMAGING_PROCESSING_COMPLETENESS_L1C", "op": "like"},
                                                           start_filters = [{"date": "2018-07-21T08:36:02.255634", "op": "=="}],
                                                           stop_filters = [{"date": "2018-07-21T08:52:31", "op": "=="}])

        assert len(missing_planning_completeness) == 1

        missing_planning_completeness_statuses = [event for event in missing_planning_completeness if len([value for value in event.eventTexts if value.name == "status" and value.value == "MISSING"]) > 0]

        assert len(missing_planning_completeness_statuses) == 1

        missing_planning_completeness = self.query_eboa.get_events(gauge_names = {"filter": "PLANNED_IMAGING_PROCESSING_COMPLETENESS_L1C", "op": "like"},
                                                           start_filters = [{"date": "2018-07-21T08:54:14", "op": "=="}],
                                                           stop_filters = [{"date": "2018-07-21T09:08:56.195941", "op": "=="}])

        assert len(missing_planning_completeness) == 1

        # Check timeliness
        timeliness = self.query_eboa.get_events(explicit_refs = {"filter": "S2A_OPER_MSI_L0__DS_MPS__20180721T103920_S20180721T085229_N02.06", "op": "like"},
                                                           gauge_names = {"filter": "TIMELINESS", "op": "like"},
                                                           start_filters = [{"date": "2018-07-21T10:39:13.800000", "op": "=="}],
                                                           stop_filters = [{"date": "2018-07-21T10:43:26.741000", "op": "=="}])

        assert len(timeliness) == 1
        timeliness_l0 = timeliness[0]

        timeliness = self.query_eboa.get_events(explicit_refs = {"filter": "S2A_OPER_MSI_L1B_DS_MPS__20180721T104253_S20180721T085229_N02.06", "op": "like"},
                                                           gauge_names = {"filter": "TIMELINESS", "op": "like"},
                                             start_filters = [{"date": "2018-07-21T10:42:47.800000", "op": "=="}],
                                             stop_filters = [{"date": "2018-07-21T10:55:38.778000", "op": "=="}])

        assert len(timeliness) == 1
        timeliness_l1b = timeliness[0]

        timeliness = self.query_eboa.get_events(explicit_refs = {"filter": "S2A_OPER_MSI_L1C_DS_MPS__20180721T104253_S20180721T085229_N02.06", "op": "like"},
                                                           gauge_names = {"filter": "TIMELINESS", "op": "like"},
                                             start_filters = [{"date": "2018-07-21T10:50:39.900000", "op": "=="}],
                                             stop_filters = [{"date": "2018-07-21T11:00:38.066000", "op": "=="}])

        assert len(timeliness) == 1
        timeliness_l1c = timeliness[0]
