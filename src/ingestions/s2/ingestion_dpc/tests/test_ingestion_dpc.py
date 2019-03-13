"""
Automated tests for the ingestion of the Station Acquisition Report files

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
import eboa.engine.engine
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
import ingestions.s2.ingestion_dpc.ingestion_dpc as ingestion
import ingestions.s2.ingestion_orbpre.ingestion_orbpre as ingestion_orbpre
import ingestions.s2.ingestion_nppf.ingestion_nppf as ingestion_nppf

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

    def test_insert_sources_dpc_report(self):
        filename = "S2A_OPER_REP_OPDPC_INSERTION.EOF"
        #filename = "S2A_OPER_REP_INSERTION.EOF"

        file_path = os.path.dirname(os.path.abspath(__file__)) + "/inputs/" + filename

        ingestion.command_process_file(file_path)

        source = self.session.query(Source).filter(Source.generation_time == "2018-07-21T02:21:01") .all()

        assert len(source) == 1

    #     #Check that the source is correctly taken
    #     source = self.session.query(Source).filter(Source.name == "S2A_OPER_REP_INSERTION.EOF",
    #                                                 Source.generation_time == "2018-07-21T02:21:01",
    #                                                 Source.validity_start == "2018-07-21T00:16:10",
    #                                                 Source.validity_stop == "2018-07-21T00:21:49").all()
    #
    #     assert len(source) == 1
    #
    #     #Check that the completeness source is correctly taken (atm receives both, validity to determine)
    #     sources_completeness = self.session.query(Source).filter(Source.generation_time == "2018-07-21T02:21:01",
    #                                                         Source.name == "S2A_OPER_REP_INSERTION.EOF").all()
    #                                                         #Source.validity_start == "2018-07-21T00:16:10",
    #                                                         #Source.validity_stop == "2018-07-21T00:21:49").all()
    #
    #     assert len(sources_completeness) == 2
    #
    # def test_insert_dim_signatures_dpc_report(self):
    #     filename = "S2A_OPER_REP_INSERTION.EOF"
    #
    #     file_path = os.path.dirname(os.path.abspath(__file__)) + "/inputs/" + filename
    #
    #     ingestion.command_process_file(file_path)
    #
    #     #Check DIM signatures are correctly taken
    #     dim_signature = self.session.query(DimSignature).filter(DimSignature.dim_signature == "PROCESSING_EPAE_S2A").all()
    #
    #     assert len(dim_signature) == 2
    #
    # def test_insert_annotations_dpc_report(self):
    #     filename = "S2A_OPER_REP_INSERTION.EOF"
    #
    #     file_path = os.path.dirname(os.path.abspath(__file__)) + "/inputs/" + filename
    #
    #     ingestion.command_process_file(file_path)
    #
    #     #Check baseline annotation is correctly taken
    #     baseline_annotation = self.session.query(Annotation).filter(Annotation.explicitRef == "S2A_OPER_MSI_L1B_DS_EPAE_20180721T020145_S20180721T001610_N02.06").all()
    #
    #     assert len(baseline_annotation) == 1
    #
    #     #Check baseline annotation configuration is correctly taken
    #     baseline_annotation_cnf = self.session.query(AnnotationCnf).join(Annotation).filter(Annotation.explicitRef == "S2A_OPER_MSI_L1B_DS_EPAE_20180721T020145_S20180721T001610_N02.06",
    #                                                                                         AnnotationCnf.name == "PRODUCTION-BASELINE",
    #                                                                                         AnnotationCnf.system == "EPAE").all()
    #
    #     assert len(baseline_annotation_cnf) == 1
    #
    #     #Check baseline annotation value is correctly taken
    #     baseline_annotation_text = self.session.query(AnnotationText).join(Annotation).filter(Annotation.explicitRef == "S2A_OPER_MSI_L1B_DS_EPAE_20180721T020145_S20180721T001610_N02.06",
    #                                                                                          AnnotationText.name == "baseline",
    #                                                                                          AnnotationCnf.system == "N02.06").all()
    #
    #     assert len(baseline_annotation_text) == 1
    #
    # def tes_insert_explicit_refs_dpc_report(self):
    #     filename = "S2A_OPER_REP_INSERTION.EOF"
    #
    #     file_path = os.path.dirname(os.path.abspath(__file__)) + "/inputs/" + filename
    #
    #     ingestion.command_process_file(file_path)
    #
    #     #Check input-output explicit_ref is correctly taken
    #     definite_explicit_ref_1 = self.session.query(ExplicitRefLink,ExplicitRefGrp).join(ExplicitRef).filter(ExplicitRef.name == "S2A_OPER_MSI_L1B_DS_EPAE_20180721T020145_S20180721T001610_N02.06",
    #                                                                                          ExplicitRefGrp.name == "L1B_DS",
    #                                                                                          ExplicitRefLink.name == "S2A_OPER_MSI_L0__DS_EPAE_20180721T015334_S20180721T001610_N02.06").all()
    #
    #     assert len(definite_explicit_ref_1) == 1
    #
    #     #Check a single granule explicit_ref is correctly taken
    #     definite_explicit_ref_2 = self.session.query(ExplicitRefGrp,ExplicitRefLink).join(ExplicitRef).filter(ExplicitRef.name == "S2A_OPER_MSI_L1B_DS_EPAE_20180721T020145_S20180721T001610_N02.06",
    #                                                                                                  ExplicitRefGrp.name == "L1B_GR",
    #                                                                                                  ExplicitRefLink.name == "S2A_OPER_MSI_L1B_GR_EPAE_20180721T020145_S20180721T001612_D02_N02.06").all()
    #
    #     assert len(definite_explicit_ref_2) == 1
    #
    #     #Check all granules explicit_refs are correctly taken
    #     explicit_ref_2_multi = self.session.query(ExplicitRefGrp).join(ExplicitRef).filter(ExplicitRef.name == "S2A_OPER_MSI_L1B_DS_EPAE_20180721T020145_S20180721T001610_N02.06",
    #                                                                                  ExplicitRefGrp.name == "L1B_GR").all()
    #
    #     assert len(explicit_ref_2_multi) == 1104
    #
    #     #Check a single mrf explicit_ref is correctly taken
    #     definite_explicit_ref_3 = self.session.query(ExplicitRefGrp,ExplicitRefLink).join(ExplicitRef).filter(ExplicitRef.name == "S2A_OPER_MSI_L1B_DS_EPAE_20180721T020145_S20180721T001610_N02.06",
    #                                                                                                  ExplicitRefGrp.name == "MISSION CONFIGURATION",
    #                                                                                                  ExplicitRefLink.name == "S2A_OPER_GIP_LREXTR_MPC__20150605T094736_V20150622T000000_21000101T000000_B00").all()
    #
    #     assert len(definite_explicit_ref_3) == 1
    #
    #     #Check all mrf explicit_refs are correctly taken
    #     explicit_ref_3_multi = self.session.query(ExplicitRefGrp).join(ExplicitRef).filter(ExplicitRef.name == "S2A_OPER_MSI_L1B_DS_EPAE_20180721T020145_S20180721T001610_N02.06",
    #                                                                                  ExplicitRefGrp.name == "MISSION CONFIGURATION").all()
    #
    #     assert len(explicit_ref_3_multi) == 131
    #
    # def test_insert_events_dpc_report(self):
    #     filename = "S2A_OPER_REP_INSERTION.EOF"
    #
    #     file_path = os.path.dirname(os.path.abspath(__file__)) + "/inputs/" + filename
    #
    #     ingestion.command_process_file(file_path)
    #
    #     #Check timeliness event is correctly taken
    #     definite_timeliness_event = self.session.query(Event).join(Gauge).filter(Gauge.name == "TIMELINESS",
    #                                                                              Gauge.system == "EPAE").all()
    #
    #     assert len(definite_timeliness_event) == 1
    #
    #     #Check all step events are correctly taken
    #     steps_events = self.session.query(Event).join(Gauge).filter(Gauge.name == "STEP-INFO",
    #                                                                           Gauge.system == "EPAE",
    #                                                                           Event.explicitRef == "S2A_OPER_MSI_L1B_DS_EPAE_20180721T020145_S20180721T001610_N02.06").all()
    #
    #     assert len(steps_events) == 18
    #
    #     #Check that the values from the steps are correctly taken
    #     definite_step_values_id = self.session.query(EventText).join(Event,Gauge).filter(Gauge.name == "STEP-INFO",
    #                                                                          EventText.name == "id",
    #                                                                          EventText.value == "step_Format_Image_L1B").all()
    #
    #     assert len(definite_step_values_id) == 1
    #
    #     definite_step_values_exec_mode = self.session.query(EventText).join(Event,Gauge).filter(Gauge.name == "STEP-INFO",
    #                                                                          EventText.name == "exec_mode",
    #                                                                          EventText.value == "nominal").all()
    #
    #     assert len(definite_step_values_exec_mode) == 1
    #
    #     #Check all mrf events are correctly taken
    #     mrfs_events = self.session.query(EventKey).join(Event,Gauge).filter(Gauge.name == "MRF-VALIDITY",
    #                                                                      Gauge.system == "EPAE").all()
    #
    #     assert len(mrfs_events) == 131
    #
    #     #Check a definite mrf event is correctly taken
    #     definite_mrf_event = self.session.query(EventKey,EventTimestamp).join(Event,Gauge).filter(Gauge.name == "MRF-VALIDITY",
    #                                                                      Gauge.system == "EPAE",
    #                                                                      EventKey.event_key == "S2A_OPER_GIP_PRDLOC_MPC__20180301T130000_V20180305T005000_21000101T000000_B00",
    #                                                                      Event.explicitRef == "S2A_OPER_GIP_PRDLOC_MPC__20180301T130000_V20180305T005000_21000101T000000_B00",
    #                                                                      Event.start == "2018-03-05T00:50:00.000",
    #                                                                      Event.stop == "2100-01-01T00:00:00.000",
    #                                                                      EventTimestamp.name == "generation_time",
    #                                                                      EventTimestamp.value == "20180301T130000").all()
    #
    #     assert len(definite_mrf_event) == 131









    #
    # def test_timeliness(self):
    #     filename = "S2A_OPER_REP_OPDPC__MPS__20190120T124914_V20190120T121524_20190120T121542.EOF"
    #
    #     file_path = os.path.dirname(os.path.abspath(__file__)) + "/inputs/" + filename
    #
    #     ingestion.command_process_file(file_path)
    #
    #     # Check that the Gauge is correctly taken
    #     gauges = self.session.query(Event).join(Gauge).filter(Gauge.name == "TIMELINESS",
    #                                                           Gauge.system == "MPS_").all()
    #
    #     assert len(gauges) == 2
    #
    #     #Check that the completed steps are all taken
    #     steps = self.session.query(Event).join(Gauge).filter(Gauge.name == "STEP-INFO").all()
    #
    #     assert len(steps) == 84
    #
    #     #Check that the values from the steps are correctly taken
    #     definite_step_values = self.session.query(EventText).join(Event,Gauge).filter(Gauge.name == "STEP-INFO",
    #                                                                          EventText.name == "id",
    #                                                                          EventText.value == "Gen_Ortho_Toa_005").all()
    #
    #     assert len(definite_step_values) == 1
    #
    #     # Check that the Event dates correspond to the first and last steps ones
    #     events_with_dates = self.session.query(Event).filter(Event.explicit_reference == "S2A_OPER_MSI_L1C_DS_MPS__20190120T123339_S20190120T121524_N02.07",
    #                                                     Event.start == "2019-01-20T12:38:50.400",
    #                                                     Event.stop == "2019-01-20T12:49:06.871").all()
    #
    #     assert len(events_with_dates) == 2
    #
    # def test_configuration(self):
    #     filename = "S2A_OPER_REP_OPDPC__MPS__20190120T124914_V20190120T121524_20190120T121542.EOF"
    #
    #     file_path = os.path.dirname(os.path.abspath(__file__)) + "/inputs/" + filename
    #
    #     ingestion.command_process_file(file_path)
    #
    #     #Check mrfs are all taken as explicit_refs
    #     mrfs_explicit_refs = self.session.query(ExplicitRef).filter(ExplicitRef.name = "S2A_OPER_MSI_L1C_DS_MPS__20190120T123339_S20190120T121524_N02.07",
    #                                                                        ExplicitRef.group = "MISSION CONFIGURATION").all()
    #
    #     assert len(mrfs_explicit_refs) == 268
    #
    #     #Check that a definite mrf is taken as explicit_ref
    #     definite_mrf_explicit_ref = self.session.query(ExplicitRef).filter(ExplicitRef.links[0].name = "S2A_OPER_GIP_VIEDIR_MPC__20151117T131050_V20150703T000000_21000101T000000_B07").all()
    #
    #     assert len(definite_mrf_explicit_ref) == 2
    #
    #     #Check that the MRF are all taken as events
    #     mrfs_events = self.session.query(Gauge).filter(Gauge.name == "MRF-VALIDITY").all()
    #
    #     assert len(mrfs_events) == 134
    #
    #     #Check that a definite MRF is taken as event
    #     definite_mrf_event = self.session.query(Gauge).join(Gauge,Event).filter(Gauge.name == "MRF-VALIDITY",
    #                                                                             Event.key == "S2A_OPER_GIP_VIEDIR_MPC__20151117T131050_V20150703T000000_21000101T000000_B07",
    #                                                                             Event.explicit_reference == "S2A_OPER_GIP_VIEDIR_MPC__20151117T131050_V20150703T000000_21000101T000000_B07",
    #                                                                             Event.start == "2015-07-03T00:00:00.000",
    #                                                                             Event.stop == "2100-01-01T00:00:00.000").all()
    #
    #     assert len(definite_mrf_event) == 1
    #
    #     #Check that a definite MRF values are correctly taken
    #     definite_mrf_event_values = self.session.query(EventTimestamp).join(EventTimestamp,Event).filter(Event.key == "S2A_OPER_GIP_VIEDIR_MPC__20151117T131050_V20150703T000000_21000101T000000_B07",
    #                                                                             Event.explicit_reference == "S2A_OPER_GIP_VIEDIR_MPC__20151117T131050_V20150703T000000_21000101T000000_B07",
    #                                                                             Event.start == "2015-07-03T00:00:00.000",
    #                                                                             Event.stop == "2100-01-01T00:00:00.000",
    #                                                                             EventTimestamp == "20151117T131050").all()
    #
    #     assert len(definite_mrf_event_values) == 1
    #
    # def test_relationships_gr(self):
    #     filename = "S2A_OPER_REP_OPDPC__MPS__20190120T124108_V20190120T122649_20190120T122711.EOF"
    #
    #     file_path = os.path.dirname(os.path.abspath(__file__)) + "/inputs/" + filename
    #
    #     ingestion.command_process_file(file_path)
    #
    #     #Check TLs are all taken as explicit_refs
    #     GR_explicit_refs = self.query(ExplicitRef).filter(ExplicitRef.group == "_L1B_GR",
    #                                                       ExplicitRef.name == "S2A_OPER_MSI_L1B_DS_MPS__20190120T123341_S20190120T122649_N02.07").all()
    #
    #     assert len(TL_explicit_refs) == 11
    #
    #     #Check a definite TL is taken as explicit_ref
    #     Definite_TL = self.query(ExplicitRef).filter(ExplicitRef.group == "L1B_GR",
    #                                                  ExplicitRef.links[0].name == "S2A_OPER_MSI_L1B_DS_MPS__20190120T123341_S20190120T122649_N02.07",
    #                                                  ExplicitRef.name == "S2A_OPER_MSI_L1B_GR_MPS__20190120T123341_S20190120T122655_D08_N02.07").all()
    #
    #     assert len(Definite_TL) == 1
    #
    # def test_relationships_tl_and_tc(self):
    #     filename = "S2A_OPER_REP_OPDPC__MPS__20190120T124914_V20190120T121524_20190120T121542.EOF"
    #
    #     file_path = os.path.dirname(os.path.abspath(__file__)) + "/inputs/" + filename
    #
    #     ingestion.command_process_file(file_path)
    #
    #     #Check TLs are all taken as explicit_refs
    #     TL_explicit_refs = self.query(ExplicitRef).filter(ExplicitRef.group == "L1C_TL",
    #                                                       ExplicitRef.name == "S2A_OPER_MSI_L1C_DS_MPS__20190120T123339_S20190120T121524_N02.07").all()
    #
    #     assert len(TL_explicit_refs) == 11
    #
    #     #Check a definite TL is taken as explicit_ref
    #     Definite_TL = self.query(ExplicitRef).filter(ExplicitRef.group == "L1C_TL",
    #                                                  ExplicitRef.links[0].name == "S2A_OPER_MSI_L1C_TL_MPS__20190120T123339_A018696_T28VFH_N02.07",
    #                                                  ExplicitRef.name == "S2A_OPER_MSI_L1C_DS_MPS__20190120T123339_S20190120T121524_N02.07").all()
    #
    #     assert len(Definite_TL) == 1
    #
    #     #Check TCs are all taken as explicit_refs
    #     TC_explicit_refs = self.query(ExplicitRef).filter(ExplicitRef.group == "L1C_TC",
    #                                                       ExplicitRef.name == "S2A_OPER_MSI_L1C_DS_MPS__20190120T123339_S20190120T121524_N02.07").all()
    #
    #     assert len(TC_explicit_refs) == 11
    #
    #     #Check a definite TL is taken as explicit_ref
    #     Definite_TC = self.query(ExplicitRef).filter(ExplicitRef.group == "L1C_TC",
    #                                                  ExplicitRef.links[0].name == "S2A_OPER_MSI_L1C_TC_MPS__20190120T123339_A018696_T28VFJ_N02.07",
    #                                                  ExplicitRef.name == "S2A_OPER_MSI_L1C_DS_MPS__20190120T123339_S20190120T121524_N02.07").all()
    #
    #     assert len(Definite_TC) == 1
