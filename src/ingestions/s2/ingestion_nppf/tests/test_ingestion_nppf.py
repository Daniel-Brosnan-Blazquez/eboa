"""
Automated tests for the ingestion of the NPPF files

Written by DEIMOS Space S.L. (dibb)

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

# Import ingestion
import ingestions.s2.ingestion_nppf.ingestion_nppf as ingestion

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

    def test_insert_nppf(self):
        filename = "S2A_NPPF_CONTAINING_ALL_DATA_TO_BE_PROCESS.EOF"
        file_path = os.path.dirname(os.path.abspath(__file__)) + "/inputs/" + filename

        ingestion.command_process_file(file_path)

        # Check that events before the queue deletion are not inserted
        events_before_validity_period = self.session.query(Event).filter(Event.stop < "2018-07-20T13:40:00.000").all()

        assert len(events_before_validity_period) == 0

        # Check number of events generated
        events = self.session.query(Event).all()

        assert len(events) == 38

        # Check that the validity period of the input has taken into consideration the deletion queue
        source = self.session.query(Source).filter(Source.validity_start == "2018-07-20T13:40:00.000",
                                                                                 Source.validity_stop == "2018-08-06T14:00:00").all()

        assert len(source) == 1

        # Check XBAND playback operations
        xband_operations = self.session.query(Event).join(Gauge).filter(Gauge.name == "PLANNED_PLAYBACK_MEAN_XBAND").all()

        assert len(xband_operations) == 2

        # Check specific XBAND playback operations
        specific_xband_operation1 = self.session.query(Event).join(Gauge).filter(Gauge.name == "PLANNED_PLAYBACK_MEAN_XBAND",
                                                                                 Event.start == "2018-07-20T14:02:08.695",
                                                                                 Event.stop == "2018-07-20T14:14:57.748").all()

        assert len(specific_xband_operation1) == 1

        specific_xband_operation2 = self.session.query(Event).join(Gauge).filter(Gauge.name == "PLANNED_PLAYBACK_MEAN_XBAND",
                                                                                 Event.start == "2018-07-20T15:41:28.723",
                                                                                 Event.stop == "2018-07-20T15:54:14.614").all()

        assert len(specific_xband_operation2) == 1

        # Check OCP playback operations
        ocp_operations = self.session.query(Event).join(Gauge).filter(Gauge.name == "PLANNED_PLAYBACK_MEAN_OCP").all()

        assert len(ocp_operations) == 1

        # Check specific OCP playback operations
        specific_ocp_operation1 = self.session.query(Event).join(Gauge).filter(Gauge.name == "PLANNED_PLAYBACK_MEAN_OCP",
                                                                                 Event.start == "2018-07-20T23:23:09.000",
                                                                                 Event.stop == "2018-07-20T23:46:56.000").all()

        assert len(specific_ocp_operation1) == 1

        # Check nominal playback operations
        playback_nominal_operations = self.session.query(Event).join(Gauge).filter(Gauge.name == "PLANNED_PLAYBACK_TYPE_NOMINAL").all()

        assert len(playback_nominal_operations) == 1

        # Check specific nominal playback operations
        specific_playback_nominal_operation1 = self.session.query(Event).join(Gauge).filter(Gauge.name == "PLANNED_PLAYBACK_TYPE_NOMINAL",
                                                                                           Event.start == "2018-07-20T14:02:33.695",
                                                                                           Event.stop == "2018-07-20T14:14:14.748").all()

        assert len(specific_playback_nominal_operation1) == 1

        # Check nrt playback operations
        playback_nrt_operations = self.session.query(Event).join(Gauge).filter(Gauge.name == "PLANNED_PLAYBACK_TYPE_NRT").all()

        assert len(playback_nrt_operations) == 1

        # Check specific nrt playback operations
        specific_playback_nrt_operation1 = self.session.query(Event).join(Gauge).filter(Gauge.name == "PLANNED_PLAYBACK_TYPE_NRT",
                                                                                           Event.start == "2018-07-20T23:30:04.000",
                                                                                           Event.stop == "2018-07-20T23:45:42.000").all()

        assert len(specific_playback_nrt_operation1) == 1

        # Check rt playback operations
        playback_rt_operations = self.session.query(Event).join(Gauge).filter(Gauge.name == "PLANNED_PLAYBACK_TYPE_RT").all()

        assert len(playback_rt_operations) == 1

        # Check specific rt playback operations
        specific_playback_rt_operation1 = self.session.query(Event).join(Gauge).filter(Gauge.name == "PLANNED_PLAYBACK_TYPE_RT",
                                                                                           Event.start == "2018-07-20T15:41:53.723",
                                                                                           Event.stop == "2018-07-20T15:53:31.614").all()

        assert len(specific_playback_rt_operation1) == 1

        # Check HKTM_SAD playback operations
        playback_hktm_sad_operations = self.session.query(Event).join(Gauge).filter(Gauge.name == "PLANNED_PLAYBACK_TYPE_HKTM_SAD").all()

        assert len(playback_hktm_sad_operations) == 2

        # Check specific HKTM_SAD playback operations
        specific_playback_hktm_sad_operation1 = self.session.query(Event).join(Gauge).filter(Gauge.name == "PLANNED_PLAYBACK_TYPE_HKTM_SAD",
                                                                                           Event.start == "2018-07-20T14:14:25.748",
                                                                                           Event.stop == "2018-07-20T14:14:25.748").all()

        assert len(specific_playback_hktm_sad_operation1) == 1

        specific_playback_hktm_sad_operation2 = self.session.query(Event).join(Gauge).filter(Gauge.name == "PLANNED_PLAYBACK_TYPE_HKTM_SAD",
                                                                                           Event.start == "2018-07-20T15:53:42.614",
                                                                                           Event.stop == "2018-07-20T15:53:42.614").all()

        assert len(specific_playback_hktm_sad_operation2) == 1

        # Check SAD playback operations
        playback_sad_operations = self.session.query(Event).join(Gauge).filter(Gauge.name == "PLANNED_PLAYBACK_TYPE_SAD").all()

        assert len(playback_sad_operations) == 1

        # Check specific HKTM_SAD playback operations
        specific_playback_sad_operation1 = self.session.query(Event).join(Gauge).filter(Gauge.name == "PLANNED_PLAYBACK_TYPE_SAD",
                                                                                           Event.start == "2018-07-20T23:45:53",
                                                                                           Event.stop == "2018-07-20T23:45:53").all()

        assert len(specific_playback_sad_operation1) == 1

        # Check link between playback operations
        link_nominal_playback = self.session.query(EventLink).filter(EventLink.name == "PLANNED_PLAYBACK_TYPE",
                                                              EventLink.event_uuid_link == specific_playback_nominal_operation1[0].event_uuid,
                                                              EventLink.event_uuid == specific_xband_operation1[0].event_uuid).all()

        assert len(link_nominal_playback) == 1

        back_link_nominal_playback = self.session.query(EventLink).filter(EventLink.name == "PLANNED_PLAYBACK_MEAN",
                                                              EventLink.event_uuid == specific_playback_nominal_operation1[0].event_uuid,
                                                              EventLink.event_uuid_link == specific_xband_operation1[0].event_uuid).all()

        assert len(back_link_nominal_playback) == 1

        link_hktm_sad_playback1 = self.session.query(EventLink).filter(EventLink.name == "PLANNED_PLAYBACK_TYPE",
                                                              EventLink.event_uuid_link == specific_playback_hktm_sad_operation1[0].event_uuid,
                                                              EventLink.event_uuid == specific_xband_operation1[0].event_uuid).all()

        assert len(link_hktm_sad_playback1) == 1

        back_link_hktm_sad_playback1 = self.session.query(EventLink).filter(EventLink.name == "PLANNED_PLAYBACK_MEAN",
                                                              EventLink.event_uuid == specific_playback_hktm_sad_operation1[0].event_uuid,
                                                              EventLink.event_uuid_link == specific_xband_operation1[0].event_uuid).all()

        assert len(back_link_hktm_sad_playback1) == 1

        link_rt_playback = self.session.query(EventLink).filter(EventLink.name == "PLANNED_PLAYBACK_TYPE",
                                                              EventLink.event_uuid_link == specific_playback_rt_operation1[0].event_uuid,
                                                              EventLink.event_uuid == specific_xband_operation2[0].event_uuid).all()

        assert len(link_rt_playback) == 1

        back_link_rt_playback = self.session.query(EventLink).filter(EventLink.name == "PLANNED_PLAYBACK_MEAN",
                                                              EventLink.event_uuid == specific_playback_rt_operation1[0].event_uuid,
                                                              EventLink.event_uuid_link == specific_xband_operation2[0].event_uuid).all()

        assert len(back_link_rt_playback) == 1

        link_hktm_sad_playback2 = self.session.query(EventLink).filter(EventLink.name == "PLANNED_PLAYBACK_TYPE",
                                                              EventLink.event_uuid_link == specific_playback_hktm_sad_operation2[0].event_uuid,
                                                              EventLink.event_uuid == specific_xband_operation2[0].event_uuid).all()

        assert len(link_hktm_sad_playback2) == 1

        back_link_hktm_sad_playback2 = self.session.query(EventLink).filter(EventLink.name == "PLANNED_PLAYBACK_MEAN",
                                                              EventLink.event_uuid == specific_playback_hktm_sad_operation2[0].event_uuid,
                                                              EventLink.event_uuid_link == specific_xband_operation2[0].event_uuid).all()

        assert len(back_link_hktm_sad_playback2) == 1

        link_nrt_playback = self.session.query(EventLink).filter(EventLink.name == "PLANNED_PLAYBACK_TYPE",
                                                              EventLink.event_uuid_link == specific_playback_nrt_operation1[0].event_uuid,
                                                              EventLink.event_uuid == specific_ocp_operation1[0].event_uuid).all()

        assert len(link_nrt_playback) == 1

        back_link_nrt_playback = self.session.query(EventLink).filter(EventLink.name == "PLANNED_PLAYBACK_MEAN",
                                                              EventLink.event_uuid == specific_playback_nrt_operation1[0].event_uuid,
                                                              EventLink.event_uuid_link == specific_ocp_operation1[0].event_uuid).all()

        assert len(back_link_nrt_playback) == 1

        link_sad_playback1 = self.session.query(EventLink).filter(EventLink.name == "PLANNED_PLAYBACK_TYPE",
                                                              EventLink.event_uuid_link == specific_playback_sad_operation1[0].event_uuid,
                                                              EventLink.event_uuid == specific_ocp_operation1[0].event_uuid).all()

        assert len(link_sad_playback1) == 1

        back_link_sad_playback1 = self.session.query(EventLink).filter(EventLink.name == "PLANNED_PLAYBACK_MEAN",
                                                              EventLink.event_uuid == specific_playback_sad_operation1[0].event_uuid,
                                                              EventLink.event_uuid_link == specific_ocp_operation1[0].event_uuid).all()

        assert len(back_link_sad_playback1) == 1

        # Check record nominal operations
        record_nominal_operations = self.session.query(Event).join(Gauge).filter(Gauge.name == "PLANNED_RECORD_NOMINAL").all()

        assert len(record_nominal_operations) == 6

        # Check specific record nominal operations
        specific_record_nominal_operation1 = self.session.query(Event).join(Gauge).filter(Gauge.name == "PLANNED_RECORD_NOMINAL",
                                                                                           Event.start == "2018-07-20T14:07:25.734",
                                                                                           Event.stop == "2018-07-20T14:08:57.718").all()

        assert len(specific_record_nominal_operation1) == 1

        specific_record_nominal_operation2 = self.session.query(Event).join(Gauge).filter(Gauge.name == "PLANNED_RECORD_NOMINAL",
                                                                                           Event.start == "2018-07-20T14:27:43.768",
                                                                                           Event.stop == "2018-07-20T14:47:59.686").all()

        assert len(specific_record_nominal_operation2) == 1

        specific_record_nominal_operation3 = self.session.query(Event).join(Gauge).filter(Gauge.name == "PLANNED_RECORD_NOMINAL",
                                                                                           Event.start == "2018-07-20T15:48:02.082",
                                                                                           Event.stop == "2018-07-20T15:48:29.122").all()

        assert len(specific_record_nominal_operation3) == 1

        specific_record_nominal_operation4 = self.session.query(Event).join(Gauge).filter(Gauge.name == "PLANNED_RECORD_NOMINAL",
                                                                                           Event.start == "2018-07-20T15:48:55.739",
                                                                                           Event.stop == "2018-07-20T16:04:24.434").all()

        assert len(specific_record_nominal_operation4) == 1

        specific_record_nominal_operation5 = self.session.query(Event).join(Gauge).filter(Gauge.name == "PLANNED_RECORD_NOMINAL",
                                                                                           Event.start == "2018-07-20T16:05:02.319",
                                                                                           Event.stop == "2018-07-20T16:11:19.335").all()

        assert len(specific_record_nominal_operation4) == 1

        specific_record_nominal_operation6 = self.session.query(Event).join(Gauge).filter(Gauge.name == "PLANNED_RECORD_NOMINAL",
                                                                                           Event.start == "2018-07-20T17:28:49.581",
                                                                                           Event.stop == "2018-07-20T17:48:45.706").all()

        assert len(specific_record_nominal_operation4) == 1

        # Check record nrt operations
        record_nrt_operations = self.session.query(Event).join(Gauge).filter(Gauge.name == "PLANNED_RECORD_NRT").all()

        assert len(record_nrt_operations) == 1

        # Check specific record nrt operations
        specific_record_nrt_operation1 = self.session.query(Event).join(Gauge).filter(Gauge.name == "PLANNED_RECORD_NRT",
                                                                                      Event.start == "2018-07-20T14:09:55.673",
                                                                                      Event.stop == "2018-07-20T14:16:09.081").all()

        assert len(specific_record_nrt_operation1) == 1

        # Check nominal imaging operations
        imaging_nominal_operations = self.session.query(Event).join(Gauge).filter(Gauge.name.like("%IMAGING_NOMINAL")).all()

        assert len(imaging_nominal_operations) == 2

        # Check specific nominal imaging operations
        specific_cut_imaging_nominal_operation1 = self.session.query(Event).join(Gauge).filter(Gauge.name.like("PLANNED_CUT_IMAGING_NOMINAL"),
                                                                                           Event.start == "2018-07-20T14:07:27.734",
                                                                                           Event.stop == "2018-07-20T14:08:53.218").all()

        assert len(specific_cut_imaging_nominal_operation1) == 1

        specific_imaging_nominal_operation1 = self.session.query(Event).join(Gauge).filter(Gauge.name.like("PLANNED_IMAGING_NOMINAL"),
                                                                                           Event.start == "2018-07-20T14:07:27.734",
                                                                                           Event.stop == "2018-07-20T14:08:53.218").all()

        assert len(specific_imaging_nominal_operation1) == 1

        # Check SUN_CAL imaging operations
        imaging_suncal_operations = self.session.query(Event).join(Gauge).filter(Gauge.name.like("%IMAGING_SUN_CAL")).all()

        assert len(imaging_suncal_operations) == 2

        # Check specific suncal imaging operations
        specific_cut_imaging_suncal_operation1 = self.session.query(Event).join(Gauge).filter(Gauge.name.like("PLANNED_CUT_IMAGING_SUN_CAL"),
                                                                                          Event.start == "2018-07-20T14:09:57.673",
                                                                                          Event.stop == "2018-07-20T14:09:57.673").all()

        assert len(specific_cut_imaging_suncal_operation1) == 1

        specific_imaging_suncal_operation1 = self.session.query(Event).join(Gauge).filter(Gauge.name.like("PLANNED_IMAGING_SUN_CAL"),
                                                                                          Event.start == "2018-07-20T14:09:57.673",
                                                                                          Event.stop == "2018-07-20T14:09:57.673").all()

        assert len(specific_imaging_suncal_operation1) == 1

        # Check dark_cal_csm_open imaging operations
        imaging_dark_cal_csm_open_operations = self.session.query(Event).join(Gauge).filter(Gauge.name.like("%IMAGING_DARK_CAL_CSM_OPEN")).all()

        assert len(imaging_dark_cal_csm_open_operations) == 2

        # Check specific dark open imaging operations
        specific_cut_imaging_dark_open_operation1 = self.session.query(Event).join(Gauge).filter(Gauge.name.like("PLANNED_CUT_IMAGING_DARK_CAL_CSM_OPEN"),
                                                                                          Event.start == "2018-07-20T14:27:45.768",
                                                                                          Event.stop == "2018-07-20T14:47:55.186").all()

        assert len(specific_cut_imaging_dark_open_operation1) == 1

        specific_imaging_dark_open_operation1 = self.session.query(Event).join(Gauge).filter(Gauge.name.like("PLANNED_IMAGING_DARK_CAL_CSM_OPEN"),
                                                                                          Event.start == "2018-07-20T14:27:45.768",
                                                                                          Event.stop == "2018-07-20T14:47:55.186").all()

        assert len(specific_imaging_dark_open_operation1) == 1

        # Check dark_cal_csm_close imaging operations
        imaging_dark_cal_csm_close_operations = self.session.query(Event).join(Gauge).filter(Gauge.name.like("%IMAGING_DARK_CAL_CSM_CLOSE")).all()

        assert len(imaging_dark_cal_csm_close_operations) == 2

        # Check specific dark close imaging operations
        specific_cut_imaging_dark_close_operation1 = self.session.query(Event).join(Gauge).filter(Gauge.name.like("PLANNED_CUT_IMAGING_DARK_CAL_CSM_CLOSE"),
                                                                                          Event.start == "2018-07-20T15:48:04.082",
                                                                                          Event.stop == "2018-07-20T15:48:24.622").all()

        assert len(specific_cut_imaging_dark_close_operation1) == 1

        specific_imaging_dark_close_operation1 = self.session.query(Event).join(Gauge).filter(Gauge.name.like("PLANNED_IMAGING_DARK_CAL_CSM_CLOSE"),
                                                                                          Event.start == "2018-07-20T15:48:04.082",
                                                                                          Event.stop == "2018-07-20T15:48:24.622").all()

        assert len(specific_imaging_dark_close_operation1) == 1

        # Check vicarious_cal imaging operations
        imaging_vicarious_cal_operations = self.session.query(Event).join(Gauge).filter(Gauge.name.like("%IMAGING_VICARIOUS_CAL")).all()

        assert len(imaging_vicarious_cal_operations) == 2

        # Check specific vicarious_cal imaging operations
        specific_cut_imaging_vicarious_cal_operation1 = self.session.query(Event).join(Gauge).filter(Gauge.name.like("PLANNED_CUT_IMAGING_VICARIOUS_CAL"),
                                                                                           Event.start == "2018-07-20T15:48:57.739",
                                                                                           Event.stop == "2018-07-20T16:04:19.934").all()

        assert len(specific_cut_imaging_vicarious_cal_operation1) == 1

        specific_imaging_vicarious_cal_operation1 = self.session.query(Event).join(Gauge).filter(Gauge.name.like("PLANNED_IMAGING_VICARIOUS_CAL"),
                                                                                           Event.start == "2018-07-20T15:48:57.739",
                                                                                           Event.stop == "2018-07-20T16:04:19.934").all()

        assert len(specific_imaging_vicarious_cal_operation1) == 1

        # Check raw imaging operations
        imaging_raw_operations = self.session.query(Event).join(Gauge).filter(Gauge.name.like("%IMAGING_RAW")).all()

        assert len(imaging_raw_operations) == 2

        # Check specific raw imaging operations
        specific_cut_imaging_raw_operation1 = self.session.query(Event).join(Gauge).filter(Gauge.name.like("PLANNED_CUT_IMAGING_RAW"),
                                                                                           Event.start == "2018-07-20T16:05:04.319",
                                                                                           Event.stop == "2018-07-20T16:11:14.835").all()

        assert len(specific_cut_imaging_raw_operation1) == 1

        specific_imaging_raw_operation1 = self.session.query(Event).join(Gauge).filter(Gauge.name.like("PLANNED_IMAGING_RAW"),
                                                                                           Event.start == "2018-07-20T16:05:04.319",
                                                                                           Event.stop == "2018-07-20T16:11:14.835").all()

        assert len(specific_imaging_raw_operation1) == 1

        # Check test imaging operations
        imaging_test_operations = self.session.query(Event).join(Gauge).filter(Gauge.name.like("%IMAGING_TEST")).all()

        assert len(imaging_test_operations) == 2

        # Check specific test imaging operations
        specific_cut_imaging_test_operation1 = self.session.query(Event).join(Gauge).filter(Gauge.name.like("PLANNED_CUT_IMAGING_TEST"),
                                                                                           Event.start == "2018-07-20T17:28:51.581",
                                                                                           Event.stop == "2018-07-20T17:48:41.206").all()

        assert len(specific_cut_imaging_test_operation1) == 1

        specific_imaging_test_operation1 = self.session.query(Event).join(Gauge).filter(Gauge.name.like("PLANNED_IMAGING_TEST"),
                                                                                           Event.start == "2018-07-20T17:28:51.581",
                                                                                           Event.stop == "2018-07-20T17:48:41.206").all()

        assert len(specific_imaging_test_operation1) == 1

        # Check link between imaging operations
        link_imaging_nominal_record = self.session.query(EventLink).filter(EventLink.name == "PLANNED_IMAGING_OPERATION",
                                                                           EventLink.event_uuid_link == specific_cut_imaging_nominal_operation1[0].event_uuid,
                                                                           EventLink.event_uuid == specific_record_nominal_operation1[0].event_uuid).all()

        assert len(link_imaging_nominal_record) == 1

        back_link_imaging_nominal_record = self.session.query(EventLink).filter(EventLink.name == "PLANNED_RECORD_OPERATION",
                                                                           EventLink.event_uuid == specific_cut_imaging_nominal_operation1[0].event_uuid,
                                                                           EventLink.event_uuid_link == specific_record_nominal_operation1[0].event_uuid).all()

        assert len(back_link_imaging_nominal_record) == 1

        link_imaging_suncal_record = self.session.query(EventLink).filter(EventLink.name == "PLANNED_IMAGING_OPERATION",
                                                                           EventLink.event_uuid_link == specific_cut_imaging_suncal_operation1[0].event_uuid,
                                                                           EventLink.event_uuid == specific_record_nrt_operation1[0].event_uuid).all()

        assert len(link_imaging_nominal_record) == 1

        back_link_imaging_suncal_record = self.session.query(EventLink).filter(EventLink.name == "PLANNED_RECORD_OPERATION",
                                                                           EventLink.event_uuid == specific_cut_imaging_suncal_operation1[0].event_uuid,
                                                                           EventLink.event_uuid_link == specific_record_nrt_operation1[0].event_uuid).all()

        assert len(back_link_imaging_suncal_record) == 1

        link_imaging_dark_open_record = self.session.query(EventLink).filter(EventLink.name == "PLANNED_IMAGING_OPERATION",
                                                                           EventLink.event_uuid_link == specific_cut_imaging_dark_open_operation1[0].event_uuid,
                                                                           EventLink.event_uuid == specific_record_nominal_operation2[0].event_uuid).all()

        assert len(link_imaging_dark_open_record) == 1

        back_link_imaging_dark_open_record = self.session.query(EventLink).filter(EventLink.name == "PLANNED_RECORD_OPERATION",
                                                                           EventLink.event_uuid == specific_cut_imaging_dark_open_operation1[0].event_uuid,
                                                                           EventLink.event_uuid_link == specific_record_nominal_operation2[0].event_uuid).all()

        assert len(back_link_imaging_dark_open_record) == 1

        link_imaging_dark_close_record = self.session.query(EventLink).filter(EventLink.name == "PLANNED_IMAGING_OPERATION",
                                                                           EventLink.event_uuid_link == specific_cut_imaging_dark_close_operation1[0].event_uuid,
                                                                           EventLink.event_uuid == specific_record_nominal_operation3[0].event_uuid).all()

        assert len(link_imaging_dark_close_record) == 1

        back_link_imaging_dark_close_record = self.session.query(EventLink).filter(EventLink.name == "PLANNED_RECORD_OPERATION",
                                                                           EventLink.event_uuid == specific_cut_imaging_dark_close_operation1[0].event_uuid,
                                                                           EventLink.event_uuid_link == specific_record_nominal_operation3[0].event_uuid).all()

        assert len(back_link_imaging_dark_close_record) == 1

        link_imaging_vicarious_cal_record = self.session.query(EventLink).filter(EventLink.name == "PLANNED_IMAGING_OPERATION",
                                                                           EventLink.event_uuid_link == specific_cut_imaging_vicarious_cal_operation1[0].event_uuid,
                                                                           EventLink.event_uuid == specific_record_nominal_operation4[0].event_uuid).all()

        assert len(link_imaging_vicarious_cal_record) == 1

        back_link_imaging_vicarious_cal_record = self.session.query(EventLink).filter(EventLink.name == "PLANNED_RECORD_OPERATION",
                                                                           EventLink.event_uuid == specific_cut_imaging_vicarious_cal_operation1[0].event_uuid,
                                                                           EventLink.event_uuid_link == specific_record_nominal_operation4[0].event_uuid).all()

        assert len(back_link_imaging_vicarious_cal_record) == 1

        link_imaging_raw_record = self.session.query(EventLink).filter(EventLink.name == "PLANNED_IMAGING_OPERATION",
                                                                           EventLink.event_uuid_link == specific_cut_imaging_raw_operation1[0].event_uuid,
                                                                           EventLink.event_uuid == specific_record_nominal_operation5[0].event_uuid).all()

        assert len(link_imaging_raw_record) == 1

        back_link_imaging_raw_record = self.session.query(EventLink).filter(EventLink.name == "PLANNED_RECORD_OPERATION",
                                                                           EventLink.event_uuid == specific_cut_imaging_raw_operation1[0].event_uuid,
                                                                           EventLink.event_uuid_link == specific_record_nominal_operation5[0].event_uuid).all()

        assert len(back_link_imaging_raw_record) == 1

        link_imaging_test_record = self.session.query(EventLink).filter(EventLink.name == "PLANNED_IMAGING_OPERATION",
                                                                           EventLink.event_uuid_link == specific_cut_imaging_test_operation1[0].event_uuid,
                                                                           EventLink.event_uuid == specific_record_nominal_operation6[0].event_uuid).all()

        assert len(link_imaging_test_record) == 1

        back_link_imaging_test_record = self.session.query(EventLink).filter(EventLink.name == "PLANNED_RECORD_OPERATION",
                                                                           EventLink.event_uuid == specific_cut_imaging_test_operation1[0].event_uuid,
                                                                           EventLink.event_uuid_link == specific_record_nominal_operation6[0].event_uuid).all()

        assert len(back_link_imaging_test_record) == 1

        # Check idle operations
        idle_operations = self.session.query(Event).join(Gauge).filter(Gauge.name == "PLANNED_IDLE").all()

        assert len(idle_operations) == 8

        # Check specific idle operations
        specific_idle_operation1 = self.session.query(Event).join(Gauge).filter(Gauge.name.like("PLANNED_IDLE"),
                                                                                           Event.start == "2018-07-20T14:02:32.734",
                                                                                           Event.stop == "2018-07-20T14:07:27.734").all()

        assert len(specific_idle_operation1) == 1

        specific_idle_operation2 = self.session.query(Event).join(Gauge).filter(Gauge.name.like("PLANNED_IDLE"),
                                                                                           Event.start == "2018-07-20T14:08:53.218",
                                                                                           Event.stop == "2018-07-20T14:09:57.673").all()

        assert len(specific_idle_operation2) == 1

        specific_idle_operation3 = self.session.query(Event).join(Gauge).filter(Gauge.name.like("PLANNED_IDLE"),
                                                                                           Event.start == "2018-07-20T14:16:04.581",
                                                                                           Event.stop == "2018-07-20T14:27:45.768").all()

        assert len(specific_idle_operation3) == 1

        specific_idle_operation4 = self.session.query(Event).join(Gauge).filter(Gauge.name.like("PLANNED_IDLE"),
                                                                                           Event.start == "2018-07-20T14:47:55.186",
                                                                                           Event.stop == "2018-07-20T15:48:04.082").all()

        assert len(specific_idle_operation4) == 1

        specific_idle_operation5 = self.session.query(Event).join(Gauge).filter(Gauge.name.like("PLANNED_IDLE"),
                                                                                           Event.start == "2018-07-20T15:48:24.622",
                                                                                           Event.stop == "2018-07-20T15:48:57.739").all()

        assert len(specific_idle_operation5) == 1

        specific_idle_operation6 = self.session.query(Event).join(Gauge).filter(Gauge.name.like("PLANNED_IDLE"),
                                                                                           Event.start == "2018-07-20T16:04:19.934",
                                                                                           Event.stop == "2018-07-20T16:05:04.319").all()

        assert len(specific_idle_operation6) == 1

        specific_idle_operation7 = self.session.query(Event).join(Gauge).filter(Gauge.name.like("PLANNED_IDLE"),
                                                                                           Event.start == "2018-07-20T16:11:14.835",
                                                                                           Event.stop == "2018-07-20T17:28:51.581").all()

        assert len(specific_idle_operation7) == 1

        specific_idle_operation8 = self.session.query(Event).join(Gauge).filter(Gauge.name.like("PLANNED_IDLE"),
                                                                                           Event.start == "2018-07-20T17:48:41.206",
                                                                                           Event.stop == "2018-08-06T14:00:00").all()

        assert len(specific_idle_operation8) == 1

    def test_datatake_cut_by_records(self):
        filename = "S2A_NPPF_DATATAKE_CUT_BY_RECORDS.EOF"
        file_path = os.path.dirname(os.path.abspath(__file__)) + "/inputs/" + filename

        ingestion.command_process_file(file_path)

        # Check number of events generated
        events = self.session.query(Event).all()

        assert len(events) == 8

        cut_nominal1 = self.session.query(Event).join(Gauge).filter(Gauge.name.like("PLANNED_CUT_IMAGING_NOMINAL"),
                                                                                           Event.start == "2018-07-20T14:07:27.734",
                                                                                           Event.stop == "2018-07-20T14:08:57.718").all()

        assert len(cut_nominal1) == 1

        cut_nominal2 = self.session.query(Event).join(Gauge).filter(Gauge.name.like("PLANNED_CUT_IMAGING_NOMINAL"),
                                                                                           Event.start == "2018-07-20T14:08:57.718",
                                                                                           Event.stop == "2018-07-20T14:12:57.718").all()

        assert len(cut_nominal2) == 1

        cut_nominal3 = self.session.query(Event).join(Gauge).filter(Gauge.name.like("PLANNED_CUT_IMAGING_NOMINAL"),
                                                                                           Event.start == "2018-07-20T14:12:57.718",
                                                                                           Event.stop == "2018-07-20T14:14:53.218").all()

        assert len(cut_nominal3) == 1

    def test_file_not_valid(self):

        data = {"not_valid_data": "true"}

        returned_value = ingestion.insert_data_into_DDBB(data, "NOT_VALID_FILE.EOF", self.engine_eboa)

        assert returned_value[0]["status"] == eboa_engine.exit_codes["FILE_NOT_VALID"]["status"]

    def test_empty_nppf(self):
        filename = "S2A_NPPF_CONTAINING_ALL_DATA_TO_BE_PROCESS.EOF"
        file_path = os.path.dirname(os.path.abspath(__file__)) + "/inputs/" + filename

        ingestion.command_process_file(file_path)

        # Check number of events generated
        events = self.session.query(Event).all()

        assert len(events) == 38

        filename = "S2A_NPPF_SAME_PERIOD_BUT_NO_EVENTS.EOF"
        file_path = os.path.dirname(os.path.abspath(__file__)) + "/inputs/" + filename

        ingestion.command_process_file(file_path)

        # Check number of events generated
        events = self.session.query(Event).all()

        assert len(events) == 0
