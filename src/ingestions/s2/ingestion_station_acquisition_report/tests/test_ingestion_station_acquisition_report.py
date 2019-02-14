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
import ingestions.s2.ingestion_station_acquisition_report.ingestion_station_acquisition_report as ingestion
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

    def test_insert_station_acquisition_report(self):
        filename = "REPORT_CONTAINING_ALL_DATA_TO_BE_PROCESS.EOF"

        file_path = os.path.dirname(os.path.abspath(__file__)) + "/inputs/" + filename

        ingestion.command_process_file(file_path)


        # Check number of events generated
        events = self.session.query(Event).all()

        assert len(events) == 1

        # Check that the validity period of the input is correctly taken
        source_period = self.session.query(Source).filter(Source.validity_start == "2018-07-24T10:44:12",
                                                                                 Source.validity_stop == "2018-07-24T10:48:57").all()

        assert len(source_period) == 1

        #Check that the name of the input file is correctly taken
        source_name = self.session.query(Source).filter(Source.name == "REPORT_CONTAINING_ALL_DATA_TO_BE_PROCESS.EOF").all()

        assert len(source_name) == 1

        #Check that the generation date of the input file is correctly taken
        source_gen_date = self.session.query(Source).filter(Source.generation_time == "UTC=2018-07-24T11:04:40").all()

        assert len(source_gen_date) == 1

        # Check that the Gauge name is correctly taken
        gauge_name = self.session.query(Event).join(Gauge).filter(Gauge.name == "STATION_REPORT").all()

        assert len(gauge_name) == 1

        # Check that the Gauge system is correctly taken
        gauge_system = self.session.query(Event).join(Gauge).filter(Gauge.system == "MPS_").all()

        assert len(gauge_system) == 1

        #Check that the Dim Signature is correctly taken
        definite_dim_signature = self.session.query(DimSignature).filter(DimSignature.dim_signature == "STATION_REPORT_MPS__S2A").all()

        assert len(definite_dim_signature) == 1

        #Check that the key is correctly taken
        definite_key = self.session.query(EventKey).filter(EventKey.event_key == "STATION_REPORT_MPS__S2A_16121_01").all()

        assert len(definite_key) == 1

        #Check that the event is correctly taken
        definite_event = self.session.query(Event).join(Gauge).filter(Gauge.name == "STATION_REPORT",
                                                                                 Event.start == "2018-07-24T10:45:09",
                                                                                 Event.stop == "2018-07-24T10:47:39"
                                                                                 ).all()

        assert len(definite_event) == 1

        #Check downlink_status is correctly taken
        definite_downlink_status = self.session.query(EventText).join(Event,Gauge).filter(Gauge.name == "STATION_REPORT",
                                                                             Event.start == "2018-07-24T10:45:09",
                                                                             Event.stop == "2018-07-24T10:47:39",
                                                                             EventText.name == "downlink_status",
                                                                             EventText.value == "OK").all()

        assert len(definite_downlink_status) == 1

        #Check characterized_downlink_status is correctly taken
        definite_characterized_downlink_status = self.session.query(EventText).join(Event,Gauge).filter(Gauge.name == "STATION_REPORT",
                                                                             Event.start == "2018-07-24T10:45:09",
                                                                             Event.stop == "2018-07-24T10:47:39",
                                                                             EventText.name == "characterized_downlink_status",
                                                                             EventText.value == "OK").all()

        assert len(definite_characterized_downlink_status) == 1

        #Check comments is correctly taken
        definite_comments = self.session.query(EventText).join(Event,Gauge).filter(Gauge.name == "STATION_REPORT",
                                                                             Event.start == "2018-07-24T10:45:09",
                                                                             Event.stop == "2018-07-24T10:47:39",
                                                                             EventText.name == "comments",
                                                                             EventText.value == " ").all()

        assert len(definite_comments) == 1

        #Check antenna_id is correctly taken
        definite_antenna_id = self.session.query(EventText).join(Event,Gauge).filter(Gauge.name == "STATION_REPORT",
                                                                             Event.start == "2018-07-24T10:45:09",
                                                                             Event.stop == "2018-07-24T10:47:39",
                                                                             EventText.name == "antenna_id",
                                                                             EventText.value == "MSP21").all()

        assert len(definite_antenna_id) == 1

        #Check satellite is correctly taken
        definite_satellite = self.session.query(EventText).join(Event,Gauge).filter(Gauge.name == "STATION_REPORT",
                                                                             Event.start == "2018-07-24T10:45:09",
                                                                             Event.stop == "2018-07-24T10:47:39",
                                                                             EventText.name == "satellite",
                                                                             EventText.value == "S2A").all()

        assert len(definite_satellite) == 1

        #Check station is correctly taken
        definite_station = self.session.query(EventText).join(Event,Gauge).filter(Gauge.name == "STATION_REPORT",
                                                                             Event.start == "2018-07-24T10:45:09",
                                                                             Event.stop == "2018-07-24T10:47:39",
                                                                             EventText.name == "station",
                                                                             EventText.value == "MPS_").all()

        assert len(definite_station) == 1

        #Check orbit is correctly taken
        definite_orbit = self.session.query(EventDouble).join(Event,Gauge).filter(Gauge.name == "STATION_REPORT",
                                                                             Event.start == "2018-07-24T10:45:09",
                                                                             Event.stop == "2018-07-24T10:47:39",
                                                                             EventDouble.name == "orbit",
                                                                             EventDouble.value == "16121").all()

        assert len(definite_orbit) == 1

        #Check support_number is correctly taken
        definite_support_number = self.session.query(EventDouble).join(Event,Gauge).filter(Gauge.name == "STATION_REPORT",
                                                                             Event.start == "2018-07-24T10:45:09",
                                                                             Event.stop == "2018-07-24T10:47:39",
                                                                             EventDouble.name == "support_number",
                                                                             EventDouble.value == "01").all()

        assert len(definite_support_number) == 1

    def test_station_acquisition_report_with_links(self):
        #Tests related with linked events
        orbpre_filename = "S2A_ORBPRE_RIPPED.EOF"
        nppf_filename = "S2A_NPPF_RIPPED.EOF"
        filename = "FILE_WITH_LINKS.EOF"

        file_path = os.path.dirname(os.path.abspath(__file__)) + "/inputs/" + filename
        orbpre_file_path = os.path.dirname(os.path.abspath(__file__)) + "/inputs/" + orbpre_filename
        nppf_file_path = os.path.dirname(os.path.abspath(__file__)) + "/inputs/" + nppf_filename

        ingestion_nppf.command_process_file(nppf_file_path)
        ingestion_orbpre.command_process_file(orbpre_file_path)
        ingestion.command_process_file(file_path)

        #Check status is correctly taken
        definite_status = self.session.query(EventText).join(Event,Gauge).filter(Gauge.name == "STATION_REPORT",
                                                                             Event.start == "2018-07-24T12:17:23",
                                                                             Event.stop == "2018-07-24T12:29:32",
                                                                             EventText.name == "status",
                                                                             EventText.value == "MATCHED_PLAYBACK").all()

        assert len(definite_status) == 1

        #Check number of links related
        total_links = self.session.query(EventLink).join(Event,Gauge).filter(Gauge.name == "STATION_REPORT",
                                                                             Event.start == "2018-07-24T12:17:23",
                                                                             Event.stop == "2018-07-24T12:29:32").all()

        assert len(total_links) == 2

        #Check if a definite link exists
        linked_uuids = (link.event_uuid_link for link in total_links)
        definite_linked_event = self.session.query(Event).join(Gauge).filter(Gauge.name == "PLANNED_PLAYBACK_TYPE_SAD",
                                                                             Event.start == "2018-07-24 12:28:55.968000",
                                                                             Event.stop == "2018-07-24 12:28:55.968000").all()

        assert definite_linked_event[0].event_uuid in linked_uuids

    def test_station_acquisition_report_corrected_validity_period(self):
        #Check that the validity period changes if the downlink period isn't covered
        filename = "REPORT_WITH_VALIDITY_PERIOD_NOT_COVERING.EOF"
        file_path = os.path.dirname(os.path.abspath(__file__)) + "/inputs/" + filename

        ingestion.command_process_file(file_path)
        
        period = self.session.query(Source).filter(Source.validity_start >= "2018-07-24T08:53:17",
                                                                                 Source.validity_stop <= "2018-07-24T09:05:17").all()

        assert len(period) == 1

    def test_station_acquisition_report_nok_status(self):
        #Check that the characterized_downlink_status NOK is correctly set
        filename = "REPORT_WITH_NOK_CHARATERIZED_STATUS.EOF"
        file_path = os.path.dirname(os.path.abspath(__file__)) + "/inputs/" + filename

        ingestion.command_process_file(file_path)

        nok_event = self.session.query(EventText).join(Event,Gauge).filter(Gauge.name == "STATION_REPORT",
                                                                                 Event.start == "2018-07-24T21:51:07",
                                                                                 EventText.name == "characterized_downlink_status",
                                                                                 EventText.value == "NOK").all()

        assert len(nok_event) == 1
