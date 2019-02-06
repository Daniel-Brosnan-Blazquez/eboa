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

        # Check that events before the queue deletion are not inserted
        events_before_validity_period = self.session.query(Event).filter(Event.stop < "2018-07-24T10:44:12").all()

        assert len(events_before_validity_period) == 0

        # Check number of events generated
        events = self.session.query(Event).all()

        assert len(events) == 1

        # Check that the validity period of the input is correctly taken
        source = self.session.query(Source).filter(Source.validity_start == "2018-07-24T10:44:12",
                                                                                 Source.validity_stop == "2018-07-24T10:48:57").all()

        assert len(source) == 1

        # Check operations
        operations = self.session.query(Event).join(Gauge).filter(Gauge.name == "STATION_REPORT").all()

        assert len(operations) == 1

        # Check specific operation
        specific_operation = self.session.query(Event).join(Gauge).filter(Gauge.name == "STATION_REPORT",
                                                                                 Event.start == "2018-07-24T10:45:09",
                                                                                 Event.stop == "2018-07-24T10:47:39").all()

        assert len(specific_operation) == 1

        #Check that the validity period changes if the downlink period isn't covered
        filename = "REPORT_WITH_VALIDITY_PERIOD_NOT_COVERING.EOF"
        file_path = os.path.dirname(os.path.abspath(__file__)) + "/inputs/" + filename

        ingestion.command_process_file(file_path)

        period = self.session.query(Source).filter(Source.validity_start >= "2018-07-24T10:44:12",
                                                                                 Source.validity_stop <= "2018-07-24T10:48:57").all()

        assert len(period) == 1

        #Check that the caracterized_downlink_status NOK is correctly set
        filename = "REPORT_WITH_NOK_CARATERIZED_STATUS.EOF"
        file_path = os.path.dirname(os.path.abspath(__file__)) + "/inputs/" + filename

        ingestion.command_process_file(file_path)

        nok_event = self.session.query(EventText).join(Event,Gauge).filter(Gauge.name == "STATION_REPORT",
                                                                                 Event.start == "2018-07-24T21:51:07",
                                                                                 EventText.name == "caracterized_downlink_status",
                                                                                 EventText.value == "NOK").all()

        assert len(nok_event) == 1
