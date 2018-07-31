"""
Automated tests for the engine submodule

Written by DEIMOS Space S.L. (dibb)

module gsdm
"""
# Import python utilities
import os
import sys
import unittest
import datetime

# Import engine of the DDBB
import gsdm.engine.engine
from gsdm.engine.engine import Engine
from gsdm.engine.query import Query
from gsdm.datamodel.base import Session, engine, Base
from gsdm.engine.errors import WrongEventLink, WrongPeriod, SourceAlreadyIngested, WrongValue, OddNumberOfCoordinates, GsdmResourcesPathNotAvailable, WrongGeometry
from gsdm.engine.query import Query

# Import datamodel
from gsdm.datamodel.dim_signatures import DimSignature
from gsdm.datamodel.events import Event, EventLink, EventKey, EventText, EventDouble, EventObject, EventGeometry, EventBoolean, EventTimestamp
from gsdm.datamodel.gauges import Gauge
from gsdm.datamodel.dim_processings import DimProcessing, DimProcessingStatus
from gsdm.datamodel.explicit_refs import ExplicitRef, ExplicitRefGrp, ExplicitRefLink
from gsdm.datamodel.annotations import Annotation, AnnotationCnf, AnnotationText, AnnotationDouble, AnnotationObject, AnnotationGeometry, AnnotationBoolean, AnnotationTimestamp

# Import ingestion
import ingestions.s2.ingestion_nppf.ingestion_nppf as ingestion

class TestEngine(unittest.TestCase):
    def setUp(self):
        # Create the engine to manage the data
        self.engine_gsdm = Engine()
        self.query_gsdm = Query()

        # Create session to connectx to the database
        self.session = Session()

        # Clear all tables before executing the test
        for table in reversed(Base.metadata.sorted_tables):
            engine.execute(table.delete())

    def test_insert_insert_nppf(self):
        filename = "NPPF_CONTAINING_ALL_DATA_TO_BE_PROCESS.EOF"
        file_path = os.path.dirname(os.path.abspath(__file__)) + "/inputs/" + filename

        ingestion.command_process_file(file_path)

        # Check that events before the queue deletion are not inserted
        events_before_validity_period = self.session.query(Event).filter(Event.stop < "2018-07-20T13:40:00.000").all()

        assert len(events_before_validity_period) == 0

        # Check nominal imaging operations
        imaging_operations = self.session.query(Event).join(Gauge).filter(Gauge.name == "imaging").all()

        assert len(imaging_operations) == 2

        # Check record operations
        record_operations = self.session.query(Event).join(Gauge).filter(Gauge.name == "record").all()

        assert len(record_operations) == 2

        # Check idle operations
        idle_operations = self.session.query(Event).join(Gauge).filter(Gauge.name == "idle").all()

        assert len(idle_operations) == 3

