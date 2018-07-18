"""
Automated tests for the datamodel submodule

Written by DEIMOS Space S.L. (dibb)

module gsdm
"""
# Import python utilities
import os
import sys
import unittest
import datetime
import uuid
import random
import before_after

# Import engine of the DDBB
import gsdm.engine.engine
from gsdm.engine.engine import Engine
from gsdm.engine.query import Query
from gsdm.datamodel.base import Session, engine, Base

# Import datamodel
from gsdm.datamodel.dim_signatures import DimSignature
from gsdm.datamodel.events import Event, EventLink, EventKey, EventText, EventDouble, EventObject, EventGeometry, EventBoolean, EventTimestamp
from gsdm.datamodel.gauges import Gauge
from gsdm.datamodel.dim_processings import DimProcessing, DimProcessingStatus
from gsdm.datamodel.explicit_refs import ExplicitRef, ExplicitRefGrp, ExplicitRefLink
from gsdm.datamodel.annotations import Annotation, AnnotationCnf, AnnotationText, AnnotationDouble, AnnotationObject, AnnotationGeometry, AnnotationBoolean, AnnotationTimestamp

# Import GEOalchemy entities
from geoalchemy2 import functions
from geoalchemy2.shape import to_shape

# Import SQLalchemy entities
from sqlalchemy import func

class TestEngine(unittest.TestCase):
    def setUp(self):
        # Create the engine to manage the data
        self.engine_gsdm = Engine()
        self.engine_gsdm_race_conditions = Engine()

        # Create session to connect to the database
        self.session = Session()

        # Clear all tables before executing the test
        for table in reversed(Base.metadata.sorted_tables):
            engine.execute(table.delete())

    def test_insert_dim_signature(self):
        data = {"dim_signature": {"name": "dim_signature",
                                  "exec": "exec",
                                  "version": "1.0"}
                                  }
        self.engine_gsdm.operation = data
        self.engine_gsdm._insert_dim_signature()

        dim_signature_ddbb = self.session.query(DimSignature).filter(DimSignature.dim_signature == data["dim_signature"]["name"], DimSignature.dim_exec_name == data["dim_signature"]["exec"]).all()

        assert len(dim_signature_ddbb) == 1

    def test_insert_again_dim_signature(self):
        """
        Method with no asserts as the expected behaviour is to discard the insertion
        and to have only once the same dim_sigature registered
        """
        self.test_insert_dim_signature()

    def test_race_condition_dim_signature(self):
        """
        Method to test the race condition that could be produced if the
        dim_signature does not exist and it is going to be created by
        two instances
        """
        data = {"dim_signature": {"name": "dim_signature_race_condition",
                                  "exec": "exec",
                                  "version": "1.0"}
        }
        self.engine_gsdm.operation = data
        self.engine_gsdm_race_conditions.operation = data
        with before_after.before("gsdm.engine.engine.race_condition", self.engine_gsdm_race_conditions._insert_dim_signature):
            self.engine_gsdm._insert_dim_signature()

        dim_signature_ddbb = self.session.query(DimSignature).filter(DimSignature.dim_signature == data["dim_signature"]["name"], DimSignature.dim_exec_name == data["dim_signature"]["exec"]).all()

        assert len(dim_signature_ddbb) == 1


    def test_insert_source(self):
        data = {"dim_signature": {"name": "dim_signature",
                                  "exec": "exec",
                                  "version": "1.0"},
                "source": {"name": "source.xml",
                           "generation_time": "2018-07-05T02:07:03",
                           "validity_start": "2018-06-05T02:07:03",
                           "validity_stop": "2018-06-05T08:07:36"}
            }
        self.engine_gsdm.operation = data
        self.engine_gsdm._insert_dim_signature()
        self.engine_gsdm._insert_source()

        dim_signature_ddbb = self.session.query(DimSignature).filter(DimSignature.dim_signature == data["dim_signature"]["name"], DimSignature.dim_exec_name == data["dim_signature"]["exec"]).all()

        assert len(dim_signature_ddbb) == 1

        source_ddbb = self.session.query(DimProcessing).filter(DimProcessing.name == data["source"]["name"], DimProcessing.validity_start == data["source"]["validity_start"], DimProcessing.validity_stop == data["source"]["validity_stop"], DimProcessing.generation_time == data["source"]["generation_time"], DimProcessing.dim_exec_version == data["dim_signature"]["version"]).all()

        assert len(source_ddbb) == 1

    def test_insert_source_without_dim_signature(self):
        name = "source_withoud_dim_signature.xml"
        self.engine_gsdm._insert_source_without_dim_signature(name)

        source_ddbb = self.session.query(DimProcessing).filter(DimProcessing.name == name).all()

        assert len(source_ddbb) == 1
