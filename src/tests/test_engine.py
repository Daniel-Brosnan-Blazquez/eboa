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
        self.test_insert_dim_signature()

    def test_race_condition_insert_dim_signature(self):
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

        source_ddbb = self.session.query(DimProcessing).filter(DimProcessing.name == data["source"]["name"], DimProcessing.validity_start == data["source"]["validity_start"], DimProcessing.validity_stop == data["source"]["validity_stop"], DimProcessing.generation_time == data["source"]["generation_time"], DimProcessing.dim_exec_version == data["dim_signature"]["version"]).all()

        assert len(source_ddbb) == 1

    def test_insert_source_again(self):
        self.test_insert_source()
        data = self.engine_gsdm.operation

        self.engine_gsdm.ingestion_start = datetime.datetime.now()
        self.engine_gsdm._insert_proc_status(0, final = True)
        
        try:
            self.test_insert_source()
        except:
            pass

        source_ddbb = self.session.query(DimProcessing).filter(DimProcessing.name == data["source"]["name"], DimProcessing.validity_start == data["source"]["validity_start"], DimProcessing.validity_stop == data["source"]["validity_stop"], DimProcessing.generation_time == data["source"]["generation_time"], DimProcessing.dim_exec_version == data["dim_signature"]["version"]).all()

        assert len(source_ddbb) == 1

    def test_insert_source_wrong_validity(self):
        """
        Method to test that the engine protects the ingestion from a wrong
        validity period where the start is greater than the stop
        """
        data = {"dim_signature": {"name": "dim_signature",
                                  "exec": "exec",
                                  "version": "1.0"},
                "source": {"name": "source_wrong_period.xml",
                           "generation_time": "2018-07-05T02:07:03",
                           "validity_start": "2018-06-05T10:07:03",
                           "validity_stop": "2018-06-05T08:07:36"}
            }
        self.engine_gsdm.operation = data
        self.engine_gsdm._insert_dim_signature()
        try:
            self.engine_gsdm._insert_source()
        except:
            pass

        source_ddbb = self.session.query(DimProcessing).filter(DimProcessing.name == data["source"]["name"], DimProcessing.generation_time == data["source"]["generation_time"], DimProcessing.dim_exec_version == data["dim_signature"]["version"]).all()

        assert len(source_ddbb) == 1

    def test_race_condition_insert_source_wrong_validity(self):
        """Method to test the race condition that could be produced if two
        processes try to ingest the same source with a wrong period

        """
        data = {"dim_signature": {"name": "dim_signature",
                                  "exec": "exec",
                                  "version": "1.0"},
                "source": {"name": "source_wrong_period.xml",
                           "generation_time": "2018-07-05T02:07:03",
                           "validity_start": "2018-06-05T10:07:03",
                           "validity_stop": "2018-06-05T08:07:36"}
            }
        self.engine_gsdm.operation = data
        self.engine_gsdm_race_conditions.operation = data
        self.engine_gsdm._insert_dim_signature()
        self.engine_gsdm_race_conditions._insert_dim_signature()
        def insert_source_race_condition():
            try:
                self.engine_gsdm_race_conditions._insert_source()
            except:
                pass

        def insert_source():
            try:
                self.engine_gsdm._insert_source()
            except:
                pass

        with before_after.before("gsdm.engine.engine.race_condition", insert_source_race_condition):
            insert_source()

        source_ddbb = self.session.query(DimProcessing).filter(DimProcessing.name == data["source"]["name"], DimProcessing.generation_time == data["source"]["generation_time"], DimProcessing.dim_exec_version == data["dim_signature"]["version"]).all()

        assert len(source_ddbb) == 1

    def test_race_condition_insert_source(self):
        """Method to test the race condition that could be produced if two
        processes try to ingest the same source

        """
        data = {"dim_signature": {"name": "dim_signature",
                                  "exec": "exec",
                                  "version": "1.0"},
                "source": {"name": "source.xml",
                           "generation_time": "2018-07-05T02:07:03",
                           "validity_start": "2018-06-05T02:07:03",
                           "validity_stop": "2018-06-05T08:07:36"}
            }
        self.engine_gsdm.operation = data
        self.engine_gsdm_race_conditions.operation = data
        self.engine_gsdm._insert_dim_signature()
        self.engine_gsdm_race_conditions._insert_dim_signature()
        def insert_source_race_condition():
            try:
                self.engine_gsdm_race_conditions._insert_source()
            except:
                pass

        def insert_source():
            try:
                self.engine_gsdm._insert_source()
            except:
                pass

        with before_after.before("gsdm.engine.engine.race_condition", insert_source_race_condition):
            insert_source()

        source_ddbb = self.session.query(DimProcessing).filter(DimProcessing.name == data["source"]["name"], DimProcessing.validity_start == data["source"]["validity_start"], DimProcessing.validity_stop == data["source"]["validity_stop"], DimProcessing.generation_time == data["source"]["generation_time"], DimProcessing.dim_exec_version == data["dim_signature"]["version"]).all()

        assert len(source_ddbb) == 1

    def test_insert_source_without_dim_signature(self):
        name = "source_withoud_dim_signature.xml"
        self.engine_gsdm._insert_source_without_dim_signature(name)

        source_ddbb = self.session.query(DimProcessing).filter(DimProcessing.name == name).all()

        assert len(source_ddbb) == 1

    def test_insert_source_without_dim_signature_again(self):
        self.test_insert_source_without_dim_signature()
        self.test_insert_source_without_dim_signature()

    def test_insert_gauge(self):

        self.engine_gsdm._initialize_context_insert_data()
        self.test_insert_source()
        data = {"gauge": {
            "name": "GAUGE",
            "system": "SYSTEM"
        }}
        self.engine_gsdm.operation["events"] = [data]
        self.engine_gsdm._insert_gauges()
        # Call commit as the method uses the nested operation
        self.engine_gsdm.session.commit()

        gauge_ddbb = self.session.query(Gauge).filter(Gauge.name == data["gauge"]["name"], Gauge.system == data["gauge"]["system"]).all()

        assert len(gauge_ddbb) == 1

    def test_race_condition_insert_gauge(self):

        self.engine_gsdm._initialize_context_insert_data()
        self.engine_gsdm_race_conditions._initialize_context_insert_data()
        data = {"dim_signature": {"name": "dim_signature",
                                  "exec": "exec",
                                  "version": "1.0"},
                "source": {"name": "source.xml",
                           "generation_time": "2018-07-05T02:07:03",
                           "validity_start": "2018-06-05T02:07:03",
                           "validity_stop": "2018-06-05T08:07:36"},
                "events": [{"gauge": {
                    "name": "GAUGE",
                    "system": "SYSTEM"
                }}]
            }
        self.engine_gsdm.operation = data
        self.engine_gsdm._insert_dim_signature()
        self.engine_gsdm._insert_source()
        self.engine_gsdm_race_conditions.operation = data
        self.engine_gsdm_race_conditions._insert_dim_signature()
        try:
            self.engine_gsdm_race_conditions._insert_source()
        except:
            pass
        # end try

        def insert_gauges():
            self.engine_gsdm._insert_gauges()
            self.engine_gsdm.session.commit()
            self.engine_gsdm.session.close()

        def insert_gauges_race_condition():
            self.engine_gsdm_race_conditions._insert_gauges()
            self.engine_gsdm_race_conditions.session.commit()
            self.engine_gsdm_race_conditions.session.close()

        with before_after.before("gsdm.engine.engine.race_condition", insert_gauges_race_condition):
            insert_gauges()

        # Call commit as the method uses the nested operation
        self.engine_gsdm_race_conditions.session.commit()

        gauge_ddbb = self.session.query(Gauge).filter(Gauge.name == data["events"][0]["gauge"]["name"], Gauge.system == data["events"][0]["gauge"]["system"]).all()

        assert len(gauge_ddbb) == 1

    def test_insert_annotation(self):

        self.engine_gsdm._initialize_context_insert_data()
        self.test_insert_source()
        data = {"annotation_cnf": {
            "name": "ANNOTATION_CNF",
            "system": "SYSTEM"
        }}
        self.engine_gsdm.operation["annotations"] = [data]
        self.engine_gsdm._insert_annotation_cnfs()
        # Call commit as the method uses the nested operation
        self.engine_gsdm.session.commit()

        annotation_cnf_ddbb = self.session.query(AnnotationCnf).filter(AnnotationCnf.name == data["annotation_cnf"]["name"], AnnotationCnf.system == data["annotation_cnf"]["system"]).all()

        assert len(annotation_cnf_ddbb) == 1

    def test_race_condition_insert_annotation(self):

        self.engine_gsdm._initialize_context_insert_data()
        self.engine_gsdm_race_conditions._initialize_context_insert_data()
        data = {"dim_signature": {"name": "dim_signature",
                                  "exec": "exec",
                                  "version": "1.0"},
                "source": {"name": "source.xml",
                           "generation_time": "2018-07-05T02:07:03",
                           "validity_start": "2018-06-05T02:07:03",
                           "validity_stop": "2018-06-05T08:07:36"},
                "annotations": [{"annotation_cnf": {
                    "name": "ANNOTATION_CNF",
                    "system": "SYSTEM"
                }}]
            }
        self.engine_gsdm.operation = data
        self.engine_gsdm._insert_dim_signature()
        self.engine_gsdm._insert_source()
        self.engine_gsdm_race_conditions.operation = data
        self.engine_gsdm_race_conditions._insert_dim_signature()
        try:
            self.engine_gsdm_race_conditions._insert_source()
        except:
            pass
        # end try

        def insert_annotations_cnf():
            self.engine_gsdm._insert_annotation_cnfs()
            self.engine_gsdm.session.commit()

        def insert_annotations_cnf_race_condition():
            self.engine_gsdm_race_conditions._insert_annotation_cnfs()
            self.engine_gsdm_race_conditions.session.commit()

        with before_after.before("gsdm.engine.engine.race_condition", insert_annotations_cnf_race_condition):
            insert_annotations_cnf()

        # Call commit as the method uses the nested operation
        self.engine_gsdm_race_conditions.session.commit()

        annotation_cnf_ddbb = self.session.query(AnnotationCnf).filter(AnnotationCnf.name == data["annotations"][0]["annotation_cnf"]["name"], AnnotationCnf.system == data["annotations"][0]["annotation_cnf"]["system"]).all()

        assert len(annotation_cnf_ddbb) == 1

    def test_insert_expl_group(self):

        self.engine_gsdm._initialize_context_insert_data()
        data = {"explicit_references": [{
            "group": "EXPL_GROUP"
        }]}
        self.engine_gsdm.operation = data
        self.engine_gsdm._insert_expl_groups()
        # Call commit as the method uses the nested operation
        self.engine_gsdm.session.commit()

        expl_group_ddbb = self.session.query(ExplicitRefGrp).filter(ExplicitRefGrp.name == data["explicit_references"][0]["group"]).all()

        assert len(expl_group_ddbb) == 1

    def test_race_condition_insert_expl_group(self):
        """
        Method to test the race condition that could be produced if the
        group for explicit references does not exist and it is going to be created by
        two instances
        """
        self.engine_gsdm._initialize_context_insert_data()
        self.engine_gsdm_race_conditions._initialize_context_insert_data()
        data = {"explicit_references": [{
            "group": "EXPL_GROUP"
        }]}
        self.engine_gsdm.operation = data
        self.engine_gsdm_race_conditions.operation = data

        def insert_expl_group():
            self.engine_gsdm._insert_expl_groups()
            self.engine_gsdm.session.commit()

        def insert_expl_group_race_condition():
            self.engine_gsdm_race_conditions._insert_expl_groups()
            self.engine_gsdm_race_conditions.session.commit()

        with before_after.before("gsdm.engine.engine.race_condition", insert_expl_group_race_condition):
            insert_expl_group()

        expl_group_ddbb = self.session.query(ExplicitRefGrp).filter(ExplicitRefGrp.name == data["explicit_references"][0]["group"]).all()

        assert len(expl_group_ddbb) == 1

    def test_insert_explicit_reference(self):

        self.engine_gsdm._initialize_context_insert_data()
        data = {"explicit_references": [{
            "name": "EXPLICIT_REFERENCE",
            "links": [{"link": "EXPLICIT_REFERENCE_LINK"}]
        }],
                "events": [{
                    "explicit_reference": "EXPLICIT_REFERENCE_EVENT"
                }],
                "annotations": [{
                    "explicit_reference": "EXPLICIT_REFERENCE_ANNOTATION"
                }]
            }
        self.engine_gsdm.operation = data
        self.engine_gsdm._insert_explicit_refs()
        # Call commit as the method uses the nested operation
        self.engine_gsdm.session.commit()

        explicit_reference_ddbb = self.session.query(ExplicitRef).filter(ExplicitRef.explicit_ref == data["explicit_references"][0]["name"]).all()

        assert len(explicit_reference_ddbb) == 1

        explicit_reference_link_ddbb = self.session.query(ExplicitRef).filter(ExplicitRef.explicit_ref == data["explicit_references"][0]["links"][0]["link"]).all()

        assert len(explicit_reference_link_ddbb) == 1

        explicit_reference_event_ddbb = self.session.query(ExplicitRef).filter(ExplicitRef.explicit_ref == data["events"][0]["explicit_reference"]).all()

        assert len(explicit_reference_event_ddbb) == 1

        explicit_reference_annotation_ddbb = self.session.query(ExplicitRef).filter(ExplicitRef.explicit_ref == data["annotations"][0]["explicit_reference"]).all()

        assert len(explicit_reference_annotation_ddbb) == 1

    def test_insert_race_condition_explicit_reference(self):

        self.engine_gsdm._initialize_context_insert_data()
        self.engine_gsdm_race_conditions._initialize_context_insert_data()
        data = {"explicit_references": [{
            "name": "EXPLICIT_REFERENCE"}]
            }
        self.engine_gsdm.operation = data
        self.engine_gsdm_race_conditions.operation = data

        def insert_explicit_reference():
            self.engine_gsdm._insert_explicit_refs()
            self.engine_gsdm.session.commit()

        def insert_explicit_reference_race_condition():
            self.engine_gsdm_race_conditions._insert_explicit_refs()
            self.engine_gsdm_race_conditions.session.commit()

        with before_after.before("gsdm.engine.engine.race_condition", insert_explicit_reference_race_condition):
            insert_explicit_reference()

        explicit_reference_ddbb = self.session.query(ExplicitRef).filter(ExplicitRef.explicit_ref == data["explicit_references"][0]["name"]).all()

        assert len(explicit_reference_ddbb) == 1
