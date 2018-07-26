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
import logging

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
        self.query_gsdm = Query()

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
        except SourceAlreadyIngested:
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
        except WrongPeriod:
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
            except WrongPeriod:
                pass

        def insert_source():
            try:
                self.engine_gsdm._insert_source()
            except WrongPeriod:
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
            except SourceAlreadyIngested:
                pass

        def insert_source():
            try:
                self.engine_gsdm._insert_source()
            except SourceAlreadyIngested:
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
            "system": "SYSTEM",
            "insertion_type": "SIMPLE_UPDATE"
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
                    "system": "SYSTEM",
                    "insertion_type": "SIMPLE_UPDATE"
                }}]
            }
        self.engine_gsdm.operation = data
        self.engine_gsdm._insert_dim_signature()
        self.engine_gsdm._insert_source()
        self.engine_gsdm_race_conditions.operation = data
        self.engine_gsdm_race_conditions._insert_dim_signature()
        try:
            self.engine_gsdm_race_conditions._insert_source()
        except SourceAlreadyIngested:
            pass

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
        except SourceAlreadyIngested:
            pass

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

    def test_insert_link_explicit_refs(self):
        self.engine_gsdm._initialize_context_insert_data()
        data = {"explicit_references": [{
            "name": "EXPLICIT_REFERENCE",
            "links": [{"name": "LINK_NAME",
                       "link": "EXPLICIT_REFERENCE_EVENT",
                       "back_ref": "true"}]
        }],
                "events": [{
                    "explicit_reference": "EXPLICIT_REFERENCE_EVENT"
                }]
            }
        self.engine_gsdm.operation = data
        self.engine_gsdm._insert_explicit_refs()
        # Call commit as the method uses the nested operation
        self.engine_gsdm.session.commit()

        self.engine_gsdm._insert_links_explicit_refs()
        # Call commit as the method uses the nested operation
        self.engine_gsdm.session.commit()

        explicit_reference_ddbb = self.session.query(ExplicitRef).filter(ExplicitRef.explicit_ref == data["explicit_references"][0]["name"]).first()
        explicit_reference_event_ddbb = self.session.query(ExplicitRef).filter(ExplicitRef.explicit_ref == data["events"][0]["explicit_reference"]).first()

        explicit_reference_link_ddbb = self.session.query(ExplicitRefLink).filter(ExplicitRefLink.name == data["explicit_references"][0]["links"][0]["name"],
                                                                                  ExplicitRefLink.explicit_ref_id_link == explicit_reference_ddbb.explicit_ref_id,
                                                                                  ExplicitRefLink.explicit_ref_id == explicit_reference_event_ddbb.explicit_ref_id).all()

        assert len(explicit_reference_link_ddbb) == 1

    def test_insert_race_condition_links_explicit_reference(self):

        self.engine_gsdm._initialize_context_insert_data()
        self.engine_gsdm_race_conditions._initialize_context_insert_data()
        data = {"explicit_references": [{
            "name": "EXPLICIT_REFERENCE",
            "links": [{"name": "LINK_NAME",
                       "link": "EXPLICIT_REFERENCE_EVENT"}]
        }],
                "events": [{
                    "explicit_reference": "EXPLICIT_REFERENCE_EVENT"
                }]
            }
        self.engine_gsdm.operation = data
        self.engine_gsdm_race_conditions.operation = data
        self.engine_gsdm._insert_explicit_refs()
        self.engine_gsdm.session.commit()
        self.engine_gsdm_race_conditions._insert_explicit_refs()
        self.engine_gsdm_race_conditions.session.commit()

        def insert_link_explicit_reference():
            self.engine_gsdm._insert_links_explicit_refs()
            self.engine_gsdm.session.commit()

        def insert_link_explicit_reference_race_condition():
            self.engine_gsdm_race_conditions._insert_links_explicit_refs()
            self.engine_gsdm_race_conditions.session.commit()

        with before_after.before("gsdm.engine.engine.race_condition", insert_link_explicit_reference_race_condition):
            insert_link_explicit_reference()

        explicit_reference_ddbb = self.session.query(ExplicitRef).filter(ExplicitRef.explicit_ref == data["explicit_references"][0]["name"]).first()
        explicit_reference_event_ddbb = self.session.query(ExplicitRef).filter(ExplicitRef.explicit_ref == data["events"][0]["explicit_reference"]).first()
        
        explicit_reference_link_ddbb = self.session.query(ExplicitRefLink).filter(ExplicitRefLink.name == data["explicit_references"][0]["links"][0]["name"],
                                                                                  ExplicitRefLink.explicit_ref_id_link == explicit_reference_ddbb.explicit_ref_id,
                                                                                  ExplicitRefLink.explicit_ref_id == explicit_reference_event_ddbb.explicit_ref_id).all()

        assert len(explicit_reference_link_ddbb) == 1

    def test_insert_race_condition_back_ref_links_explicit_reference(self):

        self.engine_gsdm._initialize_context_insert_data()
        self.engine_gsdm_race_conditions._initialize_context_insert_data()
        data = {"explicit_references": [{
            "name": "EXPLICIT_REFERENCE",
            "links": [{"name": "LINK_NAME",
                       "link": "EXPLICIT_REFERENCE_EVENT"}]
        }],
                "events": [{
                    "explicit_reference": "EXPLICIT_REFERENCE_EVENT"
                }]
            }
        self.engine_gsdm.operation = data
        self.engine_gsdm_race_conditions.operation = data
        self.engine_gsdm._insert_explicit_refs()
        self.engine_gsdm.session.commit()
        self.engine_gsdm_race_conditions._insert_explicit_refs()
        self.engine_gsdm_race_conditions.session.commit()
        self.engine_gsdm._insert_links_explicit_refs()
        self.engine_gsdm.session.commit()
        self.engine_gsdm_race_conditions._insert_links_explicit_refs()
        self.engine_gsdm_race_conditions.session.commit()

        data = {"explicit_references": [{
            "name": "EXPLICIT_REFERENCE",
            "links": [{"name": "LINK_NAME",
                       "link": "EXPLICIT_REFERENCE_EVENT",
                       "back_ref": "true"}]
        }],
                "events": [{
                    "explicit_reference": "EXPLICIT_REFERENCE_EVENT"
                }]
            }
        self.engine_gsdm.operation = data
        self.engine_gsdm_race_conditions.operation = data

        def insert_link_explicit_reference():
            self.engine_gsdm._insert_links_explicit_refs()
            self.engine_gsdm.session.commit()

        def insert_link_explicit_reference_race_condition():
            self.engine_gsdm_race_conditions._insert_links_explicit_refs()
            self.engine_gsdm_race_conditions.session.commit()

        with before_after.before("gsdm.engine.engine.race_condition", insert_link_explicit_reference_race_condition):
            insert_link_explicit_reference()

        explicit_reference_ddbb = self.session.query(ExplicitRef).filter(ExplicitRef.explicit_ref == data["explicit_references"][0]["name"]).first()
        explicit_reference_event_ddbb = self.session.query(ExplicitRef).filter(ExplicitRef.explicit_ref == data["events"][0]["explicit_reference"]).first()
        
        explicit_reference_link_ddbb = self.session.query(ExplicitRefLink).filter(ExplicitRefLink.name == data["explicit_references"][0]["links"][0]["name"],
                                                                                  ExplicitRefLink.explicit_ref_id_link == explicit_reference_ddbb.explicit_ref_id,
                                                                                  ExplicitRefLink.explicit_ref_id == explicit_reference_event_ddbb.explicit_ref_id).all()

        assert len(explicit_reference_link_ddbb) == 1

    def test_insert_event_simple_update(self):

        self.engine_gsdm._initialize_context_insert_data()
        data = {"dim_signature": {"name": "dim_signature",
                                  "exec": "exec",
                                  "version": "1.0"},
                "source": {"name": "source.xml",
                           "generation_time": "2018-07-05T02:07:03",
                           "validity_start": "2018-06-05T02:07:03",
                           "validity_stop": "2018-06-05T08:07:36"},
                "events": [{
                    "explicit_reference": "EXPLICIT_REFERENCE_EVENT",
                    "gauge": {"name": "GAUGE_NAME",
                              "system": "GAUGE_SYSTEM",
                              "insertion_type": "SIMPLE_UPDATE"},
                    "start": "2018-06-05T02:07:03",
                    "stop": "2018-06-05T08:07:36"
                }]
            }
        self.engine_gsdm.operation = data
        self.engine_gsdm._insert_dim_signature()
        self.engine_gsdm._insert_source()
        self.engine_gsdm._insert_gauges()
        self.engine_gsdm.session.commit()
        self.engine_gsdm._insert_explicit_refs()
        self.engine_gsdm.session.commit()
        gauge_ddbb = self.session.query(Gauge).filter(Gauge.name == data["events"][0]["gauge"]["name"], Gauge.system == data["events"][0]["gauge"]["system"]).first()
        source_ddbb = self.session.query(DimProcessing).filter(DimProcessing.name == data["source"]["name"], DimProcessing.validity_start == data["source"]["validity_start"], DimProcessing.validity_stop == data["source"]["validity_stop"], DimProcessing.generation_time == data["source"]["generation_time"], DimProcessing.dim_exec_version == data["dim_signature"]["version"]).first()
        dim_signature_ddbb = self.session.query(DimSignature).filter(DimSignature.dim_signature == data["dim_signature"]["name"], DimSignature.dim_exec_name == data["dim_signature"]["exec"]).first()
        explicit_reference_ddbb = self.session.query(ExplicitRef).filter(ExplicitRef.explicit_ref == data["events"][0]["explicit_reference"]).first()
        self.engine_gsdm._insert_events()
        self.engine_gsdm.session.commit()
        event_ddbb = self.session.query(Event).filter(Event.start == data["events"][0]["start"],
                                                      Event.stop == data["events"][0]["stop"],
                                                      Event.gauge_id == gauge_ddbb.gauge_id,
                                                      Event.processing_uuid == source_ddbb.processing_uuid,
                                                      Event.explicit_ref_id == explicit_reference_ddbb.explicit_ref_id,
                                                      Event.visible == True).all()

        assert len(event_ddbb) == 1

    def test_insert_event_start_gt_stop(self):

        self.engine_gsdm._initialize_context_insert_data()
        data = {"dim_signature": {"name": "dim_signature",
                                  "exec": "exec",
                                  "version": "1.0"},
                "source": {"name": "source.xml",
                           "generation_time": "2018-07-05T02:07:03",
                           "validity_start": "2018-06-05T02:07:03",
                           "validity_stop": "2018-06-05T08:07:36"},
                "events": [{
                    "gauge": {"name": "GAUGE_NAME",
                              "system": "GAUGE_SYSTEM",
                              "insertion_type": "SIMPLE_UPDATE"},
                    "start": "2018-06-07T02:07:03",
                    "stop": "2018-06-05T08:07:36"
                }]
            }
        self.engine_gsdm.operation = data
        self.engine_gsdm._insert_dim_signature()
        self.engine_gsdm._insert_source()
        self.engine_gsdm._insert_gauges()
        self.engine_gsdm.session.commit()
        try:
            self.engine_gsdm._insert_events()
        except WrongPeriod:
            pass

        event_ddbb = self.session.query(Event).all()

        assert len(event_ddbb) == 0


    def test_insert_event_period_out_of_validity(self):

        self.engine_gsdm._initialize_context_insert_data()
        data = {"dim_signature": {"name": "dim_signature",
                                  "exec": "exec",
                                  "version": "1.0"},
                "source": {"name": "source.xml",
                           "generation_time": "2018-07-05T02:07:03",
                           "validity_start": "2018-06-05T02:07:03",
                           "validity_stop": "2018-06-05T08:07:36"},
                "events": [{
                    "gauge": {"name": "GAUGE_NAME",
                              "system": "GAUGE_SYSTEM",
                              "insertion_type": "SIMPLE_UPDATE"},
                    "start": "2018-06-04T02:07:03",
                    "stop": "2018-06-05T08:07:36"
                }]
            }
        self.engine_gsdm.operation = data
        self.engine_gsdm._insert_dim_signature()
        self.engine_gsdm._insert_source()
        self.engine_gsdm._insert_gauges()
        self.engine_gsdm.session.commit()
        try:
            self.engine_gsdm._insert_events()
        except WrongPeriod:
            pass

        event_ddbb = self.session.query(Event).all()

        assert len(event_ddbb) == 0

    def test_insert_event_erase_and_replace(self):

        self.engine_gsdm._initialize_context_insert_data()
        data = {"dim_signature": {"name": "dim_signature",
                                  "exec": "exec",
                                  "version": "1.0"},
                "source": {"name": "source.xml",
                           "generation_time": "2018-07-05T02:07:03",
                           "validity_start": "2018-06-05T02:07:03",
                           "validity_stop": "2018-06-05T08:07:36"},
                "events": [{
                    "explicit_reference": "EXPLICIT_REFERENCE_EVENT",
                    "gauge": {"name": "GAUGE_NAME",
                              "system": "GAUGE_SYSTEM",
                              "insertion_type": "ERASE_and_REPLACE"},
                    "start": "2018-06-05T02:07:03",
                    "stop": "2018-06-05T08:07:36"
                }]
            }
        self.engine_gsdm.operation = data
        self.engine_gsdm._insert_dim_signature()
        self.engine_gsdm._insert_source()
        self.engine_gsdm._insert_gauges()
        self.engine_gsdm.session.commit()
        self.engine_gsdm._insert_explicit_refs()
        self.engine_gsdm.session.commit()
        gauge_ddbb = self.session.query(Gauge).filter(Gauge.name == data["events"][0]["gauge"]["name"], Gauge.system == data["events"][0]["gauge"]["system"]).first()
        source_ddbb = self.session.query(DimProcessing).filter(DimProcessing.name == data["source"]["name"], DimProcessing.validity_start == data["source"]["validity_start"], DimProcessing.validity_stop == data["source"]["validity_stop"], DimProcessing.generation_time == data["source"]["generation_time"], DimProcessing.dim_exec_version == data["dim_signature"]["version"]).first()
        dim_signature_ddbb = self.session.query(DimSignature).filter(DimSignature.dim_signature == data["dim_signature"]["name"], DimSignature.dim_exec_name == data["dim_signature"]["exec"]).first()
        explicit_reference_ddbb = self.session.query(ExplicitRef).filter(ExplicitRef.explicit_ref == data["events"][0]["explicit_reference"]).first()
        self.engine_gsdm._insert_events()
        self.engine_gsdm.session.commit()
        event_ddbb = self.session.query(Event).filter(Event.start == data["events"][0]["start"],
                                                      Event.stop == data["events"][0]["stop"],
                                                      Event.gauge_id == gauge_ddbb.gauge_id,
                                                      Event.processing_uuid == source_ddbb.processing_uuid,
                                                      Event.explicit_ref_id == explicit_reference_ddbb.explicit_ref_id,
                                                      Event.visible == False).all()

        assert len(event_ddbb) == 1


    def test_insert_event_event_keys(self):

        self.engine_gsdm._initialize_context_insert_data()
        data = {"dim_signature": {"name": "dim_signature",
                                  "exec": "exec",
                                  "version": "1.0"},
                "source": {"name": "source.xml",
                           "generation_time": "2018-07-05T02:07:03",
                           "validity_start": "2018-06-05T02:07:03",
                           "validity_stop": "2018-06-05T08:07:36"},
                "events": [{
                    "explicit_reference": "EXPLICIT_REFERENCE_EVENT",
                    "gauge": {"name": "GAUGE_NAME",
                              "system": "GAUGE_SYSTEM",
                              "insertion_type": "EVENT_KEYS"},
                    "start": "2018-06-05T02:07:03",
                    "stop": "2018-06-05T08:07:36",
                    "key": "EVENT_KEY"
                }]
            }
        self.engine_gsdm.operation = data
        self.engine_gsdm._insert_dim_signature()
        self.engine_gsdm._insert_source()
        self.engine_gsdm._insert_gauges()
        self.engine_gsdm.session.commit()
        self.engine_gsdm._insert_explicit_refs()
        self.engine_gsdm.session.commit()
        gauge_ddbb = self.session.query(Gauge).filter(Gauge.name == data["events"][0]["gauge"]["name"], Gauge.system == data["events"][0]["gauge"]["system"]).first()
        source_ddbb = self.session.query(DimProcessing).filter(DimProcessing.name == data["source"]["name"], DimProcessing.validity_start == data["source"]["validity_start"], DimProcessing.validity_stop == data["source"]["validity_stop"], DimProcessing.generation_time == data["source"]["generation_time"], DimProcessing.dim_exec_version == data["dim_signature"]["version"]).first()
        dim_signature_ddbb = self.session.query(DimSignature).filter(DimSignature.dim_signature == data["dim_signature"]["name"], DimSignature.dim_exec_name == data["dim_signature"]["exec"]).first()
        explicit_reference_ddbb = self.session.query(ExplicitRef).filter(ExplicitRef.explicit_ref == data["events"][0]["explicit_reference"]).first()
        self.engine_gsdm._insert_events()
        self.engine_gsdm.session.commit()
        event_ddbb = self.session.query(Event).filter(Event.start == data["events"][0]["start"],
                                                      Event.stop == data["events"][0]["stop"],
                                                      Event.gauge_id == gauge_ddbb.gauge_id,
                                                      Event.processing_uuid == source_ddbb.processing_uuid,
                                                      Event.explicit_ref_id == explicit_reference_ddbb.explicit_ref_id,
                                                      Event.visible == False).all()

        event_keys = self.session.query(EventKey).filter(EventKey.event_key == data["events"][0]["key"],
                                                         EventKey.event_uuid == event_ddbb[0].event_uuid,
                                                         EventKey.visible == False).all()

        assert len(event_keys) == 1

    def test_insert_event_simple_update_values(self):

        self.engine_gsdm._initialize_context_insert_data()
        data = {"dim_signature": {"name": "dim_signature",
                                  "exec": "exec",
                                  "version": "1.0"},
                "source": {"name": "source.xml",
                           "generation_time": "2018-07-05T02:07:03",
                           "validity_start": "2018-06-05T02:07:03",
                           "validity_stop": "2018-06-05T08:07:36"},
                "events": [{"gauge": {"name": "GAUGE_NAME",
                                      "system": "GAUGE_SYSTEM",
                                      "insertion_type": "SIMPLE_UPDATE"},
                            "start": "2018-06-05T02:07:03",
                            "stop": "2018-06-05T08:07:36",
                            "values": [{"name": "VALUES",
                                       "type": "object",
                                       "values": [
                                           {"type": "text",
                                            "name": "TEXT",
                                            "value": "TEXT"},
                                           {"type": "boolean",
                                            "name": "BOOLEAN",
                                            "value": "true"},
                                           {"type": "boolean",
                                            "name": "BOOLEAN2",
                                            "value": "false"},
                                           {"type": "double",
                                            "name": "DOUBLE",
                                            "value": "0.9"},
                                           {"type": "timestamp",
                                            "name": "TIMESTAMP",
                                            "value": "20180712T00:00:00"},
                                           {"type": "object",
                                            "name": "VALUES2",
                                            "values": [
                                                {"type": "geometry",
                                                 "name": "GEOMETRY",
                                                 "value": "29.012974905944 -118.33483458667 28.8650301641571 -118.372028380632 28.7171766138274 -118.409121707686 28.5693112139334 -118.44612300623 28.4213994944367 -118.483058731035 28.2734085660472 -118.519970531113 28.1253606038163 -118.556849863134 27.9772541759126 -118.593690316835 27.8291247153939 -118.630472520505 27.7544158362332 -118.64900551674 27.7282373644786 -118.48032600682 27.7015162098732 -118.314168792268 27.6742039940042 -118.150246300849 27.6462511775992 -117.98827485961 27.6176070520608 -117.827974178264 27.5882197156561 -117.669066835177 27.5580360448116 -117.511277813618 27.5270016492436 -117.354334035359 27.4950608291016 -117.197963963877 27.4621565093409 -117.041897175848 27.4282301711374 -116.885863967864 27.3932217651372 -116.729594956238 27.3570696128269 -116.572820673713 27.3197103000253 -116.415271199941 27.2810785491022 -116.256675748617 27.241107085821 -116.09676229722 27.1997272484913 -115.935260563566 27.1524952198729 -115.755436839005 27.2270348347386 -115.734960009089 27.3748346522356 -115.694299254844 27.5226008861849 -115.653563616829 27.6702779354428 -115.612760542177 27.8178690071708 -115.571901649363 27.9653506439026 -115.531000691074 28.1127600020619 -115.490011752733 28.2601469756437 -115.44890306179 28.4076546372628 -115.407649898021 28.455192866856 -115.589486631416 28.4968374106496 -115.752807970928 28.5370603096399 -115.91452902381 28.575931293904 -116.074924211465 28.6135193777855 -116.234273707691 28.6498895688451 -116.392847129762 28.6851057860975 -116.550917254638 28.7192301322012 -116.708756374584 28.752323018501 -116.866636764481 28.7844432583843 -117.024831047231 28.8156481533955 -117.183612605748 28.8459935678779 -117.343255995623 28.8755339855462 -117.504037348554 28.9043225601122 -117.666234808407 28.9324111491586 -117.830128960026 28.9598503481156 -117.996003330616 28.9866878706574 -118.164136222141 29.012974905944 -118.33483458667"}]
                                        }]}]
                }]
            }
        self.engine_gsdm.operation = data
        self.engine_gsdm._insert_dim_signature()
        self.engine_gsdm._insert_source()
        self.engine_gsdm._insert_gauges()
        self.engine_gsdm.session.commit()
        gauge_ddbb = self.session.query(Gauge).filter(Gauge.name == data["events"][0]["gauge"]["name"], Gauge.system == data["events"][0]["gauge"]["system"]).first()
        source_ddbb = self.session.query(DimProcessing).filter(DimProcessing.name == data["source"]["name"], DimProcessing.validity_start == data["source"]["validity_start"], DimProcessing.validity_stop == data["source"]["validity_stop"], DimProcessing.generation_time == data["source"]["generation_time"], DimProcessing.dim_exec_version == data["dim_signature"]["version"]).first()
        dim_signature_ddbb = self.session.query(DimSignature).filter(DimSignature.dim_signature == data["dim_signature"]["name"], DimSignature.dim_exec_name == data["dim_signature"]["exec"]).first()
        self.engine_gsdm._insert_events()
        self.engine_gsdm.session.commit()
        event_ddbb = self.session.query(Event).filter(Event.start == data["events"][0]["start"],
                                                      Event.stop == data["events"][0]["stop"],
                                                      Event.gauge_id == gauge_ddbb.gauge_id,
                                                      Event.processing_uuid == source_ddbb.processing_uuid,
                                                      Event.visible == True).all()

        assert len(event_ddbb) == 1

        values_ddbb = self.query_gsdm.get_event_values()

        assert len(values_ddbb) == 8

    def test_insert_event_simple_update_not_a_float_geometry(self):

        self.engine_gsdm._initialize_context_insert_data()
        data = {"dim_signature": {"name": "dim_signature",
                                  "exec": "exec",
                                  "version": "1.0"},
                "source": {"name": "source.xml",
                           "generation_time": "2018-07-05T02:07:03",
                           "validity_start": "2018-06-05T02:07:03",
                           "validity_stop": "2018-06-05T08:07:36"},
                "events": [{"gauge": {"name": "GAUGE_NAME",
                                      "system": "GAUGE_SYSTEM",
                                      "insertion_type": "SIMPLE_UPDATE"},
                            "start": "2018-06-05T02:07:03",
                            "stop": "2018-06-05T08:07:36",
                            "values": [{"name": "VALUES",
                                       "type": "object",
                                       "values": [
                                           {"type": "geometry",
                                            "name": "GEOMETRY",
                                            "value": "29.012974905944 NOT_A_FLOAT"}]
                                    }]
                        }]
            }
        self.engine_gsdm.operation = data
        self.engine_gsdm._insert_dim_signature()
        self.engine_gsdm._insert_source()
        self.engine_gsdm._insert_gauges()
        self.engine_gsdm.session.commit()
        gauge_ddbb = self.session.query(Gauge).filter(Gauge.name == data["events"][0]["gauge"]["name"], Gauge.system == data["events"][0]["gauge"]["system"]).first()
        source_ddbb = self.session.query(DimProcessing).filter(DimProcessing.name == data["source"]["name"], DimProcessing.validity_start == data["source"]["validity_start"], DimProcessing.validity_stop == data["source"]["validity_stop"], DimProcessing.generation_time == data["source"]["generation_time"], DimProcessing.dim_exec_version == data["dim_signature"]["version"]).first()
        dim_signature_ddbb = self.session.query(DimSignature).filter(DimSignature.dim_signature == data["dim_signature"]["name"], DimSignature.dim_exec_name == data["dim_signature"]["exec"]).first()
        try:
            self.engine_gsdm._insert_events()
        except WrongValue:
            self.session.rollback()
            pass
        event_ddbb = self.session.query(Event).all()

        assert len(event_ddbb) == 0

    def test_insert_event_simple_update_wrong_geometry(self):

        self.engine_gsdm._initialize_context_insert_data()
        data = {"dim_signature": {"name": "dim_signature",
                                  "exec": "exec",
                                  "version": "1.0"},
                "source": {"name": "source.xml",
                           "generation_time": "2018-07-05T02:07:03",
                           "validity_start": "2018-06-05T02:07:03",
                           "validity_stop": "2018-06-05T08:07:36"},
                "events": [{"gauge": {"name": "GAUGE_NAME",
                                      "system": "GAUGE_SYSTEM",
                                      "insertion_type": "SIMPLE_UPDATE"},
                            "start": "2018-06-05T02:07:03",
                            "stop": "2018-06-05T08:07:36",
                            "values": [{"name": "VALUES",
                                       "type": "object",
                                       "values": [
                                           {"type": "geometry",
                                            "name": "GEOMETRY",
                                            "value": "29.012974905944 -118.33483458667"}]
                                    }]
                        }]
            }
        self.engine_gsdm.operation = data
        self.engine_gsdm._insert_dim_signature()
        self.engine_gsdm._insert_source()
        self.engine_gsdm._insert_gauges()
        self.engine_gsdm.session.commit()
        gauge_ddbb = self.session.query(Gauge).filter(Gauge.name == data["events"][0]["gauge"]["name"], Gauge.system == data["events"][0]["gauge"]["system"]).first()
        source_ddbb = self.session.query(DimProcessing).filter(DimProcessing.name == data["source"]["name"], DimProcessing.validity_start == data["source"]["validity_start"], DimProcessing.validity_stop == data["source"]["validity_stop"], DimProcessing.generation_time == data["source"]["generation_time"], DimProcessing.dim_exec_version == data["dim_signature"]["version"]).first()
        dim_signature_ddbb = self.session.query(DimSignature).filter(DimSignature.dim_signature == data["dim_signature"]["name"], DimSignature.dim_exec_name == data["dim_signature"]["exec"]).first()
        try:
            self.engine_gsdm._insert_events()
        except WrongGeometry:
            self.session.rollback()
            pass
        event_ddbb = self.session.query(Event).all()

        assert len(event_ddbb) == 0

    def test_insert_event_simple_update_odd_geometry(self):

        self.engine_gsdm._initialize_context_insert_data()
        data = {"dim_signature": {"name": "dim_signature",
                                  "exec": "exec",
                                  "version": "1.0"},
                "source": {"name": "source.xml",
                           "generation_time": "2018-07-05T02:07:03",
                           "validity_start": "2018-06-05T02:07:03",
                           "validity_stop": "2018-06-05T08:07:36"},
                "events": [{"gauge": {"name": "GAUGE_NAME",
                                      "system": "GAUGE_SYSTEM",
                                      "insertion_type": "SIMPLE_UPDATE"},
                            "start": "2018-06-05T02:07:03",
                            "stop": "2018-06-05T08:07:36",
                            "values": [{"name": "VALUES",
                                       "type": "object",
                                       "values": [
                                           {"type": "geometry",
                                            "name": "ODD_GEOMETRY",
                                            "value": "29.012974905944"}]
                                    }]
                        }]
            }
        self.engine_gsdm.operation = data
        self.engine_gsdm._insert_dim_signature()
        self.engine_gsdm._insert_source()
        self.engine_gsdm._insert_gauges()
        self.engine_gsdm.session.commit()
        gauge_ddbb = self.session.query(Gauge).filter(Gauge.name == data["events"][0]["gauge"]["name"], Gauge.system == data["events"][0]["gauge"]["system"]).first()
        source_ddbb = self.session.query(DimProcessing).filter(DimProcessing.name == data["source"]["name"], DimProcessing.validity_start == data["source"]["validity_start"], DimProcessing.validity_stop == data["source"]["validity_stop"], DimProcessing.generation_time == data["source"]["generation_time"], DimProcessing.dim_exec_version == data["dim_signature"]["version"]).first()
        dim_signature_ddbb = self.session.query(DimSignature).filter(DimSignature.dim_signature == data["dim_signature"]["name"], DimSignature.dim_exec_name == data["dim_signature"]["exec"]).first()
        try:
            self.engine_gsdm._insert_events()
        except OddNumberOfCoordinates:
            self.session.rollback()
            pass
        event_ddbb = self.session.query(Event).all()

        assert len(event_ddbb) == 0

    def test_insert_event_simple_update_wrong_boolean(self):

        self.engine_gsdm._initialize_context_insert_data()
        data = {"dim_signature": {"name": "dim_signature",
                                  "exec": "exec",
                                  "version": "1.0"},
                "source": {"name": "source.xml",
                           "generation_time": "2018-07-05T02:07:03",
                           "validity_start": "2018-06-05T02:07:03",
                           "validity_stop": "2018-06-05T08:07:36"},
                "events": [{"gauge": {"name": "GAUGE_NAME",
                                      "system": "GAUGE_SYSTEM",
                                      "insertion_type": "SIMPLE_UPDATE"},
                            "start": "2018-06-05T02:07:03",
                            "stop": "2018-06-05T08:07:36",
                            "values": [{"name": "VALUES",
                                       "type": "object",
                                       "values": [
                                           {"type": "boolean",
                                            "name": "NOT_A_BOOLEAN",
                                            "value": "NOT_A_BOOLEAN"}]
                                    }]
                        }]
            }
        self.engine_gsdm.operation = data
        self.engine_gsdm._insert_dim_signature()
        self.engine_gsdm._insert_source()
        self.engine_gsdm._insert_gauges()
        self.engine_gsdm.session.commit()
        gauge_ddbb = self.session.query(Gauge).filter(Gauge.name == data["events"][0]["gauge"]["name"], Gauge.system == data["events"][0]["gauge"]["system"]).first()
        source_ddbb = self.session.query(DimProcessing).filter(DimProcessing.name == data["source"]["name"], DimProcessing.validity_start == data["source"]["validity_start"], DimProcessing.validity_stop == data["source"]["validity_stop"], DimProcessing.generation_time == data["source"]["generation_time"], DimProcessing.dim_exec_version == data["dim_signature"]["version"]).first()
        dim_signature_ddbb = self.session.query(DimSignature).filter(DimSignature.dim_signature == data["dim_signature"]["name"], DimSignature.dim_exec_name == data["dim_signature"]["exec"]).first()
        try:
            self.engine_gsdm._insert_events()
        except WrongValue:
            self.session.rollback()
            pass
        event_ddbb = self.session.query(Event).all()

        assert len(event_ddbb) == 0

    def test_insert_event_simple_update_wrong_double(self):

        self.engine_gsdm._initialize_context_insert_data()
        data = {"dim_signature": {"name": "dim_signature",
                                  "exec": "exec",
                                  "version": "1.0"},
                "source": {"name": "source.xml",
                           "generation_time": "2018-07-05T02:07:03",
                           "validity_start": "2018-06-05T02:07:03",
                           "validity_stop": "2018-06-05T08:07:36"},
                "events": [{"gauge": {"name": "GAUGE_NAME",
                                      "system": "GAUGE_SYSTEM",
                                      "insertion_type": "SIMPLE_UPDATE"},
                            "start": "2018-06-05T02:07:03",
                            "stop": "2018-06-05T08:07:36",
                            "values": [{"name": "VALUES",
                                       "type": "object",
                                       "values": [
                                           {"type": "double",
                                            "name": "NOT_A_DOUBLE",
                                            "value": "NOT_A_DOUBLE"}]
                                    }]
                        }]
            }
        self.engine_gsdm.operation = data
        self.engine_gsdm._insert_dim_signature()
        self.engine_gsdm._insert_source()
        self.engine_gsdm._insert_gauges()
        self.engine_gsdm.session.commit()
        gauge_ddbb = self.session.query(Gauge).filter(Gauge.name == data["events"][0]["gauge"]["name"], Gauge.system == data["events"][0]["gauge"]["system"]).first()
        source_ddbb = self.session.query(DimProcessing).filter(DimProcessing.name == data["source"]["name"], DimProcessing.validity_start == data["source"]["validity_start"], DimProcessing.validity_stop == data["source"]["validity_stop"], DimProcessing.generation_time == data["source"]["generation_time"], DimProcessing.dim_exec_version == data["dim_signature"]["version"]).first()
        dim_signature_ddbb = self.session.query(DimSignature).filter(DimSignature.dim_signature == data["dim_signature"]["name"], DimSignature.dim_exec_name == data["dim_signature"]["exec"]).first()
        try:
            self.engine_gsdm._insert_events()
        except WrongValue:
            self.session.rollback()
            pass
        event_ddbb = self.session.query(Event).all()

        assert len(event_ddbb) == 0

    def test_insert_event_simple_update_wrong_timestamp(self):

        self.engine_gsdm._initialize_context_insert_data()
        data = {"dim_signature": {"name": "dim_signature",
                                  "exec": "exec",
                                  "version": "1.0"},
                "source": {"name": "source.xml",
                           "generation_time": "2018-07-05T02:07:03",
                           "validity_start": "2018-06-05T02:07:03",
                           "validity_stop": "2018-06-05T08:07:36"},
                "events": [{"gauge": {"name": "GAUGE_NAME",
                                      "system": "GAUGE_SYSTEM",
                                      "insertion_type": "SIMPLE_UPDATE"},
                            "start": "2018-06-05T02:07:03",
                            "stop": "2018-06-05T08:07:36",
                            "values": [{"name": "VALUES",
                                       "type": "object",
                                       "values": [
                                           {"type": "timestamp",
                                            "name": "NOT_A_timestamp",
                                            "value": "NOT_A_TIMESTAMP"}]
                                    }]
                        }]
            }
        self.engine_gsdm.operation = data
        self.engine_gsdm._insert_dim_signature()
        self.engine_gsdm._insert_source()
        self.engine_gsdm._insert_gauges()
        self.engine_gsdm.session.commit()
        gauge_ddbb = self.session.query(Gauge).filter(Gauge.name == data["events"][0]["gauge"]["name"], Gauge.system == data["events"][0]["gauge"]["system"]).first()
        source_ddbb = self.session.query(DimProcessing).filter(DimProcessing.name == data["source"]["name"], DimProcessing.validity_start == data["source"]["validity_start"], DimProcessing.validity_stop == data["source"]["validity_stop"], DimProcessing.generation_time == data["source"]["generation_time"], DimProcessing.dim_exec_version == data["dim_signature"]["version"]).first()
        dim_signature_ddbb = self.session.query(DimSignature).filter(DimSignature.dim_signature == data["dim_signature"]["name"], DimSignature.dim_exec_name == data["dim_signature"]["exec"]).first()
        try:
            self.engine_gsdm._insert_events()
        except WrongValue:
            self.session.rollback()
            pass
        event_ddbb = self.session.query(Event).all()

        assert len(event_ddbb) == 0

    def test_insert_event_links(self):

        self.engine_gsdm._initialize_context_insert_data()
        data = {"dim_signature": {"name": "dim_signature",
                                  "exec": "exec",
                                  "version": "1.0"},
                "source": {"name": "source.xml",
                           "generation_time": "2018-07-05T02:07:03",
                           "validity_start": "2018-06-05T02:07:03",
                           "validity_stop": "2018-06-05T08:07:36"},
                "events": [{
                    "gauge": {"name": "GAUGE_NAME",
                              "system": "GAUGE_SYSTEM",
                              "insertion_type": "SIMPLE_UPDATE"},
                    "start": "2018-06-05T02:07:03",
                    "stop": "2018-06-05T08:07:36",
                    "links": [{
                        "link": "EVENT_LINK",
                        "link_mode": "by_ref",
                        "name": "EVENT_LINK_NAME"
                    }]
                },
                {
                    "gauge": {"name": "GAUGE_NAME",
                              "system": "GAUGE_SYSTEM",
                              "insertion_type": "SIMPLE_UPDATE"},
                    "start": "2018-06-05T02:07:03",
                    "stop": "2018-06-05T08:07:36",
                    "links": [{
                        "link": "EVENT_LINK",
                        "link_mode": "by_ref",
                        "name": "EVENT_LINK_NAME"
                    }]
                }]
            }
        self.engine_gsdm.operation = data
        self.engine_gsdm._insert_dim_signature()
        self.engine_gsdm._insert_source()
        self.engine_gsdm._insert_gauges()
        self.engine_gsdm.session.commit()
        self.engine_gsdm._insert_events()
        self.engine_gsdm.session.commit()
        events_ddbb = self.session.query(Event).all()

        assert len(events_ddbb) == 2

        self.engine_gsdm._initialize_context_insert_data()
        data = {"dim_signature": {"name": "dim_signature",
                                  "exec": "exec",
                                  "version": "1.0"},
                "source": {"name": "source2.xml",
                           "generation_time": "2018-07-05T02:07:03",
                           "validity_start": "2018-06-05T02:07:03",
                           "validity_stop": "2018-06-05T08:07:36"},
                "events": [{
                    "gauge": {"name": "GAUGE_NAME",
                              "system": "GAUGE_SYSTEM",
                              "insertion_type": "SIMPLE_UPDATE"},
                    "start": "2018-06-05T03:07:03",
                    "stop": "2018-06-05T08:07:36",
                    "links": [{
                        "link": events_ddbb[0].event_uuid,
                        "link_mode": "by_uuid",
                        "name": "EVENT_LINK_NAME"
                    }]
                }]
            }
        self.engine_gsdm.operation = data
        self.engine_gsdm._insert_dim_signature()
        self.engine_gsdm._insert_source()
        self.engine_gsdm._insert_gauges()
        self.engine_gsdm.session.commit()
        self.engine_gsdm._insert_events()
        self.engine_gsdm.session.commit()

        event_ddbb = self.session.query(Event).filter(Event.start == data["events"][0]["start"]).all()

        assert len(event_ddbb) == 1

        link_ddbb_1 = self.session.query(EventLink).filter(EventLink.event_uuid_link == events_ddbb[0].event_uuid, EventLink.event_uuid == events_ddbb[1].event_uuid).all()

        assert len(link_ddbb_1) == 1

        link_ddbb_2 = self.session.query(EventLink).filter(EventLink.event_uuid_link == events_ddbb[1].event_uuid, EventLink.event_uuid == events_ddbb[0].event_uuid).all()

        assert len(link_ddbb_2) == 1

        link_ddbb_3 = self.session.query(EventLink).filter(EventLink.event_uuid_link == event_ddbb[0].event_uuid, EventLink.event_uuid == events_ddbb[0].event_uuid).all()

        assert len(link_ddbb_3) == 1

    def test_insert_event_incomplete_links(self):

        self.engine_gsdm._initialize_context_insert_data()
        data = {"dim_signature": {"name": "dim_signature",
                                  "exec": "exec",
                                  "version": "1.0"},
                "source": {"name": "source.xml",
                           "generation_time": "2018-07-05T02:07:03",
                           "validity_start": "2018-06-05T02:07:03",
                           "validity_stop": "2018-06-05T08:07:36"},
                "events": [{
                    "gauge": {"name": "GAUGE_NAME",
                              "system": "GAUGE_SYSTEM",
                              "insertion_type": "SIMPLE_UPDATE"},
                    "start": "2018-06-05T02:07:03",
                    "stop": "2018-06-05T08:07:36",
                    "links": [{
                        "link": "EVENT_LINK",
                        "link_mode": "by_ref",
                        "name": "EVENT_LINK_NAME"
                    }]
                }]
            }
        self.engine_gsdm.operation = data
        self.engine_gsdm._insert_dim_signature()
        self.engine_gsdm._insert_source()
        self.engine_gsdm._insert_gauges()
        self.engine_gsdm.session.commit()
        gauge_ddbb = self.session.query(Gauge).filter(Gauge.name == data["events"][0]["gauge"]["name"], Gauge.system == data["events"][0]["gauge"]["system"]).first()
        source_ddbb = self.session.query(DimProcessing).filter(DimProcessing.name == data["source"]["name"], DimProcessing.validity_start == data["source"]["validity_start"], DimProcessing.validity_stop == data["source"]["validity_stop"], DimProcessing.generation_time == data["source"]["generation_time"], DimProcessing.dim_exec_version == data["dim_signature"]["version"]).first()
        dim_signature_ddbb = self.session.query(DimSignature).filter(DimSignature.dim_signature == data["dim_signature"]["name"], DimSignature.dim_exec_name == data["dim_signature"]["exec"]).first()
        try:
            self.engine_gsdm._insert_events()
        except WrongEventLink:
            self.session.rollback()
            pass
        event_ddbb = self.session.query(Event).all()

        assert len(event_ddbb) == 0

    def test_insert_annotations(self):

        self.engine_gsdm._initialize_context_insert_data()
        data = {"dim_signature": {"name": "dim_signature",
                                  "exec": "exec",
                                  "version": "1.0"},
                "source": {"name": "source.xml",
                           "generation_time": "2018-07-05T02:07:03",
                           "validity_start": "2018-06-05T02:07:03",
                           "validity_stop": "2018-06-05T08:07:36"},
                "annotations": [{
                    "explicit_reference": "EXPLICIT_REFERENCE_ANNOTATION",
                    "annotation_cnf": {"name": "NAME",
                                       "system": "SYSTEM"},
                            "values": [{"name": "VALUES",
                                       "type": "object",
                                       "values": [
                                           {"type": "text",
                                            "name": "TEXT",
                                            "value": "TEXT"},
                                           {"type": "boolean",
                                            "name": "BOOLEAN",
                                            "value": "true"},
                                           {"type": "boolean",
                                            "name": "BOOLEAN2",
                                            "value": "false"},
                                           {"type": "double",
                                            "name": "DOUBLE",
                                            "value": "0.9"},
                                           {"type": "timestamp",
                                            "name": "TIMESTAMP",
                                            "value": "20180712T00:00:00"},
                                           {"type": "object",
                                            "name": "VALUES2",
                                            "values": [
                                                {"type": "geometry",
                                                 "name": "GEOMETRY",
                                                 "value": "29.012974905944 -118.33483458667 28.8650301641571 -118.372028380632 28.7171766138274 -118.409121707686 28.5693112139334 -118.44612300623 28.4213994944367 -118.483058731035 28.2734085660472 -118.519970531113 28.1253606038163 -118.556849863134 27.9772541759126 -118.593690316835 27.8291247153939 -118.630472520505 27.7544158362332 -118.64900551674 27.7282373644786 -118.48032600682 27.7015162098732 -118.314168792268 27.6742039940042 -118.150246300849 27.6462511775992 -117.98827485961 27.6176070520608 -117.827974178264 27.5882197156561 -117.669066835177 27.5580360448116 -117.511277813618 27.5270016492436 -117.354334035359 27.4950608291016 -117.197963963877 27.4621565093409 -117.041897175848 27.4282301711374 -116.885863967864 27.3932217651372 -116.729594956238 27.3570696128269 -116.572820673713 27.3197103000253 -116.415271199941 27.2810785491022 -116.256675748617 27.241107085821 -116.09676229722 27.1997272484913 -115.935260563566 27.1524952198729 -115.755436839005 27.2270348347386 -115.734960009089 27.3748346522356 -115.694299254844 27.5226008861849 -115.653563616829 27.6702779354428 -115.612760542177 27.8178690071708 -115.571901649363 27.9653506439026 -115.531000691074 28.1127600020619 -115.490011752733 28.2601469756437 -115.44890306179 28.4076546372628 -115.407649898021 28.455192866856 -115.589486631416 28.4968374106496 -115.752807970928 28.5370603096399 -115.91452902381 28.575931293904 -116.074924211465 28.6135193777855 -116.234273707691 28.6498895688451 -116.392847129762 28.6851057860975 -116.550917254638 28.7192301322012 -116.708756374584 28.752323018501 -116.866636764481 28.7844432583843 -117.024831047231 28.8156481533955 -117.183612605748 28.8459935678779 -117.343255995623 28.8755339855462 -117.504037348554 28.9043225601122 -117.666234808407 28.9324111491586 -117.830128960026 28.9598503481156 -117.996003330616 28.9866878706574 -118.164136222141 29.012974905944 -118.33483458667"}]
                                        }]}]
                }]
            }
        self.engine_gsdm.operation = data
        self.engine_gsdm._insert_dim_signature()
        self.engine_gsdm._insert_source()
        self.engine_gsdm._insert_explicit_refs()
        self.engine_gsdm.session.commit()
        self.engine_gsdm._insert_annotation_cnfs()
        self.engine_gsdm.session.commit()
        self.engine_gsdm._insert_annotations()
        self.engine_gsdm.session.commit()
        annotation_cnf_ddbb = self.session.query(AnnotationCnf).filter(AnnotationCnf.name == data["annotations"][0]["annotation_cnf"]["name"], AnnotationCnf.system == data["annotations"][0]["annotation_cnf"]["system"]).first()
        source_ddbb = self.session.query(DimProcessing).filter(DimProcessing.name == data["source"]["name"], DimProcessing.validity_start == data["source"]["validity_start"], DimProcessing.validity_stop == data["source"]["validity_stop"], DimProcessing.generation_time == data["source"]["generation_time"], DimProcessing.dim_exec_version == data["dim_signature"]["version"]).first()
        dim_signature_ddbb = self.session.query(DimSignature).filter(DimSignature.dim_signature == data["dim_signature"]["name"], DimSignature.dim_exec_name == data["dim_signature"]["exec"]).first()
        dim_signature_ddbb = self.session.query(DimSignature).filter(DimSignature.dim_signature == data["dim_signature"]["name"], DimSignature.dim_exec_name == data["dim_signature"]["exec"]).first()
        explicit_reference_ddbb = self.session.query(ExplicitRef).filter(ExplicitRef.explicit_ref == data["annotations"][0]["explicit_reference"]).first()
        annotation_ddbb = self.session.query(Annotation).filter(Annotation.visible == False, Annotation.explicit_ref_id == explicit_reference_ddbb.explicit_ref_id,
                                                                Annotation.processing_uuid == source_ddbb.processing_uuid, Annotation.annotation_cnf_id == annotation_cnf_ddbb.annotation_cnf_id).all()

        assert len(annotation_ddbb) == 1

        values_ddbb = self.query_gsdm.get_annotation_values()

        assert len(values_ddbb) == 8

    def test_remove_deprecated_event_keys(self):

        self.engine_gsdm._initialize_context_insert_data()
        data = {"dim_signature": {"name": "dim_signature",
                                  "exec": "exec",
                                  "version": "1.0"},
                "source": {"name": "source.xml",
                           "generation_time": "2018-07-05T02:07:03",
                           "validity_start": "2018-06-05T02:07:03",
                           "validity_stop": "2018-06-05T08:07:36"},
                "events": [{
                    "explicit_reference": "EXPLICIT_REFERENCE_EVENT1",
                    "gauge": {"name": "GAUGE_NAME1",
                              "system": "GAUGE_SYSTEM1",
                              "insertion_type": "EVENT_KEYS"},
                    "start": "2018-06-05T02:07:03",
                    "stop": "2018-06-05T08:07:36",
                    "key": "EVENT_KEY",
                    "values": [{"name": "VALUES",
                                "type": "object",
                                "values": [
                                    {"type": "text",
                                     "name": "TEXT",
                                     "value": "TEXT"},
                                    {"type": "boolean",
                                     "name": "BOOLEAN",
                                     "value": "true"},
                                    {"type": "boolean",
                                     "name": "BOOLEAN2",
                                     "value": "false"},
                                    {"type": "double",
                                     "name": "DOUBLE",
                                     "value": "0.9"},
                                    {"type": "timestamp",
                                     "name": "TIMESTAMP",
                                     "value": "20180712T00:00:00"},
                                    {"type": "object",
                                     "name": "VALUES2",
                                     "values": [
                                         {"type": "geometry",
                                          "name": "GEOMETRY",
                                          "value": "29.012974905944 -118.33483458667 28.8650301641571 -118.372028380632 28.7171766138274 -118.409121707686 28.5693112139334 -118.44612300623 28.4213994944367 -118.483058731035 28.2734085660472 -118.519970531113 28.1253606038163 -118.556849863134 27.9772541759126 -118.593690316835 27.8291247153939 -118.630472520505 27.7544158362332 -118.64900551674 27.7282373644786 -118.48032600682 27.7015162098732 -118.314168792268 27.6742039940042 -118.150246300849 27.6462511775992 -117.98827485961 27.6176070520608 -117.827974178264 27.5882197156561 -117.669066835177 27.5580360448116 -117.511277813618 27.5270016492436 -117.354334035359 27.4950608291016 -117.197963963877 27.4621565093409 -117.041897175848 27.4282301711374 -116.885863967864 27.3932217651372 -116.729594956238 27.3570696128269 -116.572820673713 27.3197103000253 -116.415271199941 27.2810785491022 -116.256675748617 27.241107085821 -116.09676229722 27.1997272484913 -115.935260563566 27.1524952198729 -115.755436839005 27.2270348347386 -115.734960009089 27.3748346522356 -115.694299254844 27.5226008861849 -115.653563616829 27.6702779354428 -115.612760542177 27.8178690071708 -115.571901649363 27.9653506439026 -115.531000691074 28.1127600020619 -115.490011752733 28.2601469756437 -115.44890306179 28.4076546372628 -115.407649898021 28.455192866856 -115.589486631416 28.4968374106496 -115.752807970928 28.5370603096399 -115.91452902381 28.575931293904 -116.074924211465 28.6135193777855 -116.234273707691 28.6498895688451 -116.392847129762 28.6851057860975 -116.550917254638 28.7192301322012 -116.708756374584 28.752323018501 -116.866636764481 28.7844432583843 -117.024831047231 28.8156481533955 -117.183612605748 28.8459935678779 -117.343255995623 28.8755339855462 -117.504037348554 28.9043225601122 -117.666234808407 28.9324111491586 -117.830128960026 28.9598503481156 -117.996003330616 28.9866878706574 -118.164136222141 29.012974905944 -118.33483458667"}]
                                 }]}]
                }]
            }
        self.engine_gsdm.operation = data
        self.engine_gsdm._insert_dim_signature()
        self.engine_gsdm._insert_source()
        self.engine_gsdm._insert_gauges()
        self.engine_gsdm.session.commit()
        self.engine_gsdm._insert_explicit_refs()
        self.engine_gsdm.session.commit()
        self.engine_gsdm._insert_events()
        self.engine_gsdm.session.commit()
        self.engine_gsdm._initialize_context_insert_data()
        data = {"dim_signature": {"name": "dim_signature",
                                  "exec": "exec",
                                  "version": "1.0"},
                "source": {"name": "source2.xml",
                           "generation_time": "2018-07-05T02:07:04",
                           "validity_start": "2018-06-05T02:07:03",
                           "validity_stop": "2018-06-05T08:07:36"},
                "events": [{
                    "explicit_reference": "EXPLICIT_REFERENCE_EVENT2",
                    "gauge": {"name": "GAUGE_NAME2",
                              "system": "GAUGE_SYSTEM2",
                              "insertion_type": "EVENT_KEYS"},
                    "start": "2018-06-05T02:07:03",
                    "stop": "2018-06-05T08:07:36",
                    "key": "EVENT_KEY",
                    "values": [{"name": "VALUES",
                                "type": "object",
                                "values": [
                                    {"type": "text",
                                     "name": "TEXT",
                                     "value": "TEXT"},
                                    {"type": "boolean",
                                     "name": "BOOLEAN",
                                     "value": "true"},
                                    {"type": "boolean",
                                     "name": "BOOLEAN2",
                                     "value": "false"},
                                    {"type": "double",
                                     "name": "DOUBLE",
                                     "value": "0.9"},
                                    {"type": "timestamp",
                                     "name": "TIMESTAMP",
                                     "value": "20180712T00:00:00"},
                                    {"type": "object",
                                     "name": "VALUES2",
                                     "values": [
                                         {"type": "geometry",
                                          "name": "GEOMETRY",
                                          "value": "29.012974905944 -118.33483458667 28.8650301641571 -118.372028380632 28.7171766138274 -118.409121707686 28.5693112139334 -118.44612300623 28.4213994944367 -118.483058731035 28.2734085660472 -118.519970531113 28.1253606038163 -118.556849863134 27.9772541759126 -118.593690316835 27.8291247153939 -118.630472520505 27.7544158362332 -118.64900551674 27.7282373644786 -118.48032600682 27.7015162098732 -118.314168792268 27.6742039940042 -118.150246300849 27.6462511775992 -117.98827485961 27.6176070520608 -117.827974178264 27.5882197156561 -117.669066835177 27.5580360448116 -117.511277813618 27.5270016492436 -117.354334035359 27.4950608291016 -117.197963963877 27.4621565093409 -117.041897175848 27.4282301711374 -116.885863967864 27.3932217651372 -116.729594956238 27.3570696128269 -116.572820673713 27.3197103000253 -116.415271199941 27.2810785491022 -116.256675748617 27.241107085821 -116.09676229722 27.1997272484913 -115.935260563566 27.1524952198729 -115.755436839005 27.2270348347386 -115.734960009089 27.3748346522356 -115.694299254844 27.5226008861849 -115.653563616829 27.6702779354428 -115.612760542177 27.8178690071708 -115.571901649363 27.9653506439026 -115.531000691074 28.1127600020619 -115.490011752733 28.2601469756437 -115.44890306179 28.4076546372628 -115.407649898021 28.455192866856 -115.589486631416 28.4968374106496 -115.752807970928 28.5370603096399 -115.91452902381 28.575931293904 -116.074924211465 28.6135193777855 -116.234273707691 28.6498895688451 -116.392847129762 28.6851057860975 -116.550917254638 28.7192301322012 -116.708756374584 28.752323018501 -116.866636764481 28.7844432583843 -117.024831047231 28.8156481533955 -117.183612605748 28.8459935678779 -117.343255995623 28.8755339855462 -117.504037348554 28.9043225601122 -117.666234808407 28.9324111491586 -117.830128960026 28.9598503481156 -117.996003330616 28.9866878706574 -118.164136222141 29.012974905944 -118.33483458667"}]
                                 }]}]
                }]
            }
        self.engine_gsdm.operation = data
        self.engine_gsdm._insert_dim_signature()
        self.engine_gsdm._insert_source()
        self.engine_gsdm._insert_gauges()
        self.engine_gsdm.session.commit()
        self.engine_gsdm._insert_explicit_refs()
        self.engine_gsdm.session.commit()
        self.engine_gsdm._insert_events()
        self.engine_gsdm.session.commit()
        self.engine_gsdm._remove_deprecated_events_event_keys()
        self.engine_gsdm.session.commit()
        gauge_ddbb = self.session.query(Gauge).filter(Gauge.name == data["events"][0]["gauge"]["name"], Gauge.system == data["events"][0]["gauge"]["system"]).first()
        source_ddbb = self.session.query(DimProcessing).filter(DimProcessing.name == data["source"]["name"], DimProcessing.validity_start == data["source"]["validity_start"], DimProcessing.validity_stop == data["source"]["validity_stop"], DimProcessing.generation_time == data["source"]["generation_time"], DimProcessing.dim_exec_version == data["dim_signature"]["version"]).first()
        dim_signature_ddbb = self.session.query(DimSignature).filter(DimSignature.dim_signature == data["dim_signature"]["name"], DimSignature.dim_exec_name == data["dim_signature"]["exec"]).first()
        explicit_reference_ddbb = self.session.query(ExplicitRef).filter(ExplicitRef.explicit_ref == data["events"][0]["explicit_reference"]).first()

        event_ddbb = self.session.query(Event).filter(Event.start == data["events"][0]["start"],
                                                      Event.stop == data["events"][0]["stop"],
                                                      Event.gauge_id == gauge_ddbb.gauge_id,
                                                      Event.processing_uuid == source_ddbb.processing_uuid,
                                                      Event.explicit_ref_id == explicit_reference_ddbb.explicit_ref_id,
                                                      Event.visible == True).all()

        event_keys = self.session.query(EventKey).filter(EventKey.event_key == data["events"][0]["key"],
                                                         EventKey.event_uuid == event_ddbb[0].event_uuid,
                                                         EventKey.visible == True).all()

        assert len(event_keys) == 1

        events_ddbb = self.session.query(Event).all()

        assert len(events_ddbb) == 1

        values_ddbb = self.query_gsdm.get_event_values()

        assert len(values_ddbb) == 8

    def test_deprecated_annotations(self):

        self.engine_gsdm._initialize_context_insert_data()
        data = {"dim_signature": {"name": "dim_signature",
                                  "exec": "exec",
                                  "version": "1.0"},
                "source": {"name": "source.xml",
                           "generation_time": "2018-07-05T02:07:03",
                           "validity_start": "2018-06-05T02:07:03",
                           "validity_stop": "2018-06-05T08:07:36"},
                "annotations": [{
                    "explicit_reference": "EXPLICIT_REFERENCE_ANNOTATION",
                    "annotation_cnf": {"name": "NAME",
                                       "system": "SYSTEM"},
                    "values": [{"name": "VALUES",
                                "type": "object",
                                "values": [
                                    {"type": "text",
                                     "name": "TEXT",
                                     "value": "TEXT"},
                                    {"type": "boolean",
                                     "name": "BOOLEAN",
                                     "value": "true"},
                                    {"type": "boolean",
                                     "name": "BOOLEAN2",
                                     "value": "false"},
                                    {"type": "double",
                                     "name": "DOUBLE",
                                     "value": "0.9"},
                                    {"type": "timestamp",
                                     "name": "TIMESTAMP",
                                     "value": "20180712T00:00:00"},
                                    {"type": "object",
                                     "name": "VALUES2",
                                     "values": [
                                         {"type": "geometry",
                                          "name": "GEOMETRY",
                                          "value": "29.012974905944 -118.33483458667 28.8650301641571 -118.372028380632 28.7171766138274 -118.409121707686 28.5693112139334 -118.44612300623 28.4213994944367 -118.483058731035 28.2734085660472 -118.519970531113 28.1253606038163 -118.556849863134 27.9772541759126 -118.593690316835 27.8291247153939 -118.630472520505 27.7544158362332 -118.64900551674 27.7282373644786 -118.48032600682 27.7015162098732 -118.314168792268 27.6742039940042 -118.150246300849 27.6462511775992 -117.98827485961 27.6176070520608 -117.827974178264 27.5882197156561 -117.669066835177 27.5580360448116 -117.511277813618 27.5270016492436 -117.354334035359 27.4950608291016 -117.197963963877 27.4621565093409 -117.041897175848 27.4282301711374 -116.885863967864 27.3932217651372 -116.729594956238 27.3570696128269 -116.572820673713 27.3197103000253 -116.415271199941 27.2810785491022 -116.256675748617 27.241107085821 -116.09676229722 27.1997272484913 -115.935260563566 27.1524952198729 -115.755436839005 27.2270348347386 -115.734960009089 27.3748346522356 -115.694299254844 27.5226008861849 -115.653563616829 27.6702779354428 -115.612760542177 27.8178690071708 -115.571901649363 27.9653506439026 -115.531000691074 28.1127600020619 -115.490011752733 28.2601469756437 -115.44890306179 28.4076546372628 -115.407649898021 28.455192866856 -115.589486631416 28.4968374106496 -115.752807970928 28.5370603096399 -115.91452902381 28.575931293904 -116.074924211465 28.6135193777855 -116.234273707691 28.6498895688451 -116.392847129762 28.6851057860975 -116.550917254638 28.7192301322012 -116.708756374584 28.752323018501 -116.866636764481 28.7844432583843 -117.024831047231 28.8156481533955 -117.183612605748 28.8459935678779 -117.343255995623 28.8755339855462 -117.504037348554 28.9043225601122 -117.666234808407 28.9324111491586 -117.830128960026 28.9598503481156 -117.996003330616 28.9866878706574 -118.164136222141 29.012974905944 -118.33483458667"}]
                                 }]}]
                }]
            }
        self.engine_gsdm.operation = data
        self.engine_gsdm._insert_dim_signature()
        self.engine_gsdm._insert_source()
        self.engine_gsdm._insert_explicit_refs()
        self.engine_gsdm.session.commit()
        self.engine_gsdm._insert_annotation_cnfs()
        self.engine_gsdm.session.commit()
        self.engine_gsdm._insert_annotations()
        self.engine_gsdm.session.commit()
        self.engine_gsdm._initialize_context_insert_data()
        data = {"dim_signature": {"name": "dim_signature",
                                  "exec": "exec",
                                  "version": "1.0"},
                "source": {"name": "source2.xml",
                           "generation_time": "2018-07-05T02:07:04",
                           "validity_start": "2018-06-05T02:07:03",
                           "validity_stop": "2018-06-05T08:07:36"},
                "annotations": [{
                    "explicit_reference": "EXPLICIT_REFERENCE_ANNOTATION",
                    "annotation_cnf": {"name": "NAME",
                                       "system": "SYSTEM"},
                    "values": [{"name": "VALUES",
                                "type": "object",
                                "values": [
                                    {"type": "text",
                                     "name": "TEXT",
                                     "value": "TEXT"},
                                    {"type": "boolean",
                                     "name": "BOOLEAN",
                                     "value": "true"},
                                    {"type": "boolean",
                                     "name": "BOOLEAN2",
                                     "value": "false"},
                                    {"type": "double",
                                     "name": "DOUBLE",
                                     "value": "0.9"},
                                    {"type": "timestamp",
                                     "name": "TIMESTAMP",
                                     "value": "20180712T00:00:00"},
                                    {"type": "object",
                                     "name": "VALUES2",
                                     "values": [
                                         {"type": "geometry",
                                          "name": "GEOMETRY",
                                          "value": "29.012974905944 -118.33483458667 28.8650301641571 -118.372028380632 28.7171766138274 -118.409121707686 28.5693112139334 -118.44612300623 28.4213994944367 -118.483058731035 28.2734085660472 -118.519970531113 28.1253606038163 -118.556849863134 27.9772541759126 -118.593690316835 27.8291247153939 -118.630472520505 27.7544158362332 -118.64900551674 27.7282373644786 -118.48032600682 27.7015162098732 -118.314168792268 27.6742039940042 -118.150246300849 27.6462511775992 -117.98827485961 27.6176070520608 -117.827974178264 27.5882197156561 -117.669066835177 27.5580360448116 -117.511277813618 27.5270016492436 -117.354334035359 27.4950608291016 -117.197963963877 27.4621565093409 -117.041897175848 27.4282301711374 -116.885863967864 27.3932217651372 -116.729594956238 27.3570696128269 -116.572820673713 27.3197103000253 -116.415271199941 27.2810785491022 -116.256675748617 27.241107085821 -116.09676229722 27.1997272484913 -115.935260563566 27.1524952198729 -115.755436839005 27.2270348347386 -115.734960009089 27.3748346522356 -115.694299254844 27.5226008861849 -115.653563616829 27.6702779354428 -115.612760542177 27.8178690071708 -115.571901649363 27.9653506439026 -115.531000691074 28.1127600020619 -115.490011752733 28.2601469756437 -115.44890306179 28.4076546372628 -115.407649898021 28.455192866856 -115.589486631416 28.4968374106496 -115.752807970928 28.5370603096399 -115.91452902381 28.575931293904 -116.074924211465 28.6135193777855 -116.234273707691 28.6498895688451 -116.392847129762 28.6851057860975 -116.550917254638 28.7192301322012 -116.708756374584 28.752323018501 -116.866636764481 28.7844432583843 -117.024831047231 28.8156481533955 -117.183612605748 28.8459935678779 -117.343255995623 28.8755339855462 -117.504037348554 28.9043225601122 -117.666234808407 28.9324111491586 -117.830128960026 28.9598503481156 -117.996003330616 28.9866878706574 -118.164136222141 29.012974905944 -118.33483458667"}]
                                 }]}]
                }]
            }
        self.engine_gsdm.operation = data
        self.engine_gsdm._insert_dim_signature()
        self.engine_gsdm._insert_source()
        self.engine_gsdm._insert_explicit_refs()
        self.engine_gsdm.session.commit()
        self.engine_gsdm._insert_annotation_cnfs()
        self.engine_gsdm.session.commit()
        self.engine_gsdm._insert_annotations()
        self.engine_gsdm.session.commit()
        self.engine_gsdm._remove_deprecated_annotations()
        self.engine_gsdm.session.commit()
        annotation_cnf_ddbb = self.session.query(AnnotationCnf).filter(AnnotationCnf.name == data["annotations"][0]["annotation_cnf"]["name"], AnnotationCnf.system == data["annotations"][0]["annotation_cnf"]["system"]).first()
        source_ddbb = self.session.query(DimProcessing).filter(DimProcessing.name == data["source"]["name"], DimProcessing.validity_start == data["source"]["validity_start"], DimProcessing.validity_stop == data["source"]["validity_stop"], DimProcessing.generation_time == data["source"]["generation_time"], DimProcessing.dim_exec_version == data["dim_signature"]["version"]).first()
        dim_signature_ddbb = self.session.query(DimSignature).filter(DimSignature.dim_signature == data["dim_signature"]["name"], DimSignature.dim_exec_name == data["dim_signature"]["exec"]).first()
        dim_signature_ddbb = self.session.query(DimSignature).filter(DimSignature.dim_signature == data["dim_signature"]["name"], DimSignature.dim_exec_name == data["dim_signature"]["exec"]).first()
        explicit_reference_ddbb = self.session.query(ExplicitRef).filter(ExplicitRef.explicit_ref == data["annotations"][0]["explicit_reference"]).first()
        annotation_ddbb = self.session.query(Annotation).filter(Annotation.visible == True, Annotation.explicit_ref_id == explicit_reference_ddbb.explicit_ref_id,
                                                                Annotation.processing_uuid == source_ddbb.processing_uuid, Annotation.annotation_cnf_id == annotation_cnf_ddbb.annotation_cnf_id).all()

        assert len(annotation_ddbb) == 1

        annotations_ddbb = self.session.query(Annotation).all()

        assert len(annotations_ddbb) == 1

        values_ddbb = self.query_gsdm.get_annotation_values()

        assert len(values_ddbb) == 8

    def test_remove_deprecated_event_erase_and_replace_same_period(self):

        self.engine_gsdm._initialize_context_insert_data()
        data = {"dim_signature": {"name": "dim_signature",
                                  "exec": "exec",
                                  "version": "1.0"},
                "source": {"name": "source.xml",
                           "generation_time": "2018-07-05T02:07:03",
                           "validity_start": "2018-06-05T02:07:03",
                           "validity_stop": "2018-06-05T08:07:36"},
                "events": [{
                    "explicit_reference": "EXPLICIT_REFERENCE_EVENT1",
                    "gauge": {"name": "GAUGE_NAME",
                              "system": "GAUGE_SYSTEM",
                              "insertion_type": "SIMPLE_UPDATE"},
                    "start": "2018-06-05T02:07:03",
                    "stop": "2018-06-05T08:07:36",
                    "values": [{"name": "VALUES",
                                "type": "object",
                                "values": [
                                    {"type": "text",
                                     "name": "TEXT",
                                     "value": "TEXT"},
                                    {"type": "boolean",
                                     "name": "BOOLEAN",
                                     "value": "true"},
                                    {"type": "boolean",
                                     "name": "BOOLEAN2",
                                     "value": "false"},
                                    {"type": "double",
                                     "name": "DOUBLE",
                                     "value": "0.9"},
                                    {"type": "timestamp",
                                     "name": "TIMESTAMP",
                                     "value": "20180712T00:00:00"},
                                    {"type": "object",
                                     "name": "VALUES2",
                                     "values": [
                                         {"type": "geometry",
                                          "name": "GEOMETRY",
                                          "value": "29.012974905944 -118.33483458667 28.8650301641571 -118.372028380632 28.7171766138274 -118.409121707686 28.5693112139334 -118.44612300623 28.4213994944367 -118.483058731035 28.2734085660472 -118.519970531113 28.1253606038163 -118.556849863134 27.9772541759126 -118.593690316835 27.8291247153939 -118.630472520505 27.7544158362332 -118.64900551674 27.7282373644786 -118.48032600682 27.7015162098732 -118.314168792268 27.6742039940042 -118.150246300849 27.6462511775992 -117.98827485961 27.6176070520608 -117.827974178264 27.5882197156561 -117.669066835177 27.5580360448116 -117.511277813618 27.5270016492436 -117.354334035359 27.4950608291016 -117.197963963877 27.4621565093409 -117.041897175848 27.4282301711374 -116.885863967864 27.3932217651372 -116.729594956238 27.3570696128269 -116.572820673713 27.3197103000253 -116.415271199941 27.2810785491022 -116.256675748617 27.241107085821 -116.09676229722 27.1997272484913 -115.935260563566 27.1524952198729 -115.755436839005 27.2270348347386 -115.734960009089 27.3748346522356 -115.694299254844 27.5226008861849 -115.653563616829 27.6702779354428 -115.612760542177 27.8178690071708 -115.571901649363 27.9653506439026 -115.531000691074 28.1127600020619 -115.490011752733 28.2601469756437 -115.44890306179 28.4076546372628 -115.407649898021 28.455192866856 -115.589486631416 28.4968374106496 -115.752807970928 28.5370603096399 -115.91452902381 28.575931293904 -116.074924211465 28.6135193777855 -116.234273707691 28.6498895688451 -116.392847129762 28.6851057860975 -116.550917254638 28.7192301322012 -116.708756374584 28.752323018501 -116.866636764481 28.7844432583843 -117.024831047231 28.8156481533955 -117.183612605748 28.8459935678779 -117.343255995623 28.8755339855462 -117.504037348554 28.9043225601122 -117.666234808407 28.9324111491586 -117.830128960026 28.9598503481156 -117.996003330616 28.9866878706574 -118.164136222141 29.012974905944 -118.33483458667"}]
                                 }]}]
                }]
            }
        self.engine_gsdm.operation = data
        self.engine_gsdm._insert_dim_signature()
        self.engine_gsdm._insert_source()
        self.engine_gsdm._insert_gauges()
        self.engine_gsdm.session.commit()
        self.engine_gsdm._insert_explicit_refs()
        self.engine_gsdm.session.commit()
        self.engine_gsdm._insert_events()
        self.engine_gsdm.session.commit()

        self.engine_gsdm._initialize_context_insert_data()
        data = {"dim_signature": {"name": "dim_signature",
                                  "exec": "exec",
                                  "version": "1.0"},
                "source": {"name": "source2.xml",
                           "generation_time": "2018-07-05T02:07:04",
                           "validity_start": "2018-06-05T02:07:03",
                           "validity_stop": "2018-06-05T08:07:36"},
                "events": [{
                    "explicit_reference": "EXPLICIT_REFERENCE_EVENT2",
                    "gauge": {"name": "GAUGE_NAME",
                              "system": "GAUGE_SYSTEM",
                              "insertion_type": "ERASE_and_REPLACE"},
                    "start": "2018-06-05T02:07:03",
                    "stop": "2018-06-05T08:07:36",
                    "values": [{"name": "VALUES",
                                "type": "object",
                                "values": [
                                    {"type": "text",
                                     "name": "TEXT",
                                     "value": "TEXT"},
                                    {"type": "boolean",
                                     "name": "BOOLEAN",
                                     "value": "true"},
                                    {"type": "boolean",
                                     "name": "BOOLEAN2",
                                     "value": "false"},
                                    {"type": "double",
                                     "name": "DOUBLE",
                                     "value": "0.9"},
                                    {"type": "timestamp",
                                     "name": "TIMESTAMP",
                                     "value": "20180712T00:00:00"},
                                    {"type": "object",
                                     "name": "VALUES2",
                                     "values": [
                                         {"type": "geometry",
                                          "name": "GEOMETRY",
                                          "value": "29.012974905944 -118.33483458667 28.8650301641571 -118.372028380632 28.7171766138274 -118.409121707686 28.5693112139334 -118.44612300623 28.4213994944367 -118.483058731035 28.2734085660472 -118.519970531113 28.1253606038163 -118.556849863134 27.9772541759126 -118.593690316835 27.8291247153939 -118.630472520505 27.7544158362332 -118.64900551674 27.7282373644786 -118.48032600682 27.7015162098732 -118.314168792268 27.6742039940042 -118.150246300849 27.6462511775992 -117.98827485961 27.6176070520608 -117.827974178264 27.5882197156561 -117.669066835177 27.5580360448116 -117.511277813618 27.5270016492436 -117.354334035359 27.4950608291016 -117.197963963877 27.4621565093409 -117.041897175848 27.4282301711374 -116.885863967864 27.3932217651372 -116.729594956238 27.3570696128269 -116.572820673713 27.3197103000253 -116.415271199941 27.2810785491022 -116.256675748617 27.241107085821 -116.09676229722 27.1997272484913 -115.935260563566 27.1524952198729 -115.755436839005 27.2270348347386 -115.734960009089 27.3748346522356 -115.694299254844 27.5226008861849 -115.653563616829 27.6702779354428 -115.612760542177 27.8178690071708 -115.571901649363 27.9653506439026 -115.531000691074 28.1127600020619 -115.490011752733 28.2601469756437 -115.44890306179 28.4076546372628 -115.407649898021 28.455192866856 -115.589486631416 28.4968374106496 -115.752807970928 28.5370603096399 -115.91452902381 28.575931293904 -116.074924211465 28.6135193777855 -116.234273707691 28.6498895688451 -116.392847129762 28.6851057860975 -116.550917254638 28.7192301322012 -116.708756374584 28.752323018501 -116.866636764481 28.7844432583843 -117.024831047231 28.8156481533955 -117.183612605748 28.8459935678779 -117.343255995623 28.8755339855462 -117.504037348554 28.9043225601122 -117.666234808407 28.9324111491586 -117.830128960026 28.9598503481156 -117.996003330616 28.9866878706574 -118.164136222141 29.012974905944 -118.33483458667"}]
                                 }]}]
                }]
            }
        self.engine_gsdm.operation = data
        self.engine_gsdm._insert_dim_signature()
        self.engine_gsdm._insert_source()
        self.engine_gsdm._insert_gauges()
        self.engine_gsdm.session.commit()
        self.engine_gsdm._insert_explicit_refs()
        self.engine_gsdm.session.commit()
        self.engine_gsdm._insert_events()
        self.engine_gsdm.session.commit()
        self.engine_gsdm._remove_deprecated_events_by_erase_and_replace()
        self.engine_gsdm.session.commit()
        gauge_ddbb = self.session.query(Gauge).filter(Gauge.name == data["events"][0]["gauge"]["name"], Gauge.system == data["events"][0]["gauge"]["system"]).first()
        source_ddbb = self.session.query(DimProcessing).filter(DimProcessing.name == data["source"]["name"], DimProcessing.validity_start == data["source"]["validity_start"], DimProcessing.validity_stop == data["source"]["validity_stop"], DimProcessing.generation_time == data["source"]["generation_time"], DimProcessing.dim_exec_version == data["dim_signature"]["version"]).first()
        dim_signature_ddbb = self.session.query(DimSignature).filter(DimSignature.dim_signature == data["dim_signature"]["name"], DimSignature.dim_exec_name == data["dim_signature"]["exec"]).first()
        explicit_reference_ddbb = self.session.query(ExplicitRef).filter(ExplicitRef.explicit_ref == data["events"][0]["explicit_reference"]).first()

        filtered_events_ddbb = self.session.query(Event).filter(Event.start == data["events"][0]["start"],
                                                      Event.stop == data["events"][0]["stop"],
                                                      Event.gauge_id == gauge_ddbb.gauge_id,
                                                      Event.processing_uuid == source_ddbb.processing_uuid,
                                                      Event.explicit_ref_id == explicit_reference_ddbb.explicit_ref_id,
                                                      Event.visible == True).all()

        assert len(filtered_events_ddbb) == 1

        events_ddbb = self.session.query(Event).all()

        assert len(events_ddbb) == 1

        values_ddbb = self.query_gsdm.get_event_values()

        assert len(values_ddbb) == 8

    def test_remove_deprecated_event_erase_and_replace_one_starts_before_links(self):

        self.engine_gsdm._initialize_context_insert_data()
        data1 = {"dim_signature": {"name": "dim_signature",
                                  "exec": "exec",
                                  "version": "1.0"},
                "source": {"name": "source.xml",
                           "generation_time": "2018-07-05T02:07:03",
                           "validity_start": "2018-06-04T02:07:03",
                           "validity_stop": "2018-06-05T08:07:36"},
                "events": [{
                    "explicit_reference": "EXPLICIT_REFERENCE_EVENT1",
                    "gauge": {"name": "GAUGE_NAME",
                              "system": "GAUGE_SYSTEM",
                              "insertion_type": "SIMPLE_UPDATE"},
                    "start": "2018-06-04T02:07:03",
                    "stop": "2018-06-05T08:07:36",
                    "links": [{
                        "link": "EVENT_LINK",
                        "link_mode": "by_ref",
                        "name": "EVENT_LINK_NAME"
                    }],
                    "key": "EVENT_KEY",
                    "values": [{"name": "VALUES",
                                "type": "object",
                                "values": [
                                    {"type": "text",
                                     "name": "TEXT",
                                     "value": "TEXT"},
                                    {"type": "boolean",
                                     "name": "BOOLEAN",
                                     "value": "true"},
                                    {"type": "boolean",
                                     "name": "BOOLEAN2",
                                     "value": "false"},
                                    {"type": "double",
                                     "name": "DOUBLE",
                                     "value": "0.9"},
                                    {"type": "timestamp",
                                     "name": "TIMESTAMP",
                                     "value": "20180712T00:00:00"},
                                    {"type": "object",
                                     "name": "VALUES2",
                                     "values": [
                                         {"type": "geometry",
                                          "name": "GEOMETRY",
                                          "value": "29.012974905944 -118.33483458667 28.8650301641571 -118.372028380632 28.7171766138274 -118.409121707686 28.5693112139334 -118.44612300623 28.4213994944367 -118.483058731035 28.2734085660472 -118.519970531113 28.1253606038163 -118.556849863134 27.9772541759126 -118.593690316835 27.8291247153939 -118.630472520505 27.7544158362332 -118.64900551674 27.7282373644786 -118.48032600682 27.7015162098732 -118.314168792268 27.6742039940042 -118.150246300849 27.6462511775992 -117.98827485961 27.6176070520608 -117.827974178264 27.5882197156561 -117.669066835177 27.5580360448116 -117.511277813618 27.5270016492436 -117.354334035359 27.4950608291016 -117.197963963877 27.4621565093409 -117.041897175848 27.4282301711374 -116.885863967864 27.3932217651372 -116.729594956238 27.3570696128269 -116.572820673713 27.3197103000253 -116.415271199941 27.2810785491022 -116.256675748617 27.241107085821 -116.09676229722 27.1997272484913 -115.935260563566 27.1524952198729 -115.755436839005 27.2270348347386 -115.734960009089 27.3748346522356 -115.694299254844 27.5226008861849 -115.653563616829 27.6702779354428 -115.612760542177 27.8178690071708 -115.571901649363 27.9653506439026 -115.531000691074 28.1127600020619 -115.490011752733 28.2601469756437 -115.44890306179 28.4076546372628 -115.407649898021 28.455192866856 -115.589486631416 28.4968374106496 -115.752807970928 28.5370603096399 -115.91452902381 28.575931293904 -116.074924211465 28.6135193777855 -116.234273707691 28.6498895688451 -116.392847129762 28.6851057860975 -116.550917254638 28.7192301322012 -116.708756374584 28.752323018501 -116.866636764481 28.7844432583843 -117.024831047231 28.8156481533955 -117.183612605748 28.8459935678779 -117.343255995623 28.8755339855462 -117.504037348554 28.9043225601122 -117.666234808407 28.9324111491586 -117.830128960026 28.9598503481156 -117.996003330616 28.9866878706574 -118.164136222141 29.012974905944 -118.33483458667"}]
                                 }]}]
                },
                           {
                               "explicit_reference": "EXPLICIT_REFERENCE_EVENT2",
                               "gauge": {"name": "GAUGE_NAME",
                                         "system": "GAUGE_SYSTEM",
                                         "insertion_type": "SIMPLE_UPDATE"},
                               "start": "2018-06-04T02:07:03",
                               "stop": "2018-06-05T08:07:36",
                               "links": [{
                                   "link": "EVENT_LINK",
                                   "link_mode": "by_ref",
                                   "name": "EVENT_LINK_NAME"
                               }]},
                           {
                               "explicit_reference": "EXPLICIT_REFERENCE_EVENT3",
                               "gauge": {"name": "GAUGE_NAME",
                                         "system": "GAUGE_SYSTEM",
                                         "insertion_type": "SIMPLE_UPDATE"},
                               "start": "2018-06-04T02:07:03",
                               "stop": "2018-06-04T04:07:36",
                               "links": [{
                                   "link": "EVENT_LINK",
                                   "link_mode": "by_ref",
                                   "name": "EVENT_LINK_NAME"
                               }]}]
            }
        self.engine_gsdm.operation = data1
        self.engine_gsdm._insert_dim_signature()
        self.engine_gsdm._insert_source()
        self.engine_gsdm._insert_gauges()
        self.engine_gsdm.session.commit()
        self.engine_gsdm._insert_explicit_refs()
        self.engine_gsdm.session.commit()
        self.engine_gsdm._insert_events()
        self.engine_gsdm.session.commit()

        self.engine_gsdm._initialize_context_insert_data()
        data2 = {"dim_signature": {"name": "dim_signature",
                                  "exec": "exec",
                                  "version": "1.0"},
                "source": {"name": "source2.xml",
                           "generation_time": "2018-07-05T02:07:04",
                           "validity_start": "2018-06-05T02:07:03",
                           "validity_stop": "2018-06-05T08:07:36"},
                "events": [{
                    "explicit_reference": "EXPLICIT_REFERENCE_EVENT4",
                    "gauge": {"name": "GAUGE_NAME",
                              "system": "GAUGE_SYSTEM",
                              "insertion_type": "ERASE_and_REPLACE"},
                    "start": "2018-06-05T02:07:03",
                    "stop": "2018-06-05T08:07:36",
                    "values": [{"name": "VALUES",
                                "type": "object",
                                "values": [
                                    {"type": "text",
                                     "name": "TEXT",
                                     "value": "TEXT"},
                                    {"type": "boolean",
                                     "name": "BOOLEAN",
                                     "value": "true"},
                                    {"type": "boolean",
                                     "name": "BOOLEAN2",
                                     "value": "false"},
                                    {"type": "double",
                                     "name": "DOUBLE",
                                     "value": "0.9"},
                                    {"type": "timestamp",
                                     "name": "TIMESTAMP",
                                     "value": "20180712T00:00:00"},
                                    {"type": "object",
                                     "name": "VALUES2",
                                     "values": [
                                         {"type": "geometry",
                                          "name": "GEOMETRY",
                                          "value": "29.012974905944 -118.33483458667 28.8650301641571 -118.372028380632 28.7171766138274 -118.409121707686 28.5693112139334 -118.44612300623 28.4213994944367 -118.483058731035 28.2734085660472 -118.519970531113 28.1253606038163 -118.556849863134 27.9772541759126 -118.593690316835 27.8291247153939 -118.630472520505 27.7544158362332 -118.64900551674 27.7282373644786 -118.48032600682 27.7015162098732 -118.314168792268 27.6742039940042 -118.150246300849 27.6462511775992 -117.98827485961 27.6176070520608 -117.827974178264 27.5882197156561 -117.669066835177 27.5580360448116 -117.511277813618 27.5270016492436 -117.354334035359 27.4950608291016 -117.197963963877 27.4621565093409 -117.041897175848 27.4282301711374 -116.885863967864 27.3932217651372 -116.729594956238 27.3570696128269 -116.572820673713 27.3197103000253 -116.415271199941 27.2810785491022 -116.256675748617 27.241107085821 -116.09676229722 27.1997272484913 -115.935260563566 27.1524952198729 -115.755436839005 27.2270348347386 -115.734960009089 27.3748346522356 -115.694299254844 27.5226008861849 -115.653563616829 27.6702779354428 -115.612760542177 27.8178690071708 -115.571901649363 27.9653506439026 -115.531000691074 28.1127600020619 -115.490011752733 28.2601469756437 -115.44890306179 28.4076546372628 -115.407649898021 28.455192866856 -115.589486631416 28.4968374106496 -115.752807970928 28.5370603096399 -115.91452902381 28.575931293904 -116.074924211465 28.6135193777855 -116.234273707691 28.6498895688451 -116.392847129762 28.6851057860975 -116.550917254638 28.7192301322012 -116.708756374584 28.752323018501 -116.866636764481 28.7844432583843 -117.024831047231 28.8156481533955 -117.183612605748 28.8459935678779 -117.343255995623 28.8755339855462 -117.504037348554 28.9043225601122 -117.666234808407 28.9324111491586 -117.830128960026 28.9598503481156 -117.996003330616 28.9866878706574 -118.164136222141 29.012974905944 -118.33483458667"}]
                                 }]}]
                }]
            }
        self.engine_gsdm.operation = data2
        self.engine_gsdm._insert_dim_signature()
        self.engine_gsdm._insert_source()
        self.engine_gsdm._insert_gauges()
        self.engine_gsdm.session.commit()
        self.engine_gsdm._insert_explicit_refs()
        self.engine_gsdm.session.commit()
        self.engine_gsdm._insert_events()
        self.engine_gsdm.session.commit()
        self.engine_gsdm._remove_deprecated_events_by_erase_and_replace()
        self.engine_gsdm.session.commit()
        gauge_ddbb = self.session.query(Gauge).filter(Gauge.name == data2["events"][0]["gauge"]["name"], Gauge.system == data2["events"][0]["gauge"]["system"]).first()
        source_ddbb = self.session.query(DimProcessing).filter(DimProcessing.name == data2["source"]["name"], DimProcessing.validity_start == data2["source"]["validity_start"], DimProcessing.validity_stop == data2["source"]["validity_stop"], DimProcessing.generation_time == data2["source"]["generation_time"], DimProcessing.dim_exec_version == data2["dim_signature"]["version"]).first()
        dim_signature_ddbb = self.session.query(DimSignature).filter(DimSignature.dim_signature == data2["dim_signature"]["name"], DimSignature.dim_exec_name == data2["dim_signature"]["exec"]).first()
        explicit_reference_ddbb = self.session.query(ExplicitRef).filter(ExplicitRef.explicit_ref == data2["events"][0]["explicit_reference"]).first()

        filtered_events_ddbb = self.session.query(Event).filter(Event.start == data2["events"][0]["start"],
                                                      Event.stop == data2["events"][0]["stop"],
                                                      Event.gauge_id == gauge_ddbb.gauge_id,
                                                      Event.processing_uuid == source_ddbb.processing_uuid,
                                                      Event.explicit_ref_id == explicit_reference_ddbb.explicit_ref_id,
                                                      Event.visible == True).all()

        assert len(filtered_events_ddbb) == 1

        gauge_ddbb = self.session.query(Gauge).filter(Gauge.name == data1["events"][0]["gauge"]["name"], Gauge.system == data1["events"][0]["gauge"]["system"]).first()
        source_ddbb = self.session.query(DimProcessing).filter(DimProcessing.name == data1["source"]["name"], DimProcessing.validity_start == data1["source"]["validity_start"], DimProcessing.validity_stop == data1["source"]["validity_stop"], DimProcessing.generation_time == data1["source"]["generation_time"], DimProcessing.dim_exec_version == data1["dim_signature"]["version"]).first()
        dim_signature_ddbb = self.session.query(DimSignature).filter(DimSignature.dim_signature == data1["dim_signature"]["name"], DimSignature.dim_exec_name == data1["dim_signature"]["exec"]).first()

        filtered_events_ddbb = self.session.query(Event).filter(Event.start == data1["events"][0]["start"],
                                                      Event.stop == data2["events"][0]["start"],
                                                      Event.gauge_id == gauge_ddbb.gauge_id,
                                                      Event.processing_uuid == source_ddbb.processing_uuid,
                                                      Event.visible == True).all()

        assert len(filtered_events_ddbb) == 2

        events_ddbb = self.session.query(Event).all()

        assert len(events_ddbb) == 4

        values_ddbb = self.query_gsdm.get_event_values()

        assert len(values_ddbb) == 16

        links_ddbb = self.query_gsdm.get_event_links()

        assert len(links_ddbb) == 6

        events_with_links = self.session.query(Event).join(DimProcessing).filter(DimProcessing.name == "source.xml").all()
        
        rest_of_event_uuids = [events_with_links[1].event_uuid, events_with_links[2].event_uuid]
        
        event_links_ddbb = self.query_gsdm.get_event_links_pointing_to_events(rest_of_event_uuids, [events_with_links[0].event_uuid])

        assert len(event_links_ddbb) == 2

        rest_of_event_uuids = [events_with_links[0].event_uuid, events_with_links[2].event_uuid]
        
        event_links_ddbb = self.query_gsdm.get_event_links_pointing_to_events(rest_of_event_uuids, [events_with_links[1].event_uuid])

        assert len(event_links_ddbb) == 2

        rest_of_event_uuids = [events_with_links[0].event_uuid, events_with_links[1].event_uuid]
        
        event_links_ddbb = self.query_gsdm.get_event_links_pointing_to_events(rest_of_event_uuids, [events_with_links[2].event_uuid])

        assert len(event_links_ddbb) == 2

    def test_remove_deprecated_event_erase_and_replace_one_starts_after(self):

        self.engine_gsdm._initialize_context_insert_data()
        data1 = {"dim_signature": {"name": "dim_signature",
                                  "exec": "exec",
                                  "version": "1.0"},
                "source": {"name": "source.xml",
                           "generation_time": "2018-07-05T02:07:03",
                           "validity_start": "2018-06-05T02:07:15",
                           "validity_stop": "2018-06-06T08:07:36"},
                "events": [{
                    "explicit_reference": "EXPLICIT_REFERENCE_EVENT1",
                    "gauge": {"name": "GAUGE_NAME",
                              "system": "GAUGE_SYSTEM",
                              "insertion_type": "SIMPLE_UPDATE"},
                    "start": "2018-06-05T02:07:15",
                    "stop": "2018-06-06T08:07:36",
                    "values": [{"name": "VALUES",
                                "type": "object",
                                "values": [
                                    {"type": "text",
                                     "name": "TEXT",
                                     "value": "TEXT"},
                                    {"type": "boolean",
                                     "name": "BOOLEAN",
                                     "value": "true"},
                                    {"type": "boolean",
                                     "name": "BOOLEAN2",
                                     "value": "false"},
                                    {"type": "double",
                                     "name": "DOUBLE",
                                     "value": "0.9"},
                                    {"type": "timestamp",
                                     "name": "TIMESTAMP",
                                     "value": "20180712T00:00:00"},
                                    {"type": "object",
                                     "name": "VALUES2",
                                     "values": [
                                         {"type": "geometry",
                                          "name": "GEOMETRY",
                                          "value": "29.012974905944 -118.33483458667 28.8650301641571 -118.372028380632 28.7171766138274 -118.409121707686 28.5693112139334 -118.44612300623 28.4213994944367 -118.483058731035 28.2734085660472 -118.519970531113 28.1253606038163 -118.556849863134 27.9772541759126 -118.593690316835 27.8291247153939 -118.630472520505 27.7544158362332 -118.64900551674 27.7282373644786 -118.48032600682 27.7015162098732 -118.314168792268 27.6742039940042 -118.150246300849 27.6462511775992 -117.98827485961 27.6176070520608 -117.827974178264 27.5882197156561 -117.669066835177 27.5580360448116 -117.511277813618 27.5270016492436 -117.354334035359 27.4950608291016 -117.197963963877 27.4621565093409 -117.041897175848 27.4282301711374 -116.885863967864 27.3932217651372 -116.729594956238 27.3570696128269 -116.572820673713 27.3197103000253 -116.415271199941 27.2810785491022 -116.256675748617 27.241107085821 -116.09676229722 27.1997272484913 -115.935260563566 27.1524952198729 -115.755436839005 27.2270348347386 -115.734960009089 27.3748346522356 -115.694299254844 27.5226008861849 -115.653563616829 27.6702779354428 -115.612760542177 27.8178690071708 -115.571901649363 27.9653506439026 -115.531000691074 28.1127600020619 -115.490011752733 28.2601469756437 -115.44890306179 28.4076546372628 -115.407649898021 28.455192866856 -115.589486631416 28.4968374106496 -115.752807970928 28.5370603096399 -115.91452902381 28.575931293904 -116.074924211465 28.6135193777855 -116.234273707691 28.6498895688451 -116.392847129762 28.6851057860975 -116.550917254638 28.7192301322012 -116.708756374584 28.752323018501 -116.866636764481 28.7844432583843 -117.024831047231 28.8156481533955 -117.183612605748 28.8459935678779 -117.343255995623 28.8755339855462 -117.504037348554 28.9043225601122 -117.666234808407 28.9324111491586 -117.830128960026 28.9598503481156 -117.996003330616 28.9866878706574 -118.164136222141 29.012974905944 -118.33483458667"}]
                                 }]}]
                }]
            }
        self.engine_gsdm.operation = data1
        self.engine_gsdm._insert_dim_signature()
        self.engine_gsdm._insert_source()
        self.engine_gsdm._insert_gauges()
        self.engine_gsdm.session.commit()
        self.engine_gsdm._insert_explicit_refs()
        self.engine_gsdm.session.commit()
        self.engine_gsdm._insert_events()
        self.engine_gsdm.session.commit()

        self.engine_gsdm._initialize_context_insert_data()
        data2 = {"dim_signature": {"name": "dim_signature",
                                  "exec": "exec",
                                  "version": "1.0"},
                "source": {"name": "source2.xml",
                           "generation_time": "2018-07-05T02:07:04",
                           "validity_start": "2018-06-05T02:07:03",
                           "validity_stop": "2018-06-05T08:07:36"},
                "events": [{
                    "explicit_reference": "EXPLICIT_REFERENCE_EVENT2",
                    "gauge": {"name": "GAUGE_NAME",
                              "system": "GAUGE_SYSTEM",
                              "insertion_type": "ERASE_and_REPLACE"},
                    "start": "2018-06-05T02:07:03",
                    "stop": "2018-06-05T08:07:36",
                    "values": [{"name": "VALUES",
                                "type": "object",
                                "values": [
                                    {"type": "text",
                                     "name": "TEXT",
                                     "value": "TEXT"},
                                    {"type": "boolean",
                                     "name": "BOOLEAN",
                                     "value": "true"},
                                    {"type": "boolean",
                                     "name": "BOOLEAN2",
                                     "value": "false"},
                                    {"type": "double",
                                     "name": "DOUBLE",
                                     "value": "0.9"},
                                    {"type": "timestamp",
                                     "name": "TIMESTAMP",
                                     "value": "20180712T00:00:00"},
                                    {"type": "object",
                                     "name": "VALUES2",
                                     "values": [
                                         {"type": "geometry",
                                          "name": "GEOMETRY",
                                          "value": "29.012974905944 -118.33483458667 28.8650301641571 -118.372028380632 28.7171766138274 -118.409121707686 28.5693112139334 -118.44612300623 28.4213994944367 -118.483058731035 28.2734085660472 -118.519970531113 28.1253606038163 -118.556849863134 27.9772541759126 -118.593690316835 27.8291247153939 -118.630472520505 27.7544158362332 -118.64900551674 27.7282373644786 -118.48032600682 27.7015162098732 -118.314168792268 27.6742039940042 -118.150246300849 27.6462511775992 -117.98827485961 27.6176070520608 -117.827974178264 27.5882197156561 -117.669066835177 27.5580360448116 -117.511277813618 27.5270016492436 -117.354334035359 27.4950608291016 -117.197963963877 27.4621565093409 -117.041897175848 27.4282301711374 -116.885863967864 27.3932217651372 -116.729594956238 27.3570696128269 -116.572820673713 27.3197103000253 -116.415271199941 27.2810785491022 -116.256675748617 27.241107085821 -116.09676229722 27.1997272484913 -115.935260563566 27.1524952198729 -115.755436839005 27.2270348347386 -115.734960009089 27.3748346522356 -115.694299254844 27.5226008861849 -115.653563616829 27.6702779354428 -115.612760542177 27.8178690071708 -115.571901649363 27.9653506439026 -115.531000691074 28.1127600020619 -115.490011752733 28.2601469756437 -115.44890306179 28.4076546372628 -115.407649898021 28.455192866856 -115.589486631416 28.4968374106496 -115.752807970928 28.5370603096399 -115.91452902381 28.575931293904 -116.074924211465 28.6135193777855 -116.234273707691 28.6498895688451 -116.392847129762 28.6851057860975 -116.550917254638 28.7192301322012 -116.708756374584 28.752323018501 -116.866636764481 28.7844432583843 -117.024831047231 28.8156481533955 -117.183612605748 28.8459935678779 -117.343255995623 28.8755339855462 -117.504037348554 28.9043225601122 -117.666234808407 28.9324111491586 -117.830128960026 28.9598503481156 -117.996003330616 28.9866878706574 -118.164136222141 29.012974905944 -118.33483458667"}]
                                 }]}]
                }]
            }
        self.engine_gsdm.operation = data2
        self.engine_gsdm._insert_dim_signature()
        self.engine_gsdm._insert_source()
        self.engine_gsdm._insert_gauges()
        self.engine_gsdm.session.commit()
        self.engine_gsdm._insert_explicit_refs()
        self.engine_gsdm.session.commit()
        self.engine_gsdm._insert_events()
        self.engine_gsdm.session.commit()
        self.engine_gsdm._remove_deprecated_events_by_erase_and_replace()
        self.engine_gsdm.session.commit()
        gauge_ddbb = self.session.query(Gauge).filter(Gauge.name == data2["events"][0]["gauge"]["name"], Gauge.system == data2["events"][0]["gauge"]["system"]).first()
        source_ddbb = self.session.query(DimProcessing).filter(DimProcessing.name == data2["source"]["name"], DimProcessing.validity_start == data2["source"]["validity_start"], DimProcessing.validity_stop == data2["source"]["validity_stop"], DimProcessing.generation_time == data2["source"]["generation_time"], DimProcessing.dim_exec_version == data2["dim_signature"]["version"]).first()
        dim_signature_ddbb = self.session.query(DimSignature).filter(DimSignature.dim_signature == data2["dim_signature"]["name"], DimSignature.dim_exec_name == data2["dim_signature"]["exec"]).first()
        explicit_reference_ddbb = self.session.query(ExplicitRef).filter(ExplicitRef.explicit_ref == data2["events"][0]["explicit_reference"]).first()

        filtered_events_ddbb = self.session.query(Event).filter(Event.start == data2["events"][0]["start"],
                                                      Event.stop == data2["events"][0]["stop"],
                                                      Event.gauge_id == gauge_ddbb.gauge_id,
                                                      Event.processing_uuid == source_ddbb.processing_uuid,
                                                      Event.explicit_ref_id == explicit_reference_ddbb.explicit_ref_id,
                                                      Event.visible == True).all()

        assert len(filtered_events_ddbb) == 1

        gauge_ddbb = self.session.query(Gauge).filter(Gauge.name == data1["events"][0]["gauge"]["name"], Gauge.system == data1["events"][0]["gauge"]["system"]).first()
        source_ddbb = self.session.query(DimProcessing).filter(DimProcessing.name == data1["source"]["name"], DimProcessing.validity_start == data1["source"]["validity_start"], DimProcessing.validity_stop == data1["source"]["validity_stop"], DimProcessing.generation_time == data1["source"]["generation_time"], DimProcessing.dim_exec_version == data1["dim_signature"]["version"]).first()
        dim_signature_ddbb = self.session.query(DimSignature).filter(DimSignature.dim_signature == data1["dim_signature"]["name"], DimSignature.dim_exec_name == data1["dim_signature"]["exec"]).first()
        explicit_reference_ddbb = self.session.query(ExplicitRef).filter(ExplicitRef.explicit_ref == data1["events"][0]["explicit_reference"]).first()

        filtered_events_ddbb = self.session.query(Event).filter(Event.start == data2["events"][0]["stop"],
                                                      Event.stop == data1["events"][0]["stop"],
                                                      Event.gauge_id == gauge_ddbb.gauge_id,
                                                      Event.processing_uuid == source_ddbb.processing_uuid,
                                                      Event.explicit_ref_id == explicit_reference_ddbb.explicit_ref_id,
                                                      Event.visible == True).all()

        assert len(filtered_events_ddbb) == 1

        events_ddbb = self.session.query(Event).all()

        assert len(events_ddbb) == 2

        values_ddbb = self.query_gsdm.get_event_values()

        assert len(values_ddbb) == 16


    def test_remove_deprecated_event_erase_and_replace_events_not_overlapping(self):

        self.engine_gsdm._initialize_context_insert_data()
        data1 = {"dim_signature": {"name": "dim_signature",
                                   "exec": "exec",
                                   "version": "1.0"},
                 "source": {"name": "source2.xml",
                            "generation_time": "2018-07-05T02:07:03",
                            "validity_start": "2018-06-05T02:07:03",
                            "validity_stop": "2018-06-05T08:07:36"},
                 "events": [{
                     "explicit_reference": "EXPLICIT_REFERENCE_EVENT2",
                     "gauge": {"name": "GAUGE_NAME",
                               "system": "GAUGE_SYSTEM",
                               "insertion_type": "SIMPLE_UPDATE"},
                     "start": "2018-06-05T02:07:03",
                     "stop": "2018-06-05T04:07:36",
                     "values": [{"name": "VALUES",
                                 "type": "object",
                                 "values": [
                                     {"type": "text",
                                      "name": "TEXT",
                                      "value": "TEXT"},
                                     {"type": "boolean",
                                      "name": "BOOLEAN",
                                      "value": "true"},
                                     {"type": "boolean",
                                      "name": "BOOLEAN2",
                                      "value": "false"},
                                     {"type": "double",
                                      "name": "DOUBLE",
                                      "value": "0.9"},
                                     {"type": "timestamp",
                                      "name": "TIMESTAMP",
                                      "value": "20180712T00:00:00"},
                                     {"type": "object",
                                      "name": "VALUES2",
                                      "values": [
                                          {"type": "geometry",
                                           "name": "GEOMETRY",
                                           "value": "29.012974905944 -118.33483458667 28.8650301641571 -118.372028380632 28.7171766138274 -118.409121707686 28.5693112139334 -118.44612300623 28.4213994944367 -118.483058731035 28.2734085660472 -118.519970531113 28.1253606038163 -118.556849863134 27.9772541759126 -118.593690316835 27.8291247153939 -118.630472520505 27.7544158362332 -118.64900551674 27.7282373644786 -118.48032600682 27.7015162098732 -118.314168792268 27.6742039940042 -118.150246300849 27.6462511775992 -117.98827485961 27.6176070520608 -117.827974178264 27.5882197156561 -117.669066835177 27.5580360448116 -117.511277813618 27.5270016492436 -117.354334035359 27.4950608291016 -117.197963963877 27.4621565093409 -117.041897175848 27.4282301711374 -116.885863967864 27.3932217651372 -116.729594956238 27.3570696128269 -116.572820673713 27.3197103000253 -116.415271199941 27.2810785491022 -116.256675748617 27.241107085821 -116.09676229722 27.1997272484913 -115.935260563566 27.1524952198729 -115.755436839005 27.2270348347386 -115.734960009089 27.3748346522356 -115.694299254844 27.5226008861849 -115.653563616829 27.6702779354428 -115.612760542177 27.8178690071708 -115.571901649363 27.9653506439026 -115.531000691074 28.1127600020619 -115.490011752733 28.2601469756437 -115.44890306179 28.4076546372628 -115.407649898021 28.455192866856 -115.589486631416 28.4968374106496 -115.752807970928 28.5370603096399 -115.91452902381 28.575931293904 -116.074924211465 28.6135193777855 -116.234273707691 28.6498895688451 -116.392847129762 28.6851057860975 -116.550917254638 28.7192301322012 -116.708756374584 28.752323018501 -116.866636764481 28.7844432583843 -117.024831047231 28.8156481533955 -117.183612605748 28.8459935678779 -117.343255995623 28.8755339855462 -117.504037348554 28.9043225601122 -117.666234808407 28.9324111491586 -117.830128960026 28.9598503481156 -117.996003330616 28.9866878706574 -118.164136222141 29.012974905944 -118.33483458667"}]
                                  }]}]
                }]
            }
        self.engine_gsdm.operation = data1
        self.engine_gsdm._insert_dim_signature()
        self.engine_gsdm._insert_source()
        self.engine_gsdm._insert_gauges()
        self.engine_gsdm.session.commit()
        self.engine_gsdm._insert_explicit_refs()
        self.engine_gsdm.session.commit()
        self.engine_gsdm._insert_events()
        self.engine_gsdm.session.commit()

        data2 = {"dim_signature": {"name": "dim_signature",
                                   "exec": "exec",
                                   "version": "1.0"},
                 "source": {"name": "source.xml",
                            "generation_time": "2018-07-05T02:07:04",
                            "validity_start": "2018-06-04T02:07:03",
                            "validity_stop": "2018-06-06T08:07:36"},
                 "events": [{
                     "explicit_reference": "EXPLICIT_REFERENCE_EVENT1",
                     "gauge": {"name": "GAUGE_NAME",
                               "system": "GAUGE_SYSTEM",
                               "insertion_type": "ERASE_and_REPLACE"},
                     "start": "2018-06-04T05:07:03",
                     "stop": "2018-06-05T07:07:36",
                     "values": [{"name": "VALUES",
                                 "type": "object",
                                 "values": [
                                     {"type": "text",
                                      "name": "TEXT",
                                      "value": "TEXT"},
                                     {"type": "boolean",
                                      "name": "BOOLEAN",
                                      "value": "true"},
                                     {"type": "boolean",
                                      "name": "BOOLEAN2",
                                      "value": "false"},
                                     {"type": "double",
                                      "name": "DOUBLE",
                                      "value": "0.9"},
                                     {"type": "timestamp",
                                      "name": "TIMESTAMP",
                                      "value": "20180712T00:00:00"},
                                     {"type": "object",
                                      "name": "VALUES2",
                                      "values": [
                                          {"type": "geometry",
                                           "name": "GEOMETRY",
                                           "value": "29.012974905944 -118.33483458667 28.8650301641571 -118.372028380632 28.7171766138274 -118.409121707686 28.5693112139334 -118.44612300623 28.4213994944367 -118.483058731035 28.2734085660472 -118.519970531113 28.1253606038163 -118.556849863134 27.9772541759126 -118.593690316835 27.8291247153939 -118.630472520505 27.7544158362332 -118.64900551674 27.7282373644786 -118.48032600682 27.7015162098732 -118.314168792268 27.6742039940042 -118.150246300849 27.6462511775992 -117.98827485961 27.6176070520608 -117.827974178264 27.5882197156561 -117.669066835177 27.5580360448116 -117.511277813618 27.5270016492436 -117.354334035359 27.4950608291016 -117.197963963877 27.4621565093409 -117.041897175848 27.4282301711374 -116.885863967864 27.3932217651372 -116.729594956238 27.3570696128269 -116.572820673713 27.3197103000253 -116.415271199941 27.2810785491022 -116.256675748617 27.241107085821 -116.09676229722 27.1997272484913 -115.935260563566 27.1524952198729 -115.755436839005 27.2270348347386 -115.734960009089 27.3748346522356 -115.694299254844 27.5226008861849 -115.653563616829 27.6702779354428 -115.612760542177 27.8178690071708 -115.571901649363 27.9653506439026 -115.531000691074 28.1127600020619 -115.490011752733 28.2601469756437 -115.44890306179 28.4076546372628 -115.407649898021 28.455192866856 -115.589486631416 28.4968374106496 -115.752807970928 28.5370603096399 -115.91452902381 28.575931293904 -116.074924211465 28.6135193777855 -116.234273707691 28.6498895688451 -116.392847129762 28.6851057860975 -116.550917254638 28.7192301322012 -116.708756374584 28.752323018501 -116.866636764481 28.7844432583843 -117.024831047231 28.8156481533955 -117.183612605748 28.8459935678779 -117.343255995623 28.8755339855462 -117.504037348554 28.9043225601122 -117.666234808407 28.9324111491586 -117.830128960026 28.9598503481156 -117.996003330616 28.9866878706574 -118.164136222141 29.012974905944 -118.33483458667"}]
                                  }]}]
                }]
            }
        self.engine_gsdm._initialize_context_insert_data()
        self.engine_gsdm.operation = data2
        self.engine_gsdm._insert_dim_signature()
        self.engine_gsdm._insert_source()
        self.engine_gsdm._insert_gauges()
        self.engine_gsdm.session.commit()
        self.engine_gsdm._insert_explicit_refs()
        self.engine_gsdm.session.commit()
        self.engine_gsdm._insert_events()
        self.engine_gsdm.session.commit()
        self.engine_gsdm._remove_deprecated_events_by_erase_and_replace()
        self.engine_gsdm.session.commit()
        gauge_ddbb = self.session.query(Gauge).filter(Gauge.name == data2["events"][0]["gauge"]["name"], Gauge.system == data2["events"][0]["gauge"]["system"]).first()
        source_ddbb = self.session.query(DimProcessing).filter(DimProcessing.name == data2["source"]["name"], DimProcessing.validity_start == data2["source"]["validity_start"], DimProcessing.validity_stop == data2["source"]["validity_stop"], DimProcessing.generation_time == data2["source"]["generation_time"], DimProcessing.dim_exec_version == data2["dim_signature"]["version"]).first()
        dim_signature_ddbb = self.session.query(DimSignature).filter(DimSignature.dim_signature == data2["dim_signature"]["name"], DimSignature.dim_exec_name == data2["dim_signature"]["exec"]).first()
        explicit_reference_ddbb = self.session.query(ExplicitRef).filter(ExplicitRef.explicit_ref == data2["events"][0]["explicit_reference"]).first()

        filtered_events_ddbb = self.session.query(Event).filter(Event.start == data2["events"][0]["start"],
                                                      Event.stop == data2["events"][0]["stop"],
                                                      Event.gauge_id == gauge_ddbb.gauge_id,
                                                      Event.processing_uuid == source_ddbb.processing_uuid,
                                                      Event.explicit_ref_id == explicit_reference_ddbb.explicit_ref_id,
                                                      Event.visible == True).all()

        assert len(filtered_events_ddbb) == 1

        events_ddbb = self.session.query(Event).all()

        assert len(events_ddbb) == 1

        values_ddbb = self.query_gsdm.get_event_values()

        assert len(values_ddbb) == 8

    def test_remove_deprecated_event_erase_and_replace_split_events(self):

        self.engine_gsdm._initialize_context_insert_data()
        data1 = {"dim_signature": {"name": "dim_signature",
                                   "exec": "exec",
                                   "version": "1.0"},
                 "source": {"name": "source1.xml",
                            "generation_time": "2018-07-10T02:07:03",
                            "validity_start": "2018-06-05T02:07:03",
                            "validity_stop": "2018-06-05T08:07:36"},
                 "events": [{
                     "explicit_reference": "EXPLICIT_REFERENCE_EVENT2",
                     "gauge": {"name": "GAUGE_NAME",
                               "system": "GAUGE_SYSTEM",
                               "insertion_type": "SIMPLE_UPDATE"},
                     "start": "2018-06-05T02:07:03",
                     "stop": "2018-06-05T04:07:36"
                }]
            }
        self.engine_gsdm.operation = data1
        self.engine_gsdm._insert_dim_signature()
        self.engine_gsdm._insert_source()
        self.engine_gsdm._insert_gauges()
        self.engine_gsdm.session.commit()
        self.engine_gsdm._insert_explicit_refs()
        self.engine_gsdm.session.commit()
        self.engine_gsdm._insert_events()
        self.engine_gsdm.session.commit()

        self.engine_gsdm._initialize_context_insert_data()
        data2 = {"dim_signature": {"name": "dim_signature",
                                   "exec": "exec",
                                   "version": "1.0"},
                 "source": {"name": "source2.xml",
                            "generation_time": "2018-07-10T02:07:03",
                            "validity_start": "2018-06-05T11:07:03",
                            "validity_stop": "2018-06-05T17:07:36"},
                 "events": [{
                     "explicit_reference": "EXPLICIT_REFERENCE_EVENT2",
                     "gauge": {"name": "GAUGE_NAME",
                               "system": "GAUGE_SYSTEM",
                               "insertion_type": "SIMPLE_UPDATE"},
                     "start": "2018-06-05T12:07:03",
                     "stop": "2018-06-05T15:07:36"
                }]
            }
        self.engine_gsdm.operation = data2
        self.engine_gsdm._insert_dim_signature()
        self.engine_gsdm._insert_source()
        self.engine_gsdm._insert_gauges()
        self.engine_gsdm.session.commit()
        self.engine_gsdm._insert_explicit_refs()
        self.engine_gsdm.session.commit()
        self.engine_gsdm._insert_events()
        self.engine_gsdm.session.commit()

        data3 = {"dim_signature": {"name": "dim_signature",
                                   "exec": "exec",
                                   "version": "1.0"},
                 "source": {"name": "source3.xml",
                            "generation_time": "2018-07-05T02:07:04",
                            "validity_start": "2018-06-04T02:07:03",
                            "validity_stop": "2018-06-06T08:07:36"},
                 "events": [{
                     "explicit_reference": "EXPLICIT_REFERENCE_EVENT1",
                     "gauge": {"name": "GAUGE_NAME",
                               "system": "GAUGE_SYSTEM",
                               "insertion_type": "ERASE_and_REPLACE"},
                     "start": "2018-06-04T05:07:03",
                     "stop": "2018-06-06T07:07:36"
                }]
            }
        self.engine_gsdm._initialize_context_insert_data()
        self.engine_gsdm.operation = data3
        self.engine_gsdm._insert_dim_signature()
        self.engine_gsdm._insert_source()
        self.engine_gsdm._insert_gauges()
        self.engine_gsdm.session.commit()
        self.engine_gsdm._insert_explicit_refs()
        self.engine_gsdm.session.commit()
        self.engine_gsdm._insert_events()
        self.engine_gsdm.session.commit()
        self.engine_gsdm._remove_deprecated_events_by_erase_and_replace()
        self.engine_gsdm.session.commit()
        gauge_ddbb = self.session.query(Gauge).filter(Gauge.name == data1["events"][0]["gauge"]["name"], Gauge.system == data1["events"][0]["gauge"]["system"]).first()
        source_ddbb = self.session.query(DimProcessing).filter(DimProcessing.name == data1["source"]["name"], DimProcessing.validity_start == data1["source"]["validity_start"], DimProcessing.validity_stop == data1["source"]["validity_stop"], DimProcessing.generation_time == data1["source"]["generation_time"], DimProcessing.dim_exec_version == data1["dim_signature"]["version"]).first()
        dim_signature_ddbb = self.session.query(DimSignature).filter(DimSignature.dim_signature == data1["dim_signature"]["name"], DimSignature.dim_exec_name == data1["dim_signature"]["exec"]).first()
        explicit_reference_ddbb = self.session.query(ExplicitRef).filter(ExplicitRef.explicit_ref == data1["events"][0]["explicit_reference"]).first()

        filtered_events_ddbb = self.session.query(Event).filter(Event.start == data1["events"][0]["start"],
                                                      Event.stop == data1["events"][0]["stop"],
                                                      Event.gauge_id == gauge_ddbb.gauge_id,
                                                      Event.processing_uuid == source_ddbb.processing_uuid,
                                                      Event.explicit_ref_id == explicit_reference_ddbb.explicit_ref_id,
                                                      Event.visible == True).all()

        assert len(filtered_events_ddbb) == 1

        gauge_ddbb = self.session.query(Gauge).filter(Gauge.name == data2["events"][0]["gauge"]["name"], Gauge.system == data2["events"][0]["gauge"]["system"]).first()
        source_ddbb = self.session.query(DimProcessing).filter(DimProcessing.name == data2["source"]["name"], DimProcessing.validity_start == data2["source"]["validity_start"], DimProcessing.validity_stop == data2["source"]["validity_stop"], DimProcessing.generation_time == data2["source"]["generation_time"], DimProcessing.dim_exec_version == data2["dim_signature"]["version"]).first()
        dim_signature_ddbb = self.session.query(DimSignature).filter(DimSignature.dim_signature == data2["dim_signature"]["name"], DimSignature.dim_exec_name == data2["dim_signature"]["exec"]).first()
        explicit_reference_ddbb = self.session.query(ExplicitRef).filter(ExplicitRef.explicit_ref == data2["events"][0]["explicit_reference"]).first()

        filtered_events_ddbb = self.session.query(Event).filter(Event.start == data2["events"][0]["start"],
                                                      Event.stop == data2["events"][0]["stop"],
                                                      Event.gauge_id == gauge_ddbb.gauge_id,
                                                      Event.processing_uuid == source_ddbb.processing_uuid,
                                                      Event.explicit_ref_id == explicit_reference_ddbb.explicit_ref_id,
                                                      Event.visible == True).all()

        assert len(filtered_events_ddbb) == 1

        gauge_ddbb = self.session.query(Gauge).filter(Gauge.name == data3["events"][0]["gauge"]["name"], Gauge.system == data3["events"][0]["gauge"]["system"]).first()
        source_ddbb = self.session.query(DimProcessing).filter(DimProcessing.name == data3["source"]["name"], DimProcessing.validity_start == data3["source"]["validity_start"], DimProcessing.validity_stop == data3["source"]["validity_stop"], DimProcessing.generation_time == data3["source"]["generation_time"], DimProcessing.dim_exec_version == data3["dim_signature"]["version"]).first()
        dim_signature_ddbb = self.session.query(DimSignature).filter(DimSignature.dim_signature == data3["dim_signature"]["name"], DimSignature.dim_exec_name == data3["dim_signature"]["exec"]).first()
        explicit_reference_ddbb = self.session.query(ExplicitRef).filter(ExplicitRef.explicit_ref == data3["events"][0]["explicit_reference"]).first()

        filtered_events_ddbb = self.session.query(Event).filter(Event.start == data3["events"][0]["start"],
                                                      Event.stop == data1["source"]["validity_start"],
                                                      Event.gauge_id == gauge_ddbb.gauge_id,
                                                      Event.processing_uuid == source_ddbb.processing_uuid,
                                                      Event.explicit_ref_id == explicit_reference_ddbb.explicit_ref_id,
                                                      Event.visible == True).all()

        assert len(filtered_events_ddbb) == 1

        gauge_ddbb = self.session.query(Gauge).filter(Gauge.name == data3["events"][0]["gauge"]["name"], Gauge.system == data3["events"][0]["gauge"]["system"]).first()
        source_ddbb = self.session.query(DimProcessing).filter(DimProcessing.name == data3["source"]["name"], DimProcessing.validity_start == data3["source"]["validity_start"], DimProcessing.validity_stop == data3["source"]["validity_stop"], DimProcessing.generation_time == data3["source"]["generation_time"], DimProcessing.dim_exec_version == data3["dim_signature"]["version"]).first()
        dim_signature_ddbb = self.session.query(DimSignature).filter(DimSignature.dim_signature == data3["dim_signature"]["name"], DimSignature.dim_exec_name == data3["dim_signature"]["exec"]).first()
        explicit_reference_ddbb = self.session.query(ExplicitRef).filter(ExplicitRef.explicit_ref == data3["events"][0]["explicit_reference"]).first()

        filtered_events_ddbb = self.session.query(Event).filter(Event.start == data1["source"]["validity_stop"],
                                                      Event.stop == data2["source"]["validity_start"],
                                                      Event.gauge_id == gauge_ddbb.gauge_id,
                                                      Event.processing_uuid == source_ddbb.processing_uuid,
                                                      Event.explicit_ref_id == explicit_reference_ddbb.explicit_ref_id,
                                                      Event.visible == True).all()

        assert len(filtered_events_ddbb) == 1

        filtered_events_ddbb = self.session.query(Event).filter(Event.start == data2["source"]["validity_stop"],
                                                      Event.stop == data3["events"][0]["stop"],
                                                      Event.gauge_id == gauge_ddbb.gauge_id,
                                                      Event.processing_uuid == source_ddbb.processing_uuid,
                                                      Event.explicit_ref_id == explicit_reference_ddbb.explicit_ref_id,
                                                      Event.visible == True).all()

        assert len(filtered_events_ddbb) == 1

        events_ddbb = self.session.query(Event).all()

        assert len(events_ddbb) == 5

    def test_remove_deprecated_event_erase_and_replace_split_events_not_created(self):

        self.engine_gsdm._initialize_context_insert_data()
        data1 = {"dim_signature": {"name": "dim_signature",
                                   "exec": "exec",
                                   "version": "1.0"},
                 "source": {"name": "source1.xml",
                            "generation_time": "2018-07-10T02:07:03",
                            "validity_start": "2018-06-05T02:07:03",
                            "validity_stop": "2018-06-05T08:07:36"},
                 "events": [{
                     "explicit_reference": "EXPLICIT_REFERENCE_EVENT2",
                     "gauge": {"name": "GAUGE_NAME",
                               "system": "GAUGE_SYSTEM",
                               "insertion_type": "SIMPLE_UPDATE"},
                     "start": "2018-06-05T02:07:03",
                     "stop": "2018-06-05T04:07:36"
                }]
            }
        self.engine_gsdm.operation = data1
        self.engine_gsdm._insert_dim_signature()
        self.engine_gsdm._insert_source()
        self.engine_gsdm._insert_gauges()
        self.engine_gsdm.session.commit()
        self.engine_gsdm._insert_explicit_refs()
        self.engine_gsdm.session.commit()
        self.engine_gsdm._insert_events()
        self.engine_gsdm.session.commit()

        self.engine_gsdm._initialize_context_insert_data()
        data2 = {"dim_signature": {"name": "dim_signature",
                                   "exec": "exec",
                                   "version": "1.0"},
                 "source": {"name": "source2.xml",
                            "generation_time": "2018-07-10T02:07:03",
                            "validity_start": "2018-06-05T08:07:36",
                            "validity_stop": "2018-06-05T17:07:36"},
                 "events": [{
                     "explicit_reference": "EXPLICIT_REFERENCE_EVENT2",
                     "gauge": {"name": "GAUGE_NAME",
                               "system": "GAUGE_SYSTEM",
                               "insertion_type": "SIMPLE_UPDATE"},
                     "start": "2018-06-05T09:07:03",
                     "stop": "2018-06-05T15:07:36"
                }]
            }
        self.engine_gsdm.operation = data2
        self.engine_gsdm._insert_dim_signature()
        self.engine_gsdm._insert_source()
        self.engine_gsdm._insert_gauges()
        self.engine_gsdm.session.commit()
        self.engine_gsdm._insert_explicit_refs()
        self.engine_gsdm.session.commit()
        self.engine_gsdm._insert_events()
        self.engine_gsdm.session.commit()

        data3 = {"dim_signature": {"name": "dim_signature",
                                   "exec": "exec",
                                   "version": "1.0"},
                 "source": {"name": "source3.xml",
                            "generation_time": "2018-07-05T02:07:04",
                            "validity_start": "2018-06-04T02:07:03",
                            "validity_stop": "2018-06-06T08:07:36"},
                 "events": [{
                     "explicit_reference": "EXPLICIT_REFERENCE_EVENT1",
                     "gauge": {"name": "GAUGE_NAME",
                               "system": "GAUGE_SYSTEM",
                               "insertion_type": "ERASE_and_REPLACE"},
                     "start": "2018-06-04T05:07:03",
                     "stop": "2018-06-05T09:07:36"
                }]
            }
        self.engine_gsdm._initialize_context_insert_data()
        self.engine_gsdm.operation = data3
        self.engine_gsdm._insert_dim_signature()
        self.engine_gsdm._insert_source()
        self.engine_gsdm._insert_gauges()
        self.engine_gsdm.session.commit()
        self.engine_gsdm._insert_explicit_refs()
        self.engine_gsdm.session.commit()
        self.engine_gsdm._insert_events()
        self.engine_gsdm.session.commit()
        self.engine_gsdm._remove_deprecated_events_by_erase_and_replace()
        self.engine_gsdm.session.commit()
        gauge_ddbb = self.session.query(Gauge).filter(Gauge.name == data1["events"][0]["gauge"]["name"], Gauge.system == data1["events"][0]["gauge"]["system"]).first()
        source_ddbb = self.session.query(DimProcessing).filter(DimProcessing.name == data1["source"]["name"], DimProcessing.validity_start == data1["source"]["validity_start"], DimProcessing.validity_stop == data1["source"]["validity_stop"], DimProcessing.generation_time == data1["source"]["generation_time"], DimProcessing.dim_exec_version == data1["dim_signature"]["version"]).first()
        dim_signature_ddbb = self.session.query(DimSignature).filter(DimSignature.dim_signature == data1["dim_signature"]["name"], DimSignature.dim_exec_name == data1["dim_signature"]["exec"]).first()
        explicit_reference_ddbb = self.session.query(ExplicitRef).filter(ExplicitRef.explicit_ref == data1["events"][0]["explicit_reference"]).first()

        filtered_events_ddbb = self.session.query(Event).filter(Event.start == data1["events"][0]["start"],
                                                      Event.stop == data1["events"][0]["stop"],
                                                      Event.gauge_id == gauge_ddbb.gauge_id,
                                                      Event.processing_uuid == source_ddbb.processing_uuid,
                                                      Event.explicit_ref_id == explicit_reference_ddbb.explicit_ref_id,
                                                      Event.visible == True).all()

        assert len(filtered_events_ddbb) == 1

        gauge_ddbb = self.session.query(Gauge).filter(Gauge.name == data2["events"][0]["gauge"]["name"], Gauge.system == data2["events"][0]["gauge"]["system"]).first()
        source_ddbb = self.session.query(DimProcessing).filter(DimProcessing.name == data2["source"]["name"], DimProcessing.validity_start == data2["source"]["validity_start"], DimProcessing.validity_stop == data2["source"]["validity_stop"], DimProcessing.generation_time == data2["source"]["generation_time"], DimProcessing.dim_exec_version == data2["dim_signature"]["version"]).first()
        dim_signature_ddbb = self.session.query(DimSignature).filter(DimSignature.dim_signature == data2["dim_signature"]["name"], DimSignature.dim_exec_name == data2["dim_signature"]["exec"]).first()
        explicit_reference_ddbb = self.session.query(ExplicitRef).filter(ExplicitRef.explicit_ref == data2["events"][0]["explicit_reference"]).first()

        filtered_events_ddbb = self.session.query(Event).filter(Event.start == data2["events"][0]["start"],
                                                      Event.stop == data2["events"][0]["stop"],
                                                      Event.gauge_id == gauge_ddbb.gauge_id,
                                                      Event.processing_uuid == source_ddbb.processing_uuid,
                                                      Event.explicit_ref_id == explicit_reference_ddbb.explicit_ref_id,
                                                      Event.visible == True).all()

        assert len(filtered_events_ddbb) == 1

        gauge_ddbb = self.session.query(Gauge).filter(Gauge.name == data3["events"][0]["gauge"]["name"], Gauge.system == data3["events"][0]["gauge"]["system"]).first()
        source_ddbb = self.session.query(DimProcessing).filter(DimProcessing.name == data3["source"]["name"], DimProcessing.validity_start == data3["source"]["validity_start"], DimProcessing.validity_stop == data3["source"]["validity_stop"], DimProcessing.generation_time == data3["source"]["generation_time"], DimProcessing.dim_exec_version == data3["dim_signature"]["version"]).first()
        dim_signature_ddbb = self.session.query(DimSignature).filter(DimSignature.dim_signature == data3["dim_signature"]["name"], DimSignature.dim_exec_name == data3["dim_signature"]["exec"]).first()
        explicit_reference_ddbb = self.session.query(ExplicitRef).filter(ExplicitRef.explicit_ref == data3["events"][0]["explicit_reference"]).first()

        filtered_events_ddbb = self.session.query(Event).filter(Event.start == data3["events"][0]["start"],
                                                      Event.stop == data1["source"]["validity_start"],
                                                      Event.gauge_id == gauge_ddbb.gauge_id,
                                                      Event.processing_uuid == source_ddbb.processing_uuid,
                                                      Event.explicit_ref_id == explicit_reference_ddbb.explicit_ref_id,
                                                      Event.visible == True).all()

        assert len(filtered_events_ddbb) == 1

        events_ddbb = self.session.query(Event).all()

        assert len(events_ddbb) == 3

    def test_treat_data(self):

        data = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source.xml",
                       "generation_time": "2018-07-05T02:07:03",
                       "validity_start": "2018-06-05T02:07:03",
                       "validity_stop": "2018-06-05T08:07:36"},
            "events": [{
                "explicit_reference": "EXPLICIT_REFERENCE_EVENT",
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "SIMPLE_UPDATE"},
                "start": "2018-06-05T02:07:03",
                "stop": "2018-06-05T08:07:36"
            }],
            "annotations": [{
                "explicit_reference": "EXPLICIT_REFERENCE_ANNOTATION",
                "annotation_cnf": {"name": "NAME",
                                   "system": "SYSTEM"}
            }]
        }]}

        self.engine_gsdm.data = data
        returned_value = self.engine_gsdm.treat_data()

        assert returned_value == self.engine_gsdm.exit_codes["OK"]["status"]

        events_ddbb = self.session.query(Event).all()

        assert len(events_ddbb) == 1

        annotations_ddbb = self.session.query(Annotation).all()

        assert len(annotations_ddbb) == 1

    def test_treat_data_source_already_inserted(self):

        data = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source.xml",
                       "generation_time": "2018-07-05T02:07:03",
                       "validity_start": "2018-06-05T02:07:03",
                       "validity_stop": "2018-06-05T08:07:36"}
        }]}

        self.engine_gsdm.data = data
        self.engine_gsdm.treat_data()

        data = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source.xml",
                       "generation_time": "2018-07-05T02:07:03",
                       "validity_start": "2018-06-05T02:07:03",
                       "validity_stop": "2018-06-05T08:07:36"}
        }]}

        self.engine_gsdm.data = data
        returned_value = self.engine_gsdm.treat_data()

        assert returned_value == self.engine_gsdm.exit_codes["SOURCE_ALREADY_INGESTED"]["status"]

        sources_status = self.session.query(DimProcessingStatus).join(DimProcessing).filter(DimProcessingStatus.proc_status == self.engine_gsdm.exit_codes["SOURCE_ALREADY_INGESTED"]["status"],
                                                                           DimProcessing.name == data["operations"][0]["source"]["name"]).all()

        assert len(sources_status) == 1

        sources_status = self.session.query(DimProcessingStatus).join(DimProcessing).filter(DimProcessingStatus.proc_status == self.engine_gsdm.exit_codes["OK"]["status"],
                                                                           DimProcessing.name == data["operations"][0]["source"]["name"]).all()

        assert len(sources_status) == 1

    def test_treat_data_wrong_source_period(self):

        data = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source.xml",
                       "generation_time": "2018-07-05T02:07:03",
                       "validity_start": "2018-06-07T02:07:03",
                       "validity_stop": "2018-06-05T08:07:36"}
        }]}

        self.engine_gsdm.data = data
        returned_value = self.engine_gsdm.treat_data()

        assert returned_value == self.engine_gsdm.exit_codes["WRONG_SOURCE_PERIOD"]["status"]

        sources_status = self.session.query(DimProcessingStatus).join(DimProcessing).filter(DimProcessingStatus.proc_status == self.engine_gsdm.exit_codes["WRONG_SOURCE_PERIOD"]["status"],
                                                                           DimProcessing.name == data["operations"][0]["source"]["name"]).all()

        assert len(sources_status) == 1

    def test_treat_data_wrong_event_link(self):

        data = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source.xml",
                       "generation_time": "2018-07-05T02:07:03",
                       "validity_start": "2018-06-05T02:07:03",
                       "validity_stop": "2018-06-05T08:07:36"},
            "events": [{
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "SIMPLE_UPDATE"},
                "start": "2018-06-05T02:07:03",
                "stop": "2018-06-05T08:07:36",
                "links": [{
                    "link": "EVENT_LINK",
                    "link_mode": "by_ref",
                    "name": "EVENT_LINK_NAME"
                }]
            }]
        }]}

        self.engine_gsdm.data = data
        returned_value = self.engine_gsdm.treat_data()

        assert returned_value == self.engine_gsdm.exit_codes["INCOMPLETE_EVENT_LINKS"]["status"]

        sources_status = self.session.query(DimProcessingStatus).join(DimProcessing).filter(DimProcessingStatus.proc_status == self.engine_gsdm.exit_codes["INCOMPLETE_EVENT_LINKS"]["status"],
                                                                           DimProcessing.name == data["operations"][0]["source"]["name"]).all()

        assert len(sources_status) == 1

    def test_treat_data_wrong_event_period(self):

        data = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source.xml",
                       "generation_time": "2018-07-05T02:07:03",
                       "validity_start": "2018-06-05T02:07:03",
                       "validity_stop": "2018-06-05T08:07:36"},
            "events": [{
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "SIMPLE_UPDATE"},
                "start": "2018-06-05T02:07:03",
                "stop": "2018-06-05T01:07:36"
            }]
        }]}

        self.engine_gsdm.data = data
        returned_value = self.engine_gsdm.treat_data()

        assert returned_value == self.engine_gsdm.exit_codes["WRONG_EVENT_PERIOD"]["status"]

        sources_status = self.session.query(DimProcessingStatus).join(DimProcessing).filter(DimProcessingStatus.proc_status == self.engine_gsdm.exit_codes["WRONG_EVENT_PERIOD"]["status"],
                                                                           DimProcessing.name == data["operations"][0]["source"]["name"]).all()

        assert len(sources_status) == 1

    def test_treat_data_wrong_event_value(self):

        data = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source.xml",
                       "generation_time": "2018-07-05T02:07:03",
                       "validity_start": "2018-06-05T02:07:03",
                       "validity_stop": "2018-06-05T08:07:36"},
            "events": [{
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "SIMPLE_UPDATE"},
                "start": "2018-06-05T02:07:03",
                "stop": "2018-06-05T08:07:36",
                "values": [{"name": "VALUES",
                            "type": "object",
                            "values": [
                                {"type": "double",
                                 "name": "NOT_A_DOUBLE",
                                 "value": "NOT_A_DOUBLE"}]
                        }]
            }]
        }]}

        self.engine_gsdm.data = data
        returned_value = self.engine_gsdm.treat_data()

        assert returned_value == self.engine_gsdm.exit_codes["WRONG_VALUE"]["status"]

        sources_status = self.session.query(DimProcessingStatus).join(DimProcessing).filter(DimProcessingStatus.proc_status == self.engine_gsdm.exit_codes["WRONG_VALUE"]["status"],
                                                                           DimProcessing.name == data["operations"][0]["source"]["name"]).all()

        assert len(sources_status) == 1

    def test_treat_data_odd_number_event_geometry(self):

        data = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source.xml",
                       "generation_time": "2018-07-05T02:07:03",
                       "validity_start": "2018-06-05T02:07:03",
                       "validity_stop": "2018-06-05T08:07:36"},
            "events": [{
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "SIMPLE_UPDATE"},
                "start": "2018-06-05T02:07:03",
                "stop": "2018-06-05T08:07:36",
                "values": [{"name": "VALUES",
                            "type": "object",
                            "values": [
                                {"type": "geometry",
                                 "name": "ODD_GEOMETRY",
                                 "value": "29.012974905944"}]
                        }]
            }]
        }]}

        self.engine_gsdm.data = data
        returned_value = self.engine_gsdm.treat_data()

        assert returned_value == self.engine_gsdm.exit_codes["ODD_NUMBER_OF_COORDINATES"]["status"]

        sources_status = self.session.query(DimProcessingStatus).join(DimProcessing).filter(DimProcessingStatus.proc_status == self.engine_gsdm.exit_codes["ODD_NUMBER_OF_COORDINATES"]["status"],
                                                                           DimProcessing.name == data["operations"][0]["source"]["name"]).all()

        assert len(sources_status) == 1

    def test_treat_data_wrong_annotation_value(self):

        data = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source.xml",
                       "generation_time": "2018-07-05T02:07:03",
                       "validity_start": "2018-06-05T02:07:03",
                       "validity_stop": "2018-06-05T08:07:36"},
            "annotations": [{
                "explicit_reference": "EXPLICIT_REFERENCE",
                "annotation_cnf": {"name": "GAUGE_NAME",
                                   "system": "GAUGE_SYSTEM"},
                "values": [{"name": "VALUES",
                            "type": "object",
                            "values": [
                                {"type": "double",
                                 "name": "NOT_A_DOUBLE",
                                 "value": "NOT_A_DOUBLE"}]
                        }]
            }]
        }]}

        self.engine_gsdm.data = data
        returned_value = self.engine_gsdm.treat_data()

        assert returned_value == self.engine_gsdm.exit_codes["WRONG_VALUE"]["status"]

        sources_status = self.session.query(DimProcessingStatus).join(DimProcessing).filter(DimProcessingStatus.proc_status == self.engine_gsdm.exit_codes["WRONG_VALUE"]["status"],
                                                                           DimProcessing.name == data["operations"][0]["source"]["name"]).all()

        assert len(sources_status) == 1

    def test_treat_data_odd_number_annotation_geometry(self):

        data = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source.xml",
                       "generation_time": "2018-07-05T02:07:03",
                       "validity_start": "2018-06-05T02:07:03",
                       "validity_stop": "2018-06-05T08:07:36"},
            "annotations": [{
                "explicit_reference": "EXPLICIT_REFERENCE",
                "annotation_cnf": {"name": "GAUGE_NAME",
                                   "system": "GAUGE_SYSTEM"},
                "values": [{"name": "VALUES",
                            "type": "object",
                            "values": [
                                {"type": "geometry",
                                 "name": "ODD_GEOMETRY",
                                 "value": "29.012974905944"}]
                        }]
            }]
        }]}

        self.engine_gsdm.data = data
        returned_value = self.engine_gsdm.treat_data()

        assert returned_value == self.engine_gsdm.exit_codes["ODD_NUMBER_OF_COORDINATES"]["status"]

        sources_status = self.session.query(DimProcessingStatus).join(DimProcessing).filter(DimProcessingStatus.proc_status == self.engine_gsdm.exit_codes["ODD_NUMBER_OF_COORDINATES"]["status"],
                                                                           DimProcessing.name == data["operations"][0]["source"]["name"]).all()

        assert len(sources_status) == 1

    def test_insert_xml(self):

        filename = "test_simple_update.xml"
        self.engine_gsdm.parse_data_from_xml(os.path.dirname(os.path.abspath(__file__)) + "/xml_inputs/" + filename)

        returned_value = self.engine_gsdm.treat_data()

        assert returned_value == self.engine_gsdm.exit_codes["OK"]["status"]

        sources_status = self.session.query(DimProcessingStatus).join(DimProcessing).filter(DimProcessingStatus.proc_status == self.engine_gsdm.exit_codes["OK"]["status"],
                                                                                            DimProcessing.name == filename).all()

        assert len(sources_status) == 1

        events_ddbb = self.session.query(Event).all()

        assert len(events_ddbb) == 2

        annotations_ddbb = self.session.query(Annotation).all()

        assert len(annotations_ddbb) == 1

    def test_wrong_xml(self):

        filename = "test_wrong_structure.xml"
        returned_value = self.engine_gsdm.parse_data_from_xml(os.path.dirname(os.path.abspath(__file__)) + "/xml_inputs/" + filename)

        assert returned_value == self.engine_gsdm.exit_codes["FILE_NOT_VALID"]["status"]

    def test_not_xml(self):

        filename = "test_not_xml.xml"
        returned_value = self.engine_gsdm.parse_data_from_xml(os.path.dirname(os.path.abspath(__file__)) + "/xml_inputs/" + filename)

        assert returned_value == self.engine_gsdm.exit_codes["FILE_NOT_VALID"]["status"]

    def test_insert_json(self):

        filename = "test_simple_update.json"
        self.engine_gsdm.parse_data_from_json(os.path.dirname(os.path.abspath(__file__)) + "/json_inputs/" + filename)

        returned_value = self.engine_gsdm.treat_data()

        assert returned_value == self.engine_gsdm.exit_codes["OK"]["status"]

        sources_status = self.session.query(DimProcessingStatus).join(DimProcessing).filter(DimProcessingStatus.proc_status == self.engine_gsdm.exit_codes["OK"]["status"],
                                                                                            DimProcessing.name == filename).all()

        assert len(sources_status) == 1

        events_ddbb = self.session.query(Event).all()

        assert len(events_ddbb) == 2

        annotations_ddbb = self.session.query(Annotation).all()

        assert len(annotations_ddbb) == 1

    def test_wrong_json(self):

        filename = "test_wrong_structure.json"
        returned_value = self.engine_gsdm.parse_data_from_json(os.path.dirname(os.path.abspath(__file__)) + "/json_inputs/" + filename)

        assert returned_value == self.engine_gsdm.exit_codes["FILE_NOT_VALID"]["status"]

    def test_not_json(self):

        filename = "test_not_json.json"
        returned_value = self.engine_gsdm.parse_data_from_json(os.path.dirname(os.path.abspath(__file__)) + "/json_inputs/" + filename)

        assert returned_value == self.engine_gsdm.exit_codes["FILE_NOT_VALID"]["status"]

    def test_is_datetime(self):

        result = gsdm.engine.engine.is_datetime("2018-06-05T02:07:03")
        assert result == True
        result = gsdm.engine.engine.is_datetime("NOT_A_DATE")
        assert result == False

    def test_no_gsdm_resources_path(self):

        gsdm_resources_path = os.environ["GSDM_RESOURCES_PATH"]
        del os.environ["GSDM_RESOURCES_PATH"]

        try:
            gsdm.engine.engine.read_configuration()
        except GsdmResourcesPathNotAvailable:
            assert True == True
        except:
            assert False == True

        os.environ["GSDM_RESOURCES_PATH"] = gsdm_resources_path

    def test_change_logging_level(self):

        os.environ["GSDM_LOG_LEVEL"] = "DEBUG"
        os.environ["GSDM_STREAM_LOG"] = "YES"

        gsdm.engine.engine.define_logging_configuration()

        assert gsdm.engine.engine.logging_level == logging.DEBUG
        
    def test_insert_event_simple_update_debug(self):
        os.environ["GSDM_LOG_LEVEL"] = "DEBUG"

        gsdm.engine.engine.define_logging_configuration()

        self.engine_gsdm._initialize_context_insert_data()
        data = {"dim_signature": {"name": "dim_signature",
                                  "exec": "exec",
                                  "version": "1.0"},
                "source": {"name": "source.xml",
                           "generation_time": "2018-07-05T02:07:03",
                           "validity_start": "2018-06-05T02:07:03",
                           "validity_stop": "2018-06-05T08:07:36"},
                "events": [{
                    "explicit_reference": "EXPLICIT_REFERENCE_EVENT",
                    "gauge": {"name": "GAUGE_NAME",
                              "system": "GAUGE_SYSTEM",
                              "insertion_type": "SIMPLE_UPDATE"},
                    "start": "2018-06-05T02:07:03",
                    "stop": "2018-06-05T08:07:36"
                }]
            }
        self.engine_gsdm.operation = data
        self.engine_gsdm._insert_dim_signature()
        self.engine_gsdm._insert_source()
        self.engine_gsdm._insert_gauges()
        self.engine_gsdm.session.commit()
        self.engine_gsdm._insert_explicit_refs()
        self.engine_gsdm.session.commit()
        gauge_ddbb = self.session.query(Gauge).filter(Gauge.name == data["events"][0]["gauge"]["name"], Gauge.system == data["events"][0]["gauge"]["system"]).first()
        source_ddbb = self.session.query(DimProcessing).filter(DimProcessing.name == data["source"]["name"], DimProcessing.validity_start == data["source"]["validity_start"], DimProcessing.validity_stop == data["source"]["validity_stop"], DimProcessing.generation_time == data["source"]["generation_time"], DimProcessing.dim_exec_version == data["dim_signature"]["version"]).first()
        dim_signature_ddbb = self.session.query(DimSignature).filter(DimSignature.dim_signature == data["dim_signature"]["name"], DimSignature.dim_exec_name == data["dim_signature"]["exec"]).first()
        explicit_reference_ddbb = self.session.query(ExplicitRef).filter(ExplicitRef.explicit_ref == data["events"][0]["explicit_reference"]).first()
        self.engine_gsdm._insert_events()
        self.engine_gsdm.session.commit()
        event_ddbb = self.session.query(Event).filter(Event.start == data["events"][0]["start"],
                                                      Event.stop == data["events"][0]["stop"],
                                                      Event.gauge_id == gauge_ddbb.gauge_id,
                                                      Event.processing_uuid == source_ddbb.processing_uuid,
                                                      Event.explicit_ref_id == explicit_reference_ddbb.explicit_ref_id,
                                                      Event.visible == True).all()

        assert len(event_ddbb) == 1
