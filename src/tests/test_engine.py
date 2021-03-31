"""
Automated tests for the engine submodule

Written by DEIMOS Space S.L. (dibb)

module eboa
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
import eboa.engine.engine as eboa_engine
from eboa.engine.engine import Engine
from eboa.engine.query import Query
from eboa.datamodel.base import Session, engine, Base
from eboa.engine.errors import UndefinedEventLink, DuplicatedEventLinkRef, WrongPeriod, SourceAlreadyIngested, WrongValue, OddNumberOfCoordinates, EboaResourcesPathNotAvailable, WrongGeometry
from eboa.engine.errors import LinksInconsistency

# Import datamodel
from eboa.datamodel.dim_signatures import DimSignature
from eboa.datamodel.alerts import Alert, AlertGroup, EventAlert, AnnotationAlert, SourceAlert, ExplicitRefAlert
from eboa.datamodel.events import Event, EventLink, EventKey, EventText, EventDouble, EventObject, EventGeometry, EventBoolean, EventTimestamp
from eboa.datamodel.gauges import Gauge
from eboa.datamodel.sources import Source, SourceStatus
from eboa.datamodel.explicit_refs import ExplicitRef, ExplicitRefGrp, ExplicitRefLink
from eboa.datamodel.annotations import Annotation, AnnotationCnf, AnnotationText, AnnotationDouble, AnnotationObject, AnnotationGeometry, AnnotationBoolean, AnnotationTimestamp

# Import GEOalchemy entities
from geoalchemy2 import functions
from geoalchemy2.shape import to_shape

# Import SQLalchemy entities
from sqlalchemy import func

# Import logging
from eboa.logging import Log

class TestEngine(unittest.TestCase):
    def setUp(self):
        # Create the engine to manage the data
        self.engine_eboa = Engine()
        self.engine_eboa_race_conditions = Engine()
        self.query_eboa = Query()

        # Create session to connect to the database
        self.session = Session()

        # Clear all tables before executing the test
        self.query_eboa.clear_db()

    def tearDown(self):
        # Close connections to the DDBB
        self.engine_eboa.close_session()
        self.engine_eboa_race_conditions.close_session()
        self.query_eboa.close_session()
        self.session.close()

    def test_insert_dim_signature(self):
        data = {"dim_signature": {"name": "dim_signature",
                                  "exec": "exec",
                                  "version": "1.0"}
                                  }
        self.engine_eboa.operation = data
        self.engine_eboa._insert_dim_signature()

        dim_signature_ddbb = self.session.query(DimSignature).filter(DimSignature.dim_signature == data["dim_signature"]["name"]).all()

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
        self.engine_eboa.operation = data
        self.engine_eboa_race_conditions.operation = data
        with before_after.before("eboa.engine.engine.race_condition", self.engine_eboa_race_conditions._insert_dim_signature):
            self.engine_eboa._insert_dim_signature()

        dim_signature_ddbb = self.session.query(DimSignature).filter(DimSignature.dim_signature == data["dim_signature"]["name"]).all()

        assert len(dim_signature_ddbb) == 1


    def test_insert_source(self):
        data = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source.xml",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2018-07-05T02:07:03",
                       "validity_start": "2018-06-05T02:07:03",
                       "validity_stop": "2018-06-05T08:07:36"}
        }]
        }
        returned_value = self.engine_eboa.treat_data(data)[0]["status"]

        source_ddbb = self.session.query(Source).filter(Source.name == data["operations"][0]["source"]["name"], Source.validity_start == data["operations"][0]["source"]["validity_start"], Source.validity_stop == data["operations"][0]["source"]["validity_stop"], Source.generation_time == data["operations"][0]["source"]["generation_time"], Source.processor_version == data["operations"][0]["dim_signature"]["version"]).all()

        assert len(source_ddbb) == 1

    def test_insert_source_again(self):
        self.test_insert_source()
        data = self.engine_eboa.operation

        self.engine_eboa.ingestion_start = datetime.datetime.now()
        self.engine_eboa._insert_source_status(0, final = True)
        
        try:
            self.test_insert_source()
        except SourceAlreadyIngested:
            pass

        source_ddbb = self.session.query(Source).filter(Source.name == data["source"]["name"], Source.validity_start == data["source"]["validity_start"], Source.validity_stop == data["source"]["validity_stop"], Source.generation_time == data["source"]["generation_time"], Source.processor_version == data["dim_signature"]["version"]).all()

        assert len(source_ddbb) == 1

    def test_insert_source_again_no_previous_ingestion_finished(self):
        self.test_insert_source()
        data = self.engine_eboa.operation

        returned_value = self.test_insert_source()

        assert returned_value == None

    def test_insert_source_wrong_validity(self):
        """
        Method to test that the engine protects the ingestion from a wrong
        validity period where the start is greater than the stop
        """
        data = {"dim_signature": {"name": "dim_signature",
                                  "exec": "exec",
                                  "version": "1.0"},
                "source": {"name": "source_wrong_period.xml",
                           "reception_time": "2018-06-06T13:33:29",
                           "generation_time": "2018-07-05T02:07:03",
                           "validity_start": "2018-06-05T10:07:03",
                           "validity_stop": "2018-06-05T08:07:36"}
            }
        self.engine_eboa.operation = data
        self.engine_eboa._insert_dim_signature()
        try:
            self.engine_eboa._insert_source()
        except WrongPeriod:
            pass

        source_ddbb = self.session.query(Source).filter(Source.name == data["source"]["name"], Source.generation_time == data["source"]["generation_time"], Source.processor_version == data["dim_signature"]["version"]).all()

        assert len(source_ddbb) == 1

    def test_race_condition_insert_source_wrong_validity(self):
        """Method to test the race condition that could be produced if two
        processes try to ingest the same source with a wrong period

        """
        data = {"dim_signature": {"name": "dim_signature",
                                  "exec": "exec",
                                  "version": "1.0"},
                "source": {"name": "source_wrong_period.xml",
                           "reception_time": "2018-06-06T13:33:29",
                           "generation_time": "2018-07-05T02:07:03",
                           "validity_start": "2018-06-05T10:07:03",
                           "validity_stop": "2018-06-05T08:07:36"}
            }
        self.engine_eboa.operation = data
        self.engine_eboa_race_conditions.operation = data
        self.engine_eboa._insert_dim_signature()
        self.engine_eboa_race_conditions._insert_dim_signature()
        def insert_source_race_condition():
            try:
                self.engine_eboa_race_conditions._insert_source()
            except WrongPeriod:
                pass

        def insert_source():
            try:
                self.engine_eboa._insert_source()
            except WrongPeriod:
                pass

        with before_after.before("eboa.engine.engine.race_condition", insert_source_race_condition):
            insert_source()

        source_ddbb = self.session.query(Source).filter(Source.name == data["source"]["name"], Source.generation_time == data["source"]["generation_time"], Source.processor_version == data["dim_signature"]["version"]).all()

        assert len(source_ddbb) == 1

    def test_race_condition_insert_source(self):
        """Method to test the race condition that could be produced if two
        processes try to ingest the same source

        """
        data = {"dim_signature": {"name": "dim_signature",
                                  "exec": "exec",
                                  "version": "1.0"},
                "source": {"name": "source.xml",
                           "reception_time": "2018-06-06T13:33:29",
                           "generation_time": "2018-07-05T02:07:03",
                           "validity_start": "2018-06-05T02:07:03",
                           "validity_stop": "2018-06-05T08:07:36"}
            }
        self.engine_eboa.operation = data
        self.engine_eboa_race_conditions.operation = data
        self.engine_eboa._insert_dim_signature()
        self.engine_eboa_race_conditions._insert_dim_signature()
        def insert_source_race_condition():
            try:
                self.engine_eboa_race_conditions._insert_source()
            except SourceAlreadyIngested:
                pass

        def insert_source():
            try:
                self.engine_eboa._insert_source()
            except SourceAlreadyIngested:
                pass

        with before_after.before("eboa.engine.engine.race_condition", insert_source_race_condition):
            insert_source()

        source_ddbb = self.session.query(Source).filter(Source.name == data["source"]["name"], Source.validity_start == data["source"]["validity_start"], Source.validity_stop == data["source"]["validity_stop"], Source.generation_time == data["source"]["generation_time"], Source.processor_version == data["dim_signature"]["version"]).all()

        assert len(source_ddbb) == 1

    def test_insert_source_without_dim_signature(self):
        name = "source_withoud_dim_signature.xml"
        self.engine_eboa._insert_source_without_dim_signature(name)

        source_ddbb = self.session.query(Source).filter(Source.name == name).all()

        assert len(source_ddbb) == 1

    def test_insert_source_without_dim_signature_again(self):
        self.test_insert_source_without_dim_signature()
        self.test_insert_source_without_dim_signature()

    def test_insert_source_twice_first_time_marked_with_ingested_false_second_with_alerts(self):

        data = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source.xml",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2018-07-05T02:07:03",
                       "validity_start": "2018-06-05T02:07:03",
                       "validity_stop": "2018-06-05T08:07:36",
                       "ingested": "false"}
        }]
        }
        exit_status = self.engine_eboa.treat_data(data)

        assert len([item for item in exit_status if item["status"] != eboa_engine.exit_codes["OK"]["status"]]) == 0

        data = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source.xml",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2018-07-05T02:07:03",
                       "validity_start": "2018-06-05T02:07:03",
                       "validity_stop": "2018-06-05T08:07:36",
                       "alerts": [{
                           "message": "Alert message",
                           "generator": "test",
                           "notification_time": "2018-06-05T08:07:36",
                           "alert_cnf": {
                               "name": "alert_name1",
                               "severity": "critical",
                               "description": "Alert description",
                               "group": "alert_group"
                           }}]
                       }
        }]
        }
        exit_status = self.engine_eboa.treat_data(data)

        assert len([item for item in exit_status if item["status"] != eboa_engine.exit_codes["OK"]["status"]]) == 0

        source_alerts = self.query_eboa.get_source_alerts()

        assert len(source_alerts) == 1

    def test_insert_gauge(self):

        data = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source.xml",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2018-07-05T02:07:03",
                       "validity_start": "2018-06-05T02:07:03",
                       "validity_stop": "2018-06-05T08:07:36"},
            "events": [{
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "SIMPLE_UPDATE"},
                "start": "2018-06-05T02:07:03",
                "stop": "2018-06-05T08:07:36"
            }]
        }]
        }
        returned_value = self.engine_eboa.treat_data(data)[0]["status"]

        gauge_ddbb = self.session.query(Gauge).filter(Gauge.name == "GAUGE_NAME", Gauge.system == "GAUGE_SYSTEM").all()

        assert len(gauge_ddbb) == 1

    def test_race_condition_insert_gauge(self):

        self.engine_eboa._initialize_context_insert_data()
        self.engine_eboa_race_conditions._initialize_context_insert_data()
        data = {"dim_signature": {"name": "dim_signature",
                                  "exec": "exec",
                                  "version": "1.0"},
                "source": {"name": "source.xml",
                           "reception_time": "2018-06-06T13:33:29",
                           "generation_time": "2018-07-05T02:07:03",
                           "validity_start": "2018-06-05T02:07:03",
                           "validity_stop": "2018-06-05T08:07:36"},
                "events": [{"gauge": {
                    "name": "GAUGE",
                    "system": "SYSTEM",
                    "insertion_type": "SIMPLE_UPDATE"
                }}]
            }
        self.engine_eboa.operation = data
        self.engine_eboa._insert_dim_signature()
        self.engine_eboa._insert_source()
        self.engine_eboa_race_conditions.operation = data
        self.engine_eboa_race_conditions._insert_dim_signature()
        try:
            self.engine_eboa_race_conditions._insert_source()
        except SourceAlreadyIngested:
            pass

        def insert_gauges():
            self.engine_eboa._insert_gauges()
            self.engine_eboa.session.commit()

        def insert_gauges_race_condition():
            self.engine_eboa_race_conditions._insert_gauges()
            self.engine_eboa_race_conditions.session.commit()

        with before_after.before("eboa.engine.engine.race_condition", insert_gauges_race_condition):
            insert_gauges()

        # Call commit as the method uses the nested operation
        self.engine_eboa_race_conditions.session.commit()

        gauge_ddbb = self.session.query(Gauge).filter(Gauge.name == data["events"][0]["gauge"]["name"], Gauge.system == data["events"][0]["gauge"]["system"]).all()

        assert len(gauge_ddbb) == 1

    def test_insert_annotation(self):

        data = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source.xml",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2018-07-05T02:07:03",
                       "validity_start": "2018-06-05T02:07:03",
                       "validity_stop": "2018-06-05T08:07:36"},
            "annotations": [{
                "explicit_reference": "EXPLICIT_REFERENCE_ANNOTATION",
                "annotation_cnf": {
                    "name": "ANNOTATION_CNF",
                    "system": "SYSTEM"
                }}]
        }]
        }
        returned_value = self.engine_eboa.treat_data(data)[0]["status"]

        annotation_cnf_ddbb = self.session.query(AnnotationCnf).filter(AnnotationCnf.name == "ANNOTATION_CNF", AnnotationCnf.system == "SYSTEM").all()

        assert len(annotation_cnf_ddbb) == 1

    def test_race_condition_insert_annotation(self):

        self.engine_eboa._initialize_context_insert_data()
        self.engine_eboa_race_conditions._initialize_context_insert_data()
        data = {"dim_signature": {"name": "dim_signature",
                                  "exec": "exec",
                                  "version": "1.0"},
                "source": {"name": "source.xml",
                           "reception_time": "2018-06-06T13:33:29",
                           "generation_time": "2018-07-05T02:07:03",
                           "validity_start": "2018-06-05T02:07:03",
                           "validity_stop": "2018-06-05T08:07:36"},
                "annotations": [{"annotation_cnf": {
                    "name": "ANNOTATION_CNF",
                    "system": "SYSTEM"
                }}]
            }
        self.engine_eboa.operation = data
        self.engine_eboa._insert_dim_signature()
        self.engine_eboa._insert_source()
        self.engine_eboa_race_conditions.operation = data
        self.engine_eboa_race_conditions._insert_dim_signature()
        try:
            self.engine_eboa_race_conditions._insert_source()
        except SourceAlreadyIngested:
            pass

        def insert_annotations_cnf():
            self.engine_eboa._insert_annotation_cnfs()
            self.engine_eboa.session.commit()

        def insert_annotations_cnf_race_condition():
            self.engine_eboa_race_conditions._insert_annotation_cnfs()
            self.engine_eboa_race_conditions.session.commit()

        with before_after.before("eboa.engine.engine.race_condition", insert_annotations_cnf_race_condition):
            insert_annotations_cnf()

        # Call commit as the method uses the nested operation
        self.engine_eboa_race_conditions.session.commit()

        annotation_cnf_ddbb = self.session.query(AnnotationCnf).filter(AnnotationCnf.name == data["annotations"][0]["annotation_cnf"]["name"], AnnotationCnf.system == data["annotations"][0]["annotation_cnf"]["system"]).all()

        assert len(annotation_cnf_ddbb) == 1

    def test_insert_expl_group(self):

        self.engine_eboa._initialize_context_insert_data()
        data = {"explicit_references": [{
            "group": "EXPL_GROUP"
        }]}
        self.engine_eboa.operation = data
        self.engine_eboa._insert_expl_groups()
        # Call commit as the method uses the nested operation
        self.engine_eboa.session.commit()

        expl_group_ddbb = self.session.query(ExplicitRefGrp).filter(ExplicitRefGrp.name == data["explicit_references"][0]["group"]).all()

        assert len(expl_group_ddbb) == 1

    def test_race_condition_insert_expl_group(self):
        """
        Method to test the race condition that could be produced if the
        group for explicit references does not exist and it is going to be created by
        two instances
        """
        self.engine_eboa._initialize_context_insert_data()
        self.engine_eboa_race_conditions._initialize_context_insert_data()
        data = {"explicit_references": [{
            "group": "EXPL_GROUP"
        }]}
        self.engine_eboa.operation = data
        self.engine_eboa_race_conditions.operation = data

        def insert_expl_group():
            self.engine_eboa._insert_expl_groups()
            self.engine_eboa.session.commit()

        def insert_expl_group_race_condition():
            self.engine_eboa_race_conditions._insert_expl_groups()
            self.engine_eboa_race_conditions.session.commit()

        with before_after.before("eboa.engine.engine.race_condition", insert_expl_group_race_condition):
            insert_expl_group()

        expl_group_ddbb = self.session.query(ExplicitRefGrp).filter(ExplicitRefGrp.name == data["explicit_references"][0]["group"]).all()

        assert len(expl_group_ddbb) == 1

    def test_insert_explicit_reference(self):

        self.engine_eboa._initialize_context_insert_data()
        data = {"explicit_references": [{
            "name": "EXPLICIT_REFERENCE",
            "links": [{"name": "LINK_NAME",
                       "link": "EXPLICIT_REFERENCE_LINK"}]
        }],
                "events": [{
                    "explicit_reference": "EXPLICIT_REFERENCE_EVENT"
                }],
                "annotations": [{
                    "explicit_reference": "EXPLICIT_REFERENCE_ANNOTATION"
                }]
            }
        self.engine_eboa.operation = data
        self.engine_eboa._insert_explicit_refs()
        # Call commit as the method uses the nested operation
        self.engine_eboa.session.commit()

        explicit_reference_ddbb = self.session.query(ExplicitRef).filter(ExplicitRef.explicit_ref == data["explicit_references"][0]["name"]).all()

        assert len(explicit_reference_ddbb) == 1

        explicit_reference_link_ddbb = self.session.query(ExplicitRef).filter(ExplicitRef.explicit_ref == data["explicit_references"][0]["links"][0]["link"]).all()

        assert len(explicit_reference_link_ddbb) == 1

        explicit_reference_event_ddbb = self.session.query(ExplicitRef).filter(ExplicitRef.explicit_ref == data["events"][0]["explicit_reference"]).all()

        assert len(explicit_reference_event_ddbb) == 1

        explicit_reference_annotation_ddbb = self.session.query(ExplicitRef).filter(ExplicitRef.explicit_ref == data["annotations"][0]["explicit_reference"]).all()

        assert len(explicit_reference_annotation_ddbb) == 1

    def test_insert_race_condition_explicit_reference(self):

        self.engine_eboa._initialize_context_insert_data()
        self.engine_eboa_race_conditions._initialize_context_insert_data()
        data = {"explicit_references": [{
            "name": "EXPLICIT_REFERENCE"}]
            }
        self.engine_eboa.operation = data
        self.engine_eboa_race_conditions.operation = data

        def insert_explicit_reference():
            self.engine_eboa._insert_explicit_refs()
            self.engine_eboa.session.commit()

        def insert_explicit_reference_race_condition():
            self.engine_eboa_race_conditions._insert_explicit_refs()
            self.engine_eboa_race_conditions.session.commit()

        with before_after.before("eboa.engine.engine.race_condition", insert_explicit_reference_race_condition):
            insert_explicit_reference()

        explicit_reference_ddbb = self.session.query(ExplicitRef).filter(ExplicitRef.explicit_ref == data["explicit_references"][0]["name"]).all()

        assert len(explicit_reference_ddbb) == 1

    def test_insert_link_explicit_refs(self):
        self.engine_eboa._initialize_context_insert_data()
        data = {"explicit_references": [{
            "name": "EXPLICIT_REFERENCE",
            "links": [{"name": "LINK_NAME",
                       "link": "EXPLICIT_REFERENCE_EVENT",
                       "back_ref": "LINK_BACK_REF_NAME"}]
        }],
                "events": [{
                    "explicit_reference": "EXPLICIT_REFERENCE_EVENT"
                }]
            }
        self.engine_eboa.operation = data
        self.engine_eboa._insert_explicit_refs()
        # Call commit as the method uses the nested operation
        self.engine_eboa.session.commit()

        self.engine_eboa._insert_links_explicit_refs()
        # Call commit as the method uses the nested operation
        self.engine_eboa.session.commit()

        explicit_reference_ddbb = self.session.query(ExplicitRef).filter(ExplicitRef.explicit_ref == data["explicit_references"][0]["name"]).first()
        explicit_reference_event_ddbb = self.session.query(ExplicitRef).filter(ExplicitRef.explicit_ref == data["events"][0]["explicit_reference"]).first()

        explicit_reference_link_ddbb = self.session.query(ExplicitRefLink).filter(ExplicitRefLink.name == data["explicit_references"][0]["links"][0]["name"],
                                                                                  ExplicitRefLink.explicit_ref_uuid_link == explicit_reference_ddbb.explicit_ref_uuid,
                                                                                  ExplicitRefLink.explicit_ref_uuid == explicit_reference_event_ddbb.explicit_ref_uuid).all()

        assert len(explicit_reference_link_ddbb) == 1

        explicit_reference_link_ddbb2 = self.session.query(ExplicitRefLink).filter(ExplicitRefLink.name == "LINK_BACK_REF_NAME",
                                                                                  ExplicitRefLink.explicit_ref_uuid_link == explicit_reference_event_ddbb.explicit_ref_uuid,
                                                                                  ExplicitRefLink.explicit_ref_uuid == explicit_reference_ddbb.explicit_ref_uuid).all()

        assert len(explicit_reference_link_ddbb2) == 1

    def test_insert_link_to_non_existant_explicit_ref(self):
        self.engine_eboa._initialize_context_insert_data()
        data = {"explicit_references": [{
            "name": "EXPLICIT_REFERENCE",
            "links": [{"name": "LINK_NAME",
                       "link": "EXPLICIT_REFERENCE_EVENT",
                       "back_ref": "LINK_BACK_REF_NAME"},
                      {"name": "LINK_NAME2",
                       "link": "EXPLICIT_REFERENCE_EVENT2",
                       "back_ref": "LINK_BACK_REF_NAME2"}]
        }]
        }
        self.engine_eboa.operation = data
        self.engine_eboa._insert_explicit_refs()
        # Call commit as the method uses the nested operation
        self.engine_eboa.session.commit()

        self.engine_eboa._insert_links_explicit_refs()
        # Call commit as the method uses the nested operation
        self.engine_eboa.session.commit()

        explicit_reference_ddbb = self.session.query(ExplicitRef).filter(ExplicitRef.explicit_ref == data["explicit_references"][0]["name"]).first()
        explicit_reference_event_ddbb = self.session.query(ExplicitRef).filter(ExplicitRef.explicit_ref == "EXPLICIT_REFERENCE_EVENT").first()

        explicit_reference_link_ddbb = self.session.query(ExplicitRefLink).filter(ExplicitRefLink.name == data["explicit_references"][0]["links"][0]["name"],
                                                                                  ExplicitRefLink.explicit_ref_uuid_link == explicit_reference_ddbb.explicit_ref_uuid,
                                                                                  ExplicitRefLink.explicit_ref_uuid == explicit_reference_event_ddbb.explicit_ref_uuid).all()

        assert len(explicit_reference_link_ddbb) == 1

        explicit_reference_link_ddbb2 = self.session.query(ExplicitRefLink).filter(ExplicitRefLink.name == "LINK_BACK_REF_NAME",
                                                                                  ExplicitRefLink.explicit_ref_uuid_link == explicit_reference_event_ddbb.explicit_ref_uuid,
                                                                                  ExplicitRefLink.explicit_ref_uuid == explicit_reference_ddbb.explicit_ref_uuid).all()

        assert len(explicit_reference_link_ddbb2) == 1

        explicit_reference_event_ddbb2 = self.session.query(ExplicitRef).filter(ExplicitRef.explicit_ref == "EXPLICIT_REFERENCE_EVENT2").first()

        explicit_reference_link_ddbb3 = self.session.query(ExplicitRefLink).filter(ExplicitRefLink.name == "LINK_NAME2",
                                                                                  ExplicitRefLink.explicit_ref_uuid_link == explicit_reference_ddbb.explicit_ref_uuid,
                                                                                  ExplicitRefLink.explicit_ref_uuid == explicit_reference_event_ddbb2.explicit_ref_uuid).all()

        assert len(explicit_reference_link_ddbb3) == 1

        explicit_reference_link_ddbb4 = self.session.query(ExplicitRefLink).filter(ExplicitRefLink.name == "LINK_BACK_REF_NAME2",
                                                                                  ExplicitRefLink.explicit_ref_uuid_link == explicit_reference_event_ddbb2.explicit_ref_uuid,
                                                                                  ExplicitRefLink.explicit_ref_uuid == explicit_reference_ddbb.explicit_ref_uuid).all()

        assert len(explicit_reference_link_ddbb4) == 1

    def test_insert_race_condition_links_explicit_reference(self):

        self.engine_eboa._initialize_context_insert_data()
        self.engine_eboa_race_conditions._initialize_context_insert_data()
        data = {"explicit_references": [{
            "name": "EXPLICIT_REFERENCE",
            "links": [{"name": "LINK_NAME",
                       "link": "EXPLICIT_REFERENCE_EVENT"}]
        }],
                "events": [{
                    "explicit_reference": "EXPLICIT_REFERENCE_EVENT"
                }]
            }
        self.engine_eboa.operation = data
        self.engine_eboa_race_conditions.operation = data
        self.engine_eboa._insert_explicit_refs()
        self.engine_eboa.session.commit()
        self.engine_eboa_race_conditions._insert_explicit_refs()
        self.engine_eboa_race_conditions.session.commit()

        def insert_link_explicit_reference():
            self.engine_eboa._insert_links_explicit_refs()
            self.engine_eboa.session.commit()

        def insert_link_explicit_reference_race_condition():
            self.engine_eboa_race_conditions._insert_links_explicit_refs()
            self.engine_eboa_race_conditions.session.commit()

        with before_after.before("eboa.engine.engine.race_condition", insert_link_explicit_reference_race_condition):
            insert_link_explicit_reference()

        explicit_reference_ddbb = self.session.query(ExplicitRef).filter(ExplicitRef.explicit_ref == data["explicit_references"][0]["name"]).first()
        explicit_reference_event_ddbb = self.session.query(ExplicitRef).filter(ExplicitRef.explicit_ref == data["events"][0]["explicit_reference"]).first()
        
        explicit_reference_link_ddbb = self.session.query(ExplicitRefLink).filter(ExplicitRefLink.name == data["explicit_references"][0]["links"][0]["name"],
                                                                                  ExplicitRefLink.explicit_ref_uuid_link == explicit_reference_ddbb.explicit_ref_uuid,
                                                                                  ExplicitRefLink.explicit_ref_uuid == explicit_reference_event_ddbb.explicit_ref_uuid).all()

        assert len(explicit_reference_link_ddbb) == 1

    def test_insert_race_condition_back_ref_links_explicit_reference(self):

        self.engine_eboa._initialize_context_insert_data()
        self.engine_eboa_race_conditions._initialize_context_insert_data()
        data = {"explicit_references": [{
            "name": "EXPLICIT_REFERENCE",
            "links": [{"name": "LINK_NAME",
                       "link": "EXPLICIT_REFERENCE_EVENT"}]
        }],
                "events": [{
                    "explicit_reference": "EXPLICIT_REFERENCE_EVENT"
                }]
            }
        self.engine_eboa.operation = data
        self.engine_eboa_race_conditions.operation = data
        self.engine_eboa._insert_explicit_refs()
        self.engine_eboa.session.commit()
        self.engine_eboa_race_conditions._insert_explicit_refs()
        self.engine_eboa_race_conditions.session.commit()
        self.engine_eboa._insert_links_explicit_refs()
        self.engine_eboa.session.commit()
        self.engine_eboa_race_conditions._insert_links_explicit_refs()
        self.engine_eboa_race_conditions.session.commit()

        data = {"explicit_references": [{
            "name": "EXPLICIT_REFERENCE",
            "links": [{"name": "LINK_NAME",
                       "link": "EXPLICIT_REFERENCE_EVENT",
                       "back_ref": "LINK_BACK_REF_NAME"}]
        }],
                "events": [{
                    "explicit_reference": "EXPLICIT_REFERENCE_EVENT"
                }]
            }
        self.engine_eboa.operation = data
        self.engine_eboa_race_conditions.operation = data

        def insert_link_explicit_reference():
            self.engine_eboa._insert_links_explicit_refs()
            self.engine_eboa.session.commit()

        def insert_link_explicit_reference_race_condition():
            self.engine_eboa_race_conditions._insert_links_explicit_refs()
            self.engine_eboa_race_conditions.session.commit()

        with before_after.before("eboa.engine.engine.race_condition", insert_link_explicit_reference_race_condition):
            insert_link_explicit_reference()

        explicit_reference_ddbb = self.session.query(ExplicitRef).filter(ExplicitRef.explicit_ref == data["explicit_references"][0]["name"]).first()
        explicit_reference_event_ddbb = self.session.query(ExplicitRef).filter(ExplicitRef.explicit_ref == data["events"][0]["explicit_reference"]).first()
        
        explicit_reference_link_ddbb = self.session.query(ExplicitRefLink).filter(ExplicitRefLink.name == data["explicit_references"][0]["links"][0]["name"],
                                                                                  ExplicitRefLink.explicit_ref_uuid_link == explicit_reference_ddbb.explicit_ref_uuid,
                                                                                  ExplicitRefLink.explicit_ref_uuid == explicit_reference_event_ddbb.explicit_ref_uuid).all()

        assert len(explicit_reference_link_ddbb) == 1

    def test_insert_event_simple_update(self):

        self.engine_eboa._initialize_context_insert_data()
        data = {"dim_signature": {"name": "dim_signature",
                                  "exec": "exec",
                                  "version": "1.0"},
                "source": {"name": "source.xml",
                           "reception_time": "2018-06-06T13:33:29",
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
        self.engine_eboa.operation = data
        self.engine_eboa._insert_dim_signature()
        self.engine_eboa._insert_source()
        self.engine_eboa._insert_gauges()
        self.engine_eboa.session.commit()
        self.engine_eboa._insert_explicit_refs()
        self.engine_eboa.session.commit()
        gauge_ddbb = self.session.query(Gauge).filter(Gauge.name == data["events"][0]["gauge"]["name"], Gauge.system == data["events"][0]["gauge"]["system"]).first()
        source_ddbb = self.session.query(Source).filter(Source.name == data["source"]["name"], Source.validity_start == data["source"]["validity_start"], Source.validity_stop == data["source"]["validity_stop"], Source.generation_time == data["source"]["generation_time"], Source.processor_version == data["dim_signature"]["version"], Source.processor == data["dim_signature"]["exec"]).first()
        dim_signature_ddbb = self.session.query(DimSignature).filter(DimSignature.dim_signature == data["dim_signature"]["name"]).first()
        explicit_reference_ddbb = self.session.query(ExplicitRef).filter(ExplicitRef.explicit_ref == data["events"][0]["explicit_reference"]).first()
        self.engine_eboa._insert_events()
        self.engine_eboa.session.commit()
        event_ddbb = self.session.query(Event).filter(Event.start == data["events"][0]["start"],
                                                      Event.stop == data["events"][0]["stop"],
                                                      Event.gauge_uuid == gauge_ddbb.gauge_uuid,
                                                      Event.source_uuid == source_ddbb.source_uuid,
                                                      Event.explicit_ref_uuid == explicit_reference_ddbb.explicit_ref_uuid,
                                                      Event.visible == True).all()

        assert len(event_ddbb) == 1

    def test_insert_event_start_gt_stop(self):

        self.engine_eboa._initialize_context_insert_data()
        data = {"dim_signature": {"name": "dim_signature",
                                  "exec": "exec",
                                  "version": "1.0"},
                "source": {"name": "source.xml",
                           "reception_time": "2018-06-06T13:33:29",
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
        self.engine_eboa.operation = data
        self.engine_eboa._insert_dim_signature()
        self.engine_eboa._insert_source()
        self.engine_eboa._insert_gauges()
        self.engine_eboa.session.commit()
        try:
            self.engine_eboa._insert_events()
        except WrongPeriod:
            pass

        event_ddbb = self.session.query(Event).all()

        assert len(event_ddbb) == 0


    def test_insert_event_period_out_of_validity(self):

        self.engine_eboa._initialize_context_insert_data()
        data = {"dim_signature": {"name": "dim_signature",
                                  "exec": "exec",
                                  "version": "1.0"},
                "source": {"name": "source.xml",
                           "reception_time": "2018-06-06T13:33:29",
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
        self.engine_eboa.operation = data
        self.engine_eboa._insert_dim_signature()
        self.engine_eboa._insert_source()
        self.engine_eboa._insert_gauges()
        self.engine_eboa.session.commit()
        try:
            self.engine_eboa._insert_events()
        except WrongPeriod:
            pass

        event_ddbb = self.session.query(Event).all()

        assert len(event_ddbb) == 0

    def test_insert_event_insert_and_erase(self):

        self.engine_eboa._initialize_context_insert_data()
        data = {"dim_signature": {"name": "dim_signature",
                                  "exec": "exec",
                                  "version": "1.0"},
                "source": {"name": "source.xml",
                           "reception_time": "2018-06-06T13:33:29",
                           "generation_time": "2018-07-05T02:07:03",
                           "validity_start": "2018-06-05T02:07:03",
                           "validity_stop": "2018-06-05T08:07:36"},
                "events": [{
                    "explicit_reference": "EXPLICIT_REFERENCE_EVENT",
                    "gauge": {"name": "GAUGE_NAME",
                              "system": "GAUGE_SYSTEM",
                              "insertion_type": "INSERT_and_ERASE"},
                    "start": "2018-06-05T02:07:03",
                    "stop": "2018-06-05T08:07:36"
                }]
            }
        self.engine_eboa.operation = data
        self.engine_eboa._insert_dim_signature()
        self.engine_eboa._insert_source()
        self.engine_eboa._insert_gauges()
        self.engine_eboa.session.commit()
        self.engine_eboa._insert_explicit_refs()
        self.engine_eboa.session.commit()
        gauge_ddbb = self.session.query(Gauge).filter(Gauge.name == data["events"][0]["gauge"]["name"], Gauge.system == data["events"][0]["gauge"]["system"]).first()
        source_ddbb = self.session.query(Source).filter(Source.name == data["source"]["name"], Source.validity_start == data["source"]["validity_start"], Source.validity_stop == data["source"]["validity_stop"], Source.generation_time == data["source"]["generation_time"], Source.processor_version == data["dim_signature"]["version"], Source.processor == data["dim_signature"]["exec"]).first()
        dim_signature_ddbb = self.session.query(DimSignature).filter(DimSignature.dim_signature == data["dim_signature"]["name"]).first()
        explicit_reference_ddbb = self.session.query(ExplicitRef).filter(ExplicitRef.explicit_ref == data["events"][0]["explicit_reference"]).first()
        self.engine_eboa._insert_events()
        self.engine_eboa.session.commit()
        event_ddbb = self.session.query(Event).filter(Event.start == data["events"][0]["start"],
                                                      Event.stop == data["events"][0]["stop"],
                                                      Event.gauge_uuid == gauge_ddbb.gauge_uuid,
                                                      Event.source_uuid == source_ddbb.source_uuid,
                                                      Event.explicit_ref_uuid == explicit_reference_ddbb.explicit_ref_uuid,
                                                      Event.visible == False).all()

        assert len(event_ddbb) == 1

    def test_insert_event_insert_and_erase_remove_events(self):

        data1 = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source.json",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2018-07-06T02:07:03",
                       "validity_start": "2018-06-05T04:07:03",
                       "validity_stop": "2018-06-05T06:07:36"},
            "events": [{
                "explicit_reference": "EXPLICIT_REFERENCE_EVENT",
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "INSERT_and_ERASE"},
                "start": "2018-06-05T05:07:03",
                "stop": "2018-06-05T05:07:36",
        }]
        }]
        }
        self.engine_eboa.treat_data(data1)

        data2 = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source2.json",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2018-07-05T02:07:03",
                       "validity_start": "2018-06-05T02:07:03",
                       "validity_stop": "2018-06-05T08:07:36"},
            "events": [{
                "explicit_reference": "EXPLICIT_REFERENCE_EVENT",
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "INSERT_and_ERASE"},
                "start": "2018-06-05T05:07:04",
                "stop": "2018-06-05T05:07:35"
            }]
        }]
        }
        self.engine_eboa.treat_data(data2)

        events = self.session.query(Event).join(Gauge).join(Source).filter(Gauge.name == "GAUGE_NAME", 
                                                                           Gauge.system == "GAUGE_SYSTEM",
                                                                           Event.start == "2018-06-05T05:07:03",
                                                                           Event.stop == "2018-06-05T05:07:36",
                                                                           Source.name == "source.json").all()
        assert len(events) == 1

        events = self.session.query(Event).all()

        assert len(events) == 1

    def test_insert_event_insert_and_erase_with_priority_remove_events(self):

        data1 = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source.json",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2018-07-06T02:07:03",
                       "validity_start": "2018-06-05T04:07:03",
                       "validity_stop": "2018-06-05T06:07:36"},
            "events": [{
                "explicit_reference": "EXPLICIT_REFERENCE_EVENT",
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "INSERT_and_ERASE"},
                "start": "2018-06-05T05:07:03",
                "stop": "2018-06-05T05:07:36",
        }]
        }]
        }
        self.engine_eboa.treat_data(data1)

        data2 = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source2.json",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2018-07-05T02:07:03",
                       "validity_start": "2018-06-05T02:07:03",
                       "validity_stop": "2018-06-05T08:07:36"},
            "events": [{
                "explicit_reference": "EXPLICIT_REFERENCE_EVENT",
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "INSERT_and_ERASE"},
                "start": "2018-06-05T05:07:04",
                "stop": "2018-06-05T05:07:35"
            }]
        }]
        }
        self.engine_eboa.treat_data(data2)

        events = self.session.query(Event).join(Gauge).join(Source).filter(Gauge.name == "GAUGE_NAME", 
                                                                           Gauge.system == "GAUGE_SYSTEM",
                                                                           Event.start == "2018-06-05T05:07:03",
                                                                           Event.stop == "2018-06-05T05:07:36",
                                                                           Source.name == "source.json").all()
        assert len(events) == 1

        events = self.session.query(Event).all()

        assert len(events) == 1

    def test_insert_event_insert_and_erase_source_duration_0_in_the_middle_to_remove(self):

        data = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source.json",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2018-07-05T02:07:03",
                       "validity_start": "2018-06-05T02:07:03",
                       "validity_stop": "2018-06-05T08:07:36"},
            "events": [{
                "explicit_reference": "EXPLICIT_REFERENCE_EVENT",
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "INSERT_and_ERASE"},
                "start": "2018-06-05T02:07:03",
                "stop": "2018-06-05T08:07:36"
            }]
        },{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source2.json",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2018-07-04T02:07:03",
                       "validity_start": "2018-06-05T04:07:03",
                       "validity_stop": "2018-06-05T04:07:03"},
            "events": [{
                "explicit_reference": "EXPLICIT_REFERENCE_EVENT",
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "INSERT_and_ERASE"},
                "start": "2018-06-05T04:07:03",
                "stop": "2018-06-05T04:07:03"
            }]
        }]
        }
        self.engine_eboa.treat_data(data)

        events = self.session.query(Event).join(Gauge).join(Source).filter(Gauge.name == "GAUGE_NAME", 
                                                                           Gauge.system == "GAUGE_SYSTEM",
                                                                           Event.start == "2018-06-05T02:07:03",
                                                                           Event.stop == "2018-06-05T08:07:36",
                                                                           Source.name == "source.json").all()
        assert len(events) == 1

        events = self.session.query(Event).all()

        assert len(events) == 1

    def test_insert_event_insert_and_erase_source_duration_0_in_the_middle_to_stay(self):

        data = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source.json",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2018-07-05T02:07:03",
                       "validity_start": "2018-06-05T02:07:03",
                       "validity_stop": "2018-06-05T08:07:36"},
            "events": [{
                "explicit_reference": "EXPLICIT_REFERENCE_EVENT",
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "INSERT_and_ERASE"},
                "start": "2018-06-05T02:07:03",
                "stop": "2018-06-05T08:07:36"
            }]
        },{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source2.json",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2018-08-04T02:07:03",
                       "validity_start": "2018-06-05T04:07:03",
                       "validity_stop": "2018-06-05T04:07:03"},
            "events": [{
                "explicit_reference": "EXPLICIT_REFERENCE_EVENT",
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "INSERT_and_ERASE"},
                "start": "2018-06-05T04:07:03",
                "stop": "2018-06-05T04:07:03"
            }]
        }]
        }
        self.engine_eboa.treat_data(data)

        events = self.session.query(Event).join(Gauge).join(Source).filter(Gauge.name == "GAUGE_NAME", 
                                                                           Gauge.system == "GAUGE_SYSTEM",
                                                                           Event.start == "2018-06-05T02:07:03",
                                                                           Event.stop == "2018-06-05T04:07:03",
                                                                           Source.name == "source.json").all()
        assert len(events) == 1

        events = self.session.query(Event).join(Gauge).join(Source).filter(Gauge.name == "GAUGE_NAME", 
                                                                           Gauge.system == "GAUGE_SYSTEM",
                                                                           Event.start == "2018-06-05T04:07:03",
                                                                           Event.stop == "2018-06-05T08:07:36",
                                                                           Source.name == "source.json").all()
        assert len(events) == 1

        events = self.session.query(Event).join(Gauge).join(Source).filter(Gauge.name == "GAUGE_NAME", 
                                                                           Gauge.system == "GAUGE_SYSTEM",
                                                                           Event.start == "2018-06-05T04:07:03",
                                                                           Event.stop == "2018-06-05T04:07:03",
                                                                           Source.name == "source2.json").all()
        assert len(events) == 1

        events = self.session.query(Event).all()

        assert len(events) == 3
        
        events = self.session.query(Event).filter(Event.visible == True).all()

        assert len(events) == 3
        
    def test_insert_event_insert_and_erase_per_event(self):

        data = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source.json",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2018-07-05T02:07:03",
                       "validity_start": "2018-06-05T02:07:03",
                       "validity_stop": "2018-06-05T08:07:36"},
            "events": [{
                "explicit_reference": "EXPLICIT_REFERENCE_EVENT",
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "INSERT_and_ERASE_per_EVENT"},
                "start": "2018-06-05T02:07:03",
                "stop": "2018-06-05T08:07:36"
            },{
                    "explicit_reference": "EXPLICIT_REFERENCE_EVENT",
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "INSERT_and_ERASE_per_EVENT"},
                "start": "2018-06-05T02:07:03",
                "stop": "2018-06-05T08:07:36"
            }]
        }]
        }
        self.engine_eboa.treat_data(data)

        events = self.session.query(Event).join(Gauge).join(Source).filter(Gauge.name == "GAUGE_NAME", 
                                                                           Gauge.system == "GAUGE_SYSTEM",
                                                                           Event.start == "2018-06-05T02:07:03",
                                                                           Event.stop == "2018-06-05T08:07:36",
                                                                           Source.name == "source.json").all()
        assert len(events) == 2

    def test_insert_event_insert_and_erase_per_event_event_duration_0_in_the_middle_to_remove(self):

        data = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source.json",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2018-07-05T02:07:03",
                       "validity_start": "2018-06-05T02:07:03",
                       "validity_stop": "2018-06-05T08:07:36"},
            "events": [{
                "explicit_reference": "EXPLICIT_REFERENCE_EVENT",
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "INSERT_and_ERASE_per_EVENT"},
                "start": "2018-06-05T02:07:03",
                "stop": "2018-06-05T08:07:36"
            }]
        },{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source2.json",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2018-07-04T02:07:03",
                       "validity_start": "2018-06-05T04:07:03",
                       "validity_stop": "2018-06-05T04:07:03"},
            "events": [{
                "explicit_reference": "EXPLICIT_REFERENCE_EVENT",
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "INSERT_and_ERASE_per_EVENT"},
                "start": "2018-06-05T04:07:03",
                "stop": "2018-06-05T04:07:03"
            }]
        }]
        }
        self.engine_eboa.treat_data(data)

        events = self.session.query(Event).join(Gauge).join(Source).filter(Gauge.name == "GAUGE_NAME", 
                                                                           Gauge.system == "GAUGE_SYSTEM",
                                                                           Event.start == "2018-06-05T02:07:03",
                                                                           Event.stop == "2018-06-05T08:07:36",
                                                                           Source.name == "source.json").all()
        assert len(events) == 1

        events = self.session.query(Event).all()

        assert len(events) == 1

    def test_insert_event_insert_and_erase_per_event_event_duration_0_in_the_middle_to_stay(self):

        data = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source.json",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2018-07-05T02:07:03",
                       "validity_start": "2018-06-05T02:07:03",
                       "validity_stop": "2018-06-05T08:07:36"},
            "events": [{
                "explicit_reference": "EXPLICIT_REFERENCE_EVENT",
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "INSERT_and_ERASE_per_EVENT"},
                "start": "2018-06-05T02:07:03",
                "stop": "2018-06-05T08:07:36"
            }]
        },{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source2.json",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2018-08-04T02:07:03",
                       "validity_start": "2018-06-05T04:07:03",
                       "validity_stop": "2018-06-05T04:07:03"},
            "events": [{
                "explicit_reference": "EXPLICIT_REFERENCE_EVENT",
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "INSERT_and_ERASE_per_EVENT"},
                "start": "2018-06-05T04:07:03",
                "stop": "2018-06-05T04:07:03"
            }]
        }]
        }
        self.engine_eboa.treat_data(data)

        events = self.session.query(Event).join(Gauge).join(Source).filter(Gauge.name == "GAUGE_NAME", 
                                                                           Gauge.system == "GAUGE_SYSTEM",
                                                                           Event.start == "2018-06-05T02:07:03",
                                                                           Event.stop == "2018-06-05T04:07:03",
                                                                           Source.name == "source.json").all()
        assert len(events) == 1

        events = self.session.query(Event).join(Gauge).join(Source).filter(Gauge.name == "GAUGE_NAME", 
                                                                           Gauge.system == "GAUGE_SYSTEM",
                                                                           Event.start == "2018-06-05T04:07:03",
                                                                           Event.stop == "2018-06-05T08:07:36",
                                                                           Source.name == "source.json").all()
        assert len(events) == 1

        events = self.session.query(Event).join(Gauge).join(Source).filter(Gauge.name == "GAUGE_NAME", 
                                                                           Gauge.system == "GAUGE_SYSTEM",
                                                                           Event.start == "2018-06-05T04:07:03",
                                                                           Event.stop == "2018-06-05T04:07:03",
                                                                           Source.name == "source2.json").all()
        assert len(events) == 1

        events = self.session.query(Event).all()

        assert len(events) == 3
        
        events = self.session.query(Event).filter(Event.visible == True).all()

        assert len(events) == 3
        
    def test_insert_event_insert_and_erase_per_event_event_split(self):

        data1 = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source.json",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2018-07-05T02:07:03",
                       "validity_start": "2018-06-05T02:07:03",
                       "validity_stop": "2018-06-05T08:07:36"},
            "events": [{
                "explicit_reference": "EXPLICIT_REFERENCE_EVENT",
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "INSERT_and_ERASE_per_EVENT"},
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
            }]
                        }]
        }]
        }]
        }
        self.engine_eboa.treat_data(data1)

        data2 = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source2.json",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2018-07-06T02:07:03",
                       "validity_start": "2018-06-05T04:07:03",
                       "validity_stop": "2018-06-05T06:07:36"},
            "events": [{
                "explicit_reference": "EXPLICIT_REFERENCE_EVENT",
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "INSERT_and_ERASE_per_EVENT"},
                "start": "2018-06-05T04:07:03",
                "stop": "2018-06-05T06:07:36"
            }]
        }]
        }
        self.engine_eboa.treat_data(data2)

        events = self.session.query(Event).join(Gauge).join(Source).filter(Gauge.name == "GAUGE_NAME", 
                                                                           Gauge.system == "GAUGE_SYSTEM",
                                                                           Event.start == "2018-06-05T04:07:03",
                                                                           Event.stop == "2018-06-05T06:07:36",
                                                                           Source.name == "source2.json").all()
        assert len(events) == 1

        events = self.session.query(Event).join(Gauge).join(Source).filter(Gauge.name == "GAUGE_NAME", 
                                                                           Gauge.system == "GAUGE_SYSTEM",
                                                                           Event.start == "2018-06-05T02:07:03",
                                                                           Event.stop == "2018-06-05T04:07:03",
                                                                           Source.name == "source.json").all()
        assert len(events) == 1

        events = self.session.query(Event).join(Gauge).join(Source).filter(Gauge.name == "GAUGE_NAME", 
                                                                           Gauge.system == "GAUGE_SYSTEM",
                                                                           Event.start == "2018-06-05T06:07:36",
                                                                           Event.stop == "2018-06-05T08:07:36",
                                                                           Source.name == "source.json").all()
        assert len(events) == 1

        events = self.session.query(Event).all()

        assert len(events) == 3

    def test_insert_event_insert_and_erase_partial_remove_with_links(self):

        data1 = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source.json",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2018-07-07T02:07:03",
                       "validity_start": "2018-06-05T02:07:03",
                       "validity_stop": "2018-06-05T08:07:36"},
            "events": [{
                "explicit_reference": "EXPLICIT_REFERENCE_EVENT",
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "SIMPLE_UPDATE"},
                "start": "2018-06-05T04:07:03",
                "stop": "2018-06-05T06:07:36"
            }]
        }]
        }
        exit_status = self.engine_eboa.treat_data(data1)

        assert len([item for item in exit_status if item["status"] != eboa_engine.exit_codes["OK"]["status"]]) == 0

        events = self.session.query(Event).all()

        assert len(events) == 1

        event_uuid1 = events[0].event_uuid

        data2 = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source2.json",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2018-08-01T02:07:03",
                       "validity_start": "2018-08-01T02:07:03",
                       "validity_stop": "2018-08-05T06:07:36"},
            "events": [{
                "links": [{
                    "link": str(event_uuid1),
                    "link_mode": "by_uuid",
                    "name": "EVENT_LINK_NAME"
                }],
                "explicit_reference": "EXPLICIT_REFERENCE_EVENT",
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "SIMPLE_UPDATE"},
                "start": "2018-08-01T02:07:03",
                "stop": "2018-08-01T012:07:36"
            }]
        }]
        }
        exit_status = self.engine_eboa.treat_data(data2)

        assert len([item for item in exit_status if item["status"] != eboa_engine.exit_codes["OK"]["status"]]) == 0

        events = self.session.query(Event).all()

        assert len(events) == 2

        events = self.session.query(Event).join(Source).filter(Source.name == "source2.json").all()

        assert len(events) == 1

        event_uuid2 = events[0].event_uuid

        data3 = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source3.json",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2018-09-06T02:07:03",
                       "validity_start": "2018-08-02T04:07:03",
                       "validity_stop": "2018-08-06T10:07:36"},
            "events": [{
                "links": [{
                    "link": str(event_uuid1),
                    "link_mode": "by_uuid",
                    "name": "EVENT_LINK_NAME"
                }],
                "explicit_reference": "EXPLICIT_REFERENCE_EVENT",
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "INSERT_and_ERASE"},
                "start": "2018-08-05T04:07:03",
                "stop": "2018-08-05T10:07:36"
            }]
        }]
        }
        
        exit_status = self.engine_eboa.treat_data(data3)

        assert len([item for item in exit_status if item["status"] != eboa_engine.exit_codes["OK"]["status"]]) == 0

        events = self.session.query(Event).join(Source).filter(Source.name == "source3.json").all()

        assert len(events) == 1

        event_uuid2 = events[0].event_uuid

        data4 = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source4.json",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2018-10-03T02:07:03",
                       "validity_start": "2018-08-03T02:07:03",
                       "validity_stop": "2018-08-07T12:07:36"},
            "events": [{
                "links": [{
                    "link": str(event_uuid1),
                    "link_mode": "by_uuid",
                    "name": "EVENT_LINK_NAME"
                }],
                "explicit_reference": "EXPLICIT_REFERENCE_EVENT",
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "INSERT_and_ERASE"},
                "start": "2018-08-05T02:07:03",
                "stop": "2018-08-05T012:07:36"
            }]
        }]
        }
        exit_status = self.engine_eboa.treat_data(data4)

        assert len([item for item in exit_status if item["status"] != eboa_engine.exit_codes["OK"]["status"]]) == 0
        
        events = self.session.query(Event).join(Gauge).join(Source).filter(Gauge.name == "GAUGE_NAME", 
                                                                           Gauge.system == "GAUGE_SYSTEM",
                                                                           Event.start == "2018-06-05T04:07:03",
                                                                           Event.stop == "2018-06-05T06:07:36",
                                                                           Source.name == "source.json").all()
        assert len(events) == 1

        events = self.session.query(Event).join(Gauge).join(Source).filter(Gauge.name == "GAUGE_NAME", 
                                                                           Gauge.system == "GAUGE_SYSTEM",
                                                                           Event.start == "2018-08-01T02:07:03",
                                                                           Event.stop == "2018-08-01T12:07:36",
                                                                           Source.name == "source2.json").all()
        assert len(events) == 1

        event_uuid2 = events[0].event_uuid

        events = self.session.query(Event).join(Gauge).join(Source).filter(Source.name == "source3.json").all()

        assert len(events) == 0

        events = self.session.query(Event).join(Gauge).join(Source).filter(Gauge.name == "GAUGE_NAME",
                                                                           Gauge.system == "GAUGE_SYSTEM",
                                                                           Event.start == "2018-08-05T02:07:03",
                                                                           Event.stop == "2018-08-05T12:07:36",
                                                                           Source.name == "source4.json").all()
        assert len(events) == 1

        event_uuid3 = events[0].event_uuid

        events = self.session.query(Event).all()

        assert len(events) == 3

        event_links = self.session.query(EventLink).all()

        assert len(event_links) == 2

        event_links = self.session.query(EventLink).filter(EventLink.name == "EVENT_LINK_NAME",
                                                           EventLink.event_uuid == event_uuid1,
                                                           EventLink.event_uuid_link == event_uuid2).all()

        assert len(event_links) == 1

        event_links = self.session.query(EventLink).filter(EventLink.name == "EVENT_LINK_NAME",
                                                           EventLink.event_uuid == event_uuid1,
                                                           EventLink.event_uuid_link == event_uuid3).all()

        assert len(event_links) == 1

    def test_insert_event_insert_and_erase_per_event_event_in_ddbb_split(self):

        data1 = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source.json",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2018-07-07T02:07:03",
                       "validity_start": "2018-06-05T02:07:03",
                       "validity_stop": "2018-06-05T08:07:36"},
            "events": [{
                "explicit_reference": "EXPLICIT_REFERENCE_EVENT",
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "SIMPLE_UPDATE"},
                "start": "2018-06-05T04:07:03",
                "stop": "2018-06-05T06:07:36"
            }]
        }]
        }
        self.engine_eboa.treat_data(data1)

        data2 = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source2.json",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2018-07-06T02:07:03",
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
        }]
        }
        self.engine_eboa.treat_data(data2)

        data3 = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source3.json",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2018-07-05T02:07:03",
                       "validity_start": "2018-06-05T02:07:03",
                       "validity_stop": "2018-06-05T08:07:36"},
            "events": [{
                "explicit_reference": "EXPLICIT_REFERENCE_EVENT",
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "INSERT_and_ERASE_per_EVENT"},
                "start": "2018-06-05T02:07:03",
                "stop": "2018-06-05T08:07:36"
            }]
        }]
        }
        self.engine_eboa.treat_data(data3)

        events = self.session.query(Event).join(Gauge).join(Source).filter(Gauge.name == "GAUGE_NAME", 
                                                                           Gauge.system == "GAUGE_SYSTEM",
                                                                           Event.start == "2018-06-05T04:07:03",
                                                                           Event.stop == "2018-06-05T06:07:36",
                                                                           Source.name == "source.json").all()
        assert len(events) == 1

        events = self.session.query(Event).join(Gauge).join(Source).filter(Gauge.name == "GAUGE_NAME", 
                                                                           Gauge.system == "GAUGE_SYSTEM",
                                                                           Event.start == "2018-06-05T02:07:03",
                                                                           Event.stop == "2018-06-05T04:07:03",
                                                                           Source.name == "source2.json").all()
        assert len(events) == 1

        events = self.session.query(Event).join(Gauge).join(Source).filter(Gauge.name == "GAUGE_NAME", 
                                                                           Gauge.system == "GAUGE_SYSTEM",
                                                                           Event.start == "2018-06-05T06:07:36",
                                                                           Event.stop == "2018-06-05T08:07:36",
                                                                           Source.name == "source2.json").all()
        assert len(events) == 1

        events = self.session.query(Event).all()

        assert len(events) == 3

    def test_insert_event_insert_and_erase_per_event_event_in_ddbb_cut(self):

        data1 = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source.json",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2018-07-07T02:07:03",
                       "validity_start": "2018-06-05T02:07:03",
                       "validity_stop": "2018-06-05T08:07:36"},
            "events": [{
                "explicit_reference": "EXPLICIT_REFERENCE_EVENT",
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "SIMPLE_UPDATE"},
                "start": "2018-06-05T04:07:03",
                "stop": "2018-06-05T08:07:36"
            }]
        }]
        }
        self.engine_eboa.treat_data(data1)

        data2 = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source2.json",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2018-07-06T02:07:03",
                       "validity_start": "2018-06-05T02:07:03",
                       "validity_stop": "2018-06-05T08:07:36"},
            "events": [{
                "explicit_reference": "EXPLICIT_REFERENCE_EVENT",
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "SIMPLE_UPDATE"},
                "start": "2018-06-05T02:07:03",
                "stop": "2018-06-05T06:07:36"
            }]
        }]
        }
        self.engine_eboa.treat_data(data2)

        data3 = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source3.json",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2018-07-05T02:07:03",
                       "validity_start": "2018-06-05T02:07:03",
                       "validity_stop": "2018-06-05T08:07:36"},
            "events": [{
                "explicit_reference": "EXPLICIT_REFERENCE_EVENT",
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "INSERT_and_ERASE_per_EVENT"},
                "start": "2018-06-05T02:07:03",
                "stop": "2018-06-05T08:07:36"
            }]
        }]
        }
        self.engine_eboa.treat_data(data3)

        events = self.session.query(Event).join(Gauge).join(Source).filter(Gauge.name == "GAUGE_NAME", 
                                                                           Gauge.system == "GAUGE_SYSTEM",
                                                                           Event.start == "2018-06-05T04:07:03",
                                                                           Event.stop == "2018-06-05T08:07:36",
                                                                           Source.name == "source.json").all()
        assert len(events) == 1

        events = self.session.query(Event).join(Gauge).join(Source).filter(Gauge.name == "GAUGE_NAME", 
                                                                           Gauge.system == "GAUGE_SYSTEM",
                                                                           Event.start == "2018-06-05T02:07:03",
                                                                           Event.stop == "2018-06-05T04:07:03",
                                                                           Source.name == "source2.json").all()
        assert len(events) == 1

        events = self.session.query(Event).all()

        assert len(events) == 2

    def test_insert_event_insert_and_erase_per_event_event_in_ddbb_cut_by_previous_event(self):

        data1 = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source.json",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2018-07-07T02:07:03",
                       "validity_start": "2018-06-05T02:07:03",
                       "validity_stop": "2018-06-05T08:07:36"},
            "events": [{
                "explicit_reference": "EXPLICIT_REFERENCE_EVENT",
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "SIMPLE_UPDATE"},
                "start": "2018-06-05T03:07:03",
                "stop": "2018-06-05T04:07:36"
            }]
        }]
        }
        exit_status = self.engine_eboa.treat_data(data1)

        assert len([item for item in exit_status if item["status"] != eboa_engine.exit_codes["OK"]["status"]]) == 0

        data2 = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source2.json",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2018-07-06T02:07:03",
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
        }]
        }
        exit_status = self.engine_eboa.treat_data(data2)

        assert len([item for item in exit_status if item["status"] != eboa_engine.exit_codes["OK"]["status"]]) == 0

        data4 = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source4.json",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2018-07-05T02:07:03",
                       "validity_start": "2018-06-05T02:07:03",
                       "validity_stop": "2018-06-05T08:07:36"},
            "events": [{
                "explicit_reference": "EXPLICIT_REFERENCE_EVENT",
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "SIMPLE_UPDATE"},
                "start": "2018-06-05T04:07:03",
                "stop": "2018-06-05T06:07:36"
            }]
        }]
        }
        exit_status = self.engine_eboa.treat_data(data4)

        assert len([item for item in exit_status if item["status"] != eboa_engine.exit_codes["OK"]["status"]]) == 0

        data3 = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source3.json",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2018-07-05T02:07:03",
                       "validity_start": "2018-06-05T02:07:03",
                       "validity_stop": "2018-06-05T08:07:36"},
            "events": [{
                "explicit_reference": "EXPLICIT_REFERENCE_EVENT",
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "INSERT_and_ERASE_per_EVENT"},
                "start": "2018-06-05T02:07:03",
                "stop": "2018-06-05T08:07:36"
            }]
        }]
        }
        exit_status = self.engine_eboa.treat_data(data3)

        assert len([item for item in exit_status if item["status"] != eboa_engine.exit_codes["OK"]["status"]]) == 0

        events = self.session.query(Event).join(Gauge).join(Source).filter(Gauge.name == "GAUGE_NAME", 
                                                                           Gauge.system == "GAUGE_SYSTEM",
                                                                           Event.start == "2018-06-05T03:07:03",
                                                                           Event.stop == "2018-06-05T04:07:36",
                                                                           Source.name == "source.json").all()
        assert len(events) == 1

        events = self.session.query(Event).join(Gauge).join(Source).filter(Gauge.name == "GAUGE_NAME", 
                                                                           Gauge.system == "GAUGE_SYSTEM",
                                                                           Event.start == "2018-06-05T02:07:03",
                                                                           Event.stop == "2018-06-05T03:07:03",
                                                                           Source.name == "source2.json").all()
        assert len(events) == 1

        events = self.session.query(Event).join(Gauge).join(Source).filter(Gauge.name == "GAUGE_NAME", 
                                                                           Gauge.system == "GAUGE_SYSTEM",
                                                                           Event.start == "2018-06-05T04:07:36",
                                                                           Event.stop == "2018-06-05T08:07:36",
                                                                           Source.name == "source2.json").all()
        assert len(events) == 1

        events = self.session.query(Event).all()

        assert len(events) == 3

    def test_insert_event_insert_and_erase_per_event_event_in_ddbb_cut_by_previous_and_later_event(self):

        data1 = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source.json",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2018-07-07T02:07:03",
                       "validity_start": "2018-06-05T02:07:03",
                       "validity_stop": "2018-06-05T08:07:36"},
            "events": [{
                "explicit_reference": "EXPLICIT_REFERENCE_EVENT",
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "SIMPLE_UPDATE"},
                "start": "2018-06-05T03:07:03",
                "stop": "2018-06-05T04:07:36"
            },
                       {
                           "explicit_reference": "EXPLICIT_REFERENCE_EVENT",
                           "gauge": {"name": "GAUGE_NAME",
                                     "system": "GAUGE_SYSTEM",
                                     "insertion_type": "SIMPLE_UPDATE"},
                           "start": "2018-06-05T06:07:03",
                           "stop": "2018-06-05T08:07:36"
                       }]
        }]
        }
        exit_status = self.engine_eboa.treat_data(data1)

        assert len([item for item in exit_status if item["status"] != eboa_engine.exit_codes["OK"]["status"]]) == 0

        data2 = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source2.json",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2018-07-06T02:07:03",
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
        }]
        }
        exit_status = self.engine_eboa.treat_data(data2)

        assert len([item for item in exit_status if item["status"] != eboa_engine.exit_codes["OK"]["status"]]) == 0

        data4 = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source4.json",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2018-07-05T02:07:03",
                       "validity_start": "2018-06-05T02:07:03",
                       "validity_stop": "2018-06-05T08:07:36"},
            "events": [{
                "explicit_reference": "EXPLICIT_REFERENCE_EVENT",
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "SIMPLE_UPDATE"},
                "start": "2018-06-05T04:07:03",
                "stop": "2018-06-05T06:07:36"
            }]
        }]
        }
        exit_status = self.engine_eboa.treat_data(data4)

        assert len([item for item in exit_status if item["status"] != eboa_engine.exit_codes["OK"]["status"]]) == 0

        data3 = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source3.json",
                       "reception_time": "2018-06-06T13:33:29",                       
                       "generation_time": "2018-07-05T02:07:03",
                       "validity_start": "2018-06-05T02:07:03",
                       "validity_stop": "2018-06-05T08:07:36"},
            "events": [{
                "explicit_reference": "EXPLICIT_REFERENCE_EVENT",
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "INSERT_and_ERASE_per_EVENT"},
                "start": "2018-06-05T02:07:03",
                "stop": "2018-06-05T08:07:36"
            }]
        }]
        }
        exit_status = self.engine_eboa.treat_data(data3)

        assert len([item for item in exit_status if item["status"] != eboa_engine.exit_codes["OK"]["status"]]) == 0

        events = self.session.query(Event).join(Gauge).join(Source).filter(Gauge.name == "GAUGE_NAME", 
                                                                           Gauge.system == "GAUGE_SYSTEM",
                                                                           Event.start == "2018-06-05T03:07:03",
                                                                           Event.stop == "2018-06-05T04:07:36",
                                                                           Source.name == "source.json").all()
        assert len(events) == 1

        events = self.session.query(Event).join(Gauge).join(Source).filter(Gauge.name == "GAUGE_NAME", 
                                                                           Gauge.system == "GAUGE_SYSTEM",
                                                                           Event.start == "2018-06-05T06:07:03",
                                                                           Event.stop == "2018-06-05T08:07:36",
                                                                           Source.name == "source.json").all()
        assert len(events) == 1

        events = self.session.query(Event).join(Gauge).join(Source).filter(Gauge.name == "GAUGE_NAME", 
                                                                           Gauge.system == "GAUGE_SYSTEM",
                                                                           Event.start == "2018-06-05T02:07:03",
                                                                           Event.stop == "2018-06-05T03:07:03",
                                                                           Source.name == "source2.json").all()
        assert len(events) == 1

        events = self.session.query(Event).join(Gauge).join(Source).filter(Gauge.name == "GAUGE_NAME", 
                                                                           Gauge.system == "GAUGE_SYSTEM",
                                                                           Event.start == "2018-06-05T04:07:36",
                                                                           Event.stop == "2018-06-05T06:07:03",
                                                                           Source.name == "source2.json").all()
        assert len(events) == 1

        events = self.session.query(Event).all()

        assert len(events) == 4

    def test_insert_event_event_keys(self):

        self.engine_eboa._initialize_context_insert_data()
        data = {"dim_signature": {"name": "dim_signature",
                                  "exec": "exec",
                                  "version": "1.0"},
                "source": {"name": "source.xml",
                           "reception_time": "2018-06-06T13:33:29",
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
                },{
                    "explicit_reference": "EXPLICIT_REFERENCE_EVENT",
                    "gauge": {"name": "GAUGE_NAME",
                              "system": "GAUGE_SYSTEM",
                              "insertion_type": "EVENT_KEYS"},
                    "start": "2018-06-05T02:07:03",
                    "stop": "2018-06-05T08:07:36",
                    "key": "EVENT_KEY"
                }]
            }
        self.engine_eboa.operation = data
        self.engine_eboa._insert_dim_signature()
        self.engine_eboa._insert_source()
        self.engine_eboa._insert_gauges()
        self.engine_eboa.session.commit()
        self.engine_eboa._insert_explicit_refs()
        self.engine_eboa.session.commit()
        gauge_ddbb = self.session.query(Gauge).filter(Gauge.name == data["events"][0]["gauge"]["name"], Gauge.system == data["events"][0]["gauge"]["system"]).first()
        source_ddbb = self.session.query(Source).filter(Source.name == data["source"]["name"], Source.validity_start == data["source"]["validity_start"], Source.validity_stop == data["source"]["validity_stop"], Source.generation_time == data["source"]["generation_time"], Source.processor_version == data["dim_signature"]["version"], Source.processor == data["dim_signature"]["exec"]).first()
        dim_signature_ddbb = self.session.query(DimSignature).filter(DimSignature.dim_signature == data["dim_signature"]["name"]).first()
        explicit_reference_ddbb = self.session.query(ExplicitRef).filter(ExplicitRef.explicit_ref == data["events"][0]["explicit_reference"]).first()
        self.engine_eboa._insert_events()
        self.engine_eboa.session.commit()
        events_ddbb = self.session.query(Event).filter(Event.start == data["events"][0]["start"],
                                                      Event.stop == data["events"][0]["stop"],
                                                      Event.gauge_uuid == gauge_ddbb.gauge_uuid,
                                                      Event.source_uuid == source_ddbb.source_uuid,
                                                      Event.explicit_ref_uuid == explicit_reference_ddbb.explicit_ref_uuid,
                                                      Event.visible == False).all()

        assert len(events_ddbb) == 2

        event_keys = self.session.query(EventKey).filter(EventKey.event_key == data["events"][0]["key"],
                                                         EventKey.event_uuid == events_ddbb[0].event_uuid,
                                                         EventKey.visible == False).all()

        assert len(event_keys) == 1

    def test_insert_event_event_keys_with_diferent_dim_signature(self):


        data = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                                  "exec": "exec",
                                  "version": "1.0"},
                "source": {"name": "source.xml",
                           "reception_time": "2018-06-06T13:33:29",
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
        }]
        }
        self.engine_eboa.treat_data(data)

        gauge_ddbb = self.session.query(Gauge).filter(Gauge.name == data["operations"][0]["events"][0]["gauge"]["name"], Gauge.system == data["operations"][0]["events"][0]["gauge"]["system"]).first()
        source_ddbb = self.session.query(Source).filter(Source.name == data["operations"][0]["source"]["name"], Source.validity_start == data["operations"][0]["source"]["validity_start"], Source.validity_stop == data["operations"][0]["source"]["validity_stop"], Source.generation_time == data["operations"][0]["source"]["generation_time"], Source.processor_version == data["operations"][0]["dim_signature"]["version"], Source.processor == data["operations"][0]["dim_signature"]["exec"]).first()
        dim_signature_ddbb = self.session.query(DimSignature).filter(DimSignature.dim_signature == data["operations"][0]["dim_signature"]["name"]).first()
        explicit_reference_ddbb = self.session.query(ExplicitRef).filter(ExplicitRef.explicit_ref == data["operations"][0]["events"][0]["explicit_reference"]).first()
        event_ddbb = self.session.query(Event).filter(Event.start == data["operations"][0]["events"][0]["start"],
                                                      Event.stop == data["operations"][0]["events"][0]["stop"],
                                                      Event.gauge_uuid == gauge_ddbb.gauge_uuid,
                                                      Event.source_uuid == source_ddbb.source_uuid,
                                                      Event.explicit_ref_uuid == explicit_reference_ddbb.explicit_ref_uuid,
                                                      Event.visible == True).all()

        event_keys = self.session.query(EventKey).filter(EventKey.event_key == data["operations"][0]["events"][0]["key"],
                                                         EventKey.event_uuid == event_ddbb[0].event_uuid,
                                                         EventKey.visible == True).all()

        assert len(event_keys) == 1

        event_keys = self.session.query(EventKey).all()

        assert len(event_keys) == 1

        data = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature2",
                                  "exec": "exec",
                                  "version": "1.0"},
                "source": {"name": "source2.xml",
                           "reception_time": "2018-06-06T13:33:29",
                           "generation_time": "2018-07-05T02:07:03",
                           "validity_start": "2018-06-05T02:07:03",
                           "validity_stop": "2018-06-05T08:07:36"},
                "events": [{
                    "explicit_reference": "EXPLICIT_REFERENCE_EVENT",
                    "gauge": {"name": "GAUGE_NAME2",
                              "system": "GAUGE_SYSTEM2",
                              "insertion_type": "EVENT_KEYS"},
                    "start": "2018-06-05T02:07:03",
                    "stop": "2018-06-05T08:07:36",
                    "key": "EVENT_KEY"
                }]
        }]
        }
        self.engine_eboa.treat_data(data)

        gauge_ddbb = self.session.query(Gauge).filter(Gauge.name == data["operations"][0]["events"][0]["gauge"]["name"], Gauge.system == data["operations"][0]["events"][0]["gauge"]["system"]).first()
        source_ddbb = self.session.query(Source).filter(Source.name == data["operations"][0]["source"]["name"], Source.validity_start == data["operations"][0]["source"]["validity_start"], Source.validity_stop == data["operations"][0]["source"]["validity_stop"], Source.generation_time == data["operations"][0]["source"]["generation_time"], Source.processor_version == data["operations"][0]["dim_signature"]["version"], Source.processor == data["operations"][0]["dim_signature"]["exec"]).first()
        dim_signature_ddbb = self.session.query(DimSignature).filter(DimSignature.dim_signature == data["operations"][0]["dim_signature"]["name"]).first()
        explicit_reference_ddbb = self.session.query(ExplicitRef).filter(ExplicitRef.explicit_ref == data["operations"][0]["events"][0]["explicit_reference"]).first()
        event_ddbb = self.session.query(Event).filter(Event.start == data["operations"][0]["events"][0]["start"],
                                                      Event.stop == data["operations"][0]["events"][0]["stop"],
                                                      Event.gauge_uuid == gauge_ddbb.gauge_uuid,
                                                      Event.source_uuid == source_ddbb.source_uuid,
                                                      Event.explicit_ref_uuid == explicit_reference_ddbb.explicit_ref_uuid,
                                                      Event.visible == True).all()

        event_keys = self.session.query(EventKey).filter(EventKey.event_key == data["operations"][0]["events"][0]["key"],
                                                         EventKey.event_uuid == event_ddbb[0].event_uuid,
                                                         EventKey.visible == True).all()

        assert len(event_keys) == 1

        event_keys = self.session.query(EventKey).all()

        assert len(event_keys) == 2

    def test_insert_same_event_keys_same_source_different_processor(self):


        data = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                                  "exec": "exec",
                                  "version": "1.0"},
                "source": {"name": "source.xml",
                           "reception_time": "2018-06-06T13:33:29",
                           "generation_time": "2018-07-05T02:07:03",
                           "validity_start": "2018-06-05T02:07:03",
                           "validity_stop": "2018-06-05T08:07:36"},
                "events": [{
                    "gauge": {"name": "GAUGE_NAME",
                              "system": "GAUGE_SYSTEM",
                              "insertion_type": "EVENT_KEYS"},
                    "start": "2018-06-05T02:07:03",
                    "stop": "2018-06-05T08:07:36",
                    "key": "EVENT_KEY"
                }]
        }]
        }
        self.engine_eboa.treat_data(data)

        events_ddbb = self.session.query(Event).all()

        assert len(events_ddbb) == 1

        event_keys = self.session.query(EventKey).all()

        assert len(event_keys) == 1

        data = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                                  "exec": "exec2",
                                  "version": "1.0"},
                "source": {"name": "source.xml",
                           "reception_time": "2018-06-06T13:33:29",
                           "generation_time": "2018-07-05T02:07:04",
                           "validity_start": "2018-06-05T02:07:03",
                           "validity_stop": "2018-06-05T08:07:36"},
                "events": [{
                    "gauge": {"name": "GAUGE_NAME",
                              "system": "GAUGE_SYSTEM",
                              "insertion_type": "EVENT_KEYS"},
                    "start": "2018-06-05T02:07:03",
                    "stop": "2018-06-05T08:07:36",
                    "key": "EVENT_KEY"
                },{
                    "gauge": {"name": "GAUGE_NAME",
                              "system": "GAUGE_SYSTEM",
                              "insertion_type": "EVENT_KEYS"},
                    "start": "2018-06-05T02:07:03",
                    "stop": "2018-06-05T08:07:36",
                    "key": "EVENT_KEY"
                }]
        }]
        }
        self.engine_eboa.treat_data(data)

        events_ddbb = self.session.query(Event).all()

        assert len(events_ddbb) == 2

        event_keys = self.session.query(EventKey).all()

        assert len(event_keys) == 2

        sources = self.session.query(Source).all()

        assert len(sources) == 2

    def test_insert_event_keys_with_links(self):

        data = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                                  "exec": "exec",
                                  "version": "1.0"},
                "source": {"name": "source0.json",
                           "reception_time": "2018-06-06T13:33:29",
                           "generation_time": "2018-07-05T02:07:03",
                           "validity_start": "2018-06-05T02:07:03",
                           "validity_stop": "2018-06-05T08:07:36"},
                "events": [{
                    "gauge": {"name": "GAUGE_NAME",
                              "system": "GAUGE_SYSTEM",
                              "insertion_type": "SIMPLE_UPDATE"},
                    "start": "2018-06-05T02:07:03",
                    "stop": "2018-06-05T08:07:36",
                }]
        }]
        }
        exit_status = self.engine_eboa.treat_data(data)

        assert len([item for item in exit_status if item["status"] != eboa_engine.exit_codes["OK"]["status"]]) == 0

        events = self.session.query(Event).all()

        assert len(events) == 1

        event_uuid1 = events[0].event_uuid
        
        data = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source1.xml",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2018-07-05T02:07:03",
                       "validity_start": "2018-06-05T02:07:03",
                       "validity_stop": "2018-06-05T08:07:36"},
            "events": [{
                "links": [{
                    "link": str(event_uuid1),
                    "link_mode": "by_uuid",
                    "name": "EVENT_LINK_NAME"
                }],
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "EVENT_KEYS"},
                "start": "2018-06-05T02:07:03",
                "stop": "2018-06-05T08:07:36",
                "key": "EVENT_KEY"
            }]
        }]
        }
        self.engine_eboa.treat_data(data)

        events_ddbb = self.session.query(Event).all()

        assert len(events_ddbb) == 2

        event_keys = self.session.query(EventKey).all()

        assert len(event_keys) == 1

        data = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                                  "exec": "exec2",
                                  "version": "1.0"},
                "source": {"name": "source2.xml",
                           "reception_time": "2018-06-06T13:33:29",
                           "generation_time": "2018-07-05T02:07:04",
                           "validity_start": "2018-06-05T02:07:03",
                           "validity_stop": "2018-06-05T08:07:36"},
                "events": [{
                    "links": [{
                        "link": str(event_uuid1),
                        "link_mode": "by_uuid",
                        "name": "EVENT_LINK_NAME"
                    }],
                    "gauge": {"name": "GAUGE_NAME",
                              "system": "GAUGE_SYSTEM",
                              "insertion_type": "EVENT_KEYS"},
                    "start": "2018-06-05T02:07:03",
                    "stop": "2018-06-05T08:07:36",
                    "key": "EVENT_KEY"
                },{
                    "gauge": {"name": "GAUGE_NAME",
                              "system": "GAUGE_SYSTEM",
                              "insertion_type": "EVENT_KEYS"},
                    "start": "2018-06-05T02:07:03",
                    "stop": "2018-06-05T06:07:36",
                    "key": "EVENT_KEY"
                }]
        }]
        }
        self.engine_eboa.treat_data(data)

        events_ddbb = self.session.query(Event).all()

        assert len(events_ddbb) == 3

        event_keys = self.session.query(EventKey).all()

        assert len(event_keys) == 2

        events = self.session.query(Event).join(Gauge).join(Source).filter(Gauge.name == "GAUGE_NAME", 
                                                                           Gauge.system == "GAUGE_SYSTEM",
                                                                           Event.start == "2018-06-05T02:07:03",
                                                                           Event.stop == "2018-06-05T08:07:36",
                                                                           Source.name == "source2.xml").all()
        assert len(events) == 1

        event_uuid2 = events[0].event_uuid

        event_links = self.session.query(EventLink).all()

        assert len(event_links) == 1

        event_links = self.session.query(EventLink).filter(EventLink.name == "EVENT_LINK_NAME",
                                                           EventLink.event_uuid == event_uuid1,
                                                           EventLink.event_uuid_link == event_uuid2).all()

        assert len(event_links) == 1

    def test_insert_event_event_keys_with_priority(self):

        data = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                                  "exec": "exec",
                                  "version": "1.0"},
                "source": {"name": "source.xml",
                           "reception_time": "2018-06-06T13:33:29",
                           "generation_time": "2018-07-05T02:07:03",
                           "validity_start": "2018-06-05T02:07:03",
                           "validity_stop": "2018-06-05T08:07:36",
                           "priority": 10},
                "events": [{
                    "explicit_reference": "EXPLICIT_REFERENCE_EVENT",
                    "gauge": {"name": "GAUGE_NAME",
                              "system": "GAUGE_SYSTEM",
                              "insertion_type": "EVENT_KEYS_with_PRIORITY"},
                    "start": "2018-06-05T02:07:03",
                    "stop": "2018-06-05T08:07:36",
                    "key": "EVENT_KEY"
                },{
                    "explicit_reference": "EXPLICIT_REFERENCE_EVENT",
                    "gauge": {"name": "GAUGE_NAME",
                              "system": "GAUGE_SYSTEM",
                              "insertion_type": "EVENT_KEYS_with_PRIORITY"},
                    "start": "2018-06-05T02:07:03",
                    "stop": "2018-06-05T08:07:36",
                    "key": "EVENT_KEY"
                }]
        }]
        }

        self.engine_eboa.treat_data(data)

        events_ddbb = self.session.query(Event).join(ExplicitRef).join(Gauge).filter(Event.start == "2018-06-05T02:07:03",
                                                      Event.stop == "2018-06-05T08:07:36",
                                                      Gauge.name == "GAUGE_NAME",
                                                      ExplicitRef.explicit_ref == "EXPLICIT_REFERENCE_EVENT",
                                                                                     Event.visible == True).all()

        assert len(events_ddbb) == 2

        event_keys = self.session.query(EventKey).filter(EventKey.event_key == "EVENT_KEY",
                                                         EventKey.event_uuid == events_ddbb[0].event_uuid,
                                                         EventKey.visible == True).all()

        assert len(event_keys) == 1

    def test_insert_event_event_keys_with_priority_with_diferent_dim_signature(self):


        data = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                                  "exec": "exec",
                                  "version": "1.0"},
                "source": {"name": "source.xml",
                           "reception_time": "2018-06-06T13:33:29",
                           "generation_time": "2018-07-05T02:07:03",
                           "validity_start": "2018-06-05T02:07:03",
                           "validity_stop": "2018-06-05T08:07:36",
                           "priority": 10},
                "events": [{
                    "explicit_reference": "EXPLICIT_REFERENCE_EVENT",
                    "gauge": {"name": "GAUGE_NAME",
                              "system": "GAUGE_SYSTEM",
                              "insertion_type": "EVENT_KEYS_with_PRIORITY"},
                    "start": "2018-06-05T02:07:03",
                    "stop": "2018-06-05T08:07:36",
                    "key": "EVENT_KEY"
                }]
        }]
        }
        self.engine_eboa.treat_data(data)

        event_ddbb = self.session.query(Event).filter(Event.start == "2018-06-05T02:07:03",
                                                      Event.stop == "2018-06-05T08:07:36",
                                                      Event.visible == True).all()

        event_keys = self.session.query(EventKey).filter(EventKey.event_key == "EVENT_KEY",
                                                         EventKey.event_uuid == event_ddbb[0].event_uuid,
                                                         EventKey.visible == True).all()

        assert len(event_keys) == 1

        event_keys = self.session.query(EventKey).all()

        assert len(event_keys) == 1

        data = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature2",
                                  "exec": "exec",
                                  "version": "1.0"},
                "source": {"name": "source2.xml",
                           "reception_time": "2018-06-06T13:33:29",
                           "generation_time": "2018-07-05T02:07:03",
                           "validity_start": "2018-06-05T02:07:03",
                           "validity_stop": "2018-06-05T08:07:36",
                           "priority": 10},
                "events": [{
                    "explicit_reference": "EXPLICIT_REFERENCE_EVENT",
                    "gauge": {"name": "GAUGE_NAME2",
                              "system": "GAUGE_SYSTEM2",
                              "insertion_type": "EVENT_KEYS_with_PRIORITY"},
                    "start": "2018-06-05T02:07:03",
                    "stop": "2018-06-05T08:07:36",
                    "key": "EVENT_KEY"
                }]
        }]
        }
        self.engine_eboa.treat_data(data)

        event_ddbb = self.session.query(Event).join(Source).filter(Event.start == "2018-06-05T02:07:03",
                                                      Event.stop == "2018-06-05T08:07:36",
                                                      Source.name == "source2.xml",
                                                      Event.visible == True).all()

        event_keys = self.session.query(EventKey).filter(EventKey.event_key == "EVENT_KEY",
                                                         EventKey.event_uuid == event_ddbb[0].event_uuid,
                                                         EventKey.visible == True).all()

        assert len(event_keys) == 1

        event_keys = self.session.query(EventKey).all()

        assert len(event_keys) == 2

    def test_insert_same_event_keys_with_priority_same_source_different_processor(self):


        data = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                                  "exec": "exec",
                                  "version": "1.0"},
                "source": {"name": "source.xml",
                           "reception_time": "2018-06-06T13:33:29",
                           "generation_time": "2018-07-05T02:07:03",
                           "validity_start": "2018-06-05T02:07:03",
                           "validity_stop": "2018-06-05T08:07:36",
                           "priority": 10},
                "events": [{
                    "gauge": {"name": "GAUGE_NAME",
                              "system": "GAUGE_SYSTEM",
                              "insertion_type": "EVENT_KEYS_with_PRIORITY"},
                    "start": "2018-06-05T02:07:03",
                    "stop": "2018-06-05T08:07:36",
                    "key": "EVENT_KEY"
                }]
        }]
        }
        self.engine_eboa.treat_data(data)

        events_ddbb = self.session.query(Event).all()

        assert len(events_ddbb) == 1

        event_keys = self.session.query(EventKey).all()

        assert len(event_keys) == 1

        data = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                                  "exec": "exec2",
                                  "version": "1.0"},
                "source": {"name": "source.xml",
                           "reception_time": "2018-06-06T13:33:29",
                           "generation_time": "2018-07-05T02:07:04",
                           "validity_start": "2018-06-05T02:07:03",
                           "validity_stop": "2018-06-05T08:07:36",
                           "priority": 20},
                "events": [{
                    "gauge": {"name": "GAUGE_NAME",
                              "system": "GAUGE_SYSTEM",
                              "insertion_type": "EVENT_KEYS_with_PRIORITY"},
                    "start": "2018-06-05T02:07:03",
                    "stop": "2018-06-05T08:07:36",
                    "key": "EVENT_KEY"
                },{
                    "gauge": {"name": "GAUGE_NAME",
                              "system": "GAUGE_SYSTEM",
                              "insertion_type": "EVENT_KEYS_with_PRIORITY"},
                    "start": "2018-06-05T02:07:03",
                    "stop": "2018-06-05T08:07:36",
                    "key": "EVENT_KEY"
                }]
        }]
        }
        self.engine_eboa.treat_data(data)

        events_ddbb = self.session.query(Event).all()

        assert len(events_ddbb) == 2

        event_keys = self.session.query(EventKey).all()

        assert len(event_keys) == 2

        sources = self.session.query(Source).all()

        assert len(sources) == 2

    def test_insert_event_keys_with_priority_with_links(self):

        data = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                                  "exec": "exec",
                                  "version": "1.0"},
                "source": {"name": "source0.json",
                           "reception_time": "2018-06-06T13:33:29",
                           "generation_time": "2018-07-05T02:07:03",
                           "validity_start": "2018-06-05T02:07:03",
                           "validity_stop": "2018-06-05T08:07:36"},
                "events": [{
                    "gauge": {"name": "GAUGE_NAME",
                              "system": "GAUGE_SYSTEM",
                              "insertion_type": "SIMPLE_UPDATE"},
                    "start": "2018-06-05T02:07:03",
                    "stop": "2018-06-05T08:07:36",
                }]
        }]
        }
        exit_status = self.engine_eboa.treat_data(data)

        assert len([item for item in exit_status if item["status"] != eboa_engine.exit_codes["OK"]["status"]]) == 0

        events = self.session.query(Event).all()

        assert len(events) == 1

        event_uuid1 = events[0].event_uuid
        
        data = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source1.xml",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2018-07-05T02:07:03",
                       "validity_start": "2018-06-05T02:07:03",
                       "validity_stop": "2018-06-05T08:07:36",
                       "priority": 10},
            "events": [{
                "links": [{
                    "link": str(event_uuid1),
                    "link_mode": "by_uuid",
                    "name": "EVENT_LINK_NAME"
                }],
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "EVENT_KEYS_with_PRIORITY"},
                "start": "2018-06-05T02:07:03",
                "stop": "2018-06-05T08:07:36",
                "key": "EVENT_KEY"
            }]
        }]
        }
        self.engine_eboa.treat_data(data)

        events_ddbb = self.session.query(Event).all()

        assert len(events_ddbb) == 2

        event_keys = self.session.query(EventKey).all()

        assert len(event_keys) == 1

        data = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                                  "exec": "exec2",
                                  "version": "1.0"},
                "source": {"name": "source2.xml",
                           "reception_time": "2018-06-06T13:33:29",
                           "generation_time": "2018-07-05T02:07:04",
                           "validity_start": "2018-06-05T02:07:03",
                           "validity_stop": "2018-06-05T08:07:36",
                           "priority": 20},
                "events": [{
                    "links": [{
                        "link": str(event_uuid1),
                        "link_mode": "by_uuid",
                        "name": "EVENT_LINK_NAME"
                    }],
                    "gauge": {"name": "GAUGE_NAME",
                              "system": "GAUGE_SYSTEM",
                              "insertion_type": "EVENT_KEYS_with_PRIORITY"},
                    "start": "2018-06-05T02:07:03",
                    "stop": "2018-06-05T08:07:36",
                    "key": "EVENT_KEY"
                },{
                    "gauge": {"name": "GAUGE_NAME",
                              "system": "GAUGE_SYSTEM",
                              "insertion_type": "EVENT_KEYS_with_PRIORITY"},
                    "start": "2018-06-05T02:07:03",
                    "stop": "2018-06-05T06:07:36",
                    "key": "EVENT_KEY"
                }]
        }]
        }
        self.engine_eboa.treat_data(data)

        events_ddbb = self.session.query(Event).all()

        assert len(events_ddbb) == 3

        event_keys = self.session.query(EventKey).all()

        assert len(event_keys) == 2

        events = self.session.query(Event).join(Gauge).join(Source).filter(Gauge.name == "GAUGE_NAME", 
                                                                           Gauge.system == "GAUGE_SYSTEM",
                                                                           Event.start == "2018-06-05T02:07:03",
                                                                           Event.stop == "2018-06-05T08:07:36",
                                                                           Source.name == "source2.xml").all()
        assert len(events) == 1

        event_uuid2 = events[0].event_uuid

        event_links = self.session.query(EventLink).all()

        assert len(event_links) == 1

        event_links = self.session.query(EventLink).filter(EventLink.name == "EVENT_LINK_NAME",
                                                           EventLink.event_uuid == event_uuid1,
                                                           EventLink.event_uuid_link == event_uuid2).all()

        assert len(event_links) == 1

    def test_insert_event_simple_update_values(self):

        self.engine_eboa._initialize_context_insert_data()
        data = {"dim_signature": {"name": "dim_signature",
                                  "exec": "exec",
                                  "version": "1.0"},
                "source": {"name": "source.xml",
                           "reception_time": "2018-06-06T13:33:29",
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
        self.engine_eboa.operation = data
        self.engine_eboa._insert_dim_signature()
        self.engine_eboa._insert_source()
        self.engine_eboa._insert_gauges()
        self.engine_eboa.session.commit()
        gauge_ddbb = self.session.query(Gauge).filter(Gauge.name == data["events"][0]["gauge"]["name"], Gauge.system == data["events"][0]["gauge"]["system"]).first()
        source_ddbb = self.session.query(Source).filter(Source.name == data["source"]["name"], Source.validity_start == data["source"]["validity_start"], Source.validity_stop == data["source"]["validity_stop"], Source.generation_time == data["source"]["generation_time"], Source.processor_version == data["dim_signature"]["version"], Source.processor == data["dim_signature"]["exec"]).first()
        dim_signature_ddbb = self.session.query(DimSignature).filter(DimSignature.dim_signature == data["dim_signature"]["name"]).first()
        self.engine_eboa._insert_events()
        self.engine_eboa.session.commit()
        event_ddbb = self.session.query(Event).filter(Event.start == data["events"][0]["start"],
                                                      Event.stop == data["events"][0]["stop"],
                                                      Event.gauge_uuid == gauge_ddbb.gauge_uuid,
                                                      Event.source_uuid == source_ddbb.source_uuid,
                                                      Event.visible == True).all()

        assert len(event_ddbb) == 1

        values_ddbb = self.query_eboa.get_event_values()

        assert len(values_ddbb) == 8

    def test_insert_event_simple_update_not_a_float_geometry(self):

        self.engine_eboa._initialize_context_insert_data()
        data = {"dim_signature": {"name": "dim_signature",
                                  "exec": "exec",
                                  "version": "1.0"},
                "source": {"name": "source.xml",
                           "reception_time": "2018-06-06T13:33:29",
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
        self.engine_eboa.operation = data
        self.engine_eboa._insert_dim_signature()
        self.engine_eboa._insert_source()
        self.engine_eboa._insert_gauges()
        self.engine_eboa.session.commit()
        gauge_ddbb = self.session.query(Gauge).filter(Gauge.name == data["events"][0]["gauge"]["name"], Gauge.system == data["events"][0]["gauge"]["system"]).first()
        source_ddbb = self.session.query(Source).filter(Source.name == data["source"]["name"], Source.validity_start == data["source"]["validity_start"], Source.validity_stop == data["source"]["validity_stop"], Source.generation_time == data["source"]["generation_time"], Source.processor_version == data["dim_signature"]["version"], Source.processor == data["dim_signature"]["exec"]).first()
        dim_signature_ddbb = self.session.query(DimSignature).filter(DimSignature.dim_signature == data["dim_signature"]["name"]).first()
        try:
            self.engine_eboa._insert_events()
        except WrongValue:
            self.session.rollback()
            pass
        event_ddbb = self.session.query(Event).all()

        assert len(event_ddbb) == 0

    def test_insert_event_simple_update_wrong_geometry(self):

        data = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source.xml",
                       "reception_time": "2018-06-06T13:33:29",
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
        }]
        }
        self.engine_eboa.treat_data(data)
        event_ddbb = self.session.query(Event).all()

        assert len(event_ddbb) == 0

    def test_insert_event_simple_update_odd_geometry(self):

        self.engine_eboa._initialize_context_insert_data()
        data = {"dim_signature": {"name": "dim_signature",
                                  "exec": "exec",
                                  "version": "1.0"},
                "source": {"name": "source.xml",
                           "reception_time": "2018-06-06T13:33:29",
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
        self.engine_eboa.operation = data
        self.engine_eboa._insert_dim_signature()
        self.engine_eboa._insert_source()
        self.engine_eboa._insert_gauges()
        self.engine_eboa.session.commit()
        gauge_ddbb = self.session.query(Gauge).filter(Gauge.name == data["events"][0]["gauge"]["name"], Gauge.system == data["events"][0]["gauge"]["system"]).first()
        source_ddbb = self.session.query(Source).filter(Source.name == data["source"]["name"], Source.validity_start == data["source"]["validity_start"], Source.validity_stop == data["source"]["validity_stop"], Source.generation_time == data["source"]["generation_time"], Source.processor_version == data["dim_signature"]["version"], Source.processor == data["dim_signature"]["exec"]).first()
        dim_signature_ddbb = self.session.query(DimSignature).filter(DimSignature.dim_signature == data["dim_signature"]["name"]).first()
        try:
            self.engine_eboa._insert_events()
        except OddNumberOfCoordinates:
            self.session.rollback()
            pass
        event_ddbb = self.session.query(Event).all()

        assert len(event_ddbb) == 0

    def test_insert_event_simple_update_wrong_boolean(self):

        self.engine_eboa._initialize_context_insert_data()
        data = {"dim_signature": {"name": "dim_signature",
                                  "exec": "exec",
                                  "version": "1.0"},
                "source": {"name": "source.xml",
                           "reception_time": "2018-06-06T13:33:29",
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
        self.engine_eboa.operation = data
        self.engine_eboa._insert_dim_signature()
        self.engine_eboa._insert_source()
        self.engine_eboa._insert_gauges()
        self.engine_eboa.session.commit()
        gauge_ddbb = self.session.query(Gauge).filter(Gauge.name == data["events"][0]["gauge"]["name"], Gauge.system == data["events"][0]["gauge"]["system"]).first()
        source_ddbb = self.session.query(Source).filter(Source.name == data["source"]["name"], Source.validity_start == data["source"]["validity_start"], Source.validity_stop == data["source"]["validity_stop"], Source.generation_time == data["source"]["generation_time"], Source.processor_version == data["dim_signature"]["version"], Source.processor == data["dim_signature"]["exec"]).first()
        dim_signature_ddbb = self.session.query(DimSignature).filter(DimSignature.dim_signature == data["dim_signature"]["name"]).first()
        try:
            self.engine_eboa._insert_events()
        except WrongValue:
            self.session.rollback()
            pass
        event_ddbb = self.session.query(Event).all()

        assert len(event_ddbb) == 0

    def test_insert_event_simple_update_wrong_double(self):

        self.engine_eboa._initialize_context_insert_data()
        data = {"dim_signature": {"name": "dim_signature",
                                  "exec": "exec",
                                  "version": "1.0"},
                "source": {"name": "source.xml",
                           "reception_time": "2018-06-06T13:33:29",
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
        self.engine_eboa.operation = data
        self.engine_eboa._insert_dim_signature()
        self.engine_eboa._insert_source()
        self.engine_eboa._insert_gauges()
        self.engine_eboa.session.commit()
        gauge_ddbb = self.session.query(Gauge).filter(Gauge.name == data["events"][0]["gauge"]["name"], Gauge.system == data["events"][0]["gauge"]["system"]).first()
        source_ddbb = self.session.query(Source).filter(Source.name == data["source"]["name"], Source.validity_start == data["source"]["validity_start"], Source.validity_stop == data["source"]["validity_stop"], Source.generation_time == data["source"]["generation_time"], Source.processor_version == data["dim_signature"]["version"], Source.processor == data["dim_signature"]["exec"]).first()
        dim_signature_ddbb = self.session.query(DimSignature).filter(DimSignature.dim_signature == data["dim_signature"]["name"]).first()
        try:
            self.engine_eboa._insert_events()
        except WrongValue:
            self.session.rollback()
            pass
        event_ddbb = self.session.query(Event).all()

        assert len(event_ddbb) == 0

    def test_insert_event_simple_update_wrong_timestamp(self):

        self.engine_eboa._initialize_context_insert_data()
        data = {"dim_signature": {"name": "dim_signature",
                                  "exec": "exec",
                                  "version": "1.0"},
                "source": {"name": "source.xml",
                           "reception_time": "2018-06-06T13:33:29",
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
        self.engine_eboa.operation = data
        self.engine_eboa._insert_dim_signature()
        self.engine_eboa._insert_source()
        self.engine_eboa._insert_gauges()
        self.engine_eboa.session.commit()
        gauge_ddbb = self.session.query(Gauge).filter(Gauge.name == data["events"][0]["gauge"]["name"], Gauge.system == data["events"][0]["gauge"]["system"]).first()
        source_ddbb = self.session.query(Source).filter(Source.name == data["source"]["name"], Source.validity_start == data["source"]["validity_start"], Source.validity_stop == data["source"]["validity_stop"], Source.generation_time == data["source"]["generation_time"], Source.processor_version == data["dim_signature"]["version"], Source.processor == data["dim_signature"]["exec"]).first()
        dim_signature_ddbb = self.session.query(DimSignature).filter(DimSignature.dim_signature == data["dim_signature"]["name"]).first()
        try:
            self.engine_eboa._insert_events()
        except WrongValue:
            self.session.rollback()
            pass
        event_ddbb = self.session.query(Event).all()

        assert len(event_ddbb) == 0

    def test_insert_event_links(self):

        self.engine_eboa._initialize_context_insert_data()
        data = {"dim_signature": {"name": "dim_signature",
                                  "exec": "exec",
                                  "version": "1.0"},
                "source": {"name": "source.xml",
                           "reception_time": "2018-06-06T13:33:29",
                           "generation_time": "2018-07-05T02:07:03",
                           "validity_start": "2018-06-05T02:07:03",
                           "validity_stop": "2018-06-05T08:07:36"},
                "events": [{
                    "link_ref": "EVENT_LINK1",
                    "gauge": {"name": "GAUGE_NAME",
                              "system": "GAUGE_SYSTEM",
                              "insertion_type": "SIMPLE_UPDATE"},
                    "start": "2018-06-05T02:07:03",
                    "stop": "2018-06-05T08:07:36",
                    "links": [{
                        "link": "EVENT_LINK2",
                        "link_mode": "by_ref",
                        "name": "EVENT_LINK_NAME"
                    }]
                },
                {
                    "link_ref": "EVENT_LINK2",
                    "gauge": {"name": "GAUGE_NAME",
                              "system": "GAUGE_SYSTEM",
                              "insertion_type": "SIMPLE_UPDATE"},
                    "start": "2018-06-05T02:07:03",
                    "stop": "2018-06-05T08:07:36",
                    "links": [{
                        "link": "EVENT_LINK1",
                        "link_mode": "by_ref",
                        "name": "EVENT_LINK_NAME"
                    }]
                }]
            }
        self.engine_eboa.operation = data
        self.engine_eboa._insert_dim_signature()
        self.engine_eboa._insert_source()
        self.engine_eboa._insert_gauges()
        self.engine_eboa.session.commit()
        self.engine_eboa._insert_events()
        self.engine_eboa.session.commit()
        events_ddbb = self.session.query(Event).all()

        assert len(events_ddbb) == 2

        self.engine_eboa._initialize_context_insert_data()
        data = {"dim_signature": {"name": "dim_signature",
                                  "exec": "exec",
                                  "version": "1.0"},
                "source": {"name": "source2.xml",
                           "reception_time": "2018-06-06T13:33:29",
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
                        "name": "EVENT_LINK_NAME",
                        "back_ref": "LINK_BACK_REF_NAME"
                    }]
                }]
            }
        self.engine_eboa.operation = data
        self.engine_eboa._insert_dim_signature()
        self.engine_eboa._insert_source()
        self.engine_eboa._insert_gauges()
        self.engine_eboa.session.commit()
        self.engine_eboa._insert_events()
        self.engine_eboa.session.commit()

        event_ddbb = self.session.query(Event).filter(Event.start == data["events"][0]["start"]).all()

        assert len(event_ddbb) == 1

        link_ddbb_1 = self.session.query(EventLink).filter(EventLink.event_uuid_link == events_ddbb[0].event_uuid, EventLink.event_uuid == events_ddbb[1].event_uuid).all()

        assert len(link_ddbb_1) == 1

        link_ddbb_2 = self.session.query(EventLink).filter(EventLink.event_uuid_link == events_ddbb[1].event_uuid, EventLink.event_uuid == events_ddbb[0].event_uuid).all()

        assert len(link_ddbb_2) == 1

        link_ddbb_3 = self.session.query(EventLink).filter(EventLink.event_uuid_link == event_ddbb[0].event_uuid, EventLink.event_uuid == events_ddbb[0].event_uuid).all()

        assert len(link_ddbb_3) == 1

        link_ddbb_3 = self.session.query(EventLink).filter(EventLink.event_uuid_link == events_ddbb[0].event_uuid, EventLink.event_uuid == event_ddbb[0].event_uuid).all()

        assert len(link_ddbb_3) == 1


    def test_event_links_inconsistency(self):

        self.engine_eboa._initialize_context_insert_data()
        data = {"operations": [{
                "mode": "insert",
                "dim_signature": {"name": "dim_signature",
                                  "exec": "exec",
                                  "version": "1.0"},
                "source": {"name": "source.xml",
                           "reception_time": "2018-06-06T13:33:29",
                           "generation_time": "2018-07-05T02:07:03",
                           "validity_start": "2018-06-05T02:07:03",
                           "validity_stop": "2018-06-05T08:07:36"},
                "events": [{
                    "link_ref": "EVENT_LINK1",
                    "gauge": {"name": "GAUGE_NAME",
                              "system": "GAUGE_SYSTEM",
                              "insertion_type": "SIMPLE_UPDATE"},
                    "start": "2018-06-05T02:07:03",
                    "stop": "2018-06-05T08:07:36",
                    "links": [{
                        "link": "EVENT_LINK2",
                        "link_mode": "by_ref",
                        "name": "EVENT_LINK_NAME",
                        "back_ref": "EVENT_LINK_NAME"
                    }]
                },
                {
                    "link_ref": "EVENT_LINK2",
                    "gauge": {"name": "GAUGE_NAME",
                              "system": "GAUGE_SYSTEM",
                              "insertion_type": "SIMPLE_UPDATE"},
                    "start": "2018-06-05T02:07:03",
                    "stop": "2018-06-05T08:07:36",
                    "links": [{
                        "link": "EVENT_LINK1",
                        "link_mode": "by_ref",
                        "name": "EVENT_LINK_NAME"
                    }]
                }]
            }]
            }
        self.engine_eboa.data = data
        assert_value = False
        try:
            self.engine_eboa.treat_data()
        except LinksInconsistency:
            assert_value = True
        # end try

        assert assert_value == True

        sources_status = self.session.query(SourceStatus).join(Source).filter(SourceStatus.status == eboa_engine.exit_codes["LINKS_INCONSISTENCY"]["status"],
                                                                           Source.name == data["operations"][0]["source"]["name"]).all()

        assert len(sources_status) == 1

    def test_duplicated_event_links(self):

        self.engine_eboa._initialize_context_insert_data()
        data = {"operations": [{
                "mode": "insert",
                "dim_signature": {"name": "dim_signature",
                                  "exec": "exec",
                                  "version": "1.0"},
                "source": {"name": "source.xml",
                           "reception_time": "2018-06-06T13:33:29",
                           "generation_time": "2018-07-05T02:07:03",
                           "validity_start": "2018-06-05T02:07:03",
                           "validity_stop": "2018-06-05T08:07:36"},
                "events": [{
                    "link_ref": "EVENT_LINK1",
                    "gauge": {"name": "GAUGE_NAME",
                              "system": "GAUGE_SYSTEM",
                              "insertion_type": "SIMPLE_UPDATE"},
                    "start": "2018-06-05T02:07:03",
                    "stop": "2018-06-05T08:07:36",
                    "links": [{
                        "link": "EVENT_LINK1",
                        "link_mode": "by_ref",
                        "name": "EVENT_LINK_NAME",
                        "back_ref": "LINK_BACK_REF_NAME"
                    }]
                },
                {
                    "link_ref": "EVENT_LINK1",
                    "gauge": {"name": "GAUGE_NAME",
                              "system": "GAUGE_SYSTEM",
                              "insertion_type": "SIMPLE_UPDATE"},
                    "start": "2018-06-05T02:07:03",
                    "stop": "2018-06-05T08:07:36",
                    "links": [{
                        "link": "EVENT_LINK1",
                        "link_mode": "by_ref",
                        "name": "EVENT_LINK_NAME2"
                    }]
                }]
            }]
            }
        self.engine_eboa.data = data
        returned_value = self.engine_eboa.treat_data()[0]["status"]

        assert returned_value == eboa_engine.exit_codes["DUPLICATED_EVENT_LINK_REF"]["status"]

        sources_status = self.session.query(SourceStatus).join(Source).filter(SourceStatus.status == eboa_engine.exit_codes["DUPLICATED_EVENT_LINK_REF"]["status"],
                                                                           Source.name == data["operations"][0]["source"]["name"]).all()

        assert len(sources_status) == 1

    def test_insert_event_undefined_link(self):

        self.engine_eboa._initialize_context_insert_data()
        data = {"dim_signature": {"name": "dim_signature",
                                  "exec": "exec",
                                  "version": "1.0"},
                "source": {"name": "source.xml",
                           "reception_time": "2018-06-06T13:33:29",
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
        self.engine_eboa.operation = data
        self.engine_eboa._insert_dim_signature()
        self.engine_eboa._insert_source()
        self.engine_eboa._insert_gauges()
        self.engine_eboa.session.commit()
        gauge_ddbb = self.session.query(Gauge).filter(Gauge.name == data["events"][0]["gauge"]["name"], Gauge.system == data["events"][0]["gauge"]["system"]).first()
        source_ddbb = self.session.query(Source).filter(Source.name == data["source"]["name"], Source.validity_start == data["source"]["validity_start"], Source.validity_stop == data["source"]["validity_stop"], Source.generation_time == data["source"]["generation_time"], Source.processor_version == data["dim_signature"]["version"], Source.processor == data["dim_signature"]["exec"]).first()
        dim_signature_ddbb = self.session.query(DimSignature).filter(DimSignature.dim_signature == data["dim_signature"]["name"]).first()
        try:
            self.engine_eboa._insert_events()
        except UndefinedEventLink:
            self.session.rollback()
            pass
        event_ddbb = self.session.query(Event).all()

        assert len(event_ddbb) == 0

    def test_insert_annotations(self):

        self.engine_eboa._initialize_context_insert_data()
        data = {"dim_signature": {"name": "dim_signature",
                                  "exec": "exec",
                                  "version": "1.0"},
                "source": {"name": "source.xml",
                           "reception_time": "2018-06-06T13:33:29",
                           "generation_time": "2018-07-05T02:07:03",
                           "validity_start": "2018-06-05T02:07:03",
                           "validity_stop": "2018-06-05T08:07:36"},
                "annotations": [{
                    "explicit_reference": "EXPLICIT_REFERENCE_ANNOTATION",
                    "annotation_cnf": {
                        "insertion_type": "INSERT_and_ERASE",
                        "name": "NAME",
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
        self.engine_eboa.operation = data
        self.engine_eboa._insert_dim_signature()
        self.engine_eboa._insert_source()
        self.engine_eboa._insert_explicit_refs()
        self.engine_eboa.session.commit()
        self.engine_eboa._insert_annotation_cnfs()
        self.engine_eboa.session.commit()
        self.engine_eboa._insert_annotations()
        self.engine_eboa.session.commit()
        annotation_cnf_ddbb = self.session.query(AnnotationCnf).filter(AnnotationCnf.name == data["annotations"][0]["annotation_cnf"]["name"], AnnotationCnf.system == data["annotations"][0]["annotation_cnf"]["system"]).first()
        source_ddbb = self.session.query(Source).filter(Source.name == data["source"]["name"], Source.validity_start == data["source"]["validity_start"], Source.validity_stop == data["source"]["validity_stop"], Source.generation_time == data["source"]["generation_time"], Source.processor_version == data["dim_signature"]["version"], Source.processor == data["dim_signature"]["exec"]).first()
        dim_signature_ddbb = self.session.query(DimSignature).filter(DimSignature.dim_signature == data["dim_signature"]["name"]).first()
        explicit_reference_ddbb = self.session.query(ExplicitRef).filter(ExplicitRef.explicit_ref == data["annotations"][0]["explicit_reference"]).first()
        annotation_ddbb = self.session.query(Annotation).filter(Annotation.visible == False, Annotation.explicit_ref_uuid == explicit_reference_ddbb.explicit_ref_uuid,
                                                                Annotation.source_uuid == source_ddbb.source_uuid, Annotation.annotation_cnf_uuid == annotation_cnf_ddbb.annotation_cnf_uuid).all()

        assert len(annotation_ddbb) == 1

        values_ddbb = self.query_eboa.get_annotation_values()

        assert len(values_ddbb) == 8

    def test_repeated_annotation(self):

        self.engine_eboa._initialize_context_insert_data()
        data = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source.xml",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2018-07-05T02:07:03",
                       "validity_start": "2018-06-05T02:07:03",
                       "validity_stop": "2018-06-05T08:07:36"},
            "annotations": [{
                "explicit_reference": "EXPLICIT_REFERENCE_ANNOTATION",
                "annotation_cnf": {"name": "NAME",
                                   "system": "SYSTEM"}
            },{
                "explicit_reference": "EXPLICIT_REFERENCE_ANNOTATION",
                "annotation_cnf": {"name": "NAME",
                                   "system": "SYSTEM"}
            }]
        }]}
        self.engine_eboa.data = data
        self.engine_eboa.treat_data()

        annotations_ddbb = self.session.query(Annotation).all()

        assert len(annotations_ddbb) == 2

    def test_remove_deprecated_event_keys(self):

        self.engine_eboa._initialize_context_insert_data()
        data = {"dim_signature": {"name": "dim_signature",
                                  "exec": "exec",
                                  "version": "1.0"},
                "source": {"name": "source.xml",
                           "reception_time": "2018-06-06T13:33:29",
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
                },
                           {
                               "explicit_reference": "EXPLICIT_REFERENCE_EVENT1",
                               "gauge": {"name": "GAUGE_NAME1",
                                         "system": "GAUGE_SYSTEM1",
                                         "insertion_type": "EVENT_KEYS"},
                               "start": "2018-06-05T02:07:03",
                               "stop": "2018-06-05T08:07:36",
                               "key": "EVENT_KEY"
                       }]
            }
        self.engine_eboa.operation = data
        self.engine_eboa._insert_dim_signature()
        self.engine_eboa._insert_source()
        self.engine_eboa._insert_gauges()
        self.engine_eboa.session.commit()
        self.engine_eboa._insert_explicit_refs()
        self.engine_eboa.session.commit()
        self.engine_eboa._insert_events()
        self.engine_eboa.session.commit()
        self.engine_eboa._initialize_context_insert_data()
        data = {"dim_signature": {"name": "dim_signature",
                                  "exec": "exec",
                                  "version": "1.0"},
                "source": {"name": "source2.xml",
                           "reception_time": "2018-06-06T13:33:29",
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
        self.engine_eboa.operation = data
        self.engine_eboa._insert_dim_signature()
        self.engine_eboa._insert_source()
        self.engine_eboa._insert_gauges()
        self.engine_eboa.session.commit()
        self.engine_eboa._insert_explicit_refs()
        self.engine_eboa.session.commit()
        self.engine_eboa._insert_events()
        self.engine_eboa.session.commit()
        self.engine_eboa._remove_deprecated_events_event_keys()
        self.engine_eboa.session.commit()
        gauge_ddbb = self.session.query(Gauge).filter(Gauge.name == data["events"][0]["gauge"]["name"], Gauge.system == data["events"][0]["gauge"]["system"]).first()
        source_ddbb = self.session.query(Source).filter(Source.name == data["source"]["name"], Source.validity_start == data["source"]["validity_start"], Source.validity_stop == data["source"]["validity_stop"], Source.generation_time == data["source"]["generation_time"], Source.processor_version == data["dim_signature"]["version"], Source.processor == data["dim_signature"]["exec"]).first()
        dim_signature_ddbb = self.session.query(DimSignature).filter(DimSignature.dim_signature == data["dim_signature"]["name"]).first()
        explicit_reference_ddbb = self.session.query(ExplicitRef).filter(ExplicitRef.explicit_ref == data["events"][0]["explicit_reference"]).first()

        event_ddbb = self.session.query(Event).filter(Event.start == data["events"][0]["start"],
                                                      Event.stop == data["events"][0]["stop"],
                                                      Event.gauge_uuid == gauge_ddbb.gauge_uuid,
                                                      Event.source_uuid == source_ddbb.source_uuid,
                                                      Event.explicit_ref_uuid == explicit_reference_ddbb.explicit_ref_uuid,
                                                      Event.visible == True).all()

        event_keys = self.session.query(EventKey).filter(EventKey.event_key == data["events"][0]["key"],
                                                         EventKey.event_uuid == event_ddbb[0].event_uuid,
                                                         EventKey.visible == True).all()

        assert len(event_keys) == 1

        events_ddbb = self.session.query(Event).all()

        assert len(events_ddbb) == 1

        values_ddbb = self.query_eboa.get_event_values()

        assert len(values_ddbb) == 8

    def test_deprecated_annotations(self):

        self.engine_eboa._initialize_context_insert_data()
        data = {"dim_signature": {"name": "dim_signature",
                                  "exec": "exec",
                                  "version": "1.0"},
                "source": {"name": "source.xml",
                           "reception_time": "2018-06-06T13:33:29",
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
                },
                                {
                                    "explicit_reference": "EXPLICIT_REFERENCE_ANNOTATION",
                                    "annotation_cnf": {"name": "NAME",
                                                       "system": "SYSTEM"}
                                }]
            }
        self.engine_eboa.operation = data
        self.engine_eboa._insert_dim_signature()
        self.engine_eboa._insert_source()
        self.engine_eboa._insert_explicit_refs()
        self.engine_eboa.session.commit()
        self.engine_eboa._insert_annotation_cnfs()
        self.engine_eboa.session.commit()
        self.engine_eboa._insert_annotations()
        self.engine_eboa.session.commit()
        self.engine_eboa._initialize_context_insert_data()
        data = {"dim_signature": {"name": "dim_signature",
                                  "exec": "exec",
                                  "version": "1.0"},
                "source": {"name": "source2.xml",
                           "reception_time": "2018-06-06T13:33:29",
                           "generation_time": "2018-07-05T02:07:04",
                           "validity_start": "2018-06-05T02:07:03",
                           "validity_stop": "2018-06-05T08:07:36"},
                "annotations": [{
                    "explicit_reference": "EXPLICIT_REFERENCE_ANNOTATION",
                    "annotation_cnf": {
                        "insertion_type": "INSERT_and_ERASE",
                        "name": "NAME",
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
        self.engine_eboa.operation = data
        self.engine_eboa._insert_dim_signature()
        self.engine_eboa._insert_source()
        self.engine_eboa._insert_explicit_refs()
        self.engine_eboa.session.commit()
        self.engine_eboa._insert_annotation_cnfs()
        self.engine_eboa.session.commit()
        self.engine_eboa._insert_annotations()
        self.engine_eboa.session.commit()
        self.engine_eboa._remove_deprecated_annotations()
        self.engine_eboa.session.commit()
        annotation_cnf_ddbb = self.session.query(AnnotationCnf).filter(AnnotationCnf.name == data["annotations"][0]["annotation_cnf"]["name"], AnnotationCnf.system == data["annotations"][0]["annotation_cnf"]["system"]).first()
        source_ddbb = self.session.query(Source).filter(Source.name == data["source"]["name"], Source.validity_start == data["source"]["validity_start"], Source.validity_stop == data["source"]["validity_stop"], Source.generation_time == data["source"]["generation_time"], Source.processor_version == data["dim_signature"]["version"], Source.processor == data["dim_signature"]["exec"]).first()
        dim_signature_ddbb = self.session.query(DimSignature).filter(DimSignature.dim_signature == data["dim_signature"]["name"]).first()
        dim_signature_ddbb = self.session.query(DimSignature).filter(DimSignature.dim_signature == data["dim_signature"]["name"]).first()
        explicit_reference_ddbb = self.session.query(ExplicitRef).filter(ExplicitRef.explicit_ref == data["annotations"][0]["explicit_reference"]).first()
        annotation_ddbb = self.session.query(Annotation).filter(Annotation.visible == True, Annotation.explicit_ref_uuid == explicit_reference_ddbb.explicit_ref_uuid,
                                                                Annotation.source_uuid == source_ddbb.source_uuid, Annotation.annotation_cnf_uuid == annotation_cnf_ddbb.annotation_cnf_uuid).all()

        assert len(annotation_ddbb) == 1

        annotations_ddbb = self.session.query(Annotation).all()

        assert len(annotations_ddbb) == 1

        values_ddbb = self.query_eboa.get_annotation_values()

        assert len(values_ddbb) == 8

    def test_deprecated_annotations_by_insert_and_erase_with_priority(self):

        data = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                                  "exec": "exec",
                                  "version": "1.0"},
                "source": {"name": "source.xml",
                           "reception_time": "2018-06-06T13:33:29",
                           "generation_time": "2018-07-05T02:07:03",
                           "validity_start": "2018-06-05T02:07:03",
                           "validity_stop": "2018-06-05T08:07:36",
                           "priority": 5},
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
                },
                                {
                                    "explicit_reference": "EXPLICIT_REFERENCE_ANNOTATION",
                                    "annotation_cnf": {"name": "NAME",
                                                       "system": "SYSTEM"}
                                }]
        }]}
        self.engine_eboa.treat_data(data)

        data = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                                  "exec": "exec",
                                  "version": "1.0"},
                "source": {"name": "source2.xml",
                           "reception_time": "2018-06-06T13:33:29",
                           "generation_time": "2018-07-05T02:07:04",
                           "validity_start": "2018-06-05T02:07:03",
                           "validity_stop": "2018-06-05T08:07:36",
                           "priority": 10},
                "annotations": [{
                    "explicit_reference": "EXPLICIT_REFERENCE_ANNOTATION",
                    "annotation_cnf": {
                        "insertion_type": "INSERT_and_ERASE_with_PRIORITY",
                        "name": "NAME",
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
            }]}

        self.engine_eboa.treat_data(data)
        
        annotation_ddbb = self.session.query(Annotation).join(ExplicitRef).join(Source).filter(Annotation.visible == True,
                                                                                               ExplicitRef.explicit_ref == "EXPLICIT_REFERENCE_ANNOTATION",
                                                                                               Source.name == "source2.xml").all()

        assert len(annotation_ddbb) == 1

        annotations_ddbb = self.session.query(Annotation).all()

        assert len(annotations_ddbb) == 1

        values_ddbb = self.query_eboa.get_annotation_values()

        assert len(values_ddbb) == 8
        
    def test_remove_deprecated_event_insert_and_erase_same_period(self):

        self.engine_eboa._initialize_context_insert_data()
        data = {"dim_signature": {"name": "dim_signature",
                                  "exec": "exec",
                                  "version": "1.0"},
                "source": {"name": "source.xml",
                           "reception_time": "2018-06-06T13:33:29",
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
        self.engine_eboa.operation = data
        self.engine_eboa._insert_dim_signature()
        self.engine_eboa._insert_source()
        self.engine_eboa._insert_gauges()
        self.engine_eboa.session.commit()
        self.engine_eboa._insert_explicit_refs()
        self.engine_eboa.session.commit()
        self.engine_eboa._insert_events()
        self.engine_eboa.session.commit()

        self.engine_eboa._initialize_context_insert_data()
        data = {"dim_signature": {"name": "dim_signature",
                                  "exec": "exec",
                                  "version": "1.0"},
                "source": {"name": "source2.xml",
                           "reception_time": "2018-06-06T13:33:29",
                           "generation_time": "2018-07-05T02:07:04",
                           "validity_start": "2018-06-05T02:07:03",
                           "validity_stop": "2018-06-05T08:07:36"},
                "events": [{
                    "explicit_reference": "EXPLICIT_REFERENCE_EVENT2",
                    "gauge": {"name": "GAUGE_NAME",
                              "system": "GAUGE_SYSTEM",
                              "insertion_type": "INSERT_and_ERASE"},
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
        self.engine_eboa.operation = data
        self.engine_eboa._insert_dim_signature()
        self.engine_eboa._insert_source()
        self.engine_eboa._insert_gauges()
        self.engine_eboa.session.commit()
        self.engine_eboa._insert_explicit_refs()
        self.engine_eboa.session.commit()
        self.engine_eboa._insert_events()
        self.engine_eboa.session.commit()
        self.engine_eboa._remove_deprecated_events_by_insert_and_erase_at_event_level()
        self.engine_eboa.session.commit()
        gauge_ddbb = self.session.query(Gauge).filter(Gauge.name == data["events"][0]["gauge"]["name"], Gauge.system == data["events"][0]["gauge"]["system"]).first()
        source_ddbb = self.session.query(Source).filter(Source.name == data["source"]["name"], Source.validity_start == data["source"]["validity_start"], Source.validity_stop == data["source"]["validity_stop"], Source.generation_time == data["source"]["generation_time"], Source.processor_version == data["dim_signature"]["version"], Source.processor == data["dim_signature"]["exec"]).first()
        dim_signature_ddbb = self.session.query(DimSignature).filter(DimSignature.dim_signature == data["dim_signature"]["name"]).first()
        explicit_reference_ddbb = self.session.query(ExplicitRef).filter(ExplicitRef.explicit_ref == data["events"][0]["explicit_reference"]).first()

        filtered_events_ddbb = self.session.query(Event).filter(Event.start == data["events"][0]["start"],
                                                      Event.stop == data["events"][0]["stop"],
                                                      Event.gauge_uuid == gauge_ddbb.gauge_uuid,
                                                      Event.source_uuid == source_ddbb.source_uuid,
                                                      Event.explicit_ref_uuid == explicit_reference_ddbb.explicit_ref_uuid,
                                                      Event.visible == True).all()

        assert len(filtered_events_ddbb) == 1

        events_ddbb = self.session.query(Event).all()

        assert len(events_ddbb) == 1

        values_ddbb = self.query_eboa.get_event_values()

        assert len(values_ddbb) == 8

    def test_remove_deprecated_event_insert_and_erase_at_dim_signature_level_no_events(self):

        data = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                                  "exec": "exec",
                                  "version": "1.0"},
                "source": {"name": "source.xml",
                           "reception_time": "2018-06-06T13:33:29",
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
        }]}
        self.engine_eboa.data = data
        self.engine_eboa.treat_data()

        events_ddbb = self.session.query(Event).all()

        assert len(events_ddbb) == 1

        data = {"operations": [{
            "mode": "insert_and_erase",
            "dim_signature": {"name": "dim_signature",
                                  "exec": "exec",
                                  "version": "1.0"},
                "source": {"name": "source2.xml",
                           "reception_time": "2018-06-06T13:33:29",
                           "generation_time": "2018-07-05T02:07:04",
                           "validity_start": "2018-06-05T02:07:03",
                           "validity_stop": "2018-06-06T08:07:36"}
                }]
            }
        self.engine_eboa.data = data
        self.engine_eboa.treat_data()

        events_ddbb = self.session.query(Event).all()

        assert len(events_ddbb) == 0

    def test_remove_deprecated_event_insert_and_erase_one_starts_before_links(self):

        self.engine_eboa._initialize_context_insert_data()
        data1 = {"dim_signature": {"name": "dim_signature",
                                  "exec": "exec",
                                  "version": "1.0"},
                "source": {"name": "source.xml",
                           "reception_time": "2018-06-06T13:33:29",
                           "generation_time": "2018-07-05T02:07:03",
                           "validity_start": "2018-06-04T02:07:00",
                           "validity_stop": "2018-06-05T08:07:36"},
                "events": [{
                    "link_ref": "EVENT_LINK1",
                    "explicit_reference": "EXPLICIT_REFERENCE_EVENT1",
                    "gauge": {"name": "GAUGE_NAME",
                              "system": "GAUGE_SYSTEM",
                              "insertion_type": "SIMPLE_UPDATE"},
                    "start": "2018-06-04T02:07:00",
                    "stop": "2018-06-05T08:07:36",
                    "links": [{
                        "link": "EVENT_LINK2",
                        "link_mode": "by_ref",
                        "name": "EVENT_LINK_NAME1",
                        "back_ref": "LINK_BACK_REF_NAME"
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
                               "link_ref": "EVENT_LINK2",
                               "explicit_reference": "EXPLICIT_REFERENCE_EVENT2",
                               "gauge": {"name": "GAUGE_NAME",
                                         "system": "GAUGE_SYSTEM",
                                         "insertion_type": "SIMPLE_UPDATE"},
                               "start": "2018-06-04T02:07:02",
                               "stop": "2018-06-05T08:07:36",
                               "links": [{
                                   "link": "EVENT_LINK1",
                                   "link_mode": "by_ref",
                                   "name": "EVENT_LINK_NAME2",
                                   "back_ref": "LINK_BACK_REF_NAME"
                               }]},
                           {
                               "explicit_reference": "EXPLICIT_REFERENCE_EVENT3",
                               "gauge": {"name": "GAUGE_NAME",
                                         "system": "GAUGE_SYSTEM",
                                         "insertion_type": "SIMPLE_UPDATE"},
                               "start": "2018-06-04T02:07:03",
                               "stop": "2018-06-04T04:07:36",
                               "links": [{
                                   "link": "EVENT_LINK1",
                                   "link_mode": "by_ref",
                                   "name": "EVENT_LINK_NAME3",
                                   "back_ref": "LINK_BACK_REF_NAME"
                               }]}]
            }
        self.engine_eboa.operation = data1
        self.engine_eboa._insert_dim_signature()
        self.engine_eboa._insert_source()
        self.engine_eboa._insert_gauges()
        self.engine_eboa.session.commit()
        self.engine_eboa._insert_explicit_refs()
        self.engine_eboa.session.commit()
        self.engine_eboa._insert_events()
        self.engine_eboa.session.commit()

        self.engine_eboa._initialize_context_insert_data()
        data2 = {"dim_signature": {"name": "dim_signature",
                                  "exec": "exec",
                                  "version": "1.0"},
                "source": {"name": "source2.xml",
                           "reception_time": "2018-06-06T13:33:29",
                           "generation_time": "2018-07-05T02:07:04",
                           "validity_start": "2018-06-05T02:07:03",
                           "validity_stop": "2018-06-05T08:07:36"},
                "events": [{
                    "explicit_reference": "EXPLICIT_REFERENCE_EVENT4",
                    "gauge": {"name": "GAUGE_NAME",
                              "system": "GAUGE_SYSTEM",
                              "insertion_type": "INSERT_and_ERASE"},
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
        self.engine_eboa.operation = data2
        self.engine_eboa._insert_dim_signature()
        self.engine_eboa._insert_source()
        self.engine_eboa._insert_gauges()
        self.engine_eboa.session.commit()
        self.engine_eboa._insert_explicit_refs()
        self.engine_eboa.session.commit()
        self.engine_eboa._insert_events()
        self.engine_eboa.session.commit()
        self.engine_eboa._remove_deprecated_events_by_insert_and_erase_at_event_level()
        self.engine_eboa.session.commit()
        gauge_ddbb = self.session.query(Gauge).filter(Gauge.name == data2["events"][0]["gauge"]["name"], Gauge.system == data2["events"][0]["gauge"]["system"]).first()
        source_ddbb = self.session.query(Source).filter(Source.name == data2["source"]["name"], Source.validity_start == data2["source"]["validity_start"], Source.validity_stop == data2["source"]["validity_stop"], Source.generation_time == data2["source"]["generation_time"], Source.processor_version == data2["dim_signature"]["version"], Source.processor == data2["dim_signature"]["exec"]).first()
        dim_signature_ddbb = self.session.query(DimSignature).filter(DimSignature.dim_signature == data2["dim_signature"]["name"]).first()
        explicit_reference_ddbb = self.session.query(ExplicitRef).filter(ExplicitRef.explicit_ref == data2["events"][0]["explicit_reference"]).first()

        filtered_events_ddbb = self.session.query(Event).filter(Event.start == data2["events"][0]["start"],
                                                      Event.stop == data2["events"][0]["stop"],
                                                      Event.gauge_uuid == gauge_ddbb.gauge_uuid,
                                                      Event.source_uuid == source_ddbb.source_uuid,
                                                      Event.explicit_ref_uuid == explicit_reference_ddbb.explicit_ref_uuid,
                                                      Event.visible == True).all()

        assert len(filtered_events_ddbb) == 1

        gauge_ddbb = self.session.query(Gauge).filter(Gauge.name == data1["events"][0]["gauge"]["name"], Gauge.system == data1["events"][0]["gauge"]["system"]).first()
        source_ddbb = self.session.query(Source).filter(Source.name == data1["source"]["name"], Source.validity_start == data1["source"]["validity_start"], Source.validity_stop == data1["source"]["validity_stop"], Source.generation_time == data1["source"]["generation_time"], Source.processor_version == data1["dim_signature"]["version"], Source.processor == data1["dim_signature"]["exec"]).first()
        dim_signature_ddbb = self.session.query(DimSignature).filter(DimSignature.dim_signature == data1["dim_signature"]["name"]).first()

        filtered_events_ddbb = self.session.query(Event).filter(Event.start == data1["events"][0]["start"],
                                                      Event.stop == data2["source"]["validity_start"],
                                                      Event.gauge_uuid == gauge_ddbb.gauge_uuid,
                                                      Event.source_uuid == source_ddbb.source_uuid,
                                                      Event.visible == True).all()

        assert len(filtered_events_ddbb) == 1

        filtered_events_ddbb = self.session.query(Event).filter(Event.start == data1["events"][1]["start"],
                                                      Event.stop == data2["source"]["validity_start"],
                                                      Event.gauge_uuid == gauge_ddbb.gauge_uuid,
                                                      Event.source_uuid == source_ddbb.source_uuid,
                                                      Event.visible == True).all()

        assert len(filtered_events_ddbb) == 1

        filtered_events_ddbb = self.session.query(Event).filter(Event.start == data1["events"][2]["start"],
                                                      Event.stop == data1["events"][2]["stop"],
                                                      Event.gauge_uuid == gauge_ddbb.gauge_uuid,
                                                      Event.source_uuid == source_ddbb.source_uuid,
                                                      Event.visible == True).all()

        assert len(filtered_events_ddbb) == 1

        events_ddbb = self.session.query(Event).all()

        assert len(events_ddbb) == 4

        values_ddbb = self.query_eboa.get_event_values()

        assert len(values_ddbb) == 16

        links_ddbb = self.query_eboa.get_event_links()

        assert len(links_ddbb) == 6

        events_with_links = self.session.query(Event).join(Source).filter(Source.name == "source.xml").order_by(Event.start).all()
        
        rest_of_event_uuids = [events_with_links[1].event_uuid, events_with_links[2].event_uuid]
        
        event_links_ddbb = self.query_eboa.get_event_links(event_uuids = {"filter": rest_of_event_uuids, "op": "in"}, event_uuid_links = {"filter": [events_with_links[0].event_uuid], "op": "in"})

        assert len(event_links_ddbb) == 3

        rest_of_event_uuids = [events_with_links[0].event_uuid, events_with_links[2].event_uuid]
        
        event_links_ddbb = self.query_eboa.get_event_links(event_uuids = {"filter": rest_of_event_uuids, "op": "in"}, event_uuid_links = {"filter": [events_with_links[1].event_uuid], "op": "in"})

        assert len(event_links_ddbb) == 2

        rest_of_event_uuids = [events_with_links[0].event_uuid, events_with_links[1].event_uuid]
        
        event_links_ddbb = self.query_eboa.get_event_links(event_uuids = {"filter": rest_of_event_uuids, "op": "in"}, event_uuid_links = {"filter": [events_with_links[2].event_uuid], "op": "in"})

        assert len(event_links_ddbb) == 1

    def test_remove_deprecated_event_insert_and_erase_only_remains_last_inserted_source(self):

        data1 = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                                  "exec": "exec",
                                  "version": "1.0"},
                "source": {"name": "source.xml",
                           "reception_time": "2018-06-06T13:33:29",
                           "generation_time": "2018-07-05T02:07:03",
                           "validity_start": "2018-06-04T02:07:04",
                           "validity_stop": "2018-06-05T08:07:36"},
                "events": [{
                               "explicit_reference": "EXPLICIT_REFERENCE_EVENT3",
                               "gauge": {"name": "GAUGE_NAME",
                                         "system": "GAUGE_SYSTEM",
                                         "insertion_type": "SIMPLE_UPDATE"},
                               "start": "2018-06-04T02:07:04",
                               "stop": "2018-06-04T04:07:36",
                               "links": [{
                                   "link": "EVENT_LINK1",
                                   "link_mode": "by_ref",
                                   "name": "EVENT_LINK_NAME4",
                                   "back_ref": "LINK_BACK_REF_NAME"
                               }]},
                           {
                    "link_ref": "EVENT_LINK1",
                    "explicit_reference": "EXPLICIT_REFERENCE_EVENT1",
                    "gauge": {"name": "GAUGE_NAME",
                              "system": "GAUGE_SYSTEM",
                              "insertion_type": "SIMPLE_UPDATE"},
                    "start": "2018-06-05T02:07:04",
                    "stop": "2018-06-05T08:07:36",
                    "links": [{
                        "link": "EVENT_LINK2",
                        "link_mode": "by_ref",
                        "name": "EVENT_LINK_NAME1",
                        "back_ref": "LINK_BACK_REF_NAME"
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
                               "link_ref": "EVENT_LINK2",
                               "explicit_reference": "EXPLICIT_REFERENCE_EVENT2",
                               "gauge": {"name": "GAUGE_NAME",
                                         "system": "GAUGE_SYSTEM",
                                         "insertion_type": "SIMPLE_UPDATE"},
                               "start": "2018-06-05T02:07:04",
                               "stop": "2018-06-05T08:07:36",
                               "links": [{
                                   "link": "EVENT_LINK1",
                                   "link_mode": "by_ref",
                                   "name": "EVENT_LINK_NAME2",
                                   "back_ref": "LINK_BACK_REF_NAME"
                               }]},
                           {
                               "explicit_reference": "EXPLICIT_REFERENCE_EVENT3",
                               "gauge": {"name": "GAUGE_NAME",
                                         "system": "GAUGE_SYSTEM",
                                         "insertion_type": "SIMPLE_UPDATE"},
                               "start": "2018-06-05T02:07:04",
                               "stop": "2018-06-05T04:07:36",
                               "links": [{
                                   "link": "EVENT_LINK1",
                                   "link_mode": "by_ref",
                                   "name": "EVENT_LINK_NAME3",
                                   "back_ref": "LINK_BACK_REF_NAME"
                               }]}]
            }]}

        self.engine_eboa.treat_data(data1)

        data2 = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                                  "exec": "exec",
                                  "version": "1.0"},
                "source": {"name": "source2.xml",
                           "reception_time": "2018-06-06T13:33:29",
                           "generation_time": "2018-07-05T02:07:04",
                           "validity_start": "2018-06-05T02:07:03",
                           "validity_stop": "2018-06-05T08:07:36"},
                "events": [{
                    "explicit_reference": "EXPLICIT_REFERENCE_EVENT4",
                    "gauge": {"name": "GAUGE_NAME",
                              "system": "GAUGE_SYSTEM",
                              "insertion_type": "INSERT_and_ERASE"},
                    "start": "2018-06-05T02:07:03",
                    "stop": "2018-06-05T08:07:36"
                }]
            }]}
        self.engine_eboa.treat_data(data2)
        
        events = self.query_eboa.get_events()

        assert len(events) == 2

        event_links = self.session.query(EventLink).all()

        assert len(event_links) == 0

        event_values = self.query_eboa.get_event_values()

        assert len(event_values) == 0

    def test_remove_deprecated_event_insert_and_erase_long_event_cut(self):

        data1 = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                                  "exec": "exec",
                                  "version": "1.0"},
                "source": {"name": "source.json",
                           "reception_time": "2018-06-06T13:33:29",
                           "generation_time": "2018-07-06T02:07:03",
                           "validity_start": "2018-06-05T03:07:03",
                           "validity_stop": "2018-06-05T04:07:36"},
                "events": [{
                               "explicit_reference": "EXPLICIT_REFERENCE_EVENT3",
                               "gauge": {"name": "GAUGE_NAME",
                                         "system": "GAUGE_SYSTEM",
                                         "insertion_type": "SIMPLE_UPDATE"},
                               "start": "2018-06-05T03:07:03",
                               "stop": "2018-06-05T04:07:36"
                }]
            }]}

        exit_status = self.engine_eboa.treat_data(data1)

        assert len([item for item in exit_status if item["status"] != eboa_engine.exit_codes["OK"]["status"]]) == 0

        data3 = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                                  "exec": "exec",
                                  "version": "1.0"},
                "source": {"name": "source3.json",
                           "reception_time": "2018-06-06T13:33:29",
                           "generation_time": "2018-07-04T02:07:03",
                           "validity_start": "2018-06-05T06:07:03",
                           "validity_stop": "2018-06-05T07:07:36"},
                "events": [{
                               "explicit_reference": "EXPLICIT_REFERENCE_EVENT3",
                               "gauge": {"name": "GAUGE_NAME",
                                         "system": "GAUGE_SYSTEM",
                                         "insertion_type": "SIMPLE_UPDATE"},
                               "start": "2018-06-05T06:07:03",
                               "stop": "2018-06-05T07:07:36"
                }]
            }]}

        exit_status = self.engine_eboa.treat_data(data3)

        assert len([item for item in exit_status if item["status"] != eboa_engine.exit_codes["OK"]["status"]]) == 0

        data2 = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                                  "exec": "exec",
                                  "version": "1.0"},
                "source": {"name": "source2.json",
                           "reception_time": "2018-06-06T13:33:29",
                           "generation_time": "2018-07-05T02:07:04",
                           "validity_start": "2018-06-05T02:07:03",
                           "validity_stop": "2018-06-05T08:07:36"},
                "events": [{
                    "explicit_reference": "EXPLICIT_REFERENCE_EVENT4",
                    "gauge": {"name": "GAUGE_NAME",
                              "system": "GAUGE_SYSTEM",
                              "insertion_type": "INSERT_and_ERASE"},
                    "start": "2018-06-05T02:07:03",
                    "stop": "2018-06-05T08:07:36"
                }]
            }]}

        exit_status = self.engine_eboa.treat_data(data2)

        assert len([item for item in exit_status if item["status"] != eboa_engine.exit_codes["OK"]["status"]]) == 0
        
        events = self.session.query(Event).join(Gauge).join(Source).filter(Gauge.name == "GAUGE_NAME", 
                                                                           Gauge.system == "GAUGE_SYSTEM",
                                                                           Event.start == "2018-06-05T3:07:03",
                                                                           Event.stop == "2018-06-05T04:07:36",
                                                                           Source.name == "source.json").all()
        assert len(events) == 1

        events = self.session.query(Event).join(Gauge).join(Source).filter(Gauge.name == "GAUGE_NAME", 
                                                                           Gauge.system == "GAUGE_SYSTEM",
                                                                           Event.start == "2018-06-05T2:07:03",
                                                                           Event.stop == "2018-06-05T03:07:03",
                                                                           Source.name == "source2.json").all()
        assert len(events) == 1

        events = self.session.query(Event).join(Gauge).join(Source).filter(Gauge.name == "GAUGE_NAME", 
                                                                           Gauge.system == "GAUGE_SYSTEM",
                                                                           Event.start == "2018-06-05T4:07:36",
                                                                           Event.stop == "2018-06-05T08:07:36",
                                                                           Source.name == "source2.json").all()
        assert len(events) == 1

        events = self.session.query(Event).all()

        assert len(events) == 3

    def test_remove_deprecated_event_insert_and_erase_one_starts_after(self):

        self.engine_eboa._initialize_context_insert_data()
        data1 = {"dim_signature": {"name": "dim_signature",
                                  "exec": "exec",
                                  "version": "1.0"},
                "source": {"name": "source.xml",
                           "reception_time": "2018-06-06T13:33:29",
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
        self.engine_eboa.operation = data1
        self.engine_eboa._insert_dim_signature()
        self.engine_eboa._insert_source()
        self.engine_eboa._insert_gauges()
        self.engine_eboa.session.commit()
        self.engine_eboa._insert_explicit_refs()
        self.engine_eboa.session.commit()
        self.engine_eboa._insert_events()
        self.engine_eboa.session.commit()

        self.engine_eboa._initialize_context_insert_data()
        data2 = {"dim_signature": {"name": "dim_signature",
                                  "exec": "exec",
                                  "version": "1.0"},
                "source": {"name": "source2.xml",
                           "reception_time": "2018-06-06T13:33:29",
                           "generation_time": "2018-07-05T02:07:04",
                           "validity_start": "2018-06-05T02:07:03",
                           "validity_stop": "2018-06-05T08:07:36"},
                "events": [{
                    "explicit_reference": "EXPLICIT_REFERENCE_EVENT2",
                    "gauge": {"name": "GAUGE_NAME",
                              "system": "GAUGE_SYSTEM",
                              "insertion_type": "INSERT_and_ERASE"},
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
        self.engine_eboa.operation = data2
        self.engine_eboa._insert_dim_signature()
        self.engine_eboa._insert_source()
        self.engine_eboa._insert_gauges()
        self.engine_eboa.session.commit()
        self.engine_eboa._insert_explicit_refs()
        self.engine_eboa.session.commit()
        self.engine_eboa._insert_events()
        self.engine_eboa.session.commit()
        self.engine_eboa._remove_deprecated_events_by_insert_and_erase_at_event_level()
        self.engine_eboa.session.commit()
        gauge_ddbb = self.session.query(Gauge).filter(Gauge.name == data2["events"][0]["gauge"]["name"], Gauge.system == data2["events"][0]["gauge"]["system"]).first()
        source_ddbb = self.session.query(Source).filter(Source.name == data2["source"]["name"], Source.validity_start == data2["source"]["validity_start"], Source.validity_stop == data2["source"]["validity_stop"], Source.generation_time == data2["source"]["generation_time"], Source.processor_version == data2["dim_signature"]["version"], Source.processor == data2["dim_signature"]["exec"]).first()
        dim_signature_ddbb = self.session.query(DimSignature).filter(DimSignature.dim_signature == data2["dim_signature"]["name"]).first()
        explicit_reference_ddbb = self.session.query(ExplicitRef).filter(ExplicitRef.explicit_ref == data2["events"][0]["explicit_reference"]).first()

        filtered_events_ddbb = self.session.query(Event).filter(Event.start == data2["events"][0]["start"],
                                                      Event.stop == data2["events"][0]["stop"],
                                                      Event.gauge_uuid == gauge_ddbb.gauge_uuid,
                                                      Event.source_uuid == source_ddbb.source_uuid,
                                                      Event.explicit_ref_uuid == explicit_reference_ddbb.explicit_ref_uuid,
                                                      Event.visible == True).all()

        assert len(filtered_events_ddbb) == 1

        gauge_ddbb = self.session.query(Gauge).filter(Gauge.name == data1["events"][0]["gauge"]["name"], Gauge.system == data1["events"][0]["gauge"]["system"]).first()
        source_ddbb = self.session.query(Source).filter(Source.name == data1["source"]["name"], Source.validity_start == data1["source"]["validity_start"], Source.validity_stop == data1["source"]["validity_stop"], Source.generation_time == data1["source"]["generation_time"], Source.processor_version == data1["dim_signature"]["version"], Source.processor == data1["dim_signature"]["exec"]).first()
        dim_signature_ddbb = self.session.query(DimSignature).filter(DimSignature.dim_signature == data1["dim_signature"]["name"]).first()
        explicit_reference_ddbb = self.session.query(ExplicitRef).filter(ExplicitRef.explicit_ref == data1["events"][0]["explicit_reference"]).first()

        filtered_events_ddbb = self.session.query(Event).filter(Event.start == data2["events"][0]["stop"],
                                                      Event.stop == data1["events"][0]["stop"],
                                                      Event.gauge_uuid == gauge_ddbb.gauge_uuid,
                                                      Event.source_uuid == source_ddbb.source_uuid,
                                                      Event.explicit_ref_uuid == explicit_reference_ddbb.explicit_ref_uuid,
                                                      Event.visible == True).all()

        assert len(filtered_events_ddbb) == 1

        events_ddbb = self.session.query(Event).all()

        assert len(events_ddbb) == 2

        values_ddbb = self.query_eboa.get_event_values()

        assert len(values_ddbb) == 16


    def test_remove_deprecated_event_insert_and_erase_events_not_overlapping(self):

        self.engine_eboa._initialize_context_insert_data()
        data1 = {"dim_signature": {"name": "dim_signature",
                                   "exec": "exec",
                                   "version": "1.0"},
                 "source": {"name": "source2.xml",
                            "reception_time": "2018-06-06T13:33:29",
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
        self.engine_eboa.operation = data1
        self.engine_eboa._insert_dim_signature()
        self.engine_eboa._insert_source()
        self.engine_eboa._insert_gauges()
        self.engine_eboa.session.commit()
        self.engine_eboa._insert_explicit_refs()
        self.engine_eboa.session.commit()
        self.engine_eboa._insert_events()
        self.engine_eboa.session.commit()

        data2 = {"dim_signature": {"name": "dim_signature",
                                   "exec": "exec",
                                   "version": "1.0"},
                 "source": {"name": "source.xml",
                            "reception_time": "2018-06-06T13:33:29",
                            "generation_time": "2018-07-05T02:07:04",
                            "validity_start": "2018-06-04T02:07:03",
                            "validity_stop": "2018-06-06T08:07:36"},
                 "events": [{
                     "explicit_reference": "EXPLICIT_REFERENCE_EVENT1",
                     "gauge": {"name": "GAUGE_NAME",
                               "system": "GAUGE_SYSTEM",
                               "insertion_type": "INSERT_and_ERASE"},
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
        self.engine_eboa._initialize_context_insert_data()
        self.engine_eboa.operation = data2
        self.engine_eboa._insert_dim_signature()
        self.engine_eboa._insert_source()
        self.engine_eboa._insert_gauges()
        self.engine_eboa.session.commit()
        self.engine_eboa._insert_explicit_refs()
        self.engine_eboa.session.commit()
        self.engine_eboa._insert_events()
        self.engine_eboa.session.commit()
        self.engine_eboa._remove_deprecated_events_by_insert_and_erase_at_event_level()
        self.engine_eboa.session.commit()
        gauge_ddbb = self.session.query(Gauge).filter(Gauge.name == data2["events"][0]["gauge"]["name"], Gauge.system == data2["events"][0]["gauge"]["system"]).first()
        source_ddbb = self.session.query(Source).filter(Source.name == data2["source"]["name"], Source.validity_start == data2["source"]["validity_start"], Source.validity_stop == data2["source"]["validity_stop"], Source.generation_time == data2["source"]["generation_time"], Source.processor_version == data2["dim_signature"]["version"], Source.processor == data2["dim_signature"]["exec"]).first()
        dim_signature_ddbb = self.session.query(DimSignature).filter(DimSignature.dim_signature == data2["dim_signature"]["name"]).first()
        explicit_reference_ddbb = self.session.query(ExplicitRef).filter(ExplicitRef.explicit_ref == data2["events"][0]["explicit_reference"]).first()

        filtered_events_ddbb = self.session.query(Event).filter(Event.start == data2["events"][0]["start"],
                                                      Event.stop == data2["events"][0]["stop"],
                                                      Event.gauge_uuid == gauge_ddbb.gauge_uuid,
                                                      Event.source_uuid == source_ddbb.source_uuid,
                                                      Event.explicit_ref_uuid == explicit_reference_ddbb.explicit_ref_uuid,
                                                      Event.visible == True).all()

        assert len(filtered_events_ddbb) == 1

        events_ddbb = self.session.query(Event).all()

        assert len(events_ddbb) == 1

        values_ddbb = self.query_eboa.get_event_values()

        assert len(values_ddbb) == 8

    def test_remove_deprecated_event_insert_and_erase_split_events(self):

        self.engine_eboa._initialize_context_insert_data()
        data1 = {"dim_signature": {"name": "dim_signature",
                                   "exec": "exec",
                                   "version": "1.0"},
                 "source": {"name": "source1.xml",
                            "reception_time": "2018-06-06T13:33:29",
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
        self.engine_eboa.operation = data1
        self.engine_eboa._insert_dim_signature()
        self.engine_eboa._insert_source()
        self.engine_eboa._insert_gauges()
        self.engine_eboa.session.commit()
        self.engine_eboa._insert_explicit_refs()
        self.engine_eboa.session.commit()
        self.engine_eboa._insert_events()
        self.engine_eboa.session.commit()

        self.engine_eboa._initialize_context_insert_data()
        data2 = {"dim_signature": {"name": "dim_signature",
                                   "exec": "exec",
                                   "version": "1.0"},
                 "source": {"name": "source2.xml",
                            "reception_time": "2018-06-06T13:33:29",
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
        self.engine_eboa.operation = data2
        self.engine_eboa._insert_dim_signature()
        self.engine_eboa._insert_source()
        self.engine_eboa._insert_gauges()
        self.engine_eboa.session.commit()
        self.engine_eboa._insert_explicit_refs()
        self.engine_eboa.session.commit()
        self.engine_eboa._insert_events()
        self.engine_eboa.session.commit()

        data3 = {"dim_signature": {"name": "dim_signature",
                                   "exec": "exec",
                                   "version": "1.0"},
                 "source": {"name": "source3.xml",
                            "reception_time": "2018-06-06T13:33:29",
                            "generation_time": "2018-07-05T02:07:04",
                            "validity_start": "2018-06-04T02:07:03",
                            "validity_stop": "2018-06-06T08:07:36"},
                 "events": [{
                     "explicit_reference": "EXPLICIT_REFERENCE_EVENT1",
                     "gauge": {"name": "GAUGE_NAME",
                               "system": "GAUGE_SYSTEM",
                               "insertion_type": "INSERT_and_ERASE"},
                     "start": "2018-06-04T05:07:03",
                     "stop": "2018-06-06T07:07:36"
                }]
            }
        self.engine_eboa._initialize_context_insert_data()
        self.engine_eboa.operation = data3
        self.engine_eboa._insert_dim_signature()
        self.engine_eboa._insert_source()
        self.engine_eboa._insert_gauges()
        self.engine_eboa.session.commit()
        self.engine_eboa._insert_explicit_refs()
        self.engine_eboa.session.commit()
        self.engine_eboa._insert_events()
        self.engine_eboa.session.commit()
        self.engine_eboa._remove_deprecated_events_by_insert_and_erase_at_event_level()
        self.engine_eboa.session.commit()
        gauge_ddbb = self.session.query(Gauge).filter(Gauge.name == data1["events"][0]["gauge"]["name"], Gauge.system == data1["events"][0]["gauge"]["system"]).first()
        source_ddbb = self.session.query(Source).filter(Source.name == data1["source"]["name"], Source.validity_start == data1["source"]["validity_start"], Source.validity_stop == data1["source"]["validity_stop"], Source.generation_time == data1["source"]["generation_time"], Source.processor_version == data1["dim_signature"]["version"], Source.processor == data1["dim_signature"]["exec"]).first()
        dim_signature_ddbb = self.session.query(DimSignature).filter(DimSignature.dim_signature == data1["dim_signature"]["name"]).first()
        explicit_reference_ddbb = self.session.query(ExplicitRef).filter(ExplicitRef.explicit_ref == data1["events"][0]["explicit_reference"]).first()

        filtered_events_ddbb = self.session.query(Event).filter(Event.start == data1["events"][0]["start"],
                                                      Event.stop == data1["events"][0]["stop"],
                                                      Event.gauge_uuid == gauge_ddbb.gauge_uuid,
                                                      Event.source_uuid == source_ddbb.source_uuid,
                                                      Event.explicit_ref_uuid == explicit_reference_ddbb.explicit_ref_uuid,
                                                      Event.visible == True).all()

        assert len(filtered_events_ddbb) == 1

        gauge_ddbb = self.session.query(Gauge).filter(Gauge.name == data2["events"][0]["gauge"]["name"], Gauge.system == data2["events"][0]["gauge"]["system"]).first()
        source_ddbb = self.session.query(Source).filter(Source.name == data2["source"]["name"], Source.validity_start == data2["source"]["validity_start"], Source.validity_stop == data2["source"]["validity_stop"], Source.generation_time == data2["source"]["generation_time"], Source.processor_version == data2["dim_signature"]["version"], Source.processor == data2["dim_signature"]["exec"]).first()
        dim_signature_ddbb = self.session.query(DimSignature).filter(DimSignature.dim_signature == data2["dim_signature"]["name"]).first()
        explicit_reference_ddbb = self.session.query(ExplicitRef).filter(ExplicitRef.explicit_ref == data2["events"][0]["explicit_reference"]).first()

        filtered_events_ddbb = self.session.query(Event).filter(Event.start == data2["events"][0]["start"],
                                                      Event.stop == data2["events"][0]["stop"],
                                                      Event.gauge_uuid == gauge_ddbb.gauge_uuid,
                                                      Event.source_uuid == source_ddbb.source_uuid,
                                                      Event.explicit_ref_uuid == explicit_reference_ddbb.explicit_ref_uuid,
                                                      Event.visible == True).all()

        assert len(filtered_events_ddbb) == 1

        gauge_ddbb = self.session.query(Gauge).filter(Gauge.name == data3["events"][0]["gauge"]["name"], Gauge.system == data3["events"][0]["gauge"]["system"]).first()
        source_ddbb = self.session.query(Source).filter(Source.name == data3["source"]["name"], Source.validity_start == data3["source"]["validity_start"], Source.validity_stop == data3["source"]["validity_stop"], Source.generation_time == data3["source"]["generation_time"], Source.processor_version == data3["dim_signature"]["version"], Source.processor == data3["dim_signature"]["exec"]).first()
        dim_signature_ddbb = self.session.query(DimSignature).filter(DimSignature.dim_signature == data3["dim_signature"]["name"]).first()
        explicit_reference_ddbb = self.session.query(ExplicitRef).filter(ExplicitRef.explicit_ref == data3["events"][0]["explicit_reference"]).first()

        filtered_events_ddbb = self.session.query(Event).filter(Event.start == data3["events"][0]["start"],
                                                      Event.stop == data1["source"]["validity_start"],
                                                      Event.gauge_uuid == gauge_ddbb.gauge_uuid,
                                                      Event.source_uuid == source_ddbb.source_uuid,
                                                      Event.explicit_ref_uuid == explicit_reference_ddbb.explicit_ref_uuid,
                                                      Event.visible == True).all()

        assert len(filtered_events_ddbb) == 1

        gauge_ddbb = self.session.query(Gauge).filter(Gauge.name == data3["events"][0]["gauge"]["name"], Gauge.system == data3["events"][0]["gauge"]["system"]).first()
        source_ddbb = self.session.query(Source).filter(Source.name == data3["source"]["name"], Source.validity_start == data3["source"]["validity_start"], Source.validity_stop == data3["source"]["validity_stop"], Source.generation_time == data3["source"]["generation_time"], Source.processor_version == data3["dim_signature"]["version"], Source.processor == data3["dim_signature"]["exec"]).first()
        dim_signature_ddbb = self.session.query(DimSignature).filter(DimSignature.dim_signature == data3["dim_signature"]["name"]).first()
        explicit_reference_ddbb = self.session.query(ExplicitRef).filter(ExplicitRef.explicit_ref == data3["events"][0]["explicit_reference"]).first()

        filtered_events_ddbb = self.session.query(Event).filter(Event.start == data1["source"]["validity_stop"],
                                                      Event.stop == data2["source"]["validity_start"],
                                                      Event.gauge_uuid == gauge_ddbb.gauge_uuid,
                                                      Event.source_uuid == source_ddbb.source_uuid,
                                                      Event.explicit_ref_uuid == explicit_reference_ddbb.explicit_ref_uuid,
                                                      Event.visible == True).all()

        assert len(filtered_events_ddbb) == 1

        filtered_events_ddbb = self.session.query(Event).filter(Event.start == data2["source"]["validity_stop"],
                                                      Event.stop == data3["events"][0]["stop"],
                                                      Event.gauge_uuid == gauge_ddbb.gauge_uuid,
                                                      Event.source_uuid == source_ddbb.source_uuid,
                                                      Event.explicit_ref_uuid == explicit_reference_ddbb.explicit_ref_uuid,
                                                      Event.visible == True).all()

        assert len(filtered_events_ddbb) == 1

        events_ddbb = self.session.query(Event).all()

        assert len(events_ddbb) == 5

    def test_remove_deprecated_event_insert_and_erase_split_events_not_created(self):

        self.engine_eboa._initialize_context_insert_data()
        data1 = {"dim_signature": {"name": "dim_signature",
                                   "exec": "exec",
                                   "version": "1.0"},
                 "source": {"name": "source1.xml",
                            "reception_time": "2018-06-06T13:33:29",
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
        self.engine_eboa.operation = data1
        self.engine_eboa._insert_dim_signature()
        self.engine_eboa._insert_source()
        self.engine_eboa._insert_gauges()
        self.engine_eboa.session.commit()
        self.engine_eboa._insert_explicit_refs()
        self.engine_eboa.session.commit()
        self.engine_eboa._insert_events()
        self.engine_eboa.session.commit()

        self.engine_eboa._initialize_context_insert_data()
        data2 = {"dim_signature": {"name": "dim_signature",
                                   "exec": "exec",
                                   "version": "1.0"},
                 "source": {"name": "source2.xml",
                            "reception_time": "2018-06-06T13:33:29",
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
        self.engine_eboa.operation = data2
        self.engine_eboa._insert_dim_signature()
        self.engine_eboa._insert_source()
        self.engine_eboa._insert_gauges()
        self.engine_eboa.session.commit()
        self.engine_eboa._insert_explicit_refs()
        self.engine_eboa.session.commit()
        self.engine_eboa._insert_events()
        self.engine_eboa.session.commit()

        data3 = {"dim_signature": {"name": "dim_signature",
                                   "exec": "exec",
                                   "version": "1.0"},
                 "source": {"name": "source3.xml",
                            "reception_time": "2018-06-06T13:33:29",
                            "generation_time": "2018-07-05T02:07:04",
                            "validity_start": "2018-06-04T02:07:03",
                            "validity_stop": "2018-06-06T08:07:36"},
                 "events": [{
                     "explicit_reference": "EXPLICIT_REFERENCE_EVENT1",
                     "gauge": {"name": "GAUGE_NAME",
                               "system": "GAUGE_SYSTEM",
                               "insertion_type": "INSERT_and_ERASE"},
                     "start": "2018-06-04T05:07:03",
                     "stop": "2018-06-05T09:07:36"
                }]
            }
        self.engine_eboa._initialize_context_insert_data()
        self.engine_eboa.operation = data3
        self.engine_eboa._insert_dim_signature()
        self.engine_eboa._insert_source()
        self.engine_eboa._insert_gauges()
        self.engine_eboa.session.commit()
        self.engine_eboa._insert_explicit_refs()
        self.engine_eboa.session.commit()
        self.engine_eboa._insert_events()
        self.engine_eboa.session.commit()
        self.engine_eboa._remove_deprecated_events_by_insert_and_erase_at_event_level()
        self.engine_eboa.session.commit()
        gauge_ddbb = self.session.query(Gauge).filter(Gauge.name == data1["events"][0]["gauge"]["name"], Gauge.system == data1["events"][0]["gauge"]["system"]).first()
        source_ddbb = self.session.query(Source).filter(Source.name == data1["source"]["name"], Source.validity_start == data1["source"]["validity_start"], Source.validity_stop == data1["source"]["validity_stop"], Source.generation_time == data1["source"]["generation_time"], Source.processor_version == data1["dim_signature"]["version"], Source.processor == data1["dim_signature"]["exec"]).first()
        dim_signature_ddbb = self.session.query(DimSignature).filter(DimSignature.dim_signature == data1["dim_signature"]["name"]).first()
        explicit_reference_ddbb = self.session.query(ExplicitRef).filter(ExplicitRef.explicit_ref == data1["events"][0]["explicit_reference"]).first()

        filtered_events_ddbb = self.session.query(Event).filter(Event.start == data1["events"][0]["start"],
                                                      Event.stop == data1["events"][0]["stop"],
                                                      Event.gauge_uuid == gauge_ddbb.gauge_uuid,
                                                      Event.source_uuid == source_ddbb.source_uuid,
                                                      Event.explicit_ref_uuid == explicit_reference_ddbb.explicit_ref_uuid,
                                                      Event.visible == True).all()

        assert len(filtered_events_ddbb) == 1

        gauge_ddbb = self.session.query(Gauge).filter(Gauge.name == data2["events"][0]["gauge"]["name"], Gauge.system == data2["events"][0]["gauge"]["system"]).first()
        source_ddbb = self.session.query(Source).filter(Source.name == data2["source"]["name"], Source.validity_start == data2["source"]["validity_start"], Source.validity_stop == data2["source"]["validity_stop"], Source.generation_time == data2["source"]["generation_time"], Source.processor_version == data2["dim_signature"]["version"], Source.processor == data2["dim_signature"]["exec"]).first()
        dim_signature_ddbb = self.session.query(DimSignature).filter(DimSignature.dim_signature == data2["dim_signature"]["name"]).first()
        explicit_reference_ddbb = self.session.query(ExplicitRef).filter(ExplicitRef.explicit_ref == data2["events"][0]["explicit_reference"]).first()

        filtered_events_ddbb = self.session.query(Event).filter(Event.start == data2["events"][0]["start"],
                                                      Event.stop == data2["events"][0]["stop"],
                                                      Event.gauge_uuid == gauge_ddbb.gauge_uuid,
                                                      Event.source_uuid == source_ddbb.source_uuid,
                                                      Event.explicit_ref_uuid == explicit_reference_ddbb.explicit_ref_uuid,
                                                      Event.visible == True).all()

        assert len(filtered_events_ddbb) == 1

        gauge_ddbb = self.session.query(Gauge).filter(Gauge.name == data3["events"][0]["gauge"]["name"], Gauge.system == data3["events"][0]["gauge"]["system"]).first()
        source_ddbb = self.session.query(Source).filter(Source.name == data3["source"]["name"], Source.validity_start == data3["source"]["validity_start"], Source.validity_stop == data3["source"]["validity_stop"], Source.generation_time == data3["source"]["generation_time"], Source.processor_version == data3["dim_signature"]["version"], Source.processor == data3["dim_signature"]["exec"]).first()
        dim_signature_ddbb = self.session.query(DimSignature).filter(DimSignature.dim_signature == data3["dim_signature"]["name"]).first()
        explicit_reference_ddbb = self.session.query(ExplicitRef).filter(ExplicitRef.explicit_ref == data3["events"][0]["explicit_reference"]).first()

        filtered_events_ddbb = self.session.query(Event).filter(Event.start == data3["events"][0]["start"],
                                                      Event.stop == data1["source"]["validity_start"],
                                                      Event.gauge_uuid == gauge_ddbb.gauge_uuid,
                                                      Event.source_uuid == source_ddbb.source_uuid,
                                                      Event.explicit_ref_uuid == explicit_reference_ddbb.explicit_ref_uuid,
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
                       "reception_time": "2018-06-06T13:33:29",
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

        returned_value = self.engine_eboa.treat_data(data)[0]["status"]

        assert returned_value == eboa_engine.exit_codes["OK"]["status"]

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
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2018-07-05T02:07:03",
                       "validity_start": "2018-06-05T02:07:03",
                       "validity_stop": "2018-06-05T08:07:36"}
        }]}

        self.engine_eboa.data = data
        self.engine_eboa.treat_data()

        data = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source.xml",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2018-07-05T02:07:03",
                       "validity_start": "2018-06-05T02:07:03",
                       "validity_stop": "2018-06-05T08:07:36"}
        }]}

        self.engine_eboa.data = data
        returned_value = self.engine_eboa.treat_data()[0]["status"]

        assert returned_value == eboa_engine.exit_codes["SOURCE_ALREADY_INGESTED"]["status"]

        sources_status = self.session.query(SourceStatus).join(Source).filter(SourceStatus.status == eboa_engine.exit_codes["SOURCE_ALREADY_INGESTED"]["status"],
                                                                           Source.name == data["operations"][0]["source"]["name"]).all()

        assert len(sources_status) == 1

        sources_status = self.session.query(SourceStatus).join(Source).filter(SourceStatus.status == eboa_engine.exit_codes["OK"]["status"],
                                                                           Source.name == data["operations"][0]["source"]["name"]).all()

        assert len(sources_status) == 1

    def test_treat_data_insert_and_erase_at_dim_signature_level_same_dim_signature_diferent_processors(self):

        data = {"operations": [{
            "mode": "insert_and_erase",
            "dim_signature": {"name": "dim_signature",
                              "exec": "processor1",
                              "version": "1.0"},
            "source": {"name": "source.xml",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2018-07-05T02:07:03",
                       "validity_start": "2018-06-05T02:07:03",
                       "validity_stop": "2018-06-05T08:07:36"},
            "events": [{
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "INSERT_and_ERASE"},
                "start": "2018-06-05T02:07:03",
                "stop": "2018-06-05T08:07:36"
            }],

        }]}

        self.engine_eboa.data = data
        self.engine_eboa.treat_data()

        data = {"operations": [{
            "mode": "insert_and_erase",
            "dim_signature": {"name": "dim_signature",
                              "exec": "processor2",
                              "version": "1.0"},
            "source": {"name": "source.xml",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2018-07-05T02:07:03",
                       "validity_start": "2018-06-05T02:07:03",
                       "validity_stop": "2018-06-05T08:07:36"},
            "events": [{
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "INSERT_and_ERASE"},
                "start": "2018-06-05T02:07:03",
                "stop": "2018-06-05T08:07:36"
            }],

        }]}

        self.engine_eboa.data = data
        returned_value = self.engine_eboa.treat_data()[0]["status"]

        assert returned_value == eboa_engine.exit_codes["OK"]["status"]

        events = self.session.query(Event).all()

        assert len(events) == 1

    def test_insert_and_erase_at_dim_signature_level_with_sources_with_duration_0(self):

        data = {"operations": [{
            "mode": "insert_and_erase",
            "dim_signature": {"name": "dim_signature",
                              "exec": "processor1",
                              "version": "1.0"},
            "source": {"name": "source.xml",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2018-07-05T02:07:03",
                       "validity_start": "2018-06-05T02:07:03",
                       "validity_stop": "2018-06-05T08:07:36"},
            "events": [{
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "SIMPLE_UPDATE"},
                "start": "2018-06-05T02:07:03",
                "stop": "2018-06-05T08:07:36"
            }],

        }]}

        self.engine_eboa.data = data
        returned_value = self.engine_eboa.treat_data()[0]["status"]
        
        assert returned_value == eboa_engine.exit_codes["OK"]["status"]

        data = {"operations": [{
            "mode": "insert_and_erase",
            "dim_signature": {"name": "dim_signature",
                              "exec": "processor3",
                              "version": "1.0"},
            "source": {"name": "source.xml",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2018-07-05T02:07:03",
                       "validity_start": "2018-06-05T02:07:03",
                       "validity_stop": "2018-06-05T02:07:03"},
            "events": [{
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "SIMPLE_UPDATE"},
                "start": "2018-06-05T02:07:03",
                "stop": "2018-06-05T02:07:03"
            }],

        }]}

        self.engine_eboa.data = data
        returned_value = self.engine_eboa.treat_data()[0]["status"]
        
        assert returned_value == eboa_engine.exit_codes["OK"]["status"]

        data = {"operations": [{
            "mode": "insert_and_erase",
            "dim_signature": {"name": "dim_signature",
                              "exec": "processor2",
                              "version": "1.0"},
            "source": {"name": "source.xml",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2018-07-05T02:07:04",
                       "validity_start": "2018-06-05T02:07:03",
                       "validity_stop": "2018-06-05T08:07:36"},
            "events": [{
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "SIMPLE_UPDATE"},
                "start": "2018-06-05T02:07:03",
                "stop": "2018-06-05T08:07:36"
            }],

        }]}

        self.engine_eboa.data = data
        returned_value = self.engine_eboa.treat_data()[0]["status"]

        assert returned_value == eboa_engine.exit_codes["OK"]["status"]

        events = self.session.query(Event).all()

        assert len(events) == 1

        events = self.session.query(Event).join(Source).filter(Source.processor == "processor2").all()

        assert len(events) == 1

    def test_insert_and_erase_at_dim_signature_level_with_split_event_at_the_end(self):

        data = {"operations": [{
            "mode": "insert_and_erase",
            "dim_signature": {"name": "dim_signature",
                              "exec": "processor1",
                              "version": "1.0"},
            "source": {"name": "source.xml",
                       "reception_time": "2018-06-06T13:33:29",
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
            }]
                        }]
            }],

        }]}

        self.engine_eboa.data = data
        returned_value = self.engine_eboa.treat_data()[0]["status"]
        
        assert returned_value == eboa_engine.exit_codes["OK"]["status"]

        data = {"operations": [{
            "mode": "insert_and_erase",
            "dim_signature": {"name": "dim_signature",
                              "exec": "processor2",
                              "version": "1.0"},
            "source": {"name": "source.xml",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2018-07-05T02:07:04",
                       "validity_start": "2018-06-05T02:07:03",
                       "validity_stop": "2018-06-05T06:07:36"},
            "events": [{
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "SIMPLE_UPDATE"},
                "start": "2018-06-05T02:07:03",
                "stop": "2018-06-05T06:07:36"
            }],

        }]}

        self.engine_eboa.data = data
        returned_value = self.engine_eboa.treat_data()[0]["status"]

        assert returned_value == eboa_engine.exit_codes["OK"]["status"]

        events = self.session.query(Event).all()

        assert len(events) == 2

        events = self.session.query(Event).join(Source).filter(Source.processor == "processor1",
                                                               Event.start == "2018-06-05T06:07:36",
                                                               Event.stop == "2018-06-05T08:07:36").all()


        assert len(events) == 1

        events = self.session.query(Event).join(Source).filter(Source.processor == "processor2",
                                                               Event.start == "2018-06-05T02:07:03",
                                                               Event.stop == "2018-06-05T06:07:36").all()

        assert len(events) == 1

    def test_insert_and_erase_at_dim_signature_level_with_split_event_in_the_middle(self):

        data = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "processor",
                              "version": "1.0"},
            "source": {"name": "source1.xml",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2018-07-07T02:07:03",
                       "validity_start": "2018-06-05T02:07:03",
                       "validity_stop": "2018-06-05T06:07:36"},
            "events": [{
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "SIMPLE_UPDATE"},
                "start": "2018-06-05T02:07:03",
                "stop": "2018-06-05T06:07:36"
            }],

        }]}

        self.engine_eboa.data = data
        returned_value = self.engine_eboa.treat_data()[0]["status"]
        
        assert returned_value == eboa_engine.exit_codes["OK"]["status"]

        data = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "processor",
                              "version": "1.0"},
            "source": {"name": "source2.xml",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2018-07-06T02:07:03",
                       "validity_start": "2018-06-05T05:07:03",
                       "validity_stop": "2018-06-05T07:07:03"},
            "events": [{
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "SIMPLE_UPDATE"},
                "start": "2018-06-05T05:07:03",
                "stop": "2018-06-05T07:07:03"
            }],

        }]}

        self.engine_eboa.data = data
        returned_value = self.engine_eboa.treat_data()[0]["status"]
        
        assert returned_value == eboa_engine.exit_codes["OK"]["status"]

        data = {"operations": [{
            "mode": "insert_and_erase",
            "dim_signature": {"name": "dim_signature",
                              "exec": "processor",
                              "version": "1.0"},
            "source": {"name": "source3.xml",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2018-07-06T02:07:03",
                       "validity_start": "2018-06-05T02:07:03",
                       "validity_stop": "2018-06-05T08:07:36"},
            "events": [{
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "SIMPLE_UPDATE"},
                "start": "2018-06-05T02:07:03",
                "stop": "2018-06-05T08:07:36"
            }],

        }]}

        self.engine_eboa.data = data
        returned_value = self.engine_eboa.treat_data()[0]["status"]

        assert returned_value == eboa_engine.exit_codes["OK"]["status"]

        events = self.session.query(Event).all()

        assert len(events) == 3

        events = self.session.query(Event).join(Source).filter(Source.name == "source1.xml",
                                                               Event.start == "2018-06-05T02:07:03",
                                                               Event.stop == "2018-06-05T06:07:36").all()


        assert len(events) == 1

        events = self.session.query(Event).join(Source).filter(Source.name == "source2.xml",
                                                               Event.start == "2018-06-05T06:07:36",
                                                               Event.stop == "2018-06-05T07:07:03").all()


        assert len(events) == 1

        events = self.session.query(Event).join(Source).filter(Source.name == "source3.xml",
                                                               Event.start == "2018-06-05T07:07:03",
                                                               Event.stop == "2018-06-05T08:07:36").all()

        assert len(events) == 1

    def test_insert_and_erase_at_dim_signature_level_with_split_event_crossing_validity(self):

        data = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "processor",
                              "version": "1.0"},
            "source": {"name": "source1.xml",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2018-07-07T02:07:03",
                       "validity_start": "2018-06-05T02:07:03",
                       "validity_stop": "2018-06-05T06:07:36"},
            "events": [{
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "SIMPLE_UPDATE"},
                "start": "2018-06-05T02:07:03",
                "stop": "2018-06-05T06:07:36"
            }],

        }]}

        self.engine_eboa.data = data
        returned_value = self.engine_eboa.treat_data()[0]["status"]
        
        assert returned_value == eboa_engine.exit_codes["OK"]["status"]

        data = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "processor",
                              "version": "1.0"},
            "source": {"name": "source2.xml",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2018-07-06T02:07:03",
                       "validity_start": "2018-06-05T05:07:03",
                       "validity_stop": "2018-06-05T09:07:03"},
            "events": [{
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "SIMPLE_UPDATE"},
                "start": "2018-06-05T05:07:03",
                "stop": "2018-06-05T09:07:03"
            }],

        }]}

        self.engine_eboa.data = data
        returned_value = self.engine_eboa.treat_data()[0]["status"]
        
        assert returned_value == eboa_engine.exit_codes["OK"]["status"]

        data = {"operations": [{
            "mode": "insert_and_erase",
            "dim_signature": {"name": "dim_signature",
                              "exec": "processor",
                              "version": "1.0"},
            "source": {"name": "source3.xml",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2018-07-06T02:07:03",
                       "validity_start": "2018-06-05T02:07:03",
                       "validity_stop": "2018-06-05T08:07:36"},
            "events": [{
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "SIMPLE_UPDATE"},
                "start": "2018-06-05T02:07:03",
                "stop": "2018-06-05T08:07:36"
            }],

        }]}

        self.engine_eboa.data = data
        returned_value = self.engine_eboa.treat_data()[0]["status"]

        assert returned_value == eboa_engine.exit_codes["OK"]["status"]

        events = self.session.query(Event).all()

        assert len(events) == 2

        events = self.session.query(Event).join(Source).filter(Source.name == "source1.xml",
                                                               Event.start == "2018-06-05T02:07:03",
                                                               Event.stop == "2018-06-05T06:07:36").all()


        assert len(events) == 1

        events = self.session.query(Event).join(Source).filter(Source.name == "source2.xml",
                                                               Event.start == "2018-06-05T06:07:36",
                                                               Event.stop == "2018-06-05T09:07:03").all()


        assert len(events) == 1

    def test_insert_and_erase_at_event_level_with_split_event_crossing_validity(self):

        data = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "processor",
                              "version": "1.0"},
            "source": {"name": "source1.xml",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2018-07-07T02:07:03",
                       "validity_start": "2018-06-05T02:07:03",
                       "validity_stop": "2018-06-05T06:07:36"},
            "events": [{
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "SIMPLE_UPDATE"},
                "start": "2018-06-05T02:07:03",
                "stop": "2018-06-05T06:07:36"
            }],

        }]}

        self.engine_eboa.data = data
        returned_value = self.engine_eboa.treat_data()[0]["status"]
        
        assert returned_value == eboa_engine.exit_codes["OK"]["status"]

        data = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "processor",
                              "version": "1.0"},
            "source": {"name": "source2.xml",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2018-07-06T02:07:03",
                       "validity_start": "2018-06-05T05:07:03",
                       "validity_stop": "2018-06-05T09:07:03"},
            "events": [{
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "SIMPLE_UPDATE"},
                "start": "2018-06-05T05:07:03",
                "stop": "2018-06-05T09:07:03"
            }],

        }]}

        self.engine_eboa.data = data
        returned_value = self.engine_eboa.treat_data()[0]["status"]
        
        assert returned_value == eboa_engine.exit_codes["OK"]["status"]

        data = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "processor",
                              "version": "1.0"},
            "source": {"name": "source3.xml",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2018-07-06T02:07:03",
                       "validity_start": "2018-06-05T02:07:03",
                       "validity_stop": "2018-06-05T08:07:36"},
            "events": [{
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "INSERT_and_ERASE"},
                "start": "2018-06-05T02:07:03",
                "stop": "2018-06-05T08:07:36"
            }],

        }]}

        self.engine_eboa.data = data
        returned_value = self.engine_eboa.treat_data()[0]["status"]

        assert returned_value == eboa_engine.exit_codes["OK"]["status"]

        events = self.session.query(Event).all()

        assert len(events) == 2

        events = self.session.query(Event).join(Source).filter(Source.name == "source1.xml",
                                                               Event.start == "2018-06-05T02:07:03",
                                                               Event.stop == "2018-06-05T06:07:36").all()


        assert len(events) == 1

        events = self.session.query(Event).join(Source).filter(Source.name == "source2.xml",
                                                               Event.start == "2018-06-05T06:07:36",
                                                               Event.stop == "2018-06-05T09:07:03").all()


        assert len(events) == 1

    def test_insert_and_erase_at_event_level_with_priority_with_split_event_crossing_validity(self):

        data = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "processor",
                              "version": "1.0"},
            "source": {"name": "source1.xml",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2018-07-07T02:07:03",
                       "validity_start": "2018-06-05T02:07:03",
                       "validity_stop": "2018-06-05T06:07:36",
                       "priority": 30},
            "events": [{
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "SIMPLE_UPDATE"},
                "start": "2018-06-05T02:07:03",
                "stop": "2018-06-05T06:07:36"
            }],

        }]}

        self.engine_eboa.data = data
        returned_value = self.engine_eboa.treat_data()[0]["status"]
        
        assert returned_value == eboa_engine.exit_codes["OK"]["status"]

        data = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "processor",
                              "version": "1.0"},
            "source": {"name": "source2.xml",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2018-07-06T02:07:03",
                       "validity_start": "2018-06-05T05:07:03",
                       "validity_stop": "2018-06-05T09:07:03",
                       "priority": 20},
            "events": [{
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "SIMPLE_UPDATE"},
                "start": "2018-06-05T05:07:03",
                "stop": "2018-06-05T09:07:03"
            }],

        }]}

        self.engine_eboa.data = data
        returned_value = self.engine_eboa.treat_data()[0]["status"]
        
        assert returned_value == eboa_engine.exit_codes["OK"]["status"]

        data = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "processor",
                              "version": "1.0"},
            "source": {"name": "source3.xml",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2018-07-06T02:07:03",
                       "validity_start": "2018-06-05T02:07:03",
                       "validity_stop": "2018-06-05T08:07:36",
                       "priority": 10},
            "events": [{
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "INSERT_and_ERASE_with_PRIORITY"},
                "start": "2018-06-05T02:07:03",
                "stop": "2018-06-05T08:07:36"
            }],

        }]}

        self.engine_eboa.data = data
        returned_value = self.engine_eboa.treat_data()[0]["status"]

        assert returned_value == eboa_engine.exit_codes["OK"]["status"]

        events = self.session.query(Event).all()

        assert len(events) == 2

        events = self.session.query(Event).join(Source).filter(Source.name == "source1.xml",
                                                               Event.start == "2018-06-05T02:07:03",
                                                               Event.stop == "2018-06-05T06:07:36").all()


        assert len(events) == 1

        events = self.session.query(Event).join(Source).filter(Source.name == "source2.xml",
                                                               Event.start == "2018-06-05T06:07:36",
                                                               Event.stop == "2018-06-05T09:07:03").all()


        assert len(events) == 1


    def test_insert_and_erase_at_event_level_with_priority_with_no_events_during_one_period(self):

        data = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "processor",
                              "version": "1.0"},
            "source": {"name": "source1.xml",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2018-07-07T02:07:03",
                       "validity_start": "2018-06-05T02:07:03",
                       "validity_stop": "2018-06-05T06:07:36",
                       "priority": 30},
            "events": [{
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "SIMPLE_UPDATE"},
                "start": "2018-06-05T02:07:03",
                "stop": "2018-06-05T06:07:36"
            }],

        }]}

        self.engine_eboa.data = data
        returned_value = self.engine_eboa.treat_data()[0]["status"]
        
        assert returned_value == eboa_engine.exit_codes["OK"]["status"]

        data = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "processor",
                              "version": "1.0"},
            "source": {"name": "source2.xml",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2018-07-06T02:07:03",
                       "validity_start": "2018-06-05T05:07:03",
                       "validity_stop": "2018-06-05T07:07:03",
                       "priority": 20},
            "events": [{
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "SIMPLE_UPDATE"},
                "start": "2018-06-05T05:07:03",
                "stop": "2018-06-05T07:07:03"
            }],

        }]}

        self.engine_eboa.data = data
        returned_value = self.engine_eboa.treat_data()[0]["status"]
        
        assert returned_value == eboa_engine.exit_codes["OK"]["status"]

        data = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "processor",
                              "version": "1.0"},
            "source": {"name": "source3.xml",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2018-07-06T02:07:03",
                       "validity_start": "2018-06-05T02:07:03",
                       "validity_stop": "2018-06-05T08:07:36",
                       "priority": 10},
            "events": [{
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "INSERT_and_ERASE_with_PRIORITY"},
                "start": "2018-06-05T05:07:03",
                "stop": "2018-06-05T06:07:36"
            }],

        }]}

        self.engine_eboa.data = data
        returned_value = self.engine_eboa.treat_data()[0]["status"]

        assert returned_value == eboa_engine.exit_codes["OK"]["status"]

        events = self.session.query(Event).all()

        assert len(events) == 2

        events = self.session.query(Event).join(Source).filter(Source.name == "source1.xml",
                                                               Event.start == "2018-06-05T02:07:03",
                                                               Event.stop == "2018-06-05T06:07:36").all()


        assert len(events) == 1

        events = self.session.query(Event).join(Source).filter(Source.name == "source2.xml",
                                                               Event.start == "2018-06-05T06:07:36",
                                                               Event.stop == "2018-06-05T07:07:03").all()


        assert len(events) == 1

    def test_treat_data_wrong_source_period(self):

        data = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source.xml",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2018-07-05T02:07:03",
                       "validity_start": "2018-06-07T02:07:03",
                       "validity_stop": "2018-06-05T08:07:36"}
        }]}

        self.engine_eboa.data = data
        returned_value = self.engine_eboa.treat_data()[0]["status"]

        assert returned_value == eboa_engine.exit_codes["WRONG_SOURCE_PERIOD"]["status"]

        sources_status = self.session.query(SourceStatus).join(Source).filter(SourceStatus.status == eboa_engine.exit_codes["WRONG_SOURCE_PERIOD"]["status"],
                                                                           Source.name == data["operations"][0]["source"]["name"]).all()

        assert len(sources_status) == 1

    def test_treat_data_undefined_event_link(self):

        data = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source.xml",
                       "reception_time": "2018-06-06T13:33:29",
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

        self.engine_eboa.data = data
        returned_value = self.engine_eboa.treat_data()[0]["status"]

        assert returned_value == eboa_engine.exit_codes["UNDEFINED_EVENT_LINK_REF"]["status"]

        sources_status = self.session.query(SourceStatus).join(Source).filter(SourceStatus.status == eboa_engine.exit_codes["UNDEFINED_EVENT_LINK_REF"]["status"],
                                                                           Source.name == data["operations"][0]["source"]["name"]).all()

        assert len(sources_status) == 1

    def test_treat_data_wrong_event_period(self):

        data = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source.xml",
                       "reception_time": "2018-06-06T13:33:29",
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

        self.engine_eboa.data = data
        returned_value = self.engine_eboa.treat_data()[0]["status"]

        assert returned_value == eboa_engine.exit_codes["WRONG_EVENT_PERIOD"]["status"]

        sources_status = self.session.query(SourceStatus).join(Source).filter(SourceStatus.status == eboa_engine.exit_codes["WRONG_EVENT_PERIOD"]["status"],
                                                                           Source.name == data["operations"][0]["source"]["name"]).all()

        assert len(sources_status) == 1

    def test_treat_data_wrong_event_value(self):

        data = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source.xml",
                       "reception_time": "2018-06-06T13:33:29",
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

        self.engine_eboa.data = data
        returned_value = self.engine_eboa.treat_data(source="source.xml")[0]["status"]

        assert returned_value == eboa_engine.exit_codes["FILE_NOT_VALID"]["status"]

        sources_status = self.session.query(SourceStatus).join(Source).filter(SourceStatus.status == eboa_engine.exit_codes["FILE_NOT_VALID"]["status"],
                                                                           Source.name == data["operations"][0]["source"]["name"]).all()

        assert len(sources_status) == 1

    def test_treat_data_odd_number_event_geometry(self):

        data = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source.xml",
                       "reception_time": "2018-06-06T13:33:29",
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

        self.engine_eboa.data = data
        returned_value = self.engine_eboa.treat_data()[0]["status"]

        assert returned_value == eboa_engine.exit_codes["ODD_NUMBER_OF_COORDINATES"]["status"]

        sources_status = self.session.query(SourceStatus).join(Source).filter(SourceStatus.status == eboa_engine.exit_codes["ODD_NUMBER_OF_COORDINATES"]["status"],
                                                                           Source.name == data["operations"][0]["source"]["name"]).all()

        assert len(sources_status) == 1

    def test_treat_data_wrong_annotation_value(self):

        data = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source.xml",
                       "reception_time": "2018-06-06T13:33:29",
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

        self.engine_eboa.data = data
        returned_value = self.engine_eboa.treat_data(source="source.xml")[0]["status"]

        assert returned_value == eboa_engine.exit_codes["FILE_NOT_VALID"]["status"]

        sources_status = self.session.query(SourceStatus).join(Source).filter(SourceStatus.status == eboa_engine.exit_codes["FILE_NOT_VALID"]["status"],
                                                                           Source.name == data["operations"][0]["source"]["name"]).all()

        assert len(sources_status) == 1

    def test_treat_data_odd_number_annotation_geometry(self):

        data = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source.xml",
                       "reception_time": "2018-06-06T13:33:29",
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

        self.engine_eboa.data = data
        returned_value = self.engine_eboa.treat_data()[0]["status"]

        assert returned_value == eboa_engine.exit_codes["ODD_NUMBER_OF_COORDINATES"]["status"]

        sources_status = self.session.query(SourceStatus).join(Source).filter(SourceStatus.status == eboa_engine.exit_codes["ODD_NUMBER_OF_COORDINATES"]["status"],
                                                                           Source.name == data["operations"][0]["source"]["name"]).all()

        assert len(sources_status) == 1

    def test_treat_data_annotation_wrong_geometry(self):

        data = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source.xml",
                       "reception_time": "2018-06-06T13:33:29",
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
                                 "name": "GEOMETRY",
                                 "value": "29.012974905944 -118.33483458667"}]
                        }]
            }]
        }]
        }
        self.engine_eboa.treat_data(data)
        annotation_ddbb = self.session.query(Annotation).all()

        assert len(annotation_ddbb) == 0

    def test_insert_xml(self):

        filename = "test_simple_update.xml"
        self.engine_eboa.parse_data_from_xml(os.path.dirname(os.path.abspath(__file__)) + "/xml_inputs/" + filename)

        returned_value = self.engine_eboa.treat_data()[0]["status"]

        assert returned_value == eboa_engine.exit_codes["OK"]["status"]

        sources_status = self.session.query(SourceStatus).join(Source).filter(SourceStatus.status == eboa_engine.exit_codes["OK"]["status"],
                                                                                            Source.name == filename).all()

        assert len(sources_status) == 1

        events_ddbb = self.session.query(Event).all()

        assert len(events_ddbb) == 2

        annotations_ddbb = self.session.query(Annotation).all()

        assert len(annotations_ddbb) == 1

        gauges_ddbb = self.session.query(Gauge).filter(Gauge.name == "test_gauge_name1",
                                                       Gauge.system == "test_gauge_system1",
                                                       Gauge.description == "test_gauge_description1").all()

        assert len(gauges_ddbb) == 1

        gauges_ddbb = self.session.query(Gauge).filter(Gauge.name == "test_gauge_name2",
                                                       Gauge.system == "test_gauge_system2",
                                                       Gauge.description == "test_gauge_description2").all()

        assert len(gauges_ddbb) == 1

        annotation_cnfs_ddbb = self.session.query(AnnotationCnf).filter(AnnotationCnf.name == "test_annotation_cnf_name1",
                                                                        AnnotationCnf.system == "test_annotation_cnf_system1",
                                                                        AnnotationCnf.description == "test_annotation_cnf_description1").all()

        assert len(annotation_cnfs_ddbb) == 1

    def test_wrong_xml(self):

        filename = "test_wrong_structure.xml"
        returned_value = self.engine_eboa.parse_data_from_xml(os.path.dirname(os.path.abspath(__file__)) + "/xml_inputs/" + filename)

        assert returned_value == eboa_engine.exit_codes["FILE_NOT_VALID"]["status"]

    def test_not_xml(self):

        filename = "test_not_xml.xml"
        returned_value = self.engine_eboa.parse_data_from_xml(os.path.dirname(os.path.abspath(__file__)) + "/xml_inputs/" + filename)

        assert returned_value == eboa_engine.exit_codes["FILE_NOT_VALID"]["status"]

    def test_insert_json(self):

        filename = "test_simple_update.json"
        self.engine_eboa.parse_data_from_json(os.path.dirname(os.path.abspath(__file__)) + "/json_inputs/" + filename)

        returned_value = self.engine_eboa.treat_data(source = filename)[0]["status"]

        assert returned_value == eboa_engine.exit_codes["OK"]["status"]

        sources_status = self.session.query(SourceStatus).join(Source).filter(SourceStatus.status == eboa_engine.exit_codes["OK"]["status"],
                                                                                            Source.name == filename).all()

        assert len(sources_status) == 1

        events_ddbb = self.session.query(Event).all()

        assert len(events_ddbb) == 2

        annotations_ddbb = self.session.query(Annotation).all()

        assert len(annotations_ddbb) == 1

        gauges_ddbb = self.session.query(Gauge).filter(Gauge.name == "test_gauge_name1",
                                                       Gauge.system == "test_gauge_system1",
                                                       Gauge.description == "test_gauge_description1").all()

        assert len(gauges_ddbb) == 1

        gauges_ddbb = self.session.query(Gauge).filter(Gauge.name == "test_gauge_name2",
                                                       Gauge.system == "test_gauge_system2",
                                                       Gauge.description == "test_gauge_description2").all()

        assert len(gauges_ddbb) == 1

        annotation_cnfs_ddbb = self.session.query(AnnotationCnf).filter(AnnotationCnf.name == "test_annotation_cnf_name1",
                                                                        AnnotationCnf.system == "test_annotation_cnf_system1",
                                                                        AnnotationCnf.description == "test_annotation_cnf_description1").all()

        assert len(annotation_cnfs_ddbb) == 1

    def test_wrong_json(self):

        filename = "test_wrong_structure.json"
        returned_value = self.engine_eboa.parse_data_from_json(os.path.dirname(os.path.abspath(__file__)) + "/json_inputs/" + filename)

        assert returned_value == eboa_engine.exit_codes["FILE_NOT_VALID"]["status"]

    def test_wrong_json_validating_its_data(self):

        filename = "test_wrong_structure.json"
        self.engine_eboa.parse_data_from_json(os.path.dirname(os.path.abspath(__file__)) + "/json_inputs/" + filename, check_schema = False)

        returned_value = self.engine_eboa.treat_data(self.engine_eboa.data, filename)[0]["status"]

        assert returned_value == eboa_engine.exit_codes["FILE_NOT_VALID"]["status"]

    def test_not_json(self):

        filename = "test_not_json.json"
        returned_value = self.engine_eboa.parse_data_from_json(os.path.dirname(os.path.abspath(__file__)) + "/json_inputs/" + filename)

        assert returned_value == eboa_engine.exit_codes["FILE_NOT_VALID"]["status"]

    def test_insert_event_simple_update_debug(self):

        logging_module = Log(name = __name__)

        previous_logging_level = None
        if "EBOA_LOG_LEVEL" in os.environ:
            previous_logging_level = os.environ["EBOA_LOG_LEVEL"]
        # end if

        os.environ["EBOA_LOG_LEVEL"] = "DEBUG"

        logging_module.define_logging_configuration()

        self.engine_eboa._initialize_context_insert_data()
        data = {"dim_signature": {"name": "dim_signature",
                                  "exec": "exec",
                                  "version": "1.0"},
                "source": {"name": "source.xml",
                           "reception_time": "2018-06-06T13:33:29",
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
        self.engine_eboa.operation = data
        self.engine_eboa._insert_dim_signature()
        self.engine_eboa._insert_source()
        self.engine_eboa._insert_gauges()
        self.engine_eboa.session.commit()
        self.engine_eboa._insert_explicit_refs()
        self.engine_eboa.session.commit()
        gauge_ddbb = self.session.query(Gauge).filter(Gauge.name == data["events"][0]["gauge"]["name"], Gauge.system == data["events"][0]["gauge"]["system"]).first()
        source_ddbb = self.session.query(Source).filter(Source.name == data["source"]["name"], Source.validity_start == data["source"]["validity_start"], Source.validity_stop == data["source"]["validity_stop"], Source.generation_time == data["source"]["generation_time"], Source.processor_version == data["dim_signature"]["version"], Source.processor == data["dim_signature"]["exec"]).first()
        dim_signature_ddbb = self.session.query(DimSignature).filter(DimSignature.dim_signature == data["dim_signature"]["name"]).first()
        explicit_reference_ddbb = self.session.query(ExplicitRef).filter(ExplicitRef.explicit_ref == data["events"][0]["explicit_reference"]).first()
        self.engine_eboa._insert_events()
        self.engine_eboa.session.commit()
        event_ddbb = self.session.query(Event).filter(Event.start == data["events"][0]["start"],
                                                      Event.stop == data["events"][0]["stop"],
                                                      Event.gauge_uuid == gauge_ddbb.gauge_uuid,
                                                      Event.source_uuid == source_ddbb.source_uuid,
                                                      Event.explicit_ref_uuid == explicit_reference_ddbb.explicit_ref_uuid,
                                                      Event.visible == True).all()

        assert len(event_ddbb) == 1

        if previous_logging_level:
            os.environ["EBOA_LOG_LEVEL"] = previous_logging_level
        else:
            del os.environ["EBOA_LOG_LEVEL"]
        # end if
        logging_module.define_logging_configuration()

    def test_race_condition_insert_event_value_same_name(self):
        """
        Method to test the race condition that could be produced if the
        value are inserted with the same name by two different processes
        """
        data = {"operations": [{
                "mode": "insert",
                "dim_signature": {"name": "dim_signature",
                                  "exec": "exec",
                                  "version": "1.0"},
                "source": {"name": "source.xml",
                           "reception_time": "2018-06-06T13:33:29",
                           "generation_time": "2018-07-05T02:07:03",
                           "validity_start": "2018-06-05T02:07:03",
                           "validity_stop": "2018-06-05T08:07:36"},
                "events": [{
                    "gauge": {"name": "GAUGE_NAME",
                              "system": "GAUGE_SYSTEM",
                              "insertion_type": "SIMPLE_UPDATE"},
                    "start": "2018-06-05T02:07:03",
                    "stop": "2018-06-05T08:07:36",
                }]
            }]
            }
        self.engine_eboa.treat_data(data)

        events = self.session.query(Event).all()

        assert len(events) == 1

        event_uuid = events[0].event_uuid
        
        value = {
            "name": "first_object",
            "type": "object",
            "values": []
        }

        def insert_event_value_race_condition():
            exit_status = self.engine_eboa_race_conditions.insert_event_value(event_uuid, value)
            self.engine_eboa_race_conditions.session.commit()
            assert exit_status["error"] == False
            assert exit_status["inserted"] == True
            assert exit_status["status"] == "OK"
        # end def

        def insert_event_value():
            exit_status = self.engine_eboa.insert_event_value(event_uuid, value)
            self.engine_eboa.session.commit()
            assert exit_status["error"] == False
            assert exit_status["inserted"] == False
            assert exit_status["status"] == "VALUE_WAS_INGESTED_BY_OTHER_PROCESS"
        # end def

        with before_after.before("eboa.engine.engine.race_condition", insert_event_value_race_condition):
            insert_event_value()
        # end with

        event_objects = self.session.query(EventObject).filter(EventObject.name == "first_object").all()

        assert len(event_objects) == 1

    def test_insert_event_value_event_uuid_not_exists(self):
        """
        Method to test the method to associate a value to an event
        which event_uuid does not exist in DDBB
        """

        events = self.session.query(Event).all()

        assert len(events) == 0

        event_uuid = "1950364a-89d0-11ea-a91e-000000001ddd"
        
        value = {
            "name": "first_object",
            "type": "object",
            "values": []
        }

        exit_status = self.engine_eboa.insert_event_value(event_uuid, value)
        self.engine_eboa.session.commit()
        assert exit_status["error"] == False
        assert exit_status["inserted"] == False
        assert exit_status["status"] == "EVENT_DOES_NOT_EXIST"

        event_objects = self.session.query(EventObject).all()

        assert len(event_objects) == 0

    def test_race_condition_insert_event_value_different_names_no_previous_values(self):
        """
        Method to test the race condition that could be produced if the
        value is inserted with no created structure of value by different processes
        as all of them are going to try to initiate this structure of values
        """
        data = {"operations": [{
                "mode": "insert",
                "dim_signature": {"name": "dim_signature",
                                  "exec": "exec",
                                  "version": "1.0"},
                "source": {"name": "source.xml",
                           "reception_time": "2018-06-06T13:33:29",
                           "generation_time": "2018-07-05T02:07:03",
                           "validity_start": "2018-06-05T02:07:03",
                           "validity_stop": "2018-06-05T08:07:36"},
                "events": [{
                    "gauge": {"name": "GAUGE_NAME",
                              "system": "GAUGE_SYSTEM",
                              "insertion_type": "SIMPLE_UPDATE"},
                    "start": "2018-06-05T02:07:03",
                    "stop": "2018-06-05T08:07:36",
                }]
            }]
            }
        self.engine_eboa.treat_data(data)

        events = self.session.query(Event).all()

        assert len(events) == 1

        event_uuid = events[0].event_uuid
        
        value1 = {
            "name": "first_object1",
            "type": "object",
            "values": []
        }

        value2 = {
            "name": "first_object2",
            "type": "object",
            "values": []
        }

        def insert_event_value1():
            exit_status = self.engine_eboa_race_conditions.insert_event_value(event_uuid, value1)
            self.engine_eboa_race_conditions.session.commit()
            assert exit_status["error"] == False
            assert exit_status["inserted"] == True
        # end def

        def insert_event_value2():
            exit_status = self.engine_eboa.insert_event_value(event_uuid, value2)
            self.engine_eboa.session.commit()
            assert exit_status["error"] == False
            assert exit_status["inserted"] == True
        # end def

        with before_after.before("eboa.engine.engine.race_condition", insert_event_value1):
            insert_event_value2()
        # end with

        event_objects = self.session.query(EventObject).filter(EventObject.name == "first_object1").all()

        assert len(event_objects) == 1

        event_objects = self.session.query(EventObject).filter(EventObject.name == "first_object2").all()

        assert len(event_objects) == 1

    def test_race_condition_insert_event_value_different_names_previous_values(self):
        """
        Method to test the race condition that could be produced if the
        several processes try to insert the value even with different name in the same position
        """
        data = {"operations": [{
                "mode": "insert",
                "dim_signature": {"name": "dim_signature",
                                  "exec": "exec",
                                  "version": "1.0"},
                "source": {"name": "source.xml",
                           "reception_time": "2018-06-06T13:33:29",
                           "generation_time": "2018-07-05T02:07:03",
                           "validity_start": "2018-06-05T02:07:03",
                           "validity_stop": "2018-06-05T08:07:36"},
                "events": [{
                    "gauge": {"name": "GAUGE_NAME",
                              "system": "GAUGE_SYSTEM",
                              "insertion_type": "SIMPLE_UPDATE"},
                    "start": "2018-06-05T02:07:03",
                    "stop": "2018-06-05T08:07:36",
                    "values": [{
                        "name": "values",
                        "type": "object",
                        "values": []
                    }]
                }]
            }]
            }
        self.engine_eboa.treat_data(data)

        events = self.session.query(Event).all()

        assert len(events) == 1

        event_uuid = events[0].event_uuid
        
        value1 = {
            "name": "first_object1",
            "type": "object",
            "values": []
        }

        value2 = {
            "name": "first_object2",
            "type": "object",
            "values": []
        }

        def insert_event_value1():
            exit_status = self.engine_eboa_race_conditions.insert_event_value(event_uuid, value1)
            self.engine_eboa_race_conditions.session.commit()
            assert exit_status["error"] == False
            assert exit_status["inserted"] == True
        # end def

        def insert_event_value2():
            exit_status = self.engine_eboa.insert_event_value(event_uuid, value2)
            self.engine_eboa.session.commit()
            assert exit_status["error"] == False
            assert exit_status["inserted"] == True
        # end def

        with before_after.before("eboa.engine.engine.race_condition", insert_event_value1):
            insert_event_value2()
        # end with

        event_objects = self.session.query(EventObject).filter(EventObject.name == "first_object1").all()

        assert len(event_objects) == 1

        event_objects = self.session.query(EventObject).filter(EventObject.name == "first_object2").all()

        assert len(event_objects) == 1

        event_objects = self.session.query(EventObject).filter(EventObject.position == 0, EventObject.parent_level == -1, EventObject.parent_position == 0).all()

        assert len(event_objects) == 1

        event_objects = self.session.query(EventObject).all()

        assert len(event_objects) == 3

    def test_insert_event_value_all_types(self):
        """
        Method to test that the value can contain all the types
        """
        data = {"operations": [{
                "mode": "insert",
                "dim_signature": {"name": "dim_signature",
                                  "exec": "exec",
                                  "version": "1.0"},
                "source": {"name": "source.xml",
                           "reception_time": "2018-06-06T13:33:29",
                           "generation_time": "2018-07-05T02:07:03",
                           "validity_start": "2018-06-05T02:07:03",
                           "validity_stop": "2018-06-05T08:07:36"},
                "events": [{
                    "gauge": {"name": "GAUGE_NAME",
                              "system": "GAUGE_SYSTEM",
                              "insertion_type": "SIMPLE_UPDATE"},
                    "start": "2018-06-05T02:07:03",
                    "stop": "2018-06-05T08:07:36",
                    "values": [{
                        "name": "values",
                        "type": "object",
                        "values": []
                    }]
                }]
            }]
            }
        self.engine_eboa.treat_data(data)

        events = self.session.query(Event).all()

        assert len(events) == 1

        event_uuid = events[0].event_uuid
        
        value = {
            "name": "OBJECT",
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
                {"type": "geometry",
                 "name": "GEOMETRY",
                 "value": "29.012974905944 -118.33483458667 28.8650301641571 -118.372028380632 28.7171766138274 -118.409121707686 28.5693112139334 -118.44612300623 28.4213994944367 -118.483058731035 28.2734085660472 -118.519970531113 28.1253606038163 -118.556849863134 27.9772541759126 -118.593690316835 27.8291247153939 -118.630472520505 27.7544158362332 -118.64900551674 27.7282373644786 -118.48032600682 27.7015162098732 -118.314168792268 27.6742039940042 -118.150246300849 27.6462511775992 -117.98827485961 27.6176070520608 -117.827974178264 27.5882197156561 -117.669066835177 27.5580360448116 -117.511277813618 27.5270016492436 -117.354334035359 27.4950608291016 -117.197963963877 27.4621565093409 -117.041897175848 27.4282301711374 -116.885863967864 27.3932217651372 -116.729594956238 27.3570696128269 -116.572820673713 27.3197103000253 -116.415271199941 27.2810785491022 -116.256675748617 27.241107085821 -116.09676229722 27.1997272484913 -115.935260563566 27.1524952198729 -115.755436839005 27.2270348347386 -115.734960009089 27.3748346522356 -115.694299254844 27.5226008861849 -115.653563616829 27.6702779354428 -115.612760542177 27.8178690071708 -115.571901649363 27.9653506439026 -115.531000691074 28.1127600020619 -115.490011752733 28.2601469756437 -115.44890306179 28.4076546372628 -115.407649898021 28.455192866856 -115.589486631416 28.4968374106496 -115.752807970928 28.5370603096399 -115.91452902381 28.575931293904 -116.074924211465 28.6135193777855 -116.234273707691 28.6498895688451 -116.392847129762 28.6851057860975 -116.550917254638 28.7192301322012 -116.708756374584 28.752323018501 -116.866636764481 28.7844432583843 -117.024831047231 28.8156481533955 -117.183612605748 28.8459935678779 -117.343255995623 28.8755339855462 -117.504037348554 28.9043225601122 -117.666234808407 28.9324111491586 -117.830128960026 28.9598503481156 -117.996003330616 28.9866878706574 -118.164136222141 29.012974905944 -118.33483458667"}
            ]
        }

        exit_status = self.engine_eboa.insert_event_value(event_uuid, value)
        self.engine_eboa.session.commit()
        assert exit_status["error"] == False
        assert exit_status["inserted"] == True

        event_objects = self.session.query(EventObject).filter(EventObject.name == "OBJECT").all()

        assert len(event_objects) == 1

        event_texts = self.session.query(EventText).filter(EventText.name == "TEXT").all()

        assert len(event_texts) == 1

        event_booleans = self.session.query(EventBoolean).filter(EventBoolean.name == "BOOLEAN").all()

        assert len(event_texts) == 1

        event_doubles = self.session.query(EventDouble).filter(EventDouble.name == "DOUBLE").all()

        assert len(event_doubles) == 1

        event_timestamps = self.session.query(EventTimestamp).filter(EventTimestamp.name == "TIMESTAMP").all()

        assert len(event_timestamps) == 1

        event_geometries = self.session.query(EventGeometry).filter(EventGeometry.name == "GEOMETRY").all()

        assert len(event_geometries) == 1

    def test_insert_event_value_wrong_format(self):
        """
        Method to test that the engine does not allow not admitted values
        """
        data = {"operations": [{
                "mode": "insert",
                "dim_signature": {"name": "dim_signature",
                                  "exec": "exec",
                                  "version": "1.0"},
                "source": {"name": "source.xml",
                           "reception_time": "2018-06-06T13:33:29",
                           "generation_time": "2018-07-05T02:07:03",
                           "validity_start": "2018-06-05T02:07:03",
                           "validity_stop": "2018-06-05T08:07:36"},
                "events": [{
                    "gauge": {"name": "GAUGE_NAME",
                              "system": "GAUGE_SYSTEM",
                              "insertion_type": "SIMPLE_UPDATE"},
                    "start": "2018-06-05T02:07:03",
                    "stop": "2018-06-05T08:07:36"
                }]
            }]
            }
        self.engine_eboa.treat_data(data)

        events = self.session.query(Event).all()

        assert len(events) == 1

        event_uuid = events[0].event_uuid
        
        values = {
            "no_valid_structure": "OBJECT"
        }

        exit_status = self.engine_eboa.insert_event_value(event_uuid, values)
        self.engine_eboa.session.commit()
        assert exit_status["error"] == True
        assert exit_status["inserted"] == False

    def test_insert_event_value_wrong_geometry(self):
        """
        Method to test the treatment of a wrong geomerty value
        """

        data = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source.xml",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2018-07-05T02:07:03",
                       "validity_start": "2018-06-05T02:07:03",
                       "validity_stop": "2018-06-05T08:07:36"},
            "events": [{"gauge": {"name": "GAUGE_NAME",
                                  "system": "GAUGE_SYSTEM",
                                  "insertion_type": "SIMPLE_UPDATE"},
                        "start": "2018-06-05T02:07:03",
                        "stop": "2018-06-05T08:07:36"
                    }]
        }]
        }
        self.engine_eboa.treat_data(data)

        events = self.session.query(Event).all()

        assert len(events) == 1

        event_uuid = events[0].event_uuid
        
        values = {"type": "geometry",
                  "name": "GEOMETRY",
                  "value": "29.012974905944 -118.33483458667"}

        exit_status = self.engine_eboa.insert_event_value(event_uuid, values)
        self.engine_eboa.session.commit()
        assert exit_status["error"] == True
        assert exit_status["inserted"] == False

        event_values = self.query_eboa.get_event_values()

        assert len(event_values) == 0


    def test_insert_same_values_to_different_events(self):
        """
        Method to test the insertion of the same values to different events
        """

        data = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source.xml",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2018-07-05T02:07:03",
                       "validity_start": "2018-06-05T02:07:03",
                       "validity_stop": "2018-06-05T08:07:36"},
            "events": [{"gauge": {"name": "GAUGE_NAME",
                                  "system": "GAUGE_SYSTEM",
                                  "insertion_type": "SIMPLE_UPDATE"},
                        "start": "2018-06-05T02:07:03",
                        "stop": "2018-06-05T08:07:36"
                    },
                       {"gauge": {"name": "GAUGE_NAME",
                                  "system": "GAUGE_SYSTEM",
                                  "insertion_type": "SIMPLE_UPDATE"},
                        "start": "2018-06-05T02:07:03",
                        "stop": "2018-06-05T08:07:36"
                    }]
        }]
        }
        self.engine_eboa.treat_data(data)

        events = self.session.query(Event).all()

        assert len(events) == 2

        event_uuid1 = events[0].event_uuid
        event_uuid2 = events[1].event_uuid
        
        values = {
            "name": "first_object",
            "type": "object",
            "values": []
        }

        exit_status = self.engine_eboa.insert_event_value(event_uuid1, values)
        self.engine_eboa.session.commit()
        assert exit_status["error"] == False
        assert exit_status["inserted"] == True

        event_values = self.query_eboa.get_event_values(event_uuids=[event_uuid1])

        assert len(event_values) == 1

        exit_status = self.engine_eboa.insert_event_value(event_uuid2, values)
        self.engine_eboa.session.commit()
        assert exit_status["error"] == False
        assert exit_status["inserted"] == True

        event_values = self.query_eboa.get_event_values(event_uuids=[event_uuid2])

        assert len(event_values) == 1

    def test_insert_duplicated_values_inside_event(self):
        """
        Method to test the insertion of duplicated values inside an event
        """

        data = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source.xml",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2018-07-05T02:07:03",
                       "validity_start": "2018-06-05T02:07:03",
                       "validity_stop": "2018-06-05T08:07:36"},
            "events": [{"gauge": {"name": "GAUGE_NAME",
                                  "system": "GAUGE_SYSTEM",
                                  "insertion_type": "SIMPLE_UPDATE"},
                        "start": "2018-06-05T02:07:03",
                        "stop": "2018-06-05T08:07:36"
                    },
                       {"gauge": {"name": "GAUGE_NAME",
                                  "system": "GAUGE_SYSTEM",
                                  "insertion_type": "SIMPLE_UPDATE"},
                        "start": "2018-06-05T02:07:03",
                        "stop": "2018-06-05T08:07:36",
                        "values": [{
                            "name": "values",
                            "type": "object",
                            "values": [
                                {"type": "text",
                                 "name": "TEXT",
                                 "value": "TEXT"},
                                {"type": "text",
                                 "name": "TEXT",
                                 "value": "TEXT"}
                            ]
                        }]
                    }]
        }]
        }
        returned_value = self.engine_eboa.treat_data(data)[0]["status"]

        assert returned_value == eboa_engine.exit_codes["DUPLICATED_VALUES"]["status"]

    def test_insert_alerts_by_ref(self):

        data = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source.json",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2018-07-05T02:07:03",
                       "validity_start": "2018-06-05T02:07:03",
                       "validity_stop": "2018-06-05T08:07:36"},
            "events": [{
                "link_ref": "EVENT_REF",
                "explicit_reference": "EXPLICIT_REFERENCE_EVENT",
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "SIMPLE_UPDATE"},
                "start": "2018-06-05T02:07:03",
                "stop": "2018-06-05T08:07:36"
            }],
            "alerts": [{
                "message": "Alert message",
                "generator": "test",
                "notification_time": "2018-06-05T08:07:36",
                "alert_cnf": {
                    "name": "alert_name1",
                    "severity": "critical",
                    "description": "Alert description",
                    "group": "alert_group"
                },
                "entity": {
                    "reference_mode": "by_ref",
                    "reference": "EVENT_REF",
                    "type": "event"
                }
            },{
                "message": "Alert message",
                "generator": "test",
                "notification_time": "2018-06-05T08:07:36",
                "alert_cnf": {
                    "name": "alert_name2",
                    "severity": "warning",
                    "description": "Alert description",
                    "group": "alert_group"
                },
                "entity": {
                    "reference_mode": "by_ref",
                    "reference": "EXPLICIT_REFERENCE_EVENT",
                    "type": "explicit_ref"
                }
            },{
                "message": "Alert message",
                "generator": "test",
                "notification_time": "2018-06-05T08:07:36",
                "alert_cnf": {
                    "name": "alert_name3",
                    "severity": "critical",
                    "description": "Alert description",
                    "group": "alert_group"
                },
                "entity": {
                    "reference_mode": "by_ref",
                    "reference": "source.json",
                    "type": "source"
                }
            }]
        }]
        }
        returned_value = self.engine_eboa.treat_data(data)[0]["status"]

        assert returned_value == eboa_engine.exit_codes["OK"]["status"]

        sources = self.session.query(Source).all()

        assert len(sources) == 1
        
        events = self.session.query(Event).all()

        assert len(events) == 1

        explicit_refs = self.session.query(ExplicitRef).all()

        assert len(explicit_refs) == 1

        event_alert = self.session.query(EventAlert).join(Alert).join(AlertGroup).filter(EventAlert.message == "Alert message",
                                                                                         EventAlert.generator == "test",
                                                                                         EventAlert.notification_time == "2018-06-05T08:07:36",
                                                                                         EventAlert.event_uuid == events[0].event_uuid,
                                                                                         Alert.name == "alert_name1",
                                                                                         Alert.description == "Alert description",
                                                                                         Alert.severity == 4,
                                                                                         AlertGroup.name == "alert_group").all()
        assert len(event_alert) == 1

        explicit_ref_alert = self.session.query(ExplicitRefAlert).join(Alert).join(AlertGroup).filter(ExplicitRefAlert.message == "Alert message",
                                                                                         ExplicitRefAlert.generator == "test",
                                                                                         ExplicitRefAlert.notification_time == "2018-06-05T08:07:36",
                                                                                         ExplicitRefAlert.explicit_ref_uuid == explicit_refs[0].explicit_ref_uuid,
                                                                                         Alert.name == "alert_name2",
                                                                                         Alert.description == "Alert description",
                                                                                         Alert.severity == 1,
                                                                                         AlertGroup.name == "alert_group").all()
        assert len(explicit_ref_alert) == 1

        source_alert = self.session.query(SourceAlert).join(Alert).join(AlertGroup).filter(SourceAlert.message == "Alert message",
                                                                                         SourceAlert.generator == "test",
                                                                                         SourceAlert.notification_time == "2018-06-05T08:07:36",
                                                                                         SourceAlert.source_uuid == sources[0].source_uuid,
                                                                                         Alert.name == "alert_name3",
                                                                                         Alert.description == "Alert description",
                                                                                         Alert.severity == 4,
                                                                                         AlertGroup.name == "alert_group").all()
        assert len(source_alert) == 1

    def test_insert_data_with_processing_duration_as_argument(self):

        data = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source.json",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2018-07-05T02:07:03",
                       "validity_start": "2018-06-05T02:07:03",
                       "validity_stop": "2018-06-05T08:07:36"}
        }]
        }
        returned_value = self.engine_eboa.treat_data(data, processing_duration = datetime.timedelta(seconds=3))[0]["status"]

        assert returned_value == eboa_engine.exit_codes["OK"]["status"]

        sources = self.session.query(Source).all()

        assert len(sources) == 1
        
        assert sources[0].processing_duration == datetime.timedelta(seconds=3)

    def test_insert_data_with_processing_duration_as_argument_wrong_value(self):

        data = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source.json",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2018-07-05T02:07:03",
                       "validity_start": "2018-06-05T02:07:03",
                       "validity_stop": "2018-06-05T08:07:36"}
        }]
        }
        returned_value = self.engine_eboa.treat_data(data, processing_duration = "Not a timedelta")[0]["status"]

        assert returned_value == eboa_engine.exit_codes["OK"]["status"]

        sources = self.session.query(Source).all()

        assert len(sources) == 1
        
        assert sources[0].processing_duration == None

    def test_insert_data_with_processing_duration_in_data(self):

        data = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source.json",
                       "reception_time": "2018-06-06T13:33:29",
                       "processing_duration": "6.0",
                       "generation_time": "2018-07-05T02:07:03",
                       "validity_start": "2018-06-05T02:07:03",
                       "validity_stop": "2018-06-05T08:07:36"}
        }]
        }
        returned_value = self.engine_eboa.treat_data(data, processing_duration = datetime.timedelta(seconds=3))[0]["status"]

        assert returned_value == eboa_engine.exit_codes["OK"]["status"]

        sources = self.session.query(Source).all()

        assert len(sources) == 1
        
        assert sources[0].processing_duration == datetime.timedelta(seconds=6)

    def test_insert_data_with_processing_duration_in_data_wrong_format(self):

        data = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source.json",
                       "reception_time": "2018-06-06T13:33:29",
                       "processing_duration": "not_a_convertable_float",
                       "generation_time": "2018-07-05T02:07:03",
                       "validity_start": "2018-06-05T02:07:03",
                       "validity_stop": "2018-06-05T08:07:36"}
        }]
        }
        returned_value = self.engine_eboa.treat_data(data)[0]["status"]

        assert returned_value == eboa_engine.exit_codes["FILE_NOT_VALID"]["status"]

        sources = self.session.query(Source).all()

        assert len(sources) == 0

    def test_insert_alerts_by_uuid_which_does_not_exist(self):

        id = uuid.uuid1(node = os.getpid(), clock_seq = random.getrandbits(14))
        
        data = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source.json",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2018-07-05T02:07:03",
                       "validity_start": "2018-06-05T02:07:03",
                       "validity_stop": "2018-06-05T08:07:36"},
            "events": [{
                "link_ref": "EVENT_REF",
                "explicit_reference": "EXPLICIT_REFERENCE_EVENT",
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "SIMPLE_UPDATE"},
                "start": "2018-06-05T02:07:03",
                "stop": "2018-06-05T08:07:36"
            }],
            "alerts": [{
                "message": "Alert message",
                "generator": "test",
                "notification_time": "2018-06-05T08:07:36",
                "alert_cnf": {
                    "name": "alert_name1",
                    "severity": "critical",
                    "description": "Alert description",
                    "group": "alert_group"
                },
                "entity": {
                    "reference_mode": "by_uuid",
                    "reference": str(id),
                    "type": "source"
                }
            },{
                "message": "Alert message",
                "generator": "test",
                "notification_time": "2018-06-05T08:07:36",
                "alert_cnf": {
                    "name": "alert_name1",
                    "severity": "critical",
                    "description": "Alert description",
                    "group": "alert_group"
                },
                "entity": {
                    "reference_mode": "by_ref",
                    "reference": "source.json",
                    "type": "source"
                }
            },{
                "message": "Alert message",
                "generator": "test",
                "notification_time": "2018-06-05T08:07:36",
                "alert_cnf": {
                    "name": "alert_name1",
                    "severity": "critical",
                    "description": "Alert description",
                    "group": "alert_group"
                },
                "entity": {
                    "reference_mode": "by_uuid",
                    "reference": str(id),
                    "type": "event"
                }
            },{
                "message": "Alert message",
                "generator": "test",
                "notification_time": "2018-06-05T08:07:36",
                "alert_cnf": {
                    "name": "alert_name1",
                    "severity": "critical",
                    "description": "Alert description",
                    "group": "alert_group"
                },
                "entity": {
                    "reference_mode": "by_ref",
                    "reference": "EVENT_REF",
                    "type": "event"
                }
            },{
                "message": "Alert message",
                "generator": "test",
                "notification_time": "2018-06-05T08:07:36",
                "alert_cnf": {
                    "name": "alert_name1",
                    "severity": "critical",
                    "description": "Alert description",
                    "group": "alert_group"
                },
                "entity": {
                    "reference_mode": "by_uuid",
                    "reference": str(id),
                    "type": "explicit_ref"
                }
            },{
                "message": "Alert message",
                "generator": "test",
                "notification_time": "2018-06-05T08:07:36",
                "alert_cnf": {
                    "name": "alert_name1",
                    "severity": "critical",
                    "description": "Alert description",
                    "group": "alert_group"
                },
                "entity": {
                    "reference_mode": "by_ref",
                    "reference": "EXPLICIT_REFERENCE_EVENT",
                    "type": "explicit_ref"
                }
            }]
        }]
        }
        returned_value = self.engine_eboa.treat_data(data)[0]["status"]

        assert returned_value == eboa_engine.exit_codes["OK"]["status"]

        sources = self.session.query(Source).all()

        assert len(sources) == 1

        events = self.session.query(Event).all()

        assert len(events) == 1

        explicit_refs = self.session.query(ExplicitRef).all()

        assert len(explicit_refs) == 1

        source_alerts = self.session.query(SourceAlert).all()

        assert len(source_alerts) == 1

        event_alerts = self.session.query(EventAlert).all()

        assert len(event_alerts) == 1

        explicit_ref_alerts = self.session.query(ExplicitRefAlert).all()

        assert len(explicit_ref_alerts) == 1

    def test_remove_deprecated_event_insert_and_erase_dim_signature_more_gauges_lower_generation_time(self):

        data = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                                  "exec": "exec",
                                  "version": "1.0"},
                "source": {"name": "source.xml",
                           "reception_time": "2018-06-06T13:33:29",
                           "generation_time": "2018-07-05T02:07:03",
                           "validity_start": "2018-06-05T02:07:03",
                           "validity_stop": "2018-06-05T08:07:36"},
                "events": [{
                    "explicit_reference": "EXPLICIT_REFERENCE_EVENT1",
                    "gauge": {"name": "GAUGE_NAME1",
                              "system": "GAUGE_SYSTEM1",
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
                                     "value": "20180712T00:00:00"}]
                }]
        }]}]}
        self.engine_eboa.data = data
        self.engine_eboa.treat_data()

        events_ddbb = self.session.query(Event).all()

        assert len(events_ddbb) == 1

        data = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                                  "exec": "exec",
                                  "version": "1.0"},
                "source": {"name": "source2.xml",
                           "reception_time": "2018-06-06T13:33:29",
                           "generation_time": "2018-07-04T02:07:03",
                           "validity_start": "2018-06-05T02:07:03",
                           "validity_stop": "2018-06-05T08:07:36"},
                "events": [{
                    "explicit_reference": "EXPLICIT_REFERENCE_EVENT1",
                    "gauge": {"name": "GAUGE_NAME1",
                              "system": "GAUGE_SYSTEM1",
                              "insertion_type": "INSERT_and_ERASE"},
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
                                     "value": "20180712T00:00:00"}]
                                }]
                },
                           {
                    "explicit_reference": "EXPLICIT_REFERENCE_EVENT2",
                    "gauge": {"name": "GAUGE_NAME2",
                              "system": "GAUGE_SYSTEM2",
                              "insertion_type": "INSERT_and_ERASE"},
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
                                     "value": "20180712T00:00:00"}]
                                }]
                },
                           {
                    "explicit_reference": "EXPLICIT_REFERENCE_EVENT3",
                    "gauge": {"name": "GAUGE_NAME3",
                              "system": "GAUGE_SYSTEM3",
                              "insertion_type": "INSERT_and_ERASE"},
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
                                     "value": "20180712T00:00:00"}]
                                }]
                           },
                           {
                    "explicit_reference": "EXPLICIT_REFERENCE_EVENT4",
                    "gauge": {"name": "GAUGE_NAME4",
                              "system": "GAUGE_SYSTEM4",
                              "insertion_type": "INSERT_and_ERASE"},
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
                                     "value": "20180712T00:00:00"}]
                                }]
                           }]}]}
        self.engine_eboa.data = data
        self.engine_eboa.treat_data()

        events_ddbb = self.session.query(Event).all()

        assert len(events_ddbb) == 4

    def test_remove_deprecated_event_insert_and_erase_with_priority_lower_priority_greater_generation_time(self):

        data = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                                  "exec": "exec",
                                  "version": "1.0"},
                "source": {"name": "source.xml",
                           "reception_time": "2018-06-06T13:33:29",
                           "generation_time": "2018-05-05T02:07:03",
                           "validity_start": "2018-06-05T02:07:03",
                           "validity_stop": "2018-06-05T08:07:36",
                           "priority": 40},
                "events": [{
                    "explicit_reference": "EXPLICIT_REFERENCE_EVENT1",
                    "gauge": {"name": "GAUGE_NAME1",
                              "system": "GAUGE_SYSTEM1",
                              "insertion_type": "INSERT_and_ERASE_with_PRIORITY"},
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
                                     "value": "20180712T00:00:00"}]
                }]
        }]}]}
        self.engine_eboa.data = data
        self.engine_eboa.treat_data()

        events_ddbb = self.session.query(Event).all()

        assert len(events_ddbb) == 1

        data = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                                  "exec": "exec",
                                  "version": "1.0"},
                "source": {"name": "source2.xml",
                           "reception_time": "2018-06-06T13:33:29",
                           "generation_time": "2018-07-04T02:07:03",
                           "validity_start": "2018-06-05T02:07:03",
                           "validity_stop": "2018-06-05T08:07:36",
                           "priority": 30},
                "events": [{
                    "explicit_reference": "EXPLICIT_REFERENCE_EVENT1",
                    "gauge": {"name": "GAUGE_NAME1",
                              "system": "GAUGE_SYSTEM1",
                              "insertion_type": "INSERT_and_ERASE_with_PRIORITY"},
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
                                     "value": "20180712T00:00:00"}]
                                }]
                }]}]}
        self.engine_eboa.data = data
        self.engine_eboa.treat_data()

        events_ddbb = self.session.query(Event).all()

        assert len(events_ddbb) == 1

        events_source_ddbb = self.session.query(Event).join(Source).filter(Source.name == "source.xml").all()

        assert len(events_source_ddbb) == 1

    def test_remove_deprecated_event_insert_and_erase_with_priority_lower_priority_greater_generation_time_no_defined_first_priority(self):

        data = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                                  "exec": "exec",
                                  "version": "1.0"},
                "source": {"name": "source.xml",
                           "reception_time": "2018-06-06T13:33:29",
                           "generation_time": "2018-05-05T02:07:03",
                           "validity_start": "2018-06-05T02:07:03",
                           "validity_stop": "2018-06-05T08:07:36"},
                "events": [{
                    "explicit_reference": "EXPLICIT_REFERENCE_EVENT1",
                    "gauge": {"name": "GAUGE_NAME1",
                              "system": "GAUGE_SYSTEM1",
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
                                     "value": "20180712T00:00:00"}]
                }]
        }]}]}
        self.engine_eboa.data = data
        self.engine_eboa.treat_data()

        events_ddbb = self.session.query(Event).all()

        assert len(events_ddbb) == 1

        data = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                                  "exec": "exec",
                                  "version": "1.0"},
                "source": {"name": "source2.xml",
                           "reception_time": "2018-06-06T13:33:29",
                           "generation_time": "2018-07-04T02:07:03",
                           "validity_start": "2018-06-05T02:07:03",
                           "validity_stop": "2018-06-05T08:07:36",
                           "priority": 30},
                "events": [{
                    "explicit_reference": "EXPLICIT_REFERENCE_EVENT1",
                    "gauge": {"name": "GAUGE_NAME1",
                              "system": "GAUGE_SYSTEM1",
                              "insertion_type": "INSERT_and_ERASE_with_PRIORITY"},
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
                                     "value": "20180712T00:00:00"}]
                                }]
                }]}]}
        self.engine_eboa.data = data
        self.engine_eboa.treat_data()

        events_ddbb = self.session.query(Event).all()

        assert len(events_ddbb) == 2

    def test_remove_deprecated_event_insert_and_erase_with_priority_more_gauges_lower_generation_time(self):

        data = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                                  "exec": "exec",
                                  "version": "1.0"},
                "source": {"name": "source.xml",
                           "reception_time": "2018-06-06T13:33:29",
                           "generation_time": "2018-07-05T02:07:03",
                           "validity_start": "2018-06-05T02:07:03",
                           "validity_stop": "2018-06-05T08:07:36",
                           "priority": 40,
                           "ingestion_completeness": {
                               "check": "true",
                               "message": ""
                           }},
                "events": [{
                    "explicit_reference": "EXPLICIT_REFERENCE_EVENT1",
                    "gauge": {"name": "GAUGE_NAME1",
                              "system": "GAUGE_SYSTEM1",
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
                                     "value": "20180712T00:00:00"}]
                }]
        }]}]}
        self.engine_eboa.data = data
        self.engine_eboa.treat_data()

        events_ddbb = self.session.query(Event).all()

        assert len(events_ddbb) == 1

        data = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                                  "exec": "exec",
                                  "version": "1.0"},
                "source": {"name": "source2.xml",
                           "reception_time": "2018-06-06T13:33:29",
                           "generation_time": "2018-07-04T02:07:03",
                           "validity_start": "2018-06-05T02:07:03",
                           "validity_stop": "2018-06-05T08:07:36",
                           "priority": 30},
                "events": [{
                    "explicit_reference": "EXPLICIT_REFERENCE_EVENT1",
                    "gauge": {"name": "GAUGE_NAME1",
                              "system": "GAUGE_SYSTEM1",
                              "insertion_type": "INSERT_and_ERASE_with_PRIORITY"},
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
                                     "value": "20180712T00:00:00"}]
                                }]
                },
                           {
                    "explicit_reference": "EXPLICIT_REFERENCE_EVENT2",
                    "gauge": {"name": "GAUGE_NAME2",
                              "system": "GAUGE_SYSTEM2",
                              "insertion_type": "INSERT_and_ERASE_with_PRIORITY"},
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
                                     "value": "20180712T00:00:00"}]
                                }]
                },
                           {
                    "explicit_reference": "EXPLICIT_REFERENCE_EVENT3",
                    "gauge": {"name": "GAUGE_NAME3",
                              "system": "GAUGE_SYSTEM3",
                              "insertion_type": "INSERT_and_ERASE_with_PRIORITY"},
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
                                     "value": "20180712T00:00:00"}]
                                }]
                           },
                           {
                    "explicit_reference": "EXPLICIT_REFERENCE_EVENT4",
                    "gauge": {"name": "GAUGE_NAME4",
                              "system": "GAUGE_SYSTEM4",
                              "insertion_type": "INSERT_and_ERASE_with_PRIORITY"},
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
                                     "value": "20180712T00:00:00"}]
                                }]
                           }]}]}
        self.engine_eboa.data = data
        self.engine_eboa.treat_data()

        events_ddbb = self.session.query(Event).all()

        assert len(events_ddbb) == 4

        events_source_ddbb = self.session.query(Event).join(Source).filter(Source.name == "source.xml").all()

        assert len(events_source_ddbb) == 1

    def test_insert_event_insert_and_erase_with_priority_source_duration_0_in_the_middle_to_remove(self):

        data = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source.json",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2018-07-05T02:07:03",
                       "validity_start": "2018-06-05T02:07:03",
                       "validity_stop": "2018-06-05T08:07:36",
                       "priority": 40},
            "events": [{
                "explicit_reference": "EXPLICIT_REFERENCE_EVENT",
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "INSERT_and_ERASE_with_PRIORITY"},
                "start": "2018-06-05T02:07:03",
                "stop": "2018-06-05T08:07:36"
            }]
        },{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source2.json",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2018-07-04T02:07:03",
                       "validity_start": "2018-06-05T04:07:03",
                       "validity_stop": "2018-06-05T04:07:03",
                       "priority": 30},
            "events": [{
                "explicit_reference": "EXPLICIT_REFERENCE_EVENT",
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "INSERT_and_ERASE_with_PRIORITY"},
                "start": "2018-06-05T04:07:03",
                "stop": "2018-06-05T04:07:03"
            }]
        }]
        }
        self.engine_eboa.treat_data(data)

        events = self.session.query(Event).join(Gauge).join(Source).filter(Gauge.name == "GAUGE_NAME", 
                                                                           Gauge.system == "GAUGE_SYSTEM",
                                                                           Event.start == "2018-06-05T02:07:03",
                                                                           Event.stop == "2018-06-05T08:07:36",
                                                                           Source.name == "source.json").all()
        assert len(events) == 1

        events = self.session.query(Event).all()

        assert len(events) == 1

    def test_insert_event_insert_and_erase_with_priority_source_duration_0_in_the_middle_to_stay(self):

        data = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source.json",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2018-07-05T02:07:03",
                       "validity_start": "2018-06-05T02:07:03",
                       "validity_stop": "2018-06-05T08:07:36",
                       "priority": 30},
            "events": [{
                "explicit_reference": "EXPLICIT_REFERENCE_EVENT",
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "INSERT_and_ERASE_with_PRIORITY"},
                "start": "2018-06-05T02:07:03",
                "stop": "2018-06-05T08:07:36"
            }]
        },{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source2.json",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2018-08-04T02:07:03",
                       "validity_start": "2018-06-05T04:07:03",
                       "validity_stop": "2018-06-05T04:07:03",
                       "priority": 40},
            "events": [{
                "explicit_reference": "EXPLICIT_REFERENCE_EVENT",
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "INSERT_and_ERASE_with_PRIORITY"},
                "start": "2018-06-05T04:07:03",
                "stop": "2018-06-05T04:07:03"
            }]
        }]
        }
        self.engine_eboa.treat_data(data)

        events = self.session.query(Event).join(Gauge).join(Source).filter(Gauge.name == "GAUGE_NAME", 
                                                                           Gauge.system == "GAUGE_SYSTEM",
                                                                           Event.start == "2018-06-05T02:07:03",
                                                                           Event.stop == "2018-06-05T04:07:03",
                                                                           Source.name == "source.json").all()
        assert len(events) == 1

        events = self.session.query(Event).join(Gauge).join(Source).filter(Gauge.name == "GAUGE_NAME", 
                                                                           Gauge.system == "GAUGE_SYSTEM",
                                                                           Event.start == "2018-06-05T04:07:03",
                                                                           Event.stop == "2018-06-05T08:07:36",
                                                                           Source.name == "source.json").all()
        assert len(events) == 1

        events = self.session.query(Event).join(Gauge).join(Source).filter(Gauge.name == "GAUGE_NAME", 
                                                                           Gauge.system == "GAUGE_SYSTEM",
                                                                           Event.start == "2018-06-05T04:07:03",
                                                                           Event.stop == "2018-06-05T04:07:03",
                                                                           Source.name == "source2.json").all()
        assert len(events) == 1

        events = self.session.query(Event).all()

        assert len(events) == 3
        
        events = self.session.query(Event).filter(Event.visible == True).all()

        assert len(events) == 3

    def test_insert_event_insert_and_erase_with_priority_partial_remove_with_links(self):

        data1 = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source.json",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2018-07-07T02:07:03",
                       "validity_start": "2018-06-05T02:07:03",
                       "validity_stop": "2018-06-05T08:07:36",
                       "priority": 10},
            "events": [{
                "explicit_reference": "EXPLICIT_REFERENCE_EVENT",
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "SIMPLE_UPDATE"},
                "start": "2018-06-05T04:07:03",
                "stop": "2018-06-05T06:07:36"
            }]
        }]
        }
        exit_status = self.engine_eboa.treat_data(data1)

        assert len([item for item in exit_status if item["status"] != eboa_engine.exit_codes["OK"]["status"]]) == 0

        events = self.session.query(Event).all()

        assert len(events) == 1

        event_uuid1 = events[0].event_uuid

        data2 = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source2.json",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2018-08-01T02:07:03",
                       "validity_start": "2018-08-01T02:07:03",
                       "validity_stop": "2018-08-05T06:07:36",
                       "priority": 20},
            "events": [{
                "links": [{
                    "link": str(event_uuid1),
                    "link_mode": "by_uuid",
                    "name": "EVENT_LINK_NAME"
                }],
                "explicit_reference": "EXPLICIT_REFERENCE_EVENT",
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "SIMPLE_UPDATE"},
                "start": "2018-08-01T02:07:03",
                "stop": "2018-08-01T012:07:36"
            }]
        }]
        }
        exit_status = self.engine_eboa.treat_data(data2)

        assert len([item for item in exit_status if item["status"] != eboa_engine.exit_codes["OK"]["status"]]) == 0

        events = self.session.query(Event).all()

        assert len(events) == 2

        events = self.session.query(Event).join(Source).filter(Source.name == "source2.json").all()

        assert len(events) == 1

        event_uuid2 = events[0].event_uuid

        data3 = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source3.json",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2018-09-06T02:07:03",
                       "validity_start": "2018-08-02T04:07:03",
                       "validity_stop": "2018-08-06T10:07:36",
                       "priority": 30},
            "events": [{
                "links": [{
                    "link": str(event_uuid1),
                    "link_mode": "by_uuid",
                    "name": "EVENT_LINK_NAME"
                }],
                "explicit_reference": "EXPLICIT_REFERENCE_EVENT",
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "INSERT_and_ERASE_with_PRIORITY"},
                "start": "2018-08-05T04:07:03",
                "stop": "2018-08-05T10:07:36"
            }]
        }]
        }
        
        exit_status = self.engine_eboa.treat_data(data3)

        assert len([item for item in exit_status if item["status"] != eboa_engine.exit_codes["OK"]["status"]]) == 0

        events = self.session.query(Event).join(Source).filter(Source.name == "source3.json").all()

        assert len(events) == 1

        event_uuid2 = events[0].event_uuid

        data4 = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source4.json",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2018-10-03T02:07:03",
                       "validity_start": "2018-08-03T02:07:03",
                       "validity_stop": "2018-08-07T12:07:36",
                       "priority": 40},
            "events": [{
                "links": [{
                    "link": str(event_uuid1),
                    "link_mode": "by_uuid",
                    "name": "EVENT_LINK_NAME"
                }],
                "explicit_reference": "EXPLICIT_REFERENCE_EVENT",
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "INSERT_and_ERASE_with_PRIORITY"},
                "start": "2018-08-05T02:07:03",
                "stop": "2018-08-05T012:07:36"
            }]
        }]
        }
        exit_status = self.engine_eboa.treat_data(data4)

        assert len([item for item in exit_status if item["status"] != eboa_engine.exit_codes["OK"]["status"]]) == 0
        
        events = self.session.query(Event).join(Gauge).join(Source).filter(Gauge.name == "GAUGE_NAME", 
                                                                           Gauge.system == "GAUGE_SYSTEM",
                                                                           Event.start == "2018-06-05T04:07:03",
                                                                           Event.stop == "2018-06-05T06:07:36",
                                                                           Source.name == "source.json").all()
        assert len(events) == 1

        events = self.session.query(Event).join(Gauge).join(Source).filter(Gauge.name == "GAUGE_NAME", 
                                                                           Gauge.system == "GAUGE_SYSTEM",
                                                                           Event.start == "2018-08-01T02:07:03",
                                                                           Event.stop == "2018-08-01T12:07:36",
                                                                           Source.name == "source2.json").all()
        assert len(events) == 1

        event_uuid2 = events[0].event_uuid

        events = self.session.query(Event).join(Gauge).join(Source).filter(Source.name == "source3.json").all()

        assert len(events) == 0

        events = self.session.query(Event).join(Gauge).join(Source).filter(Gauge.name == "GAUGE_NAME",
                                                                           Gauge.system == "GAUGE_SYSTEM",
                                                                           Event.start == "2018-08-05T02:07:03",
                                                                           Event.stop == "2018-08-05T12:07:36",
                                                                           Source.name == "source4.json").all()
        assert len(events) == 1

        event_uuid3 = events[0].event_uuid

        events = self.session.query(Event).all()

        assert len(events) == 3

        event_links = self.session.query(EventLink).all()

        assert len(event_links) == 2

        event_links = self.session.query(EventLink).filter(EventLink.name == "EVENT_LINK_NAME",
                                                           EventLink.event_uuid == event_uuid1,
                                                           EventLink.event_uuid_link == event_uuid2).all()

        assert len(event_links) == 1

        event_links = self.session.query(EventLink).filter(EventLink.name == "EVENT_LINK_NAME",
                                                           EventLink.event_uuid == event_uuid1,
                                                           EventLink.event_uuid_link == event_uuid3).all()

        assert len(event_links) == 1

    def test_remove_deprecated_event_insert_and_erase_with_priority_same_period(self):

        data = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                                  "exec": "exec",
                                  "version": "1.0"},
                "source": {"name": "source.xml",
                           "reception_time": "2018-06-06T13:33:29",
                           "generation_time": "2018-07-05T02:07:03",
                           "validity_start": "2018-06-05T02:07:03",
                           "validity_stop": "2018-06-05T08:07:36",
                           "priority": 30},
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
        }]}

        self.engine_eboa.data = data
        self.engine_eboa.treat_data()

        events_ddbb = self.session.query(Event).all()

        assert len(events_ddbb) == 1
        
        data = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                                  "exec": "exec",
                                  "version": "1.0"},
                "source": {"name": "source2.xml",
                           "reception_time": "2018-06-06T13:33:29",
                           "generation_time": "2018-07-05T02:07:04",
                           "validity_start": "2018-06-05T02:07:03",
                           "validity_stop": "2018-06-05T08:07:36",
                           "priority": 40},
                "events": [{
                    "explicit_reference": "EXPLICIT_REFERENCE_EVENT2",
                    "gauge": {"name": "GAUGE_NAME",
                              "system": "GAUGE_SYSTEM",
                              "insertion_type": "INSERT_and_ERASE_with_PRIORITY"},
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
        }]}
        self.engine_eboa.data = data
        self.engine_eboa.treat_data()

        filtered_events_ddbb = self.session.query(Event).join(Source).filter(Source.name == "source2.xml",
                                                      Event.visible == True).all()

        assert len(filtered_events_ddbb) == 1

        events_ddbb = self.session.query(Event).all()

        assert len(events_ddbb) == 1

        values_ddbb = self.query_eboa.get_event_values()

        assert len(values_ddbb) == 8

    def test_insert_event_insert_and_erase_at_dim_signature_level_create_event_max_generation_time_not_ending_on_period(self):

        data1 = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source.json",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2020-07-07T02:07:03",
                       "validity_start": "2018-06-05T02:07:03",
                       "validity_stop": "2018-06-05T04:07:36"},
            "events": [{
                "explicit_reference": "EXPLICIT_REFERENCE_EVENT",
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "SIMPLE_UPDATE"},
                "start": "2018-06-05T02:07:03",
                "stop": "2018-06-05T04:07:36"
            }]
        }]
        }
        exit_status = self.engine_eboa.treat_data(data1)

        assert len([item for item in exit_status if item["status"] != eboa_engine.exit_codes["OK"]["status"]]) == 0

        events = self.session.query(Event).all()

        assert len(events) == 1

        event_uuid1 = events[0].event_uuid

        data2 = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source2.json",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2019-08-01T02:07:03",
                       "validity_start": "2018-06-05T03:07:03",
                       "validity_stop": "2018-06-05T08:07:36"},
            "events": [{
                "explicit_reference": "EXPLICIT_REFERENCE_EVENT",
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "SIMPLE_UPDATE"},
                "start": "2018-06-05T03:07:03",
                "stop": "2018-06-05T008:07:36"
            }]
        }]
        }
        exit_status = self.engine_eboa.treat_data(data2)

        assert len([item for item in exit_status if item["status"] != eboa_engine.exit_codes["OK"]["status"]]) == 0

        events = self.session.query(Event).all()

        assert len(events) == 2

        events = self.session.query(Event).join(Source).filter(Source.name == "source2.json").all()

        assert len(events) == 1

        event_uuid2 = events[0].event_uuid

        data3 = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source3.json",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2016-09-06T02:07:03",
                       "validity_start": "2018-06-05T04:07:20",
                       "validity_stop": "2018-06-05T06:07:36"}
        }]
        }
        
        exit_status = self.engine_eboa.treat_data(data3)

        assert len([item for item in exit_status if item["status"] != eboa_engine.exit_codes["OK"]["status"]]) == 0

        data4 = {"operations": [{
            "mode": "insert_and_erase",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source4.json",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2016-10-03T02:07:03",
                       "validity_start": "2018-06-05T02:07:03",
                       "validity_stop": "2018-06-05T08:07:36"}
        }]
        }
        exit_status = self.engine_eboa.treat_data(data4)

        assert len([item for item in exit_status if item["status"] != eboa_engine.exit_codes["OK"]["status"]]) == 0

        events = self.session.query(Event).all()

        assert len(events) == 2
        
        events = self.session.query(Event).join(Gauge).join(Source).filter(Gauge.name == "GAUGE_NAME", 
                                                                           Gauge.system == "GAUGE_SYSTEM",
                                                                           Event.start == "2018-06-05T02:07:03",
                                                                           Event.stop == "2018-06-05T04:07:36",
                                                                           Source.name == "source.json").all()
        assert len(events) == 1

        events = self.session.query(Event).join(Gauge).join(Source).filter(Gauge.name == "GAUGE_NAME", 
                                                                           Gauge.system == "GAUGE_SYSTEM",
                                                                           Event.start == "2018-06-05T04:07:36",
                                                                           Event.stop == "2018-06-05T08:07:36",
                                                                           Source.name == "source2.json").all()
        assert len(events) == 1

    def test_insert_event_insert_and_erase_at_dim_signature_level_remove_events_starting_before_validity_start(self):

        data1 = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source.json",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2020-07-07T02:07:03",
                       "validity_start": "2018-06-05T04:07:03",
                       "validity_stop": "2018-06-05T04:07:36"},
            "events": [{
                "explicit_reference": "EXPLICIT_REFERENCE_EVENT",
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "SIMPLE_UPDATE"},
                "start": "2018-06-05T04:07:03",
                "stop": "2018-06-05T04:07:36"
            }]
        }]
        }
        exit_status = self.engine_eboa.treat_data(data1)

        assert len([item for item in exit_status if item["status"] != eboa_engine.exit_codes["OK"]["status"]]) == 0

        events = self.session.query(Event).all()

        assert len(events) == 1

        event_uuid1 = events[0].event_uuid

        data2 = {"operations": [{
            "mode": "insert_and_erase",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source2.json",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2019-08-01T02:07:03",
                       "validity_start": "2018-06-05T03:07:03",
                       "validity_stop": "2018-06-05T08:07:36"},
            "events": [{
                "explicit_reference": "EXPLICIT_REFERENCE_EVENT",
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "SIMPLE_UPDATE"},
                "start": "2018-06-05T03:07:03",
                "stop": "2018-06-05T004:07:36"
            }]
        }]
        }
        exit_status = self.engine_eboa.treat_data(data2)

        assert len([item for item in exit_status if item["status"] != eboa_engine.exit_codes["OK"]["status"]]) == 0

        events = self.session.query(Event).all()

        assert len(events) == 2

        events = self.session.query(Event).join(Gauge).join(Source).filter(Gauge.name == "GAUGE_NAME", 
                                                                           Gauge.system == "GAUGE_SYSTEM",
                                                                           Event.start == "2018-06-05T03:07:03",
                                                                           Event.stop == "2018-06-05T04:07:03",
                                                                           Source.name == "source2.json").all()
        assert len(events) == 1

        data3 = {"operations": [{
            "mode": "insert_and_erase",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source3.json",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2019-09-01T02:07:03",
                       "validity_start": "2018-06-05T03:07:03",
                       "validity_stop": "2018-06-05T08:07:36"},
            "events": [{
                "explicit_reference": "EXPLICIT_REFERENCE_EVENT",
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "SIMPLE_UPDATE"},
                "start": "2018-06-05T03:07:03",
                "stop": "2018-06-05T006:07:36"
            }]
        }]
        }
        exit_status = self.engine_eboa.treat_data(data3)

        assert len([item for item in exit_status if item["status"] != eboa_engine.exit_codes["OK"]["status"]]) == 0

        events = self.session.query(Event).all()

        assert len(events) == 3

        events = self.session.query(Event).join(Gauge).join(Source).filter(Gauge.name == "GAUGE_NAME", 
                                                                           Gauge.system == "GAUGE_SYSTEM",
                                                                           Event.start == "2018-06-05T03:07:03",
                                                                           Event.stop == "2018-06-05T04:07:03",
                                                                           Source.name == "source3.json").all()
        assert len(events) == 1

        events = self.session.query(Event).join(Gauge).join(Source).filter(Gauge.name == "GAUGE_NAME", 
                                                                           Gauge.system == "GAUGE_SYSTEM",
                                                                           Event.start == "2018-06-05T04:07:36",
                                                                           Event.stop == "2018-06-05T06:07:36",
                                                                           Source.name == "source3.json").all()
        assert len(events) == 1

    def test_insert_event_insert_and_erase_at_dim_signature_level_remove_events_starting_before_validity_start_not_ending_on_period(self):

        data1 = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source.json",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2020-07-07T02:07:03",
                       "validity_start": "2018-06-05T04:07:03",
                       "validity_stop": "2018-06-05T04:07:36"},
            "events": [{
                "explicit_reference": "EXPLICIT_REFERENCE_EVENT",
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "SIMPLE_UPDATE"},
                "start": "2018-06-05T04:07:03",
                "stop": "2018-06-05T04:07:36"
            }]
        }]
        }
        exit_status = self.engine_eboa.treat_data(data1)

        assert len([item for item in exit_status if item["status"] != eboa_engine.exit_codes["OK"]["status"]]) == 0

        events = self.session.query(Event).all()

        assert len(events) == 1

        event_uuid1 = events[0].event_uuid

        data2 = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source2.json",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2019-08-01T02:07:03",
                       "validity_start": "2018-06-05T03:07:03",
                       "validity_stop": "2018-06-05T08:07:36"},
            "events": [{
                "explicit_reference": "EXPLICIT_REFERENCE_EVENT",
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "SIMPLE_UPDATE"},
                "start": "2018-06-05T04:07:20",
                "stop": "2018-06-05T008:07:36"
            }]
        }]
        }
        exit_status = self.engine_eboa.treat_data(data2)

        assert len([item for item in exit_status if item["status"] != eboa_engine.exit_codes["OK"]["status"]]) == 0

        data3 = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source3.json",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2016-08-01T02:07:03",
                       "validity_start": "2018-06-05T04:07:36",
                       "validity_stop": "2018-06-05T05:07:36"},
            "events": [{
                "explicit_reference": "EXPLICIT_REFERENCE_EVENT",
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "SIMPLE_UPDATE"},
                "start": "2018-06-05T04:07:36",
                "stop": "2018-06-05T005:07:36"
            }]
        }]
        }
        exit_status = self.engine_eboa.treat_data(data3)

        assert len([item for item in exit_status if item["status"] != eboa_engine.exit_codes["OK"]["status"]]) == 0

        data4 = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source4.json",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2020-08-01T02:07:03",
                       "validity_start": "2018-06-05T06:07:36",
                       "validity_stop": "2018-06-05T08:07:36"}
        }]
        }
        exit_status = self.engine_eboa.treat_data(data4)

        assert len([item for item in exit_status if item["status"] != eboa_engine.exit_codes["OK"]["status"]]) == 0

        data5 = {"operations": [{
            "mode": "insert_and_erase",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source5.json",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2016-10-03T02:07:03",
                       "validity_start": "2018-06-05T02:07:03",
                       "validity_stop": "2018-06-05T08:07:36"},
            "events": [{
                "explicit_reference": "EXPLICIT_REFERENCE_EVENT",
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "SIMPLE_UPDATE"},
                "start": "2018-06-05T04:07:20",
                "stop": "2018-06-05T005:07:36"
            }]
        }]
        }
        exit_status = self.engine_eboa.treat_data(data5)

        assert len([item for item in exit_status if item["status"] != eboa_engine.exit_codes["OK"]["status"]]) == 0

        events = self.session.query(Event).all()

        assert len(events) == 2
        
        events = self.session.query(Event).join(Gauge).join(Source).filter(Gauge.name == "GAUGE_NAME", 
                                                                           Gauge.system == "GAUGE_SYSTEM",
                                                                           Event.start == "2018-06-05T04:07:03",
                                                                           Event.stop == "2018-06-05T04:07:36",
                                                                           Source.name == "source.json").all()
        assert len(events) == 1

        events = self.session.query(Event).join(Gauge).join(Source).filter(Gauge.name == "GAUGE_NAME", 
                                                                           Gauge.system == "GAUGE_SYSTEM",
                                                                           Event.start == "2018-06-05T04:07:36",
                                                                           Event.stop == "2018-06-05T06:07:36",
                                                                           Source.name == "source2.json").all()
        assert len(events) == 1

        
        
    def test_remove_deprecated_event_insert_and_erase_with_priority_one_starts_before_links(self):

        self.engine_eboa._initialize_context_insert_data()
        data1 = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                                  "exec": "exec",
                                  "version": "1.0"},
                "source": {"name": "source.xml",
                           "reception_time": "2018-06-06T13:33:29",
                           "generation_time": "2018-07-05T02:07:03",
                           "validity_start": "2018-06-04T02:07:00",
                           "validity_stop": "2018-06-05T08:07:36",
                           "priority": 30},
                "events": [{
                    "link_ref": "EVENT_LINK1",
                    "explicit_reference": "EXPLICIT_REFERENCE_EVENT1",
                    "gauge": {"name": "GAUGE_NAME",
                              "system": "GAUGE_SYSTEM",
                              "insertion_type": "SIMPLE_UPDATE"},
                    "start": "2018-06-04T02:07:00",
                    "stop": "2018-06-05T08:07:36",
                    "links": [{
                        "link": "EVENT_LINK2",
                        "link_mode": "by_ref",
                        "name": "EVENT_LINK_NAME1",
                        "back_ref": "LINK_BACK_REF_NAME"
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
                               "link_ref": "EVENT_LINK2",
                               "explicit_reference": "EXPLICIT_REFERENCE_EVENT2",
                               "gauge": {"name": "GAUGE_NAME",
                                         "system": "GAUGE_SYSTEM",
                                         "insertion_type": "SIMPLE_UPDATE"},
                               "start": "2018-06-04T02:07:02",
                               "stop": "2018-06-05T08:07:36",
                               "links": [{
                                   "link": "EVENT_LINK1",
                                   "link_mode": "by_ref",
                                   "name": "EVENT_LINK_NAME2",
                                   "back_ref": "LINK_BACK_REF_NAME"
                               }]},
                           {
                               "explicit_reference": "EXPLICIT_REFERENCE_EVENT3",
                               "gauge": {"name": "GAUGE_NAME",
                                         "system": "GAUGE_SYSTEM",
                                         "insertion_type": "SIMPLE_UPDATE"},
                               "start": "2018-06-04T02:07:03",
                               "stop": "2018-06-04T04:07:36",
                               "links": [{
                                   "link": "EVENT_LINK1",
                                   "link_mode": "by_ref",
                                   "name": "EVENT_LINK_NAME3",
                                   "back_ref": "LINK_BACK_REF_NAME"
                               }]}]
        }]}
        self.engine_eboa.data = data1
        self.engine_eboa.treat_data()

        data2 = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                                  "exec": "exec",
                                  "version": "1.0"},
                "source": {"name": "source2.xml",
                           "reception_time": "2018-06-06T13:33:29",
                           "generation_time": "2018-07-05T02:07:04",
                           "validity_start": "2018-06-05T02:07:03",
                           "validity_stop": "2018-06-05T08:07:36",
                           "priority": 40},
                "events": [{
                    "explicit_reference": "EXPLICIT_REFERENCE_EVENT4",
                    "gauge": {"name": "GAUGE_NAME",
                              "system": "GAUGE_SYSTEM",
                              "insertion_type": "INSERT_and_ERASE_with_PRIORITY"},
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
        }]}

        self.engine_eboa.data = data2
        self.engine_eboa.treat_data()

        filtered_events_ddbb = self.session.query(Event).join(Source).filter(Event.start == "2018-06-05T02:07:03",
                                                      Event.stop == "2018-06-05T08:07:36",
                                                      Source.name == "source2.xml",
                                                      Event.visible == True).all()

        assert len(filtered_events_ddbb) == 1

        filtered_events_ddbb = self.session.query(Event).join(Source).filter(Event.start == "2018-06-04T02:07:00",
                                                      Event.stop == "2018-06-05T02:07:03",
                                                      Source.name == "source.xml",
                                                      Event.visible == True).all()

        assert len(filtered_events_ddbb) == 1

        filtered_events_ddbb = self.session.query(Event).join(Source).filter(Event.start == "2018-06-04T02:07:02",
                                                      Event.stop == "2018-06-05T02:07:03",
                                                      Source.name == "source.xml",
                                                      Event.visible == True).all()

        assert len(filtered_events_ddbb) == 1

        filtered_events_ddbb = self.session.query(Event).join(Source).filter(Event.start == "2018-06-04T02:07:03",
                                                      Event.stop == "2018-06-04T04:07:36",
                                                      Source.name == "source.xml",
                                                      Event.visible == True).all()

        assert len(filtered_events_ddbb) == 1

        events_ddbb = self.session.query(Event).all()

        assert len(events_ddbb) == 4

        values_ddbb = self.query_eboa.get_event_values()

        assert len(values_ddbb) == 16

        links_ddbb = self.query_eboa.get_event_links()

        assert len(links_ddbb) == 6

        events_with_links = self.session.query(Event).join(Source).filter(Source.name == "source.xml").order_by(Event.start).all()
        
        rest_of_event_uuids = [events_with_links[1].event_uuid, events_with_links[2].event_uuid]
        
        event_links_ddbb = self.query_eboa.get_event_links(event_uuids = {"filter": rest_of_event_uuids, "op": "in"}, event_uuid_links = {"filter": [events_with_links[0].event_uuid], "op": "in"})

        assert len(event_links_ddbb) == 3

        rest_of_event_uuids = [events_with_links[0].event_uuid, events_with_links[2].event_uuid]
        
        event_links_ddbb = self.query_eboa.get_event_links(event_uuids = {"filter": rest_of_event_uuids, "op": "in"}, event_uuid_links = {"filter": [events_with_links[1].event_uuid], "op": "in"})

        assert len(event_links_ddbb) == 2

        rest_of_event_uuids = [events_with_links[0].event_uuid, events_with_links[1].event_uuid]
        
        event_links_ddbb = self.query_eboa.get_event_links(event_uuids = {"filter": rest_of_event_uuids, "op": "in"}, event_uuid_links = {"filter": [events_with_links[2].event_uuid], "op": "in"})

        assert len(event_links_ddbb) == 1

    def test_remove_deprecated_event_insert_and_erase_with_priority_only_remains_last_inserted_source(self):

        data1 = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                                  "exec": "exec",
                                  "version": "1.0"},
                "source": {"name": "source.xml",
                           "reception_time": "2018-06-06T13:33:29",
                           "generation_time": "2018-07-05T02:07:03",
                           "validity_start": "2018-06-04T02:07:04",
                           "validity_stop": "2018-06-05T08:07:36",
                           "priority": 30},
                "events": [{
                               "explicit_reference": "EXPLICIT_REFERENCE_EVENT3",
                               "gauge": {"name": "GAUGE_NAME",
                                         "system": "GAUGE_SYSTEM",
                                         "insertion_type": "SIMPLE_UPDATE"},
                               "start": "2018-06-04T02:07:04",
                               "stop": "2018-06-04T04:07:36",
                               "links": [{
                                   "link": "EVENT_LINK1",
                                   "link_mode": "by_ref",
                                   "name": "EVENT_LINK_NAME4",
                                   "back_ref": "LINK_BACK_REF_NAME"
                               }]},
                           {
                    "link_ref": "EVENT_LINK1",
                    "explicit_reference": "EXPLICIT_REFERENCE_EVENT1",
                    "gauge": {"name": "GAUGE_NAME",
                              "system": "GAUGE_SYSTEM",
                              "insertion_type": "SIMPLE_UPDATE"},
                    "start": "2018-06-05T02:07:04",
                    "stop": "2018-06-05T08:07:36",
                    "links": [{
                        "link": "EVENT_LINK2",
                        "link_mode": "by_ref",
                        "name": "EVENT_LINK_NAME1",
                        "back_ref": "LINK_BACK_REF_NAME"
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
                               "link_ref": "EVENT_LINK2",
                               "explicit_reference": "EXPLICIT_REFERENCE_EVENT2",
                               "gauge": {"name": "GAUGE_NAME",
                                         "system": "GAUGE_SYSTEM",
                                         "insertion_type": "SIMPLE_UPDATE"},
                               "start": "2018-06-05T02:07:04",
                               "stop": "2018-06-05T08:07:36",
                               "links": [{
                                   "link": "EVENT_LINK1",
                                   "link_mode": "by_ref",
                                   "name": "EVENT_LINK_NAME2",
                                   "back_ref": "LINK_BACK_REF_NAME"
                               }]},
                           {
                               "explicit_reference": "EXPLICIT_REFERENCE_EVENT3",
                               "gauge": {"name": "GAUGE_NAME",
                                         "system": "GAUGE_SYSTEM",
                                         "insertion_type": "SIMPLE_UPDATE"},
                               "start": "2018-06-05T02:07:04",
                               "stop": "2018-06-05T04:07:36",
                               "links": [{
                                   "link": "EVENT_LINK1",
                                   "link_mode": "by_ref",
                                   "name": "EVENT_LINK_NAME3",
                                   "back_ref": "LINK_BACK_REF_NAME"
                               }]}]
        }]}

        self.engine_eboa.treat_data(data1)

        data2 = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                                  "exec": "exec",
                                  "version": "1.0"},
                "source": {"name": "source2.xml",
                           "reception_time": "2018-06-06T13:33:29",
                           "generation_time": "2018-07-05T02:07:04",
                           "validity_start": "2018-06-05T02:07:03",
                           "validity_stop": "2018-06-05T08:07:36",
                           "priority": 40},
                "events": [{
                    "explicit_reference": "EXPLICIT_REFERENCE_EVENT4",
                    "gauge": {"name": "GAUGE_NAME",
                              "system": "GAUGE_SYSTEM",
                              "insertion_type": "INSERT_and_ERASE_with_PRIORITY"},
                    "start": "2018-06-05T02:07:03",
                    "stop": "2018-06-05T08:07:36"
                }]
            }]}
        self.engine_eboa.treat_data(data2)
        
        events = self.query_eboa.get_events()

        assert len(events) == 2

        event_links = self.session.query(EventLink).all()

        assert len(event_links) == 0

        event_values = self.query_eboa.get_event_values()

        assert len(event_values) == 0

    def test_remove_deprecated_event_insert_and_erase_with_priority_long_event_cut(self):

        data1 = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                                  "exec": "exec",
                                  "version": "1.0"},
                "source": {"name": "source.json",
                           "reception_time": "2018-06-06T13:33:29",
                           "generation_time": "2018-07-06T02:07:03",
                           "validity_start": "2018-06-05T03:07:03",
                           "validity_stop": "2018-06-05T04:07:36",
                           "priority": 40},
                "events": [{
                               "explicit_reference": "EXPLICIT_REFERENCE_EVENT3",
                               "gauge": {"name": "GAUGE_NAME",
                                         "system": "GAUGE_SYSTEM",
                                         "insertion_type": "SIMPLE_UPDATE"},
                               "start": "2018-06-05T03:07:03",
                               "stop": "2018-06-05T04:07:36"
                }]
            }]}

        exit_status = self.engine_eboa.treat_data(data1)

        assert len([item for item in exit_status if item["status"] != eboa_engine.exit_codes["OK"]["status"]]) == 0

        data3 = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                                  "exec": "exec",
                                  "version": "1.0"},
                "source": {"name": "source3.json",
                           "reception_time": "2018-06-06T13:33:29",
                           "generation_time": "2018-07-04T02:07:03",
                           "validity_start": "2018-06-05T06:07:03",
                           "validity_stop": "2018-06-05T07:07:36",
                           "priority": 20},
                "events": [{
                               "explicit_reference": "EXPLICIT_REFERENCE_EVENT3",
                               "gauge": {"name": "GAUGE_NAME",
                                         "system": "GAUGE_SYSTEM",
                                         "insertion_type": "SIMPLE_UPDATE"},
                               "start": "2018-06-05T06:07:03",
                               "stop": "2018-06-05T07:07:36"
                }]
            }]}

        exit_status = self.engine_eboa.treat_data(data3)

        assert len([item for item in exit_status if item["status"] != eboa_engine.exit_codes["OK"]["status"]]) == 0

        data2 = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                                  "exec": "exec",
                                  "version": "1.0"},
                "source": {"name": "source2.json",
                           "reception_time": "2018-06-06T13:33:29",
                           "generation_time": "2018-07-05T02:07:04",
                           "validity_start": "2018-06-05T02:07:03",
                           "validity_stop": "2018-06-05T08:07:36",
                           "priority": 30},
                "events": [{
                    "explicit_reference": "EXPLICIT_REFERENCE_EVENT4",
                    "gauge": {"name": "GAUGE_NAME",
                              "system": "GAUGE_SYSTEM",
                              "insertion_type": "INSERT_and_ERASE_with_PRIORITY"},
                    "start": "2018-06-05T02:07:03",
                    "stop": "2018-06-05T08:07:36"
                }]
            }]}

        exit_status = self.engine_eboa.treat_data(data2)

        assert len([item for item in exit_status if item["status"] != eboa_engine.exit_codes["OK"]["status"]]) == 0
        
        events = self.session.query(Event).join(Gauge).join(Source).filter(Gauge.name == "GAUGE_NAME", 
                                                                           Gauge.system == "GAUGE_SYSTEM",
                                                                           Event.start == "2018-06-05T3:07:03",
                                                                           Event.stop == "2018-06-05T04:07:36",
                                                                           Source.name == "source.json").all()
        assert len(events) == 1

        events = self.session.query(Event).join(Gauge).join(Source).filter(Gauge.name == "GAUGE_NAME", 
                                                                           Gauge.system == "GAUGE_SYSTEM",
                                                                           Event.start == "2018-06-05T2:07:03",
                                                                           Event.stop == "2018-06-05T03:07:03",
                                                                           Source.name == "source2.json").all()
        assert len(events) == 1

        events = self.session.query(Event).join(Gauge).join(Source).filter(Gauge.name == "GAUGE_NAME", 
                                                                           Gauge.system == "GAUGE_SYSTEM",
                                                                           Event.start == "2018-06-05T4:07:36",
                                                                           Event.stop == "2018-06-05T08:07:36",
                                                                           Source.name == "source2.json").all()
        assert len(events) == 1

        events = self.session.query(Event).all()

        assert len(events) == 3

    def test_remove_deprecated_event_insert_and_erase_with_priority_one_starts_after(self):

        data1 = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                                  "exec": "exec",
                                  "version": "1.0"},
                "source": {"name": "source.xml",
                           "reception_time": "2018-06-06T13:33:29",
                           "generation_time": "2018-07-05T02:07:03",
                           "validity_start": "2018-06-05T02:07:15",
                           "validity_stop": "2018-06-06T08:07:36",
                           "priority": 30},
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
        }]}

        exit_status = self.engine_eboa.treat_data(data1)

        assert len([item for item in exit_status if item["status"] != eboa_engine.exit_codes["OK"]["status"]]) == 0
        
        data2 = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                                  "exec": "exec",
                                  "version": "1.0"},
                "source": {"name": "source2.xml",
                           "reception_time": "2018-06-06T13:33:29",
                           "generation_time": "2018-07-05T02:07:04",
                           "validity_start": "2018-06-05T02:07:03",
                           "validity_stop": "2018-06-05T08:07:36",
                           "priority": 40},
                "events": [{
                    "explicit_reference": "EXPLICIT_REFERENCE_EVENT2",
                    "gauge": {"name": "GAUGE_NAME",
                              "system": "GAUGE_SYSTEM",
                              "insertion_type": "INSERT_and_ERASE_with_PRIORITY"},
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
        }]}

        exit_status = self.engine_eboa.treat_data(data2)

        assert len([item for item in exit_status if item["status"] != eboa_engine.exit_codes["OK"]["status"]]) == 0

        filtered_events_ddbb = self.session.query(Event).join(Source).filter(Event.start == "2018-06-05T02:07:03",
                                                                             Event.stop == "2018-06-05T08:07:36",
                                                                             Source.name == "source2.xml",
                                                                             Event.visible == True).all()

        assert len(filtered_events_ddbb) == 1

        filtered_events_ddbb = self.session.query(Event).join(Source).filter(Event.start == "2018-06-05T08:07:36",
                                                                             Event.stop == "2018-06-06T08:07:36",
                                                                             Source.name == "source.xml",
                                                                             Event.visible == True).all()

        assert len(filtered_events_ddbb) == 1

        events_ddbb = self.session.query(Event).all()

        assert len(events_ddbb) == 2

        values_ddbb = self.query_eboa.get_event_values()

        assert len(values_ddbb) == 16

    def test_remove_deprecated_event_insert_and_erase_with_priority_events_not_overlapping(self):

        data1 = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                                   "exec": "exec",
                                   "version": "1.0"},
                 "source": {"name": "source.xml",
                            "reception_time": "2018-06-06T13:33:29",
                            "generation_time": "2018-07-05T02:07:03",
                            "validity_start": "2018-06-05T02:07:03",
                            "validity_stop": "2018-06-05T08:07:36",
                            "priority": 30},
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
        }]}

        exit_status = self.engine_eboa.treat_data(data1)

        assert len([item for item in exit_status if item["status"] != eboa_engine.exit_codes["OK"]["status"]]) == 0

        data2 = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                                   "exec": "exec",
                                   "version": "1.0"},
                 "source": {"name": "source2.xml",
                            "reception_time": "2018-06-06T13:33:29",
                            "generation_time": "2018-07-05T02:07:04",
                            "validity_start": "2018-06-04T02:07:03",
                            "validity_stop": "2018-06-06T08:07:36",
                           "priority": 40},
                 "events": [{
                     "explicit_reference": "EXPLICIT_REFERENCE_EVENT1",
                     "gauge": {"name": "GAUGE_NAME",
                               "system": "GAUGE_SYSTEM",
                               "insertion_type": "INSERT_and_ERASE_with_PRIORITY"},
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
        ]}

        exit_status = self.engine_eboa.treat_data(data2)

        assert len([item for item in exit_status if item["status"] != eboa_engine.exit_codes["OK"]["status"]]) == 0

        filtered_events_ddbb = self.session.query(Event).filter(Event.start == "2018-06-04T05:07:03",
                                                      Event.stop == "2018-06-05T07:07:36",
                                                      Source.name == "source2.xml",
                                                      Event.visible == True).all()

        assert len(filtered_events_ddbb) == 1

        events_ddbb = self.session.query(Event).all()

        assert len(events_ddbb) == 1

        values_ddbb = self.query_eboa.get_event_values()

        assert len(values_ddbb) == 8

    def test_remove_deprecated_event_insert_and_erase_with_priority_split_events(self):

        data1 = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                                   "exec": "exec",
                                   "version": "1.0"},
                 "source": {"name": "source1.xml",
                            "reception_time": "2018-06-06T13:33:29",
                            "generation_time": "2018-07-10T02:07:03",
                            "validity_start": "2018-06-05T02:07:03",
                            "validity_stop": "2018-06-05T08:07:36",
                            "priority": 40},
                 "events": [{
                     "explicit_reference": "EXPLICIT_REFERENCE_EVENT2",
                     "gauge": {"name": "GAUGE_NAME",
                               "system": "GAUGE_SYSTEM",
                               "insertion_type": "SIMPLE_UPDATE"},
                     "start": "2018-06-05T02:07:03",
                     "stop": "2018-06-05T04:07:36"
                }]
        }]}

        exit_status = self.engine_eboa.treat_data(data1)

        assert len([item for item in exit_status if item["status"] != eboa_engine.exit_codes["OK"]["status"]]) == 0
            
        data2 = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                                   "exec": "exec",
                                   "version": "1.0"},
                 "source": {"name": "source2.xml",
                            "reception_time": "2018-06-06T13:33:29",
                            "generation_time": "2018-07-10T02:07:03",
                            "validity_start": "2018-06-05T11:07:03",
                            "validity_stop": "2018-06-05T17:07:36",
                            "priority": 40},
                 "events": [{
                     "explicit_reference": "EXPLICIT_REFERENCE_EVENT2",
                     "gauge": {"name": "GAUGE_NAME",
                               "system": "GAUGE_SYSTEM",
                               "insertion_type": "SIMPLE_UPDATE"},
                     "start": "2018-06-05T12:07:03",
                     "stop": "2018-06-05T15:07:36"
                }]
        }]}

        exit_status = self.engine_eboa.treat_data(data2)

        assert len([item for item in exit_status if item["status"] != eboa_engine.exit_codes["OK"]["status"]]) == 0

        data3 = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                                   "exec": "exec",
                                   "version": "1.0"},
                 "source": {"name": "source3.xml",
                            "reception_time": "2018-06-06T13:33:29",
                            "generation_time": "2018-07-05T02:07:04",
                            "validity_start": "2018-06-04T02:07:03",
                            "validity_stop": "2018-06-06T08:07:36",
                            "priority": 30},
                 "events": [{
                     "explicit_reference": "EXPLICIT_REFERENCE_EVENT1",
                     "gauge": {"name": "GAUGE_NAME",
                               "system": "GAUGE_SYSTEM",
                               "insertion_type": "INSERT_and_ERASE_with_PRIORITY"},
                     "start": "2018-06-04T05:07:03",
                     "stop": "2018-06-06T07:07:36"
                }]
        }]}

        exit_status = self.engine_eboa.treat_data(data3)

        assert len([item for item in exit_status if item["status"] != eboa_engine.exit_codes["OK"]["status"]]) == 0
                                
        filtered_events_ddbb = self.session.query(Event).join(Source).filter(Event.start == "2018-06-05T02:07:03",
                                                                             Event.stop == "2018-06-05T04:07:36",
                                                                             Source.name == "source1.xml",
                                                                             Event.visible == True).all()

        assert len(filtered_events_ddbb) == 1

        filtered_events_ddbb = self.session.query(Event).join(Source).filter(Event.start == "2018-06-05T12:07:03",
                                                                             Event.stop == "2018-06-05T15:07:36",
                                                                             Source.name == "source2.xml",
                                                                             Event.visible == True).all()

        assert len(filtered_events_ddbb) == 1

        filtered_events_ddbb = self.session.query(Event).join(Source).filter(Event.start == "2018-06-04T05:07:03",
                                                                             Event.stop == "2018-06-05T02:07:03",
                                                                             Source.name == "source3.xml",
                                                                             Event.visible == True).all()

        assert len(filtered_events_ddbb) == 1

        filtered_events_ddbb = self.session.query(Event).join(Source).filter(Event.start == "2018-06-05T08:07:36",
                                                                             Event.stop == "2018-06-05T11:07:03",
                                                                             Source.name == "source3.xml",
                                                                             Event.visible == True).all()

        assert len(filtered_events_ddbb) == 1

        filtered_events_ddbb = self.session.query(Event).join(Source).filter(Event.start == "2018-06-05T17:07:36",
                                                                             Event.stop == "2018-06-06T07:07:36",
                                                                             Source.name == "source3.xml",
                                                                             Event.visible == True).all()

        assert len(filtered_events_ddbb) == 1

        events_ddbb = self.session.query(Event).all()

        assert len(events_ddbb) == 5

    def test_remove_deprecated_event_insert_and_erase_with_priority_split_events_not_created(self):

        data1 = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                                   "exec": "exec",
                                   "version": "1.0"},
                 "source": {"name": "source1.xml",
                            "reception_time": "2018-06-06T13:33:29",
                            "generation_time": "2018-07-10T02:07:03",
                            "validity_start": "2018-06-05T02:07:03",
                            "validity_stop": "2018-06-05T08:07:36",
                            "priority": 40},
                 "events": [{
                     "explicit_reference": "EXPLICIT_REFERENCE_EVENT2",
                     "gauge": {"name": "GAUGE_NAME",
                               "system": "GAUGE_SYSTEM",
                               "insertion_type": "SIMPLE_UPDATE"},
                     "start": "2018-06-05T02:07:03",
                     "stop": "2018-06-05T04:07:36"
                }]
        }]}

        exit_status = self.engine_eboa.treat_data(data1)

        assert len([item for item in exit_status if item["status"] != eboa_engine.exit_codes["OK"]["status"]]) == 0

        data2 = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                                   "exec": "exec",
                                   "version": "1.0"},
                 "source": {"name": "source2.xml",
                            "reception_time": "2018-06-06T13:33:29",
                            "generation_time": "2018-07-10T02:07:03",
                            "validity_start": "2018-06-05T08:07:36",
                            "validity_stop": "2018-06-05T17:07:36",
                            "priority": 40},
                 "events": [{
                     "explicit_reference": "EXPLICIT_REFERENCE_EVENT2",
                     "gauge": {"name": "GAUGE_NAME",
                               "system": "GAUGE_SYSTEM",
                               "insertion_type": "SIMPLE_UPDATE"},
                     "start": "2018-06-05T09:07:03",
                     "stop": "2018-06-05T15:07:36"
                }]
        }]}

        exit_status = self.engine_eboa.treat_data(data2)

        assert len([item for item in exit_status if item["status"] != eboa_engine.exit_codes["OK"]["status"]]) == 0
        
        data3 = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                                   "exec": "exec",
                                   "version": "1.0"},
                 "source": {"name": "source3.xml",
                            "reception_time": "2018-06-06T13:33:29",
                            "generation_time": "2018-07-05T02:07:04",
                            "validity_start": "2018-06-04T02:07:03",
                            "validity_stop": "2018-06-06T08:07:36",
                            "priority": 30},
                 "events": [{
                     "explicit_reference": "EXPLICIT_REFERENCE_EVENT1",
                     "gauge": {"name": "GAUGE_NAME",
                               "system": "GAUGE_SYSTEM",
                               "insertion_type": "INSERT_and_ERASE_with_PRIORITY"},
                     "start": "2018-06-04T05:07:03",
                     "stop": "2018-06-05T09:07:36"
                }]
        }]}

        exit_status = self.engine_eboa.treat_data(data3)

        assert len([item for item in exit_status if item["status"] != eboa_engine.exit_codes["OK"]["status"]]) == 0
 
        filtered_events_ddbb = self.session.query(Event).join(Source).filter(Event.start == "2018-06-05T02:07:03",
                                                                             Event.stop == "2018-06-05T04:07:36",
                                                                             Source.name == "source1.xml",
                                                                             Event.visible == True).all()

        assert len(filtered_events_ddbb) == 1

        filtered_events_ddbb = self.session.query(Event).join(Source).filter(Event.start == "2018-06-05T09:07:03",
                                                                             Event.stop == "2018-06-05T15:07:36",
                                                                             Source.name == "source2.xml",
                                                                             Event.visible == True).all()

        assert len(filtered_events_ddbb) == 1

        filtered_events_ddbb = self.session.query(Event).join(Source).filter(Event.start == "2018-06-04T05:07:03",
                                                                             Event.stop == "2018-06-05T02:07:03",
                                                                             Source.name == "source3.xml",
                                                                             Event.visible == True).all()

        assert len(filtered_events_ddbb) == 1

        events_ddbb = self.session.query(Event).all()

        assert len(events_ddbb) == 3

        
    def test_insert_event_insert_and_erase_per_event_with_priority_event_in_ddbb_split(self):

        data1 = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source.json",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2018-07-07T02:07:03",
                       "validity_start": "2018-06-05T02:07:03",
                       "validity_stop": "2018-06-05T08:07:36",
                       "priority": 40},
            "events": [{
                "explicit_reference": "EXPLICIT_REFERENCE_EVENT",
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "SIMPLE_UPDATE"},
                "start": "2018-06-05T04:07:03",
                "stop": "2018-06-05T06:07:36"
            }]
        }]
        }
        self.engine_eboa.treat_data(data1)

        data2 = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source2.json",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2018-07-06T02:07:03",
                       "validity_start": "2018-06-05T02:07:03",
                       "validity_stop": "2018-06-05T08:07:36",
                       "priority": 30},
            "events": [{
                "explicit_reference": "EXPLICIT_REFERENCE_EVENT",
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "SIMPLE_UPDATE"},
                "start": "2018-06-05T02:07:03",
                "stop": "2018-06-05T08:07:36"
            }]
        }]
        }
        self.engine_eboa.treat_data(data2)

        data3 = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source3.json",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2018-07-05T02:07:03",
                       "validity_start": "2018-06-05T02:07:03",
                       "validity_stop": "2018-06-05T08:07:36",
                       "priority": 20},
            "events": [{
                "explicit_reference": "EXPLICIT_REFERENCE_EVENT",
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "INSERT_and_ERASE_per_EVENT_with_PRIORITY"},
                "start": "2018-06-05T02:07:03",
                "stop": "2018-06-05T08:07:36"
            }]
        }]
        }
        self.engine_eboa.treat_data(data3)

        events = self.session.query(Event).join(Gauge).join(Source).filter(Gauge.name == "GAUGE_NAME", 
                                                                           Gauge.system == "GAUGE_SYSTEM",
                                                                           Event.start == "2018-06-05T04:07:03",
                                                                           Event.stop == "2018-06-05T06:07:36",
                                                                           Source.name == "source.json").all()
        assert len(events) == 1

        events = self.session.query(Event).join(Gauge).join(Source).filter(Gauge.name == "GAUGE_NAME", 
                                                                           Gauge.system == "GAUGE_SYSTEM",
                                                                           Event.start == "2018-06-05T02:07:03",
                                                                           Event.stop == "2018-06-05T04:07:03",
                                                                           Source.name == "source2.json").all()
        assert len(events) == 1

        events = self.session.query(Event).join(Gauge).join(Source).filter(Gauge.name == "GAUGE_NAME", 
                                                                           Gauge.system == "GAUGE_SYSTEM",
                                                                           Event.start == "2018-06-05T06:07:36",
                                                                           Event.stop == "2018-06-05T08:07:36",
                                                                           Source.name == "source2.json").all()
        assert len(events) == 1

        events = self.session.query(Event).all()

        assert len(events) == 3

    def test_insert_event_insert_and_erase_per_event_with_priority_event_in_ddbb_cut(self):

        data1 = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source.json",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2018-07-07T02:07:03",
                       "validity_start": "2018-06-05T02:07:03",
                       "validity_stop": "2018-06-05T08:07:36",
                       "priority": 40},
            "events": [{
                "explicit_reference": "EXPLICIT_REFERENCE_EVENT",
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "SIMPLE_UPDATE"},
                "start": "2018-06-05T04:07:03",
                "stop": "2018-06-05T08:07:36"
            }]
        }]
        }
        self.engine_eboa.treat_data(data1)

        data2 = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source2.json",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2018-07-06T02:07:03",
                       "validity_start": "2018-06-05T02:07:03",
                       "validity_stop": "2018-06-05T08:07:36",
                       "priority": 30},
            "events": [{
                "explicit_reference": "EXPLICIT_REFERENCE_EVENT",
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "SIMPLE_UPDATE"},
                "start": "2018-06-05T02:07:03",
                "stop": "2018-06-05T06:07:36"
            }]
        }]
        }
        self.engine_eboa.treat_data(data2)

        data3 = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source3.json",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2018-07-05T02:07:03",
                       "validity_start": "2018-06-05T02:07:03",
                       "validity_stop": "2018-06-05T08:07:36",
                       "priority": 20},
            "events": [{
                "explicit_reference": "EXPLICIT_REFERENCE_EVENT",
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "INSERT_and_ERASE_per_EVENT_with_PRIORITY"},
                "start": "2018-06-05T02:07:03",
                "stop": "2018-06-05T08:07:36"
            }]
        }]
        }
        self.engine_eboa.treat_data(data3)

        events = self.session.query(Event).join(Gauge).join(Source).filter(Gauge.name == "GAUGE_NAME", 
                                                                           Gauge.system == "GAUGE_SYSTEM",
                                                                           Event.start == "2018-06-05T04:07:03",
                                                                           Event.stop == "2018-06-05T08:07:36",
                                                                           Source.name == "source.json").all()
        assert len(events) == 1

        events = self.session.query(Event).join(Gauge).join(Source).filter(Gauge.name == "GAUGE_NAME", 
                                                                           Gauge.system == "GAUGE_SYSTEM",
                                                                           Event.start == "2018-06-05T02:07:03",
                                                                           Event.stop == "2018-06-05T04:07:03",
                                                                           Source.name == "source2.json").all()
        assert len(events) == 1

        events = self.session.query(Event).all()

        assert len(events) == 2

    def test_insert_event_insert_and_erase_per_event_with_priority_event_in_ddbb_cut_by_previous_event(self):

        data1 = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source.json",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2018-07-07T02:07:03",
                       "validity_start": "2018-06-05T02:07:03",
                       "validity_stop": "2018-06-05T08:07:36",
                       "priority": 40},
            "events": [{
                "explicit_reference": "EXPLICIT_REFERENCE_EVENT",
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "SIMPLE_UPDATE"},
                "start": "2018-06-05T03:07:03",
                "stop": "2018-06-05T04:07:36"
            }]
        }]
        }
        exit_status = self.engine_eboa.treat_data(data1)

        assert len([item for item in exit_status if item["status"] != eboa_engine.exit_codes["OK"]["status"]]) == 0

        data2 = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source2.json",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2018-07-06T02:07:03",
                       "validity_start": "2018-06-05T02:07:03",
                       "validity_stop": "2018-06-05T08:07:36",
                       "priority": 30},
            "events": [{
                "explicit_reference": "EXPLICIT_REFERENCE_EVENT",
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "SIMPLE_UPDATE"},
                "start": "2018-06-05T02:07:03",
                "stop": "2018-06-05T08:07:36"
            }]
        }]
        }
        exit_status = self.engine_eboa.treat_data(data2)

        assert len([item for item in exit_status if item["status"] != eboa_engine.exit_codes["OK"]["status"]]) == 0

        data4 = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source4.json",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2018-07-05T02:07:03",
                       "validity_start": "2018-06-05T02:07:03",
                       "validity_stop": "2018-06-05T08:07:36",
                       "priority": 20},
            "events": [{
                "explicit_reference": "EXPLICIT_REFERENCE_EVENT",
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "SIMPLE_UPDATE"},
                "start": "2018-06-05T04:07:03",
                "stop": "2018-06-05T06:07:36"
            }]
        }]
        }
        exit_status = self.engine_eboa.treat_data(data4)

        assert len([item for item in exit_status if item["status"] != eboa_engine.exit_codes["OK"]["status"]]) == 0

        data3 = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source3.json",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2018-07-05T02:07:03",
                       "validity_start": "2018-06-05T02:07:03",
                       "validity_stop": "2018-06-05T08:07:36",
                       "priority": 10},
            "events": [{
                "explicit_reference": "EXPLICIT_REFERENCE_EVENT",
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "INSERT_and_ERASE_per_EVENT_with_PRIORITY"},
                "start": "2018-06-05T02:07:03",
                "stop": "2018-06-05T08:07:36"
            }]
        }]
        }
        exit_status = self.engine_eboa.treat_data(data3)

        assert len([item for item in exit_status if item["status"] != eboa_engine.exit_codes["OK"]["status"]]) == 0

        events = self.session.query(Event).join(Gauge).join(Source).filter(Gauge.name == "GAUGE_NAME", 
                                                                           Gauge.system == "GAUGE_SYSTEM",
                                                                           Event.start == "2018-06-05T03:07:03",
                                                                           Event.stop == "2018-06-05T04:07:36",
                                                                           Source.name == "source.json").all()
        assert len(events) == 1

        events = self.session.query(Event).join(Gauge).join(Source).filter(Gauge.name == "GAUGE_NAME", 
                                                                           Gauge.system == "GAUGE_SYSTEM",
                                                                           Event.start == "2018-06-05T02:07:03",
                                                                           Event.stop == "2018-06-05T03:07:03",
                                                                           Source.name == "source2.json").all()
        assert len(events) == 1

        events = self.session.query(Event).join(Gauge).join(Source).filter(Gauge.name == "GAUGE_NAME", 
                                                                           Gauge.system == "GAUGE_SYSTEM",
                                                                           Event.start == "2018-06-05T04:07:36",
                                                                           Event.stop == "2018-06-05T08:07:36",
                                                                           Source.name == "source2.json").all()
        assert len(events) == 1

        events = self.session.query(Event).all()

        assert len(events) == 3

    def test_insert_event_insert_and_erase_per_event_with_priority_event_in_ddbb_cut_by_previous_and_later_event(self):

        data1 = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source.json",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2018-07-07T02:07:03",
                       "validity_start": "2018-06-05T02:07:03",
                       "validity_stop": "2018-06-05T08:07:36",
                       "priority": 40},
            "events": [{
                "explicit_reference": "EXPLICIT_REFERENCE_EVENT",
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "SIMPLE_UPDATE"},
                "start": "2018-06-05T03:07:03",
                "stop": "2018-06-05T04:07:36"
            },
                       {
                           "explicit_reference": "EXPLICIT_REFERENCE_EVENT",
                           "gauge": {"name": "GAUGE_NAME",
                                     "system": "GAUGE_SYSTEM",
                                     "insertion_type": "SIMPLE_UPDATE"},
                           "start": "2018-06-05T06:07:03",
                           "stop": "2018-06-05T08:07:36"
                       }]
        }]
        }
        exit_status = self.engine_eboa.treat_data(data1)

        assert len([item for item in exit_status if item["status"] != eboa_engine.exit_codes["OK"]["status"]]) == 0

        data2 = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source2.json",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2018-07-06T02:07:03",
                       "validity_start": "2018-06-05T02:07:03",
                       "validity_stop": "2018-06-05T08:07:36",
                       "priority": 30},
            "events": [{
                "explicit_reference": "EXPLICIT_REFERENCE_EVENT",
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "SIMPLE_UPDATE"},
                "start": "2018-06-05T02:07:03",
                "stop": "2018-06-05T08:07:36"
            }]
        }]
        }
        exit_status = self.engine_eboa.treat_data(data2)

        assert len([item for item in exit_status if item["status"] != eboa_engine.exit_codes["OK"]["status"]]) == 0

        data4 = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source4.json",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2018-07-05T02:07:03",
                       "validity_start": "2018-06-05T02:07:03",
                       "validity_stop": "2018-06-05T08:07:36",
                       "priority": 20},
            "events": [{
                "explicit_reference": "EXPLICIT_REFERENCE_EVENT",
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "SIMPLE_UPDATE"},
                "start": "2018-06-05T04:07:03",
                "stop": "2018-06-05T06:07:36"
            }]
        }]
        }
        exit_status = self.engine_eboa.treat_data(data4)

        assert len([item for item in exit_status if item["status"] != eboa_engine.exit_codes["OK"]["status"]]) == 0

        data3 = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source3.json",
                       "reception_time": "2018-06-06T13:33:29",                       
                       "generation_time": "2018-07-05T02:07:03",
                       "validity_start": "2018-06-05T02:07:03",
                       "validity_stop": "2018-06-05T08:07:36",
                       "priority": 10},
            "events": [{
                "explicit_reference": "EXPLICIT_REFERENCE_EVENT",
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "INSERT_and_ERASE_per_EVENT_with_PRIORITY"},
                "start": "2018-06-05T02:07:03",
                "stop": "2018-06-05T08:07:36"
            }]
        }]
        }
        exit_status = self.engine_eboa.treat_data(data3)

        assert len([item for item in exit_status if item["status"] != eboa_engine.exit_codes["OK"]["status"]]) == 0

        events = self.session.query(Event).join(Gauge).join(Source).filter(Gauge.name == "GAUGE_NAME", 
                                                                           Gauge.system == "GAUGE_SYSTEM",
                                                                           Event.start == "2018-06-05T03:07:03",
                                                                           Event.stop == "2018-06-05T04:07:36",
                                                                           Source.name == "source.json").all()
        assert len(events) == 1

        events = self.session.query(Event).join(Gauge).join(Source).filter(Gauge.name == "GAUGE_NAME", 
                                                                           Gauge.system == "GAUGE_SYSTEM",
                                                                           Event.start == "2018-06-05T06:07:03",
                                                                           Event.stop == "2018-06-05T08:07:36",
                                                                           Source.name == "source.json").all()
        assert len(events) == 1

        events = self.session.query(Event).join(Gauge).join(Source).filter(Gauge.name == "GAUGE_NAME", 
                                                                           Gauge.system == "GAUGE_SYSTEM",
                                                                           Event.start == "2018-06-05T02:07:03",
                                                                           Event.stop == "2018-06-05T03:07:03",
                                                                           Source.name == "source2.json").all()
        assert len(events) == 1

        events = self.session.query(Event).join(Gauge).join(Source).filter(Gauge.name == "GAUGE_NAME", 
                                                                           Gauge.system == "GAUGE_SYSTEM",
                                                                           Event.start == "2018-06-05T04:07:36",
                                                                           Event.stop == "2018-06-05T06:07:03",
                                                                           Source.name == "source2.json").all()
        assert len(events) == 1

        events = self.session.query(Event).all()

        assert len(events) == 4

    def test_insert_event_insert_and_erase_per_event_with_priority_event_duration_0_in_the_middle_to_remove(self):

        data = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source.json",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2018-07-05T02:07:03",
                       "validity_start": "2018-06-05T02:07:03",
                       "validity_stop": "2018-06-05T08:07:36",
                       "priority": 40},
            "events": [{
                "explicit_reference": "EXPLICIT_REFERENCE_EVENT",
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "INSERT_and_ERASE_per_EVENT_with_PRIORITY"},
                "start": "2018-06-05T02:07:03",
                "stop": "2018-06-05T08:07:36"
            }]
        },{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source2.json",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2018-07-04T02:07:03",
                       "validity_start": "2018-06-05T04:07:03",
                       "validity_stop": "2018-06-05T04:07:03",
                       "priority": 30},
            "events": [{
                "explicit_reference": "EXPLICIT_REFERENCE_EVENT",
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "INSERT_and_ERASE_per_EVENT_with_PRIORITY"},
                "start": "2018-06-05T04:07:03",
                "stop": "2018-06-05T04:07:03"
            }]
        }]
        }
        self.engine_eboa.treat_data(data)

        events = self.session.query(Event).join(Gauge).join(Source).filter(Gauge.name == "GAUGE_NAME", 
                                                                           Gauge.system == "GAUGE_SYSTEM",
                                                                           Event.start == "2018-06-05T02:07:03",
                                                                           Event.stop == "2018-06-05T08:07:36",
                                                                           Source.name == "source.json").all()
        assert len(events) == 1

        events = self.session.query(Event).all()

        assert len(events) == 1

    def test_insert_event_insert_and_erase_per_event_with_priority_event_duration_0_in_the_middle_to_stay(self):

        data = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source.json",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2018-07-05T02:07:03",
                       "validity_start": "2018-06-05T02:07:03",
                       "validity_stop": "2018-06-05T08:07:36",
                       "priority": 30},
            "events": [{
                "explicit_reference": "EXPLICIT_REFERENCE_EVENT",
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "INSERT_and_ERASE_per_EVENT_with_PRIORITY"},
                "start": "2018-06-05T02:07:03",
                "stop": "2018-06-05T08:07:36"
            }]
        },{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source2.json",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2018-08-04T02:07:03",
                       "validity_start": "2018-06-05T04:07:03",
                       "validity_stop": "2018-06-05T04:07:03",
                       "priority": 40},
            "events": [{
                "explicit_reference": "EXPLICIT_REFERENCE_EVENT",
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "INSERT_and_ERASE_per_EVENT_with_PRIORITY"},
                "start": "2018-06-05T04:07:03",
                "stop": "2018-06-05T04:07:03"
            }]
        }]
        }
        self.engine_eboa.treat_data(data)

        events = self.session.query(Event).join(Gauge).join(Source).filter(Gauge.name == "GAUGE_NAME", 
                                                                           Gauge.system == "GAUGE_SYSTEM",
                                                                           Event.start == "2018-06-05T02:07:03",
                                                                           Event.stop == "2018-06-05T04:07:03",
                                                                           Source.name == "source.json").all()
        assert len(events) == 1

        events = self.session.query(Event).join(Gauge).join(Source).filter(Gauge.name == "GAUGE_NAME", 
                                                                           Gauge.system == "GAUGE_SYSTEM",
                                                                           Event.start == "2018-06-05T04:07:03",
                                                                           Event.stop == "2018-06-05T08:07:36",
                                                                           Source.name == "source.json").all()
        assert len(events) == 1

        events = self.session.query(Event).join(Gauge).join(Source).filter(Gauge.name == "GAUGE_NAME", 
                                                                           Gauge.system == "GAUGE_SYSTEM",
                                                                           Event.start == "2018-06-05T04:07:03",
                                                                           Event.stop == "2018-06-05T04:07:03",
                                                                           Source.name == "source2.json").all()
        assert len(events) == 1

        events = self.session.query(Event).all()

        assert len(events) == 3
        
        events = self.session.query(Event).filter(Event.visible == True).all()

        assert len(events) == 3
        
    def test_insert_event_insert_and_erase_per_event_with_priority_event_split(self):

        data1 = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source.json",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2018-07-05T02:07:03",
                       "validity_start": "2018-06-05T02:07:03",
                       "validity_stop": "2018-06-05T08:07:36",
                       "priority": 30},
            "events": [{
                "explicit_reference": "EXPLICIT_REFERENCE_EVENT",
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "INSERT_and_ERASE_per_EVENT_with_PRIORITY"},
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
            }]
                        }]
        }]
        }]
        }
        self.engine_eboa.treat_data(data1)

        data2 = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source2.json",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2018-07-06T02:07:03",
                       "validity_start": "2018-06-05T04:07:03",
                       "validity_stop": "2018-06-05T06:07:36",
                       "priority": 40},
            "events": [{
                "explicit_reference": "EXPLICIT_REFERENCE_EVENT",
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "INSERT_and_ERASE_per_EVENT_with_PRIORITY"},
                "start": "2018-06-05T04:07:03",
                "stop": "2018-06-05T06:07:36"
            }]
        }]
        }
        self.engine_eboa.treat_data(data2)

        events = self.session.query(Event).join(Gauge).join(Source).filter(Gauge.name == "GAUGE_NAME", 
                                                                           Gauge.system == "GAUGE_SYSTEM",
                                                                           Event.start == "2018-06-05T04:07:03",
                                                                           Event.stop == "2018-06-05T06:07:36",
                                                                           Source.name == "source2.json").all()
        assert len(events) == 1

        events = self.session.query(Event).join(Gauge).join(Source).filter(Gauge.name == "GAUGE_NAME", 
                                                                           Gauge.system == "GAUGE_SYSTEM",
                                                                           Event.start == "2018-06-05T02:07:03",
                                                                           Event.stop == "2018-06-05T04:07:03",
                                                                           Source.name == "source.json").all()
        assert len(events) == 1

        events = self.session.query(Event).join(Gauge).join(Source).filter(Gauge.name == "GAUGE_NAME", 
                                                                           Gauge.system == "GAUGE_SYSTEM",
                                                                           Event.start == "2018-06-05T06:07:36",
                                                                           Event.stop == "2018-06-05T08:07:36",
                                                                           Source.name == "source.json").all()
        assert len(events) == 1

        events = self.session.query(Event).all()

        assert len(events) == 3

    def test_insert_explicit_reference_without_group(self):

        data = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source.json",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2018-07-05T02:07:03",
                       "validity_start": "2018-06-05T02:07:03",
                       "validity_stop": "2018-06-05T08:07:36",
                       "priority": 30},
            "explicit_references": [{
                "name": "EXPLICIT_REFERENCE"
            }]
        }]
                }

        exit_status = self.engine_eboa.treat_data(data)

        assert len([item for item in exit_status if item["status"] != eboa_engine.exit_codes["OK"]["status"]]) == 0

        ers = self.session.query(ExplicitRef).all()

        assert len(ers) == 1

    def test_insert_alerts_events_sources_annotations_ers(self):

        data = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source.json",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2018-07-05T02:07:03",
                       "validity_start": "2018-06-05T02:07:03",
                       "validity_stop": "2018-06-05T08:07:36",
                       "priority": 30,
                       "alerts": [{
                           "message": "Alert message",
                           "generator": "test",
                           "notification_time": "2018-06-05T08:07:36",
                           "alert_cnf": {
                               "name": "alert_name1",
                               "severity": "critical",
                               "description": "Alert description",
                               "group": "alert_group"
                           }}]
                       },
            "explicit_references": [{
                "name": "EXPLICIT_REFERENCE",
                "alerts": [{
                    "message": "Alert message",
                    "generator": "test",
                    "notification_time": "2018-06-05T08:07:36",
                    "alert_cnf": {
                        "name": "alert_name1",
                        "severity": "critical",
                        "description": "Alert description",
                        "group": "alert_group"
                    }}]
            }],
            "events": [{
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "SIMPLE_UPDATE"},
                "start": "2018-06-05T02:07:03",
                "stop": "2018-06-05T08:07:36",
                "alerts": [{
                    "message": "Alert message",
                    "generator": "test",
                    "notification_time": "2018-06-05T08:07:36",
                    "alert_cnf": {
                        "name": "alert_name1",
                        "severity": "critical",
                        "description": "Alert description",
                        "group": "alert_group"
                    }}]
            }],
            "annotations": [{
                "explicit_reference": "EXPLICIT_REFERENCE",
                "annotation_cnf": {
                    "name": "ANNOTATION_CNF",
                    "system": "SYSTEM"
                },
                "alerts": [{
                    "message": "Alert message",
                    "generator": "test",
                    "notification_time": "2018-06-05T08:07:36",
                    "alert_cnf": {
                        "name": "alert_name1",
                        "severity": "critical",
                        "description": "Alert description",
                        "group": "alert_group"
                    }}]
            }]
        }]
                }

        exit_status = self.engine_eboa.treat_data(data)

        assert len([item for item in exit_status if item["status"] != eboa_engine.exit_codes["OK"]["status"]]) == 0

        sources = self.session.query(Source).all()

        assert len(sources) == 1

        ers = self.session.query(ExplicitRef).all()

        assert len(ers) == 1

        events = self.session.query(Event).all()

        assert len(events) == 1

        annotations = self.session.query(Annotation).all()

        assert len(annotations) == 1

        # Check alerts
        alert_sources = self.session.query(SourceAlert) \
                                    .join(Alert) \
                                    .join(AlertGroup) \
                                    .filter(SourceAlert.message == "Alert message",
                                            SourceAlert.generator == "test",
                                            SourceAlert.notification_time > "1900-01-01",
                                            SourceAlert.message == "Alert message",
                                            Alert.name == "alert_name1",
                                            Alert.severity == 4,
                                            Alert.description == "Alert description",
                                            AlertGroup.name == "alert_group").all()

        assert len(alert_sources) == 1

        alert_ers = self.session.query(ExplicitRefAlert) \
                                    .join(Alert) \
                                    .join(AlertGroup) \
                                    .filter(ExplicitRefAlert.message == "Alert message",
                                            ExplicitRefAlert.generator == "test",
                                            ExplicitRefAlert.notification_time > "1900-01-01",
                                            ExplicitRefAlert.message == "Alert message",
                                            Alert.name == "alert_name1",
                                            Alert.severity == 4,
                                            Alert.description == "Alert description",
                                            AlertGroup.name == "alert_group").all()

        assert len(alert_sources) == 1

        alert_events = self.session.query(EventAlert) \
                                    .join(Alert) \
                                    .join(AlertGroup) \
                                    .filter(EventAlert.message == "Alert message",
                                            EventAlert.generator == "test",
                                            EventAlert.notification_time > "1900-01-01",
                                            EventAlert.message == "Alert message",
                                            Alert.name == "alert_name1",
                                            Alert.severity == 4,
                                            Alert.description == "Alert description",
                                            AlertGroup.name == "alert_group").all()

        assert len(alert_events) == 1

        alert_annotations = self.session.query(AnnotationAlert) \
                                    .join(Alert) \
                                    .join(AlertGroup) \
                                    .filter(AnnotationAlert.message == "Alert message",
                                            AnnotationAlert.generator == "test",
                                            AnnotationAlert.notification_time > "1900-01-01",
                                            AnnotationAlert.message == "Alert message",
                                            Alert.name == "alert_name1",
                                            Alert.severity == 4,
                                            Alert.description == "Alert description",
                                            AlertGroup.name == "alert_group").all()

        assert len(alert_annotations) == 1

    def test_linking_to_a_removed_event(self):

        data1 = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source1.json",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2020-07-05T02:07:03",
                       "validity_start": "2018-06-05T02:07:03",
                       "validity_stop": "2018-06-05T08:07:36"
                       },
            "events": [{
                "link_ref": "EVENT_LINK",
                "explicit_reference": "EXPLICIT_REFERENCE_EVENT",
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "SIMPLE_UPDATE"},
                "start": "2018-06-05T02:07:03",
                "stop": "2018-06-05T08:07:36"
                        }]
        }]
        }

        exit_status = self.engine_eboa.treat_data(data1)

        assert len([item for item in exit_status if item["status"] != eboa_engine.exit_codes["OK"]["status"]]) == 0

        events = self.session.query(Event).all()

        assert len(events) == 1
        
        data2 = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source2.json",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2018-07-05T02:07:03",
                       "validity_start": "2018-06-05T02:07:03",
                       "validity_stop": "2018-06-05T08:07:36"
                       },
            "events": [{
                "link_ref": "EVENT_LINK1",
                "explicit_reference": "EXPLICIT_REFERENCE_EVENT",
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "INSERT_and_ERASE"},
                "start": "2018-06-05T02:07:03",
                "stop": "2018-06-05T08:07:36"
            }]
            },{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source3.json",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2018-07-05T02:07:03",
                       "validity_start": "2018-06-05T02:07:03",
                       "validity_stop": "2018-06-05T08:07:36"
                       },
                "events": [{
                           "link_ref": "EVENT_LINK2",
                           "explicit_reference": "EXPLICIT_REFERENCE_EVENT2",
                           "gauge": {"name": "GAUGE_NAME2",
                                     "system": "GAUGE_SYSTEM2",
                                     "insertion_type": "SIMPLE_UPDATE"},
                           "start": "2018-06-05T02:07:03",
                           "stop": "2018-06-05T08:07:36",
                           "links": [{
                               "link": str(events[0].event_uuid),
                               "link_mode": "by_uuid",
                               "name": "EVENT_LINK_NAME2"
                           },{
                               "link": "EVENT_LINK1",
                               "link_mode": "by_ref",
                               "name": "EVENT_LINK_NAME"
                           }]
                       }]
        }]
        }

        exit_status = self.engine_eboa.treat_data(data2)

        assert len([item for item in exit_status if item["status"] != eboa_engine.exit_codes["OK"]["status"]]) == 0


    def test_event_links_inconsistency_with_a_link_to_a_nonexistent_uuid(self):

        self.engine_eboa._initialize_context_insert_data()
        data = {"operations": [{
                "mode": "insert",
                "dim_signature": {"name": "dim_signature",
                                  "exec": "exec",
                                  "version": "1.0"},
                "source": {"name": "source.xml",
                           "reception_time": "2018-06-06T13:33:29",
                           "generation_time": "2018-07-05T02:07:03",
                           "validity_start": "2018-06-05T02:07:03",
                           "validity_stop": "2018-06-05T08:07:36"},
                "events": [{
                    "link_ref": "EVENT_LINK1",
                    "gauge": {"name": "GAUGE_NAME",
                              "system": "GAUGE_SYSTEM",
                              "insertion_type": "SIMPLE_UPDATE"},
                    "start": "2018-06-05T02:07:03",
                    "stop": "2018-06-05T08:07:36",
                    "links": [{
                        "link": "EVENT_LINK2",
                        "link_mode": "by_ref",
                        "name": "EVENT_LINK_NAME",
                        "back_ref": "EVENT_LINK_NAME"
                    },{
                        "link": "e5b51b86-5b31-11eb-ba6f-000000000cf7",
                        "link_mode": "by_uuid",
                        "name": "EVENT_LINK_NAME2"
                    }]
                },
                {
                    "link_ref": "EVENT_LINK2",
                    "gauge": {"name": "GAUGE_NAME",
                              "system": "GAUGE_SYSTEM",
                              "insertion_type": "SIMPLE_UPDATE"},
                    "start": "2018-06-05T02:07:03",
                    "stop": "2018-06-05T08:07:36",
                    "links": [{
                        "link": "EVENT_LINK1",
                        "link_mode": "by_ref",
                        "name": "EVENT_LINK_NAME"
                    }]
                }]
            }]
            }
        self.engine_eboa.data = data
        assert_value = False
        try:
            self.engine_eboa.treat_data()
        except LinksInconsistency:
            assert_value = True
        # end try

        assert assert_value == True

        sources_status = self.session.query(SourceStatus).join(Source).filter(SourceStatus.status == eboa_engine.exit_codes["LINKS_INCONSISTENCY"]["status"],
                                                                           Source.name == data["operations"][0]["source"]["name"]).all()

        assert len(sources_status) == 1

        events = self.session.query(Event).all()

        assert len(events) == 0

    #####
    # Test insertion mode insert_and_erase_with_priority
    #####
        
    def test_remove_deprecated_event_insert_and_erase_with_priority_at_dim_signature_level_no_events(self):

        data = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                                  "exec": "exec",
                                  "version": "1.0"},
                "source": {"name": "source.xml",
                           "reception_time": "2018-06-06T13:33:29",
                           "generation_time": "2018-07-05T02:07:03",
                           "validity_start": "2018-06-05T02:07:03",
                           "validity_stop": "2018-06-05T08:07:36",
                           "priority": "10"},
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
        }]}
        self.engine_eboa.data = data
        self.engine_eboa.treat_data()

        events_ddbb = self.session.query(Event).all()

        assert len(events_ddbb) == 1

        data = {"operations": [{
            "mode": "insert_and_erase_with_priority",
            "dim_signature": {"name": "dim_signature",
                                  "exec": "exec",
                                  "version": "1.0"},
                "source": {"name": "source2.xml",
                           "reception_time": "2018-06-06T13:33:29",
                           "generation_time": "2018-07-05T02:07:04",
                           "validity_start": "2018-06-05T02:07:03",
                           "validity_stop": "2018-06-06T08:07:36",
                           "priority": "20"}
                }]
            }
        self.engine_eboa.data = data
        self.engine_eboa.treat_data()

        events_ddbb = self.session.query(Event).all()

        assert len(events_ddbb) == 0

    def test_treat_data_insert_and_erase_with_priority_at_dim_signature_level_same_dim_signature_diferent_processors(self):

        data = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "processor1",
                              "version": "1.0"},
            "source": {"name": "source.xml",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2018-07-05T02:07:03",
                       "validity_start": "2018-06-05T02:07:03",
                       "validity_stop": "2018-06-05T08:07:36",
                       "priority": "10"},
            "events": [{
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "INSERT_and_ERASE"},
                "start": "2018-06-05T02:07:03",
                "stop": "2018-06-05T08:07:36"
            }],

        }]}

        self.engine_eboa.data = data
        self.engine_eboa.treat_data()

        data = {"operations": [{
            "mode": "insert_and_erase_with_priority",
            "dim_signature": {"name": "dim_signature",
                              "exec": "processor2",
                              "version": "1.0"},
            "source": {"name": "source.xml",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2018-07-05T02:07:03",
                       "validity_start": "2018-06-05T02:07:03",
                       "validity_stop": "2018-06-05T08:07:36",
                       "priority": "20"},
            "events": [{
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "INSERT_and_ERASE"},
                "start": "2018-06-05T02:07:03",
                "stop": "2018-06-05T08:07:36"
            }],

        }]}

        self.engine_eboa.data = data
        returned_value = self.engine_eboa.treat_data()[0]["status"]

        assert returned_value == eboa_engine.exit_codes["OK"]["status"]

        events = self.session.query(Event).all()

        assert len(events) == 1

    def test_insert_and_erase_with_priority_at_dim_signature_level_with_sources_with_duration_0(self):

        data = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "processor1",
                              "version": "1.0"},
            "source": {"name": "source.xml",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2018-07-05T02:07:03",
                       "validity_start": "2018-06-05T02:07:03",
                       "validity_stop": "2018-06-05T08:07:36",
                       "priority": "10"},
            "events": [{
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "SIMPLE_UPDATE"},
                "start": "2018-06-05T02:07:03",
                "stop": "2018-06-05T08:07:36"
            }],

        }]}

        self.engine_eboa.data = data
        returned_value = self.engine_eboa.treat_data()[0]["status"]
        
        assert returned_value == eboa_engine.exit_codes["OK"]["status"]

        data = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "processor3",
                              "version": "1.0"},
            "source": {"name": "source.xml",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2018-07-05T02:07:03",
                       "validity_start": "2018-06-05T02:07:03",
                       "validity_stop": "2018-06-05T02:07:03",
                       "priority": "15"},
            "events": [{
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "SIMPLE_UPDATE"},
                "start": "2018-06-05T02:07:03",
                "stop": "2018-06-05T02:07:03"
            }],

        }]}

        self.engine_eboa.data = data
        returned_value = self.engine_eboa.treat_data()[0]["status"]
        
        assert returned_value == eboa_engine.exit_codes["OK"]["status"]

        data = {"operations": [{
            "mode": "insert_and_erase_with_priority",
            "dim_signature": {"name": "dim_signature",
                              "exec": "processor2",
                              "version": "1.0"},
            "source": {"name": "source.xml",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2018-07-05T02:07:04",
                       "validity_start": "2018-06-05T02:07:02",
                       "validity_stop": "2018-06-05T08:07:36",
                       "priority": "20"},
            "events": [{
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "SIMPLE_UPDATE"},
                "start": "2018-06-05T02:07:03",
                "stop": "2018-06-05T08:07:36"
            }],

        }]}

        self.engine_eboa.data = data
        returned_value = self.engine_eboa.treat_data()[0]["status"]

        assert returned_value == eboa_engine.exit_codes["OK"]["status"]

        events = self.session.query(Event).all()

        assert len(events) == 1

        events = self.session.query(Event).join(Source).filter(Source.processor == "processor2").all()

        assert len(events) == 1

    def test_insert_and_erase_with_priority_at_dim_signature_level_with_split_event_at_the_end(self):

        data = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "processor1",
                              "version": "1.0"},
            "source": {"name": "source.xml",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2018-07-05T02:07:03",
                       "validity_start": "2018-06-05T02:07:03",
                       "validity_stop": "2018-06-05T08:07:36",
                       "priority": "10"},
            "events": [{
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
            }]
                        }]
            }],

        }]}

        self.engine_eboa.data = data
        returned_value = self.engine_eboa.treat_data()[0]["status"]
        
        assert returned_value == eboa_engine.exit_codes["OK"]["status"]

        data = {"operations": [{
            "mode": "insert_and_erase_with_priority",
            "dim_signature": {"name": "dim_signature",
                              "exec": "processor2",
                              "version": "1.0"},
            "source": {"name": "source.xml",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2018-07-05T02:07:04",
                       "validity_start": "2018-06-05T02:07:03",
                       "validity_stop": "2018-06-05T06:07:36",
                       "priority": "20"},
            "events": [{
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "SIMPLE_UPDATE"},
                "start": "2018-06-05T02:07:03",
                "stop": "2018-06-05T06:07:36"
            }],

        }]}

        self.engine_eboa.data = data
        returned_value = self.engine_eboa.treat_data()[0]["status"]

        assert returned_value == eboa_engine.exit_codes["OK"]["status"]

        events = self.session.query(Event).all()

        assert len(events) == 2

        events = self.session.query(Event).join(Source).filter(Source.processor == "processor1",
                                                               Event.start == "2018-06-05T06:07:36",
                                                               Event.stop == "2018-06-05T08:07:36").all()


        assert len(events) == 1

        events = self.session.query(Event).join(Source).filter(Source.processor == "processor2",
                                                               Event.start == "2018-06-05T02:07:03",
                                                               Event.stop == "2018-06-05T06:07:36").all()

        assert len(events) == 1

    def test_insert_and_erase_with_priority_at_dim_signature_level_with_split_event_in_the_middle(self):

        data = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "processor",
                              "version": "1.0"},
            "source": {"name": "source1.xml",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2018-07-07T02:07:03",
                       "validity_start": "2018-06-05T02:07:03",
                       "validity_stop": "2018-06-05T06:07:36",
                       "priority": "20"},
            "events": [{
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "SIMPLE_UPDATE"},
                "start": "2018-06-05T02:07:03",
                "stop": "2018-06-05T06:07:36"
            }],

        }]}

        self.engine_eboa.data = data
        returned_value = self.engine_eboa.treat_data()[0]["status"]
        
        assert returned_value == eboa_engine.exit_codes["OK"]["status"]

        data = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "processor",
                              "version": "1.0"},
            "source": {"name": "source2.xml",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2018-07-06T02:07:03",
                       "validity_start": "2018-06-05T05:07:03",
                       "validity_stop": "2018-06-05T07:07:03",
                       "priority": "15"},
            "events": [{
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "SIMPLE_UPDATE"},
                "start": "2018-06-05T05:07:03",
                "stop": "2018-06-05T07:07:03"
            }],

        }]}

        self.engine_eboa.data = data
        returned_value = self.engine_eboa.treat_data()[0]["status"]
        
        assert returned_value == eboa_engine.exit_codes["OK"]["status"]

        data = {"operations": [{
            "mode": "insert_and_erase_with_priority",
            "dim_signature": {"name": "dim_signature",
                              "exec": "processor",
                              "version": "1.0"},
            "source": {"name": "source3.xml",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2018-07-06T02:07:03",
                       "validity_start": "2018-06-05T02:07:03",
                       "validity_stop": "2018-06-05T08:07:36",
                       "priority": "10"},
            "events": [{
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "SIMPLE_UPDATE"},
                "start": "2018-06-05T02:07:03",
                "stop": "2018-06-05T08:07:36"
            }],

        }]}

        self.engine_eboa.data = data
        returned_value = self.engine_eboa.treat_data()[0]["status"]

        assert returned_value == eboa_engine.exit_codes["OK"]["status"]

        events = self.session.query(Event).all()

        assert len(events) == 3

        events = self.session.query(Event).join(Source).filter(Source.name == "source1.xml",
                                                               Event.start == "2018-06-05T02:07:03",
                                                               Event.stop == "2018-06-05T06:07:36").all()


        assert len(events) == 1

        events = self.session.query(Event).join(Source).filter(Source.name == "source2.xml",
                                                               Event.start == "2018-06-05T06:07:36",
                                                               Event.stop == "2018-06-05T07:07:03").all()


        assert len(events) == 1

        events = self.session.query(Event).join(Source).filter(Source.name == "source3.xml",
                                                               Event.start == "2018-06-05T07:07:03",
                                                               Event.stop == "2018-06-05T08:07:36").all()

        assert len(events) == 1

    def test_insert_and_erase_with_priority_at_dim_signature_level_with_split_event_crossing_validity(self):

        data = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "processor",
                              "version": "1.0"},
            "source": {"name": "source1.xml",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2018-07-07T02:07:03",
                       "validity_start": "2018-06-05T02:07:03",
                       "validity_stop": "2018-06-05T06:07:36",
                       "priority": "20"},
            "events": [{
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "SIMPLE_UPDATE"},
                "start": "2018-06-05T02:07:03",
                "stop": "2018-06-05T06:07:36"
            }],

        }]}

        self.engine_eboa.data = data
        returned_value = self.engine_eboa.treat_data()[0]["status"]
        
        assert returned_value == eboa_engine.exit_codes["OK"]["status"]

        data = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "processor",
                              "version": "1.0"},
            "source": {"name": "source2.xml",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2018-07-06T02:07:03",
                       "validity_start": "2018-06-05T05:07:03",
                       "validity_stop": "2018-06-05T09:07:03",
                       "priority": "15"},
            "events": [{
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "SIMPLE_UPDATE"},
                "start": "2018-06-05T05:07:03",
                "stop": "2018-06-05T09:07:03"
            }],

        }]}

        self.engine_eboa.data = data
        returned_value = self.engine_eboa.treat_data()[0]["status"]
        
        assert returned_value == eboa_engine.exit_codes["OK"]["status"]

        data = {"operations": [{
            "mode": "insert_and_erase_with_priority",
            "dim_signature": {"name": "dim_signature",
                              "exec": "processor",
                              "version": "1.0"},
            "source": {"name": "source3.xml",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2018-07-06T02:07:03",
                       "validity_start": "2018-06-05T02:07:03",
                       "validity_stop": "2018-06-05T08:07:36",
                       "priority": "10"},
            "events": [{
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "SIMPLE_UPDATE"},
                "start": "2018-06-05T02:07:03",
                "stop": "2018-06-05T08:07:36"
            }],

        }]}

        self.engine_eboa.data = data
        returned_value = self.engine_eboa.treat_data()[0]["status"]

        assert returned_value == eboa_engine.exit_codes["OK"]["status"]

        events = self.session.query(Event).all()

        assert len(events) == 2

        events = self.session.query(Event).join(Source).filter(Source.name == "source1.xml",
                                                               Event.start == "2018-06-05T02:07:03",
                                                               Event.stop == "2018-06-05T06:07:36").all()


        assert len(events) == 1

        events = self.session.query(Event).join(Source).filter(Source.name == "source2.xml",
                                                               Event.start == "2018-06-05T06:07:36",
                                                               Event.stop == "2018-06-05T09:07:03").all()


        assert len(events) == 1

    def test_insert_event_insert_and_erase_with_priority_at_dim_signature_level_create_event_max_generation_time_not_ending_on_period(self):

        data1 = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source.json",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2020-07-07T02:07:03",
                       "validity_start": "2018-06-05T02:07:03",
                       "validity_stop": "2018-06-05T04:07:36",
                       "priority": "20"},
            "events": [{
                "explicit_reference": "EXPLICIT_REFERENCE_EVENT",
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "SIMPLE_UPDATE"},
                "start": "2018-06-05T02:07:03",
                "stop": "2018-06-05T04:07:36"
            }]
        }]
        }
        exit_status = self.engine_eboa.treat_data(data1)

        assert len([item for item in exit_status if item["status"] != eboa_engine.exit_codes["OK"]["status"]]) == 0

        events = self.session.query(Event).all()

        assert len(events) == 1

        event_uuid1 = events[0].event_uuid

        data2 = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source2.json",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2019-08-01T02:07:03",
                       "validity_start": "2018-06-05T03:07:03",
                       "validity_stop": "2018-06-05T08:07:36",
                       "priority": "15"},
            "events": [{
                "explicit_reference": "EXPLICIT_REFERENCE_EVENT",
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "SIMPLE_UPDATE"},
                "start": "2018-06-05T03:07:03",
                "stop": "2018-06-05T008:07:36"
            }]
        }]
        }
        exit_status = self.engine_eboa.treat_data(data2)

        assert len([item for item in exit_status if item["status"] != eboa_engine.exit_codes["OK"]["status"]]) == 0

        events = self.session.query(Event).all()

        assert len(events) == 2

        events = self.session.query(Event).join(Source).filter(Source.name == "source2.json").all()

        assert len(events) == 1

        event_uuid2 = events[0].event_uuid

        data3 = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source3.json",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2016-09-06T02:07:03",
                       "validity_start": "2018-06-05T04:07:20",
                       "validity_stop": "2018-06-05T06:07:36",
                       "priority": "5"}
        }]
        }
        
        exit_status = self.engine_eboa.treat_data(data3)

        assert len([item for item in exit_status if item["status"] != eboa_engine.exit_codes["OK"]["status"]]) == 0

        data4 = {"operations": [{
            "mode": "insert_and_erase_with_priority",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source4.json",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2016-10-03T02:07:03",
                       "validity_start": "2018-06-05T02:07:03",
                       "validity_stop": "2018-06-05T08:07:36",
                       "priority": "10"}
        }]
        }
        exit_status = self.engine_eboa.treat_data(data4)

        assert len([item for item in exit_status if item["status"] != eboa_engine.exit_codes["OK"]["status"]]) == 0

        events = self.session.query(Event).all()

        assert len(events) == 2
        
        events = self.session.query(Event).join(Gauge).join(Source).filter(Gauge.name == "GAUGE_NAME", 
                                                                           Gauge.system == "GAUGE_SYSTEM",
                                                                           Event.start == "2018-06-05T02:07:03",
                                                                           Event.stop == "2018-06-05T04:07:36",
                                                                           Source.name == "source.json").all()
        assert len(events) == 1

        events = self.session.query(Event).join(Gauge).join(Source).filter(Gauge.name == "GAUGE_NAME", 
                                                                           Gauge.system == "GAUGE_SYSTEM",
                                                                           Event.start == "2018-06-05T04:07:36",
                                                                           Event.stop == "2018-06-05T08:07:36",
                                                                           Source.name == "source2.json").all()
        assert len(events) == 1

    def test_insert_event_insert_and_erase_with_priority_at_dim_signature_level_remove_events_starting_before_validity_start(self):

        data1 = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source.json",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2020-07-07T02:07:03",
                       "validity_start": "2018-06-05T04:07:03",
                       "validity_stop": "2018-06-05T04:07:36",
                       "priority": "20"},
            "events": [{
                "explicit_reference": "EXPLICIT_REFERENCE_EVENT",
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "SIMPLE_UPDATE"},
                "start": "2018-06-05T04:07:03",
                "stop": "2018-06-05T04:07:36"
            }]
        }]
        }
        exit_status = self.engine_eboa.treat_data(data1)

        assert len([item for item in exit_status if item["status"] != eboa_engine.exit_codes["OK"]["status"]]) == 0

        events = self.session.query(Event).all()

        assert len(events) == 1

        event_uuid1 = events[0].event_uuid

        data2 = {"operations": [{
            "mode": "insert_and_erase",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source2.json",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2019-08-01T02:07:03",
                       "validity_start": "2018-06-05T03:07:03",
                       "validity_stop": "2018-06-05T08:07:36",
                       "priority": "10"},
            "events": [{
                "explicit_reference": "EXPLICIT_REFERENCE_EVENT",
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "SIMPLE_UPDATE"},
                "start": "2018-06-05T03:07:03",
                "stop": "2018-06-05T004:07:36"
            }]
        }]
        }
        exit_status = self.engine_eboa.treat_data(data2)

        assert len([item for item in exit_status if item["status"] != eboa_engine.exit_codes["OK"]["status"]]) == 0

        events = self.session.query(Event).all()

        assert len(events) == 2

        events = self.session.query(Event).join(Gauge).join(Source).filter(Gauge.name == "GAUGE_NAME", 
                                                                           Gauge.system == "GAUGE_SYSTEM",
                                                                           Event.start == "2018-06-05T03:07:03",
                                                                           Event.stop == "2018-06-05T04:07:03",
                                                                           Source.name == "source2.json").all()
        assert len(events) == 1

        data3 = {"operations": [{
            "mode": "insert_and_erase_with_priority",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source3.json",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2019-09-01T02:07:03",
                       "validity_start": "2018-06-05T03:07:03",
                       "validity_stop": "2018-06-05T08:07:36",
                       "priority": "15"},
            "events": [{
                "explicit_reference": "EXPLICIT_REFERENCE_EVENT",
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "SIMPLE_UPDATE"},
                "start": "2018-06-05T03:07:03",
                "stop": "2018-06-05T006:07:36"
            }]
        }]
        }
        exit_status = self.engine_eboa.treat_data(data3)

        assert len([item for item in exit_status if item["status"] != eboa_engine.exit_codes["OK"]["status"]]) == 0

        events = self.session.query(Event).all()

        assert len(events) == 3

        events = self.session.query(Event).join(Gauge).join(Source).filter(Gauge.name == "GAUGE_NAME", 
                                                                           Gauge.system == "GAUGE_SYSTEM",
                                                                           Event.start == "2018-06-05T03:07:03",
                                                                           Event.stop == "2018-06-05T04:07:03",
                                                                           Source.name == "source3.json").all()
        assert len(events) == 1

        events = self.session.query(Event).join(Gauge).join(Source).filter(Gauge.name == "GAUGE_NAME", 
                                                                           Gauge.system == "GAUGE_SYSTEM",
                                                                           Event.start == "2018-06-05T04:07:36",
                                                                           Event.stop == "2018-06-05T06:07:36",
                                                                           Source.name == "source3.json").all()
        assert len(events) == 1

    def test_insert_event_insert_and_erase_with_priority_at_dim_signature_level_remove_events_starting_before_validity_start_not_ending_on_period(self):

        data1 = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source.json",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2020-07-07T02:07:03",
                       "validity_start": "2018-06-05T04:07:03",
                       "validity_stop": "2018-06-05T04:07:36",
                           "priority": "20"},
            "events": [{
                "explicit_reference": "EXPLICIT_REFERENCE_EVENT",
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "SIMPLE_UPDATE"},
                "start": "2018-06-05T04:07:03",
                "stop": "2018-06-05T04:07:36"
            }]
        }]
        }
        exit_status = self.engine_eboa.treat_data(data1)

        assert len([item for item in exit_status if item["status"] != eboa_engine.exit_codes["OK"]["status"]]) == 0

        events = self.session.query(Event).all()

        assert len(events) == 1

        event_uuid1 = events[0].event_uuid

        data2 = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source2.json",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2019-08-01T02:07:03",
                       "validity_start": "2018-06-05T03:07:03",
                       "validity_stop": "2018-06-05T08:07:36",
                       "priority": "15"},
            "events": [{
                "explicit_reference": "EXPLICIT_REFERENCE_EVENT",
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "SIMPLE_UPDATE"},
                "start": "2018-06-05T04:07:20",
                "stop": "2018-06-05T008:07:36"
            }]
        }]
        }
        exit_status = self.engine_eboa.treat_data(data2)

        assert len([item for item in exit_status if item["status"] != eboa_engine.exit_codes["OK"]["status"]]) == 0

        data3 = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source3.json",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2016-08-01T02:07:03",
                       "validity_start": "2018-06-05T04:07:36",
                       "validity_stop": "2018-06-05T05:07:36",
                       "priority": "5"},
            "events": [{
                "explicit_reference": "EXPLICIT_REFERENCE_EVENT",
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "SIMPLE_UPDATE"},
                "start": "2018-06-05T04:07:36",
                "stop": "2018-06-05T005:07:36"
            }]
        }]
        }
        exit_status = self.engine_eboa.treat_data(data3)

        assert len([item for item in exit_status if item["status"] != eboa_engine.exit_codes["OK"]["status"]]) == 0

        data4 = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source4.json",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2020-08-01T02:07:03",
                       "validity_start": "2018-06-05T06:07:36",
                       "validity_stop": "2018-06-05T08:07:36",
                       "priority": "25"}
        }]
        }
        exit_status = self.engine_eboa.treat_data(data4)

        assert len([item for item in exit_status if item["status"] != eboa_engine.exit_codes["OK"]["status"]]) == 0

        data5 = {"operations": [{
            "mode": "insert_and_erase_with_priority",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source5.json",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2016-10-03T02:07:03",
                       "validity_start": "2018-06-05T02:07:03",
                       "validity_stop": "2018-06-05T08:07:36",
                       "priority": "10"},
            "events": [{
                "explicit_reference": "EXPLICIT_REFERENCE_EVENT",
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "SIMPLE_UPDATE"},
                "start": "2018-06-05T04:07:20",
                "stop": "2018-06-05T005:07:36"
            }]
        }]
        }
        exit_status = self.engine_eboa.treat_data(data5)

        assert len([item for item in exit_status if item["status"] != eboa_engine.exit_codes["OK"]["status"]]) == 0

        events = self.session.query(Event).all()

        assert len(events) == 2
        
        events = self.session.query(Event).join(Gauge).join(Source).filter(Gauge.name == "GAUGE_NAME", 
                                                                           Gauge.system == "GAUGE_SYSTEM",
                                                                           Event.start == "2018-06-05T04:07:03",
                                                                           Event.stop == "2018-06-05T04:07:36",
                                                                           Source.name == "source.json").all()
        assert len(events) == 1

        events = self.session.query(Event).join(Gauge).join(Source).filter(Gauge.name == "GAUGE_NAME", 
                                                                           Gauge.system == "GAUGE_SYSTEM",
                                                                           Event.start == "2018-06-05T04:07:36",
                                                                           Event.stop == "2018-06-05T06:07:36",
                                                                           Source.name == "source2.json").all()
        assert len(events) == 1

    def test_insert_event_insert_and_erase_with_priority_at_dim_signature_no_priority_defined(self):

        data = {"operations": [{
            "mode": "insert_and_erase_with_priority",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source.json",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2020-07-07T02:07:03",
                       "validity_start": "2018-06-05T04:07:03",
                       "validity_stop": "2018-06-05T04:07:36"}
        }]
        }
        exit_status = self.engine_eboa.treat_data(data)

        assert len([item for item in exit_status if item["status"] != eboa_engine.exit_codes["PRIORITY_NOT_DEFINED"]["status"]]) == 0

    #####
    # Test insertion mode insert_and_erase_with_equal_or_lower_priority
    #####
    def test_remove_deprecated_event_insert_and_erase_with_equal_or_lower_priority_at_dim_signature_level_no_events(self):

        data = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                                  "exec": "exec",
                                  "version": "1.0"},
                "source": {"name": "source.xml",
                           "reception_time": "2018-06-06T13:33:29",
                           "generation_time": "2018-07-05T02:07:03",
                           "validity_start": "2018-06-05T02:07:03",
                           "validity_stop": "2018-06-05T08:07:36",
                           "priority": "10"},
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
        }]}
        self.engine_eboa.data = data
        self.engine_eboa.treat_data()

        events_ddbb = self.session.query(Event).all()

        assert len(events_ddbb) == 1

        data = {"operations": [{
            "mode": "insert_and_erase_with_equal_or_lower_priority",
            "dim_signature": {"name": "dim_signature",
                                  "exec": "exec",
                                  "version": "1.0"},
                "source": {"name": "source2.xml",
                           "reception_time": "2018-06-06T13:33:29",
                           "generation_time": "2018-07-05T02:07:04",
                           "validity_start": "2018-06-05T02:07:03",
                           "validity_stop": "2018-06-06T08:07:36",
                           "priority": "20"}
                }]
            }
        self.engine_eboa.data = data
        self.engine_eboa.treat_data()

        events_ddbb = self.session.query(Event).all()

        assert len(events_ddbb) == 0

    def test_treat_data_insert_and_erase_with_equal_or_lower_priority_at_dim_signature_level_same_dim_signature_diferent_processors(self):

        data = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "processor1",
                              "version": "1.0"},
            "source": {"name": "source.xml",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2018-07-05T02:07:03",
                       "validity_start": "2018-06-05T02:07:03",
                       "validity_stop": "2018-06-05T08:07:36",
                       "priority": "10"},
            "events": [{
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "INSERT_and_ERASE"},
                "start": "2018-06-05T02:07:03",
                "stop": "2018-06-05T08:07:36"
            }],

        }]}

        self.engine_eboa.data = data
        self.engine_eboa.treat_data()

        data = {"operations": [{
            "mode": "insert_and_erase_with_equal_or_lower_priority",
            "dim_signature": {"name": "dim_signature",
                              "exec": "processor2",
                              "version": "1.0"},
            "source": {"name": "source.xml",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2018-07-05T02:07:03",
                       "validity_start": "2018-06-05T02:07:03",
                       "validity_stop": "2018-06-05T08:07:36",
                       "priority": "20"},
            "events": [{
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "INSERT_and_ERASE"},
                "start": "2018-06-05T02:07:03",
                "stop": "2018-06-05T08:07:36"
            }],

        }]}

        self.engine_eboa.data = data
        returned_value = self.engine_eboa.treat_data()[0]["status"]

        assert returned_value == eboa_engine.exit_codes["OK"]["status"]

        events = self.session.query(Event).all()

        assert len(events) == 1

    def test_insert_and_erase_with_equal_or_lower_priority_at_dim_signature_level_with_sources_with_duration_0(self):

        data = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "processor1",
                              "version": "1.0"},
            "source": {"name": "source.xml",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2018-07-05T02:07:03",
                       "validity_start": "2018-06-05T02:07:03",
                       "validity_stop": "2018-06-05T08:07:36",
                       "priority": "10"},
            "events": [{
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "SIMPLE_UPDATE"},
                "start": "2018-06-05T02:07:03",
                "stop": "2018-06-05T08:07:36"
            }],

        }]}

        self.engine_eboa.data = data
        returned_value = self.engine_eboa.treat_data()[0]["status"]
        
        assert returned_value == eboa_engine.exit_codes["OK"]["status"]

        data = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "processor3",
                              "version": "1.0"},
            "source": {"name": "source.xml",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2018-07-05T02:07:03",
                       "validity_start": "2018-06-05T02:07:03",
                       "validity_stop": "2018-06-05T02:07:03",
                       "priority": "15"},
            "events": [{
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "SIMPLE_UPDATE"},
                "start": "2018-06-05T02:07:03",
                "stop": "2018-06-05T02:07:03"
            }],

        }]}

        self.engine_eboa.data = data
        returned_value = self.engine_eboa.treat_data()[0]["status"]
        
        assert returned_value == eboa_engine.exit_codes["OK"]["status"]

        data = {"operations": [{
            "mode": "insert_and_erase_with_equal_or_lower_priority",
            "dim_signature": {"name": "dim_signature",
                              "exec": "processor2",
                              "version": "1.0"},
            "source": {"name": "source.xml",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2018-07-05T02:07:04",
                       "validity_start": "2018-06-05T02:07:02",
                       "validity_stop": "2018-06-05T08:07:36",
                       "priority": "20"},
            "events": [{
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "SIMPLE_UPDATE"},
                "start": "2018-06-05T02:07:03",
                "stop": "2018-06-05T08:07:36"
            }],

        }]}

        self.engine_eboa.data = data
        returned_value = self.engine_eboa.treat_data()[0]["status"]

        assert returned_value == eboa_engine.exit_codes["OK"]["status"]

        events = self.session.query(Event).all()

        assert len(events) == 1

        events = self.session.query(Event).join(Source).filter(Source.processor == "processor2").all()

        assert len(events) == 1

    def test_insert_and_erase_with_equal_or_lower_priority_at_dim_signature_level_with_split_event_at_the_end(self):

        data = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "processor1",
                              "version": "1.0"},
            "source": {"name": "source.xml",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2018-07-05T02:07:03",
                       "validity_start": "2018-06-05T02:07:03",
                       "validity_stop": "2018-06-05T08:07:36",
                       "priority": "10"},
            "events": [{
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
            }]
                        }]
            }],

        }]}

        self.engine_eboa.data = data
        returned_value = self.engine_eboa.treat_data()[0]["status"]
        
        assert returned_value == eboa_engine.exit_codes["OK"]["status"]

        data = {"operations": [{
            "mode": "insert_and_erase_with_equal_or_lower_priority",
            "dim_signature": {"name": "dim_signature",
                              "exec": "processor2",
                              "version": "1.0"},
            "source": {"name": "source.xml",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2018-07-05T02:07:04",
                       "validity_start": "2018-06-05T02:07:03",
                       "validity_stop": "2018-06-05T06:07:36",
                       "priority": "20"},
            "events": [{
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "SIMPLE_UPDATE"},
                "start": "2018-06-05T02:07:03",
                "stop": "2018-06-05T06:07:36"
            }],

        }]}

        self.engine_eboa.data = data
        returned_value = self.engine_eboa.treat_data()[0]["status"]

        assert returned_value == eboa_engine.exit_codes["OK"]["status"]

        events = self.session.query(Event).all()

        assert len(events) == 2

        events = self.session.query(Event).join(Source).filter(Source.processor == "processor1",
                                                               Event.start == "2018-06-05T06:07:36",
                                                               Event.stop == "2018-06-05T08:07:36").all()


        assert len(events) == 1

        events = self.session.query(Event).join(Source).filter(Source.processor == "processor2",
                                                               Event.start == "2018-06-05T02:07:03",
                                                               Event.stop == "2018-06-05T06:07:36").all()

        assert len(events) == 1

    def test_insert_and_erase_with_equal_or_lower_priority_at_dim_signature_level_with_split_event_in_the_middle(self):

        data = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "processor",
                              "version": "1.0"},
            "source": {"name": "source1.xml",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2018-07-07T02:07:03",
                       "validity_start": "2018-06-05T02:07:03",
                       "validity_stop": "2018-06-05T06:07:36",
                       "priority": "20"},
            "events": [{
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "SIMPLE_UPDATE"},
                "start": "2018-06-05T02:07:03",
                "stop": "2018-06-05T06:07:36"
            }],

        }]}

        self.engine_eboa.data = data
        returned_value = self.engine_eboa.treat_data()[0]["status"]
        
        assert returned_value == eboa_engine.exit_codes["OK"]["status"]

        data = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "processor",
                              "version": "1.0"},
            "source": {"name": "source2.xml",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2018-07-06T02:07:03",
                       "validity_start": "2018-06-05T05:07:03",
                       "validity_stop": "2018-06-05T07:07:03",
                       "priority": "15"},
            "events": [{
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "SIMPLE_UPDATE"},
                "start": "2018-06-05T05:07:03",
                "stop": "2018-06-05T07:07:03"
            }],

        }]}

        self.engine_eboa.data = data
        returned_value = self.engine_eboa.treat_data()[0]["status"]
        
        assert returned_value == eboa_engine.exit_codes["OK"]["status"]

        data = {"operations": [{
            "mode": "insert_and_erase_with_equal_or_lower_priority",
            "dim_signature": {"name": "dim_signature",
                              "exec": "processor",
                              "version": "1.0"},
            "source": {"name": "source3.xml",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2018-07-06T02:07:03",
                       "validity_start": "2018-06-05T02:07:03",
                       "validity_stop": "2018-06-05T08:07:36",
                       "priority": "10"},
            "events": [{
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "SIMPLE_UPDATE"},
                "start": "2018-06-05T02:07:03",
                "stop": "2018-06-05T08:07:36"
            }],

        }]}

        self.engine_eboa.data = data
        returned_value = self.engine_eboa.treat_data()[0]["status"]

        assert returned_value == eboa_engine.exit_codes["OK"]["status"]

        events = self.session.query(Event).all()

        assert len(events) == 3

        events = self.session.query(Event).join(Source).filter(Source.name == "source1.xml",
                                                               Event.start == "2018-06-05T02:07:03",
                                                               Event.stop == "2018-06-05T06:07:36").all()


        assert len(events) == 1

        events = self.session.query(Event).join(Source).filter(Source.name == "source2.xml",
                                                               Event.start == "2018-06-05T05:07:03",
                                                               Event.stop == "2018-06-05T07:07:03").all()


        assert len(events) == 1

        events = self.session.query(Event).join(Source).filter(Source.name == "source3.xml",
                                                               Event.start == "2018-06-05T07:07:03",
                                                               Event.stop == "2018-06-05T08:07:36").all()

        assert len(events) == 1

    def test_insert_and_erase_with_equal_or_lower_priority_at_dim_signature_level_with_split_event_crossing_validity(self):

        data = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "processor",
                              "version": "1.0"},
            "source": {"name": "source1.xml",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2018-07-07T02:07:03",
                       "validity_start": "2018-06-05T02:07:03",
                       "validity_stop": "2018-06-05T06:07:36",
                       "priority": "20"},
            "events": [{
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "SIMPLE_UPDATE"},
                "start": "2018-06-05T02:07:03",
                "stop": "2018-06-05T06:07:36"
            }],

        }]}

        self.engine_eboa.data = data
        returned_value = self.engine_eboa.treat_data()[0]["status"]
        
        assert returned_value == eboa_engine.exit_codes["OK"]["status"]

        data = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "processor",
                              "version": "1.0"},
            "source": {"name": "source2.xml",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2018-08-06T02:07:03",
                       "validity_start": "2018-06-05T05:07:03",
                       "validity_stop": "2018-06-05T09:07:03",
                       "priority": "15"},
            "events": [{
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "SIMPLE_UPDATE"},
                "start": "2018-06-05T05:07:03",
                "stop": "2018-06-05T09:07:03"
            }],

        }]}

        self.engine_eboa.data = data
        returned_value = self.engine_eboa.treat_data()[0]["status"]
        
        assert returned_value == eboa_engine.exit_codes["OK"]["status"]

        data = {"operations": [{
            "mode": "insert_and_erase_with_equal_or_lower_priority",
            "dim_signature": {"name": "dim_signature",
                              "exec": "processor",
                              "version": "1.0"},
            "source": {"name": "source3.xml",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2018-07-06T02:07:03",
                       "validity_start": "2018-06-05T02:07:03",
                       "validity_stop": "2018-06-05T08:07:36",
                       "priority": "15"},
            "events": [{
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "SIMPLE_UPDATE"},
                "start": "2018-06-05T02:07:03",
                "stop": "2018-06-05T08:07:36"
            }],

        }]}

        self.engine_eboa.data = data
        returned_value = self.engine_eboa.treat_data()[0]["status"]

        assert returned_value == eboa_engine.exit_codes["OK"]["status"]

        events = self.session.query(Event).all()

        assert len(events) == 2

        events = self.session.query(Event).join(Source).filter(Source.name == "source1.xml",
                                                               Event.start == "2018-06-05T02:07:03",
                                                               Event.stop == "2018-06-05T06:07:36").all()


        assert len(events) == 1

        events = self.session.query(Event).join(Source).filter(Source.name == "source2.xml",
                                                               Event.start == "2018-06-05T06:07:36",
                                                               Event.stop == "2018-06-05T09:07:03").all()


        assert len(events) == 1

    def test_insert_event_insert_and_erase_with_equal_or_lower_priority_at_dim_signature_level_create_event_max_generation_time_not_ending_on_period(self):

        data1 = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source.json",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2020-07-07T02:07:03",
                       "validity_start": "2018-06-05T02:07:03",
                       "validity_stop": "2018-06-05T04:07:36",
                       "priority": "20"},
            "events": [{
                "explicit_reference": "EXPLICIT_REFERENCE_EVENT",
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "SIMPLE_UPDATE"},
                "start": "2018-06-05T02:07:03",
                "stop": "2018-06-05T04:07:36"
            }]
        }]
        }
        exit_status = self.engine_eboa.treat_data(data1)

        assert len([item for item in exit_status if item["status"] != eboa_engine.exit_codes["OK"]["status"]]) == 0

        events = self.session.query(Event).all()

        assert len(events) == 1

        event_uuid1 = events[0].event_uuid

        data2 = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source2.json",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2019-08-01T02:07:03",
                       "validity_start": "2018-06-05T03:07:03",
                       "validity_stop": "2018-06-05T08:07:36",
                       "priority": "15"},
            "events": [{
                "explicit_reference": "EXPLICIT_REFERENCE_EVENT",
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "SIMPLE_UPDATE"},
                "start": "2018-06-05T03:07:03",
                "stop": "2018-06-05T008:07:36"
            }]
        }]
        }
        exit_status = self.engine_eboa.treat_data(data2)

        assert len([item for item in exit_status if item["status"] != eboa_engine.exit_codes["OK"]["status"]]) == 0

        events = self.session.query(Event).all()

        assert len(events) == 2

        events = self.session.query(Event).join(Source).filter(Source.name == "source2.json").all()

        assert len(events) == 1

        event_uuid2 = events[0].event_uuid

        data3 = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source3.json",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2016-09-06T02:07:03",
                       "validity_start": "2018-06-05T04:07:20",
                       "validity_stop": "2018-06-05T06:07:36",
                       "priority": "5"}
        }]
        }
        
        exit_status = self.engine_eboa.treat_data(data3)

        assert len([item for item in exit_status if item["status"] != eboa_engine.exit_codes["OK"]["status"]]) == 0

        data4 = {"operations": [{
            "mode": "insert_and_erase_with_equal_or_lower_priority",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source4.json",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2016-10-03T02:07:03",
                       "validity_start": "2018-06-05T02:07:03",
                       "validity_stop": "2018-06-05T08:07:36",
                       "priority": "10"}
        }]
        }
        exit_status = self.engine_eboa.treat_data(data4)

        assert len([item for item in exit_status if item["status"] != eboa_engine.exit_codes["OK"]["status"]]) == 0

        events = self.session.query(Event).all()

        assert len(events) == 2
        
        events = self.session.query(Event).join(Gauge).join(Source).filter(Gauge.name == "GAUGE_NAME", 
                                                                           Gauge.system == "GAUGE_SYSTEM",
                                                                           Event.start == "2018-06-05T02:07:03",
                                                                           Event.stop == "2018-06-05T04:07:36",
                                                                           Source.name == "source.json").all()
        assert len(events) == 1

        events = self.session.query(Event).join(Gauge).join(Source).filter(Gauge.name == "GAUGE_NAME", 
                                                                           Gauge.system == "GAUGE_SYSTEM",
                                                                           Event.start == "2018-06-05T03:07:03",
                                                                           Event.stop == "2018-06-05T08:07:36",
                                                                           Source.name == "source2.json").all()
        assert len(events) == 1

    def test_insert_event_insert_and_erase_with_equal_or_lower_priority_at_dim_signature_level_remove_events_starting_before_validity_start(self):

        data1 = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source.json",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2020-07-07T02:07:03",
                       "validity_start": "2018-06-05T04:07:03",
                       "validity_stop": "2018-06-05T04:07:36",
                       "priority": "20"},
            "events": [{
                "explicit_reference": "EXPLICIT_REFERENCE_EVENT",
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "SIMPLE_UPDATE"},
                "start": "2018-06-05T04:07:03",
                "stop": "2018-06-05T04:07:36"
            }]
        }]
        }
        exit_status = self.engine_eboa.treat_data(data1)

        assert len([item for item in exit_status if item["status"] != eboa_engine.exit_codes["OK"]["status"]]) == 0

        events = self.session.query(Event).all()

        assert len(events) == 1

        event_uuid1 = events[0].event_uuid

        data2 = {"operations": [{
            "mode": "insert_and_erase",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source2.json",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2019-08-01T02:07:03",
                       "validity_start": "2018-06-05T03:07:03",
                       "validity_stop": "2018-06-05T08:07:36",
                       "priority": "10"},
            "events": [{
                "explicit_reference": "EXPLICIT_REFERENCE_EVENT",
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "SIMPLE_UPDATE"},
                "start": "2018-06-05T03:07:03",
                "stop": "2018-06-05T004:07:36"
            }]
        }]
        }
        exit_status = self.engine_eboa.treat_data(data2)

        assert len([item for item in exit_status if item["status"] != eboa_engine.exit_codes["OK"]["status"]]) == 0

        events = self.session.query(Event).all()

        assert len(events) == 2

        events = self.session.query(Event).join(Gauge).join(Source).filter(Gauge.name == "GAUGE_NAME", 
                                                                           Gauge.system == "GAUGE_SYSTEM",
                                                                           Event.start == "2018-06-05T03:07:03",
                                                                           Event.stop == "2018-06-05T04:07:03",
                                                                           Source.name == "source2.json").all()
        assert len(events) == 1

        data3 = {"operations": [{
            "mode": "insert_and_erase_with_equal_or_lower_priority",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source3.json",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2019-09-01T02:07:03",
                       "validity_start": "2018-06-05T03:07:03",
                       "validity_stop": "2018-06-05T08:07:36",
                       "priority": "15"},
            "events": [{
                "explicit_reference": "EXPLICIT_REFERENCE_EVENT",
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "SIMPLE_UPDATE"},
                "start": "2018-06-05T03:07:03",
                "stop": "2018-06-05T006:07:36"
            }]
        }]
        }
        exit_status = self.engine_eboa.treat_data(data3)

        assert len([item for item in exit_status if item["status"] != eboa_engine.exit_codes["OK"]["status"]]) == 0

        events = self.session.query(Event).all()

        assert len(events) == 3

        events = self.session.query(Event).join(Gauge).join(Source).filter(Gauge.name == "GAUGE_NAME", 
                                                                           Gauge.system == "GAUGE_SYSTEM",
                                                                           Event.start == "2018-06-05T03:07:03",
                                                                           Event.stop == "2018-06-05T04:07:03",
                                                                           Source.name == "source3.json").all()
        assert len(events) == 1

        events = self.session.query(Event).join(Gauge).join(Source).filter(Gauge.name == "GAUGE_NAME", 
                                                                           Gauge.system == "GAUGE_SYSTEM",
                                                                           Event.start == "2018-06-05T04:07:36",
                                                                           Event.stop == "2018-06-05T06:07:36",
                                                                           Source.name == "source3.json").all()
        assert len(events) == 1

    def test_insert_event_insert_and_erase_with_equal_or_lower_priority_at_dim_signature_level_remove_events_starting_before_validity_start_not_ending_on_period(self):

        data1 = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source.json",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2020-07-07T02:07:03",
                       "validity_start": "2018-06-05T04:07:03",
                       "validity_stop": "2018-06-05T04:07:36",
                           "priority": "20"},
            "events": [{
                "explicit_reference": "EXPLICIT_REFERENCE_EVENT",
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "SIMPLE_UPDATE"},
                "start": "2018-06-05T04:07:03",
                "stop": "2018-06-05T04:07:36"
            }]
        }]
        }
        exit_status = self.engine_eboa.treat_data(data1)

        assert len([item for item in exit_status if item["status"] != eboa_engine.exit_codes["OK"]["status"]]) == 0

        events = self.session.query(Event).all()

        assert len(events) == 1

        event_uuid1 = events[0].event_uuid

        data2 = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source2.json",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2019-08-01T02:07:03",
                       "validity_start": "2018-06-05T03:07:03",
                       "validity_stop": "2018-06-05T08:07:36",
                       "priority": "15"},
            "events": [{
                "explicit_reference": "EXPLICIT_REFERENCE_EVENT",
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "SIMPLE_UPDATE"},
                "start": "2018-06-05T04:07:20",
                "stop": "2018-06-05T008:07:36"
            }]
        }]
        }
        exit_status = self.engine_eboa.treat_data(data2)

        assert len([item for item in exit_status if item["status"] != eboa_engine.exit_codes["OK"]["status"]]) == 0

        data3 = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source3.json",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2016-08-01T02:07:03",
                       "validity_start": "2018-06-05T04:07:36",
                       "validity_stop": "2018-06-05T05:07:36",
                       "priority": "5"},
            "events": [{
                "explicit_reference": "EXPLICIT_REFERENCE_EVENT",
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "SIMPLE_UPDATE"},
                "start": "2018-06-05T04:07:36",
                "stop": "2018-06-05T005:07:36"
            }]
        }]
        }
        exit_status = self.engine_eboa.treat_data(data3)

        assert len([item for item in exit_status if item["status"] != eboa_engine.exit_codes["OK"]["status"]]) == 0

        data4 = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source4.json",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2020-08-01T02:07:03",
                       "validity_start": "2018-06-05T06:07:36",
                       "validity_stop": "2018-06-05T08:07:36",
                       "priority": "25"}
        }]
        }
        exit_status = self.engine_eboa.treat_data(data4)

        assert len([item for item in exit_status if item["status"] != eboa_engine.exit_codes["OK"]["status"]]) == 0

        data5 = {"operations": [{
            "mode": "insert_and_erase_with_equal_or_lower_priority",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source5.json",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2016-10-03T02:07:03",
                       "validity_start": "2018-06-05T02:07:03",
                       "validity_stop": "2018-06-05T08:07:36",
                       "priority": "15"},
            "events": [{
                "explicit_reference": "EXPLICIT_REFERENCE_EVENT",
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "SIMPLE_UPDATE"},
                "start": "2018-06-05T04:07:20",
                "stop": "2018-06-05T005:07:36"
            }]
        }]
        }
        exit_status = self.engine_eboa.treat_data(data5)

        assert len([item for item in exit_status if item["status"] != eboa_engine.exit_codes["OK"]["status"]]) == 0

        events = self.session.query(Event).all()

        assert len(events) == 2
        
        events = self.session.query(Event).join(Gauge).join(Source).filter(Gauge.name == "GAUGE_NAME", 
                                                                           Gauge.system == "GAUGE_SYSTEM",
                                                                           Event.start == "2018-06-05T04:07:03",
                                                                           Event.stop == "2018-06-05T04:07:36",
                                                                           Source.name == "source.json").all()
        assert len(events) == 1

        events = self.session.query(Event).join(Gauge).join(Source).filter(Gauge.name == "GAUGE_NAME", 
                                                                           Gauge.system == "GAUGE_SYSTEM",
                                                                           Event.start == "2018-06-05T04:07:36",
                                                                           Event.stop == "2018-06-05T06:07:36",
                                                                           Source.name == "source2.json").all()
        assert len(events) == 1

    def test_insert_event_insert_and_erase_with_equal_or_lower_priority_at_dim_signature_level_create_event_max_generation_time_ending_on_period(self):

        data1 = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source.json",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2020-07-07T02:07:03",
                       "validity_start": "2018-06-05T04:07:03",
                       "validity_stop": "2018-06-05T04:07:36",
                           "priority": "20"},
            "events": [{
                "explicit_reference": "EXPLICIT_REFERENCE_EVENT",
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "SIMPLE_UPDATE"},
                "start": "2018-06-05T04:07:03",
                "stop": "2018-06-05T04:07:36"
            }]
        }]
        }
        exit_status = self.engine_eboa.treat_data(data1)

        assert len([item for item in exit_status if item["status"] != eboa_engine.exit_codes["OK"]["status"]]) == 0

        events = self.session.query(Event).all()

        assert len(events) == 1

        event_uuid1 = events[0].event_uuid

        data2 = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source2.json",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2019-08-01T02:07:03",
                       "validity_start": "2018-06-05T03:07:03",
                       "validity_stop": "2018-06-05T08:07:36",
                       "priority": "15"},
            "events": [{
                "explicit_reference": "EXPLICIT_REFERENCE_EVENT",
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "SIMPLE_UPDATE"},
                "start": "2018-06-05T04:07:20",
                "stop": "2018-06-05T006:07:36"
            }]
        }]
        }
        exit_status = self.engine_eboa.treat_data(data2)

        assert len([item for item in exit_status if item["status"] != eboa_engine.exit_codes["OK"]["status"]]) == 0

        data3 = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source3.json",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2016-08-01T02:07:03",
                       "validity_start": "2018-06-05T04:07:36",
                       "validity_stop": "2018-06-05T05:07:36",
                       "priority": "5"},
            "events": [{
                "explicit_reference": "EXPLICIT_REFERENCE_EVENT",
                "gauge": {"name": "GAUGE_NAME",
                          "system": "GAUGE_SYSTEM",
                          "insertion_type": "SIMPLE_UPDATE"},
                "start": "2018-06-05T04:07:36",
                "stop": "2018-06-05T005:07:36"
            }]
        }]
        }
        exit_status = self.engine_eboa.treat_data(data3)

        assert len([item for item in exit_status if item["status"] != eboa_engine.exit_codes["OK"]["status"]]) == 0

        data4 = {"operations": [{
            "mode": "insert_and_erase_with_equal_or_lower_priority",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source5.json",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2016-10-03T02:07:03",
                       "validity_start": "2018-06-05T02:07:03",
                       "validity_stop": "2018-06-05T08:07:36",
                       "priority": "15"}
        }]
        }
        exit_status = self.engine_eboa.treat_data(data4)

        assert len([item for item in exit_status if item["status"] != eboa_engine.exit_codes["OK"]["status"]]) == 0

        events = self.session.query(Event).all()

        assert len(events) == 2
        
        events = self.session.query(Event).join(Gauge).join(Source).filter(Gauge.name == "GAUGE_NAME", 
                                                                           Gauge.system == "GAUGE_SYSTEM",
                                                                           Event.start == "2018-06-05T04:07:03",
                                                                           Event.stop == "2018-06-05T04:07:36",
                                                                           Source.name == "source.json").all()
        assert len(events) == 1

        events = self.session.query(Event).join(Gauge).join(Source).filter(Gauge.name == "GAUGE_NAME", 
                                                                           Gauge.system == "GAUGE_SYSTEM",
                                                                           Event.start == "2018-06-05T04:07:36",
                                                                           Event.stop == "2018-06-05T06:07:36",
                                                                           Source.name == "source2.json").all()
        assert len(events) == 1

    def test_insert_event_insert_and_erase_with_equal_or_lower_priority_at_dim_signature_no_priority_defined(self):

        data = {"operations": [{
            "mode": "insert_and_erase_with_equal_or_lower_priority",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source.json",
                       "reception_time": "2018-06-06T13:33:29",
                       "generation_time": "2020-07-07T02:07:03",
                       "validity_start": "2018-06-05T04:07:03",
                       "validity_stop": "2018-06-05T04:07:36"}
        }]
        }
        exit_status = self.engine_eboa.treat_data(data)

        assert len([item for item in exit_status if item["status"] != eboa_engine.exit_codes["PRIORITY_NOT_DEFINED"]["status"]]) == 0
