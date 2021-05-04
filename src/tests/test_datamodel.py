"""
Automated tests for the datamodel submodule

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
import json
import tempfile

# Import engine of the DDBB
import eboa.engine.engine as eboa_engine
from eboa.engine.engine import Engine
from eboa.engine.query import Query

# Import datamodel
import eboa.datamodel.base
from eboa.datamodel.base import Session, engine, Base
from eboa.datamodel.dim_signatures import DimSignature
from eboa.datamodel.alerts import Alert
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

class TestDatamodel(unittest.TestCase):
    def setUp(self):
        # Create the engine to manage the data
        self.engine_eboa = Engine()
        self.query_eboa = Query()

        # Create session to connect to the database
        self.session = Session()

        # Clear all tables before executing the test
        self.query_eboa.clear_db()

    def tearDown(self):
        # Close connections to the DDBB
        self.engine_eboa.close_session()
        self.query_eboa.close_session()
        self.session.close()

    def test_insert_data(self):
        """ Verify the insertion of data using the classes defined in the datamodel. """
        dim_signature_uuid = uuid.uuid1(node = os.getpid(), clock_seq = random.getrandbits(14))
        dim_signature = DimSignature(dim_signature_uuid, "DIM_SIGNATURE_NAME")
        
        # Insert dim_signature into database
        self.session.add(dim_signature)
        self.session.commit()
        
        assert len (self.session.query(DimSignature).filter(DimSignature.dim_signature == "DIM_SIGNATURE_NAME").all()) == 1

        processing_time = datetime.datetime.now()
        source_uuid = uuid.uuid1(node = os.getpid(), clock_seq = random.getrandbits(14))
        source = Source(source_uuid, "DIM_PROCESSING_NAME", processing_time, processing_time, "1.0", dim_signature, processor = "PROCESSOR")
        
        # Insert source into database
        self.session.add(source)
        self.session.commit()
        
        assert len(self.session.query(Source).filter(Source.source_uuid == source_uuid, Source.name == "DIM_PROCESSING_NAME", Source.generation_time == processing_time, Source.processor_version == "1.0", Source.dim_signature_uuid == dim_signature.dim_signature_uuid, Source.processor == "PROCESSOR").all()) == 1

        # Insert status for the source
        id = uuid.uuid1(node = os.getpid(), clock_seq = random.getrandbits(14))        
        self.session.add(SourceStatus(id, processing_time, 0, source))
        self.session.commit()
        # Insert status for the source
        id = uuid.uuid1(node = os.getpid(), clock_seq = random.getrandbits(14))
        self.session.add(SourceStatus(id, processing_time, 0, source))
        self.session.commit()

        assert len(self.session.query(SourceStatus).filter(SourceStatus.source_uuid == source_uuid, SourceStatus.status == 0).all()) == 2

        # Insert explicit reference group
        explicit_ref_grp_uuid = uuid.uuid1(node = os.getpid(), clock_seq = random.getrandbits(14))
        explicit_ref_grp = ExplicitRefGrp(explicit_ref_grp_uuid, "EXPLICIT_REF_GRP_NAME")
        self.session.add (explicit_ref_grp)
        self.session.commit()

        assert len (self.session.query(ExplicitRefGrp).filter(ExplicitRefGrp.name == "EXPLICIT_REF_GRP_NAME").all()) == 1        

        # Insert explicit references
        explicit_ref1_uuid = uuid.uuid1(node = os.getpid(), clock_seq = random.getrandbits(14))
        explicit_ref_time = datetime.datetime.now()
        explicit_ref1 = ExplicitRef (explicit_ref1_uuid, explicit_ref_time, "EXPLICIT_REFERENCE_NAME1")

        self.session.add (explicit_ref1)
        self.session.commit()

        assert len (self.session.query(ExplicitRef).filter(ExplicitRef.explicit_ref == "EXPLICIT_REFERENCE_NAME1").all()) == 1

        explicit_ref2_uuid = uuid.uuid1(node = os.getpid(), clock_seq = random.getrandbits(14))
        explicit_ref2 = ExplicitRef (explicit_ref2_uuid, explicit_ref_time, "EXPLICIT_REFERENCE_NAME2")

        self.session.add (explicit_ref2)
        self.session.commit()

        assert len (self.session.query(ExplicitRef).filter(ExplicitRef.explicit_ref == "EXPLICIT_REFERENCE_NAME2").all()) == 1

        # Insert link between explicit references
        self.session.add (ExplicitRefLink(explicit_ref1.explicit_ref_uuid, "EXPLICIT_REF_LINK_NAME", explicit_ref2))
        self.session.commit()

        assert len (self.session.query(ExplicitRefLink).filter(ExplicitRefLink.explicit_ref_uuid_link == explicit_ref1.explicit_ref_uuid, ExplicitRefLink.name == "EXPLICIT_REF_LINK_NAME", ExplicitRefLink.explicit_ref_uuid == explicit_ref2.explicit_ref_uuid).all()) == 1
        
        # Insert gauge
        gauge_uuid = uuid.uuid1(node = os.getpid(), clock_seq = random.getrandbits(14))
        gauge = Gauge (gauge_uuid, "GAUGE_NAME", dim_signature, system = "GAUGE_SYSTEM")

        self.session.add (gauge)
        self.session.commit()
        
        assert len (self.session.query(Gauge).filter(Gauge.name == "GAUGE_NAME").all()) == 1

        # Insert a second gauge with same name and system but different DIM signature
        dim_signature_2_uuid = uuid.uuid1(node = os.getpid(), clock_seq = random.getrandbits(14))
        dim_signature_2 = DimSignature(dim_signature_2_uuid, "DIM_SIGNATURE_NAME_2")
        self.session.add(dim_signature_2)
        self.session.commit()
        assert len (self.session.query(DimSignature).filter(DimSignature.dim_signature == "DIM_SIGNATURE_NAME_2").all()) == 1
        gauge_2_uuid = uuid.uuid1(node = os.getpid(), clock_seq = random.getrandbits(14))
        gauge_2 = Gauge (gauge_2_uuid, "GAUGE_NAME", dim_signature_2, system = "GAUGE_SYSTEM")
        self.session.add (gauge_2)
        self.session.commit()
        assert len (self.session.query(Gauge).filter(Gauge.name == "GAUGE_NAME").all()) == 2

        # Insert events
        event_time = datetime.datetime.now()
        event1_uuid = uuid.uuid1(node = os.getpid(), clock_seq = random.getrandbits(14))
        event1 = Event(event1_uuid, event_time, event_time, event_time, gauge, source)
        self.session.add (event1)
        self.session.commit()
        
        assert len (self.session.query(Event).filter(Event.event_uuid == event1_uuid, Event.start == event_time, Event.stop == event_time, Event.gauge_uuid == gauge.gauge_uuid, Event.source_uuid == source.source_uuid).all()) == 1

        event2_uuid = uuid.uuid1(node = os.getpid(), clock_seq = random.getrandbits(14))
        event2 = Event(event2_uuid, event_time, event_time, event_time, gauge, source, explicit_ref1, False)
        self.session.add (event2)
        self.session.commit()
        
        assert len (self.session.query(Event).filter(Event.event_uuid == event2_uuid, Event.start == event_time, Event.stop == event_time, Event.gauge_uuid == gauge.gauge_uuid, Event.source_uuid == source.source_uuid, Event.explicit_ref_uuid == explicit_ref1.explicit_ref_uuid).all()) == 1

        # Insert event key
        self.session.add (EventKey("EVENT_KEY_NAME", event2, dim_signature))
        self.session.commit()

        assert len (self.session.query(EventKey).filter(EventKey.event_key == "EVENT_KEY_NAME", EventKey.visible == False, EventKey.event_uuid == event2_uuid).all()) == 1

        # Insert event link
        self.session.add (EventLink(event1_uuid, "EVENT_LINK_NAME", event2))
        self.session.commit()

        assert len (self.session.query(EventLink).filter(EventLink.event_uuid_link == event1_uuid, EventLink.name == "EVENT_LINK_NAME", EventLink.event_uuid == event2_uuid).all()) == 1

        # Associate to an event a text value
        self.session.add (EventText("TEXT_NAME", "TEXT_VALUE", 0, 0, 0, event1))
        self.session.commit()
        
        assert len (self.session.query(EventText).filter(EventText.event_uuid == event1_uuid, EventText.name == "TEXT_NAME", EventText.value == "TEXT_VALUE", EventText.position == 0, EventText.parent_level == 0, EventText.parent_position == 0).all()) == 1

        # Associate to an event a double value
        self.session.add (EventDouble("DOUBLE_NAME", 1.5, 1, 0, 0, event1))
        self.session.commit()
        
        assert len (self.session.query(EventDouble).filter(EventDouble.event_uuid == event1_uuid, EventDouble.name == "DOUBLE_NAME", EventDouble.value == 1.5, EventDouble.position == 1, EventDouble.parent_level == 0, EventDouble.parent_position == 0).all()) == 1

        # Associate to an event a boolean value
        self.session.add (EventBoolean("BOOLEAN_NAME", True, 2, 0, 0, event1))
        self.session.commit()
        
        assert len (self.session.query(EventBoolean).filter(EventBoolean.event_uuid == event1_uuid, EventBoolean.name == "BOOLEAN_NAME", EventBoolean.value == True, EventBoolean.position == 2, EventBoolean.parent_level == 0, EventBoolean.parent_position == 0).all()) == 1

        # Associate to an event a timestamp value
        timestamp = datetime.datetime.now()        
        self.session.add (EventTimestamp("TIMESTAMP_NAME", timestamp, 3, 0, 0, event1))
        self.session.commit()
        
        assert len (self.session.query(EventTimestamp).filter(EventTimestamp.event_uuid == event1_uuid, EventTimestamp.name == "TIMESTAMP_NAME", EventTimestamp.value == timestamp, EventTimestamp.position == 3, EventTimestamp.parent_level == 0, EventTimestamp.parent_position == 0).all()) == 1

        # Associate to an event an object
        self.session.add (EventObject("OBJECT_NAME", 4, 0, 0, event1))
        self.session.commit()
        
        assert len (self.session.query(EventObject).filter(EventObject.event_uuid == event1_uuid, EventObject.name == "OBJECT_NAME", EventObject.position == 4, EventObject.parent_level == 0, EventObject.parent_position == 0).all()) == 1

        # Associate to an event a geometry
        polygon = "POLYGON((3 0,6 0,6 3,3 3,3 0))"
        self.session.add (EventGeometry("GEOMETRY_NAME", polygon, 5, 0, 0, event1))
        self.session.commit()
        
        assert len (self.session.query(EventGeometry).filter(EventGeometry.event_uuid == event1_uuid, EventGeometry.name == "GEOMETRY_NAME", func.ST_AsText(EventGeometry.value) == polygon, EventGeometry.position == 5, EventGeometry.parent_level == 0, EventGeometry.parent_position == 0).all()) == 1

        # Insert annotation configuration
        annotation_cnf_uuid = uuid.uuid1(node = os.getpid(), clock_seq = random.getrandbits(14))
        annotation_cnf = AnnotationCnf (annotation_cnf_uuid, "ANNOTATION_CNF_NAME", dim_signature)

        self.session.add (annotation_cnf)
        self.session.commit()
        
        assert len (self.session.query(AnnotationCnf).filter(AnnotationCnf.name == "ANNOTATION_CNF_NAME").all()) == 1

        # Insert annotation
        annotation_time = datetime.datetime.now()
        annotation1_uuid = uuid.uuid1(node = os.getpid(), clock_seq = random.getrandbits(14))
        annotation1 = Annotation(annotation1_uuid, annotation_time, annotation_cnf, explicit_ref1, source)
        self.session.add (annotation1)
        self.session.commit()
        
        assert len (self.session.query(Annotation).filter(Annotation.annotation_uuid == annotation1_uuid, Annotation.annotation_cnf_uuid == annotation_cnf.annotation_cnf_uuid, Annotation.source_uuid == source.source_uuid).all()) == 1

        # Associate to an annotation a text value
        self.session.add (AnnotationText("TEXT_NAME", "TEXT_VALUE", 0, 0, 0, annotation1))
        self.session.commit()
        
        assert len (self.session.query(AnnotationText).filter(AnnotationText.annotation_uuid == annotation1_uuid, AnnotationText.name == "TEXT_NAME", AnnotationText.value == "TEXT_VALUE", AnnotationText.position == 0, AnnotationText.parent_level == 0, AnnotationText.parent_position == 0).all()) == 1

        # Associate to an annotation a double value
        self.session.add (AnnotationDouble("DOUBLE_NAME", 1.5, 1, 0, 0, annotation1))
        self.session.commit()
        
        assert len (self.session.query(AnnotationDouble).filter(AnnotationDouble.annotation_uuid == annotation1_uuid, AnnotationDouble.name == "DOUBLE_NAME", AnnotationDouble.value == 1.5, AnnotationDouble.position == 1, AnnotationDouble.parent_level == 0, AnnotationDouble.parent_position == 0).all()) == 1

        # Associate to an annotation a boolean value
        self.session.add (AnnotationBoolean("BOOLEAN_NAME", True, 2, 0, 0, annotation1))
        self.session.commit()
        
        assert len (self.session.query(AnnotationBoolean).filter(AnnotationBoolean.annotation_uuid == annotation1_uuid, AnnotationBoolean.name == "BOOLEAN_NAME", AnnotationBoolean.value == True, AnnotationBoolean.position == 2, AnnotationBoolean.parent_level == 0, AnnotationBoolean.parent_position == 0).all()) == 1

        # Associate to an annotation a timestamp value
        timestamp = datetime.datetime.now()        
        self.session.add (AnnotationTimestamp("TIMESTAMP_NAME", timestamp, 3, 0, 0, annotation1))
        self.session.commit()
        
        assert len (self.session.query(AnnotationTimestamp).filter(AnnotationTimestamp.annotation_uuid == annotation1_uuid, AnnotationTimestamp.name == "TIMESTAMP_NAME", AnnotationTimestamp.value == timestamp, AnnotationTimestamp.position == 3, AnnotationTimestamp.parent_level == 0, AnnotationTimestamp.parent_position == 0).all()) == 1

        # Associate to an annotation an object
        self.session.add (AnnotationObject("OBJECT_NAME", 4, 0, 0, annotation1))
        self.session.commit()
        
        assert len (self.session.query(AnnotationObject).filter(AnnotationObject.annotation_uuid == annotation1_uuid, AnnotationObject.name == "OBJECT_NAME", AnnotationObject.position == 4, AnnotationObject.parent_level == 0, AnnotationObject.parent_position == 0).all()) == 1

        # Associate to an annotation a geometry
        polygon = "POLYGON((3 0,6 0,6 3,3 3,3 0))"
        self.session.add (AnnotationGeometry("GEOMETRY_NAME", polygon, 5, 0, 0, annotation1))
        self.session.commit()
        
        assert len (self.session.query(AnnotationGeometry).filter(AnnotationGeometry.annotation_uuid == annotation1_uuid, AnnotationGeometry.name == "GEOMETRY_NAME", func.ST_AsText(AnnotationGeometry.value) == polygon, AnnotationGeometry.position == 5, AnnotationGeometry.parent_level == 0, AnnotationGeometry.parent_position == 0).all()) == 1

    def test_event_get_structure_values(self):
        data = {"operations": [{
                "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                                  "exec": "exec",
                                  "version": "1.0"},
                "source": {"name": "source.xml",
                           "reception_time": "2018-07-05T02:07:03",
                           "generation_time": "2018-07-05T02:07:03",
                           "validity_start": "2018-06-05T02:07:03",
                           "validity_stop": "2018-06-05T08:07:36"},
                "events": [{
                    "gauge": {
                        "name": "GAUGE_NAME",
                        "system": "GAUGE_SYSTEM",
                        "insertion_type": "SIMPLE_UPDATE"
                    },
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
                                    {"name": "VALUES2",
                                     "type": "object",
                                     "values": [
                                         {"type": "text",
                                          "name": "TEXT",
                                          "value": "TEXT"},
                                         {"type": "boolean",
                                          "name": "BOOLEAN",
                                          "value": "true"}]}]
                            }]
                }]
        }]}
        self.engine_eboa.treat_data(data)

        events = self.query_eboa.get_events(gauge_names = {"filter": "GAUGE_NAME", "op": "like"})

        assert len(events) == 1

        event1 = events[0]

        values = event1.get_structured_values()

        assert values == [{
            "name": "VALUES",
            "type": "object",
            "values": [
                {"type": "text",
                 "name": "TEXT",
                 "value": "TEXT"},
                {"type": "boolean",
                 "name": "BOOLEAN",
                 "value": "True"},
                {"name": "VALUES2",
                 "type": "object",
                 "values": [
                     {"type": "text",
                      "name": "TEXT",
                      "value": "TEXT"},
                     {"type": "boolean",
                      "name": "BOOLEAN",
                      "value": "True"}]}]
        }]


        # Test that if the coordinates are related to a non object the returned list is not empty
        values = event1.get_structured_values(position = 0, parent_level = 0, parent_position = 0)

        assert values == [{"name": "TEXT", "type": "text", "value": "TEXT"}]

        # Test that the export returns inner objects
        values = event1.get_structured_values(position = 2, parent_level = 0, parent_position = 0)

        assert values == [{
            "name": "VALUES2",
            "type": "object",
            "values": [
                {"type": "text",
                 "name": "TEXT",
                 "value": "TEXT"},
                {"type": "boolean",
                 "name": "BOOLEAN",
                 "value": "True"}]
        }]

    def test_get_jsonify_event(self):
        """
        Test jsonify method associated to events
        """
        
        data = {"operations": [{
                "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                                  "exec": "exec",
                                  "version": "1.0"},
                "source": {"name": "source.xml",
                           "reception_time": "2018-07-05T02:07:03",
                           "generation_time": "2018-07-05T02:07:03",
                           "validity_start": "2018-06-05T02:07:03",
                           "validity_stop": "2018-06-05T08:07:36"},
                "events": [{
                    "gauge": {
                        "name": "GAUGE_NAME",
                        "system": "GAUGE_SYSTEM",
                        "insertion_type": "SIMPLE_UPDATE"
                    },
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
                                    {"name": "VALUES2",
                                     "type": "object",
                                     "values": [
                                         {"type": "text",
                                          "name": "TEXT",
                                          "value": "TEXT"},
                                         {"type": "boolean",
                                          "name": "BOOLEAN",
                                          "value": "true"}]}]
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
                    }}]
                }]
        }]}

        exit_status = self.engine_eboa.treat_data(data)

        assert len([item for item in exit_status if item["status"] != eboa_engine.exit_codes["OK"]["status"]]) == 0


        events = self.query_eboa.get_events()

        assert len(events) == 1

        jsonified_event = events[0].jsonify()

        data_to_match = {
            "event_uuid": str(events[0].event_uuid),
            "start": "2018-06-05T02:07:03",
            "stop": "2018-06-05T08:07:36",
            "duration": 21633.0,
            "ingestion_time": events[0].ingestion_time.isoformat(),
            "gauge": {
                "gauge_uuid": str(events[0].gauge.gauge_uuid),
                "dim_signature": "dim_signature",
                "name": "GAUGE_NAME",
                "system": "GAUGE_SYSTEM",
                "description": None
            },
            "source": {
                "source_uuid": str(events[0].source.source_uuid),
                "name": "source.xml"
            },
            "values": [{"name": "VALUES",
                        "type": "object",
                        "values": [
                            {"type": "text",
                             "name": "TEXT",
                             "value": "TEXT"},
                            {"type": "boolean",
                             "name": "BOOLEAN",
                             "value": "True"},
                            {"name": "VALUES2",
                             "type": "object",
                             "values": [
                                 {"type": "text",
                                  "name": "TEXT",
                                  "value": "TEXT"},
                                 {"type": "boolean",
                                  "name": "BOOLEAN",
                                  "value": "True"}]}]
                        }],
            "links_to_me": [],
            "alerts": [{
                "event_alert_uuid": str(events[0].alerts[0].event_alert_uuid),
                "message": "Alert message",
                "validated": None,
                "ingestion_time": events[0].alerts[0].ingestion_time.isoformat(),
                "generator": "test",
                "notified": None,
                "solved": None,
                "solved_time": None,
                "notification_time": "2018-06-05T08:07:36",
                "justification": None,
                "definition": {
                    "alert_uuid": str(events[0].alerts[0].alertDefinition.alert_uuid),
                    "name": "alert_name1",
                    "severity": 4,
                    "description": "Alert description",
                    "group": "alert_group"
                },
                "event_uuid": str(events[0].event_uuid),
                }]
        }

        assert jsonified_event == data_to_match

        new_file = tempfile.NamedTemporaryFile()
        new_file_path = new_file.name
        
        with open(new_file_path, "w") as write_file:
            json.dump(jsonified_event, write_file, indent=4)

        with open(new_file_path) as input_file:
            data = json.load(input_file)

        assert data == data_to_match

        new_file.close()

    def test_get_jsonify_source(self):
        """
        Test jsonify method associated to sources
        """
        
        data = {"operations": [{
                "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                                  "exec": "exec",
                                  "version": "1.0"},
                "source": {"name": "source.xml",
                           "reception_time": "2018-07-05T02:07:03",
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
        }]}

        exit_status = self.engine_eboa.treat_data(data)

        assert len([item for item in exit_status if item["status"] != eboa_engine.exit_codes["OK"]["status"]]) == 0


        sources = self.query_eboa.get_sources()

        assert len(sources) == 1

        data_to_match = {
            "source_uuid": str(sources[0].source_uuid),
            "name": str(sources[0].name),
            "validity_start": "2018-06-05T02:07:03",
            "validity_stop": "2018-06-05T08:07:36",
            "reception_time": "2018-07-05T02:07:03",
            "generation_time": "2018-07-05T02:07:03",
            "ingested": True,
            "ingestion_error": False,
            "ingestion_time": sources[0].ingestion_time.isoformat(),
            "ingestion_duration": str(sources[0].ingestion_duration),
            "processing_duration": str(sources[0].processing_duration),
            "processor": "exec",
            "processor_version": "1.0",
            "dim_signature": "dim_signature",
            "dim_signature_uuid": str(sources[0].dim_signature_uuid),
            "alerts": [{
                "source_alert_uuid": str(sources[0].alerts[0].source_alert_uuid),
                "message": "Alert message",
                "validated": None,
                "ingestion_time": sources[0].alerts[0].ingestion_time.isoformat(),
                "generator": "test",
                "notified": None,
                "solved": None,
                "solved_time": None,
                "notification_time": "2018-06-05T08:07:36",
                "justification": None,
                "definition": {
                    "alert_uuid": str(sources[0].alerts[0].alertDefinition.alert_uuid),
                    "name": "alert_name1",
                    "severity": 4,
                    "description": "Alert description",
                    "group": "alert_group"
                },
                "source_uuid": str(sources[0].source_uuid),
                }]
        }

        jsonified_source = sources[0].jsonify()
        
        assert jsonified_source == data_to_match
        
        new_file = tempfile.NamedTemporaryFile()
        new_file_path = new_file.name
        
        with open(new_file_path, "w") as write_file:
            json.dump(jsonified_source, write_file, indent=4)

        with open(new_file_path) as input_file:
            data = json.load(input_file)

        assert data == data_to_match

        new_file.close()

    def test_get_jsonify_annotation(self):
        """
        Test jsonify method associated to annotations
        """
        
        data = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source.xml",
                       "reception_time": "2018-07-05T02:07:03",
                       "generation_time": "2018-07-05T02:07:03",
                       "validity_start": "2018-06-05T02:07:03",
                       "validity_stop": "2018-06-05T08:07:36"},
            "annotations": [{
                "explicit_reference": "EXPLICIT_REFERENCE",
                "annotation_cnf": {
                    "name": "ANNOTATION_CNF",
                    "system": "SYSTEM"
                },
                "values": [{"name": "VALUES",
                            "type": "object",
                            "values": [
                                {"type": "text",
                                 "name": "TEXT",
                                 "value": "TEXT"},
                                {"type": "boolean",
                                 "name": "BOOLEAN",
                                 "value": "true"},
                                {"name": "VALUES2",
                                 "type": "object",
                                 "values": [
                                     {"type": "text",
                                      "name": "TEXT",
                                      "value": "TEXT"},
                                     {"type": "boolean",
                                      "name": "BOOLEAN",
                                      "value": "true"}]}]
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
                    }}]
            }]
        }]}

        exit_status = self.engine_eboa.treat_data(data)

        assert len([item for item in exit_status if item["status"] != eboa_engine.exit_codes["OK"]["status"]]) == 0


        annotations = self.query_eboa.get_annotations()

        assert len(annotations) == 1

        jsonified_annotation = annotations[0].jsonify()

        data_to_match = {
            "annotation_uuid": str(annotations[0].annotation_uuid),
            "explicit_reference": {
                "uuid": str(annotations[0].explicit_ref_uuid),
                "name": "EXPLICIT_REFERENCE"
            },
            "ingestion_time": annotations[0].ingestion_time.isoformat(),
            "configuration": {
                "uuid": str(annotations[0].annotationCnf.annotation_cnf_uuid),
                "dim_signature": "dim_signature",
                "name": "ANNOTATION_CNF",
                "system": "SYSTEM",
                "description": None,
            },
            "source": {
                "source_uuid": str(annotations[0].source.source_uuid),
                "name": "source.xml"
            },
            "values": [{"name": "VALUES",
                        "type": "object",
                        "values": [
                            {"type": "text",
                             "name": "TEXT",
                             "value": "TEXT"},
                            {"type": "boolean",
                             "name": "BOOLEAN",
                             "value": "True"},
                            {"name": "VALUES2",
                             "type": "object",
                             "values": [
                                 {"type": "text",
                                  "name": "TEXT",
                                  "value": "TEXT"},
                                 {"type": "boolean",
                                  "name": "BOOLEAN",
                                  "value": "True"}]}]
                        }],
            "alerts": [{
                "annotation_alert_uuid": str(annotations[0].alerts[0].annotation_alert_uuid),
                "message": "Alert message",
                "validated": None,
                "ingestion_time": annotations[0].alerts[0].ingestion_time.isoformat(),
                "generator": "test",
                "notified": None,
                "solved": None,
                "solved_time": None,
                "notification_time": "2018-06-05T08:07:36",
                "justification": None,
                "definition": {
                    "alert_uuid": str(annotations[0].alerts[0].alertDefinition.alert_uuid),
                    "name": "alert_name1",
                    "severity": 4,
                    "description": "Alert description",
                    "group": "alert_group"
                },
                "annotation_uuid": str(annotations[0].annotation_uuid),
                }]
        }

        assert jsonified_annotation == data_to_match

        new_file = tempfile.NamedTemporaryFile()
        new_file_path = new_file.name
        
        with open(new_file_path, "w") as write_file:
            json.dump(jsonified_annotation, write_file, indent=4)

        with open(new_file_path) as input_file:
            data = json.load(input_file)

        assert data == data_to_match

        new_file.close()
        
    def test_get_jsonify_explicit_reference(self):
        """
        Test jsonify method associated to explicit references
        """
        
        data = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source.xml",
                       "reception_time": "2018-07-05T02:07:03",
                       "generation_time": "2018-07-05T02:07:03",
                       "validity_start": "2018-06-05T02:07:03",
                       "validity_stop": "2018-06-05T08:07:36"},
            "explicit_references": [{
                "name": "EXPLICIT_REFERENCE",
                "group": "EXPL_GROUP",
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
        }]}

        exit_status = self.engine_eboa.treat_data(data)

        assert len([item for item in exit_status if item["status"] != eboa_engine.exit_codes["OK"]["status"]]) == 0


        ers = self.query_eboa.get_explicit_refs()

        assert len(ers) == 1

        jsonified_er = ers[0].jsonify(False)

        data_to_match = {
            "explicit_ref_uuid": str(ers[0].explicit_ref_uuid),
            "explicit_ref": "EXPLICIT_REFERENCE",
            "group": "EXPL_GROUP",
            "ingestion_time": ers[0].ingestion_time.isoformat(),
            "alerts": [{
                "explicit_ref_alert_uuid": str(ers[0].alerts[0].explicit_ref_alert_uuid),
                "message": "Alert message",
                "validated": None,
                "ingestion_time": ers[0].alerts[0].ingestion_time.isoformat(),
                "generator": "test",
                "notified": None,
                "solved": None,
                "solved_time": None,
                "notification_time": "2018-06-05T08:07:36",
                "justification": None,
                "definition": {
                    "alert_uuid": str(ers[0].alerts[0].alertDefinition.alert_uuid),
                    "name": "alert_name1",
                    "severity": 4,
                    "description": "Alert description",
                    "group": "alert_group"
                },
                "explicit_ref_uuid": str(ers[0].explicit_ref_uuid),
                }]
        }

        assert jsonified_er == data_to_match

        new_file = tempfile.NamedTemporaryFile()
        new_file_path = new_file.name
        
        with open(new_file_path, "w") as write_file:
            json.dump(jsonified_er, write_file, indent=4)

        with open(new_file_path) as input_file:
            data = json.load(input_file)

        assert data == data_to_match

        new_file.close()

    def test_get_jsonify_explicit_reference_without_group(self):
        """
        Test jsonify method associated to explicit references without group
        """
        
        data = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source.xml",
                       "reception_time": "2018-07-05T02:07:03",
                       "generation_time": "2018-07-05T02:07:03",
                       "validity_start": "2018-06-05T02:07:03",
                       "validity_stop": "2018-06-05T08:07:36"},
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
        }]}

        exit_status = self.engine_eboa.treat_data(data)

        assert len([item for item in exit_status if item["status"] != eboa_engine.exit_codes["OK"]["status"]]) == 0


        ers = self.query_eboa.get_explicit_refs()

        assert len(ers) == 1

        jsonified_er = ers[0].jsonify(False)

        data_to_match = {
            "explicit_ref_uuid": str(ers[0].explicit_ref_uuid),
            "explicit_ref": "EXPLICIT_REFERENCE",
            "ingestion_time": ers[0].ingestion_time.isoformat(),
            "alerts": [{
                "explicit_ref_alert_uuid": str(ers[0].alerts[0].explicit_ref_alert_uuid),
                "message": "Alert message",
                "validated": None,
                "ingestion_time": ers[0].alerts[0].ingestion_time.isoformat(),
                "generator": "test",
                "notified": None,
                "solved": None,
                "solved_time": None,
                "notification_time": "2018-06-05T08:07:36",
                "justification": None,
                "definition": {
                    "alert_uuid": str(ers[0].alerts[0].alertDefinition.alert_uuid),
                    "name": "alert_name1",
                    "severity": 4,
                    "description": "Alert description",
                    "group": "alert_group"
                },
                "explicit_ref_uuid": str(ers[0].explicit_ref_uuid),
                }]
        }

        assert jsonified_er == data_to_match

        new_file = tempfile.NamedTemporaryFile()
        new_file_path = new_file.name
        
        with open(new_file_path, "w") as write_file:
            json.dump(jsonified_er, write_file, indent=4)

        with open(new_file_path) as input_file:
            data = json.load(input_file)

        assert data == data_to_match

        new_file.close()

    def test_get_jsonify_explicit_reference_with_annotations(self):
        """
        Test jsonify method associated to explicit references with annotations
        """
        
        data = {"operations": [{
            "mode": "insert",
            "dim_signature": {"name": "dim_signature",
                              "exec": "exec",
                              "version": "1.0"},
            "source": {"name": "source.xml",
                       "reception_time": "2018-07-05T02:07:03",
                       "generation_time": "2018-07-05T02:07:03",
                       "validity_start": "2018-06-05T02:07:03",
                       "validity_stop": "2018-06-05T08:07:36"},
            "explicit_references": [{
                "name": "EXPLICIT_REFERENCE",
                "group": "EXPL_GROUP",
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
                }
            }]
        }]}

        exit_status = self.engine_eboa.treat_data(data)

        assert len([item for item in exit_status if item["status"] != eboa_engine.exit_codes["OK"]["status"]]) == 0


        ers = self.query_eboa.get_explicit_refs()

        assert len(ers) == 1

        annotations = self.query_eboa.get_annotations()

        assert len(annotations) == 1
        
        jsonified_er = ers[0].jsonify()

        data_to_match = {
            "explicit_ref_uuid": str(ers[0].explicit_ref_uuid),
            "explicit_ref": "EXPLICIT_REFERENCE",
            "group": "EXPL_GROUP",
            "ingestion_time": ers[0].ingestion_time.isoformat(),
            "alerts": [{
                "explicit_ref_alert_uuid": str(ers[0].alerts[0].explicit_ref_alert_uuid),
                "message": "Alert message",
                "validated": None,
                "ingestion_time": ers[0].alerts[0].ingestion_time.isoformat(),
                "generator": "test",
                "notified": None,
                "solved": None,
                "solved_time": None,
                "notification_time": "2018-06-05T08:07:36",
                "justification": None,
                "definition": {
                    "alert_uuid": str(ers[0].alerts[0].alertDefinition.alert_uuid),
                    "name": "alert_name1",
                    "severity": 4,
                    "description": "Alert description",
                    "group": "alert_group"
                },
                "explicit_ref_uuid": str(ers[0].explicit_ref_uuid),
            }],
            "annotations": {
                "ANNOTATION_CNF": [{
                    "annotation_uuid": str(annotations[0].annotation_uuid),
                    "name": "ANNOTATION_CNF",
                    "system": "SYSTEM"}]
            }
        }
        assert jsonified_er == data_to_match

        new_file = tempfile.NamedTemporaryFile()
        new_file_path = new_file.name
        
        with open(new_file_path, "w") as write_file:
            json.dump(jsonified_er, write_file, indent=4)

        with open(new_file_path) as input_file:
            data = json.load(input_file)

        assert data == data_to_match

        new_file.close()

