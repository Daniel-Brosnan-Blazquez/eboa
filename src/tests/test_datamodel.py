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
        self.session.add(SourceStatus(processing_time, 0, source))
        self.session.commit()

        assert len(self.session.query(SourceStatus).filter(SourceStatus.source_uuid == source_uuid, SourceStatus.status == 0).all()) == 1

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


        # Test that if the coordinates are related to a non object the returned list is empty
        values = event1.get_structured_values(position = 0, parent_level = 0, parent_position = 0)

        assert values == []

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

        

