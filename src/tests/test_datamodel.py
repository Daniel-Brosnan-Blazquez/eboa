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

# Import datamodel
import gsdm.datamodel.base
from gsdm.datamodel.base import Session, engine, Base
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

class TestDatamodel(unittest.TestCase):
    def setUp(self):
        # Create session to connect to the database
        self.session = Session()

        # Clear all tables before executing the test
        for table in reversed(Base.metadata.sorted_tables):
            engine.execute(table.delete())

    def test_insert_data(self):
        """ Verify the insertion of data using the classes defined in the datamodel. """
        dim_signature_uuid = uuid.uuid1(node = os.getpid(), clock_seq = random.getrandbits(14))
        dim_signature = DimSignature(dim_signature_uuid, "DIM_SIGNATURE_NAME", "DIM_EXEC_NAME")
        
        # Insert dim_signature into database
        self.session.add(dim_signature)
        self.session.commit()
        
        assert len (self.session.query(DimSignature).filter(DimSignature.dim_signature == "DIM_SIGNATURE_NAME", DimSignature.dim_exec_name == "DIM_EXEC_NAME").all()) == 1

        processing_time = datetime.datetime.now()
        processing_uuid = uuid.uuid1(node = os.getpid(), clock_seq = random.getrandbits(14))
        dim_processing = DimProcessing(processing_uuid, "DIM_PROCESSING_NAME", processing_time, "1.0", dim_signature)
        
        # Insert dim_processing into database
        self.session.add(dim_processing)
        self.session.commit()
        
        assert len(self.session.query(DimProcessing).filter(DimProcessing.processing_uuid == processing_uuid, DimProcessing.name == "DIM_PROCESSING_NAME", DimProcessing.generation_time == processing_time, DimProcessing.dim_exec_version == "1.0", DimProcessing.dim_signature_id == dim_signature.dim_signature_id).all()) == 1

        # Insert status for the dim_processing
        self.session.add(DimProcessingStatus(processing_time, 0, dim_processing))
        self.session.commit()

        assert len(self.session.query(DimProcessingStatus).filter(DimProcessingStatus.processing_uuid == processing_uuid, DimProcessingStatus.proc_status == 0).all()) == 1

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
        self.session.add (ExplicitRefLink(explicit_ref1.explicit_ref_id, "EXPLICIT_REF_LINK_NAME", explicit_ref2))
        self.session.commit()

        assert len (self.session.query(ExplicitRefLink).filter(ExplicitRefLink.explicit_ref_id_link == explicit_ref1.explicit_ref_id, ExplicitRefLink.name == "EXPLICIT_REF_LINK_NAME", ExplicitRefLink.explicit_ref_id == explicit_ref2.explicit_ref_id).all()) == 1
        
        # Insert gauge
        gauge_uuid = uuid.uuid1(node = os.getpid(), clock_seq = random.getrandbits(14))
        gauge = Gauge (gauge_uuid, "GAUGE_NAME", dim_signature)

        self.session.add (gauge)
        self.session.commit()
        
        assert len (self.session.query(Gauge).filter(Gauge.name == "GAUGE_NAME").all()) == 1

        # Insert events
        event_time = datetime.datetime.now()
        event1_uuid = uuid.uuid1(node = os.getpid(), clock_seq = random.getrandbits(14))
        event1 = Event(event1_uuid, event_time, event_time, event_time, gauge, dim_processing)
        self.session.add (event1)
        self.session.commit()
        
        assert len (self.session.query(Event).filter(Event.event_uuid == event1_uuid, Event.start == event_time, Event.stop == event_time, Event.gauge_id == gauge.gauge_id, Event.processing_uuid == dim_processing.processing_uuid).all()) == 1

        event2_uuid = uuid.uuid1(node = os.getpid(), clock_seq = random.getrandbits(14))
        event2 = Event(event2_uuid, event_time, event_time, event_time, gauge, dim_processing, explicit_ref1, False)
        self.session.add (event2)
        self.session.commit()
        
        assert len (self.session.query(Event).filter(Event.event_uuid == event2_uuid, Event.start == event_time, Event.stop == event_time, Event.gauge_id == gauge.gauge_id, Event.processing_uuid == dim_processing.processing_uuid, Event.explicit_ref_id == explicit_ref1.explicit_ref_id).all()) == 1

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
        
        assert len (self.session.query(EventText).filter(EventText.event_uuid == event1_uuid, EventText.name == "TEXT_NAME", EventText.value == "TEXT_VALUE", EventText.level_position == 0, EventText.parent_level == 0, EventText.parent_position == 0).all()) == 1

        # Associate to an event a double value
        self.session.add (EventDouble("DOUBLE_NAME", 1.5, 1, 0, 0, event1))
        self.session.commit()
        
        assert len (self.session.query(EventDouble).filter(EventDouble.event_uuid == event1_uuid, EventDouble.name == "DOUBLE_NAME", EventDouble.value == 1.5, EventDouble.level_position == 1, EventDouble.parent_level == 0, EventDouble.parent_position == 0).all()) == 1

        # Associate to an event a boolean value
        self.session.add (EventBoolean("BOOLEAN_NAME", True, 2, 0, 0, event1))
        self.session.commit()
        
        assert len (self.session.query(EventBoolean).filter(EventBoolean.event_uuid == event1_uuid, EventBoolean.name == "BOOLEAN_NAME", EventBoolean.value == True, EventBoolean.level_position == 2, EventBoolean.parent_level == 0, EventBoolean.parent_position == 0).all()) == 1

        # Associate to an event a timestamp value
        timestamp = datetime.datetime.now()        
        self.session.add (EventTimestamp("TIMESTAMP_NAME", timestamp, 3, 0, 0, event1))
        self.session.commit()
        
        assert len (self.session.query(EventTimestamp).filter(EventTimestamp.event_uuid == event1_uuid, EventTimestamp.name == "TIMESTAMP_NAME", EventTimestamp.value == timestamp, EventTimestamp.level_position == 3, EventTimestamp.parent_level == 0, EventTimestamp.parent_position == 0).all()) == 1

        # Associate to an event an object
        self.session.add (EventObject("OBJECT_NAME", 4, 0, 0, event1))
        self.session.commit()
        
        assert len (self.session.query(EventObject).filter(EventObject.event_uuid == event1_uuid, EventObject.name == "OBJECT_NAME", EventObject.level_position == 4, EventObject.parent_level == 0, EventObject.parent_position == 0).all()) == 1

        # Associate to an event a geometry
        polygon = "POLYGON((3 0,6 0,6 3,3 3,3 0))"
        self.session.add (EventGeometry("GEOMETRY_NAME", polygon, 5, 0, 0, event1))
        self.session.commit()
        
        assert len (self.session.query(EventGeometry).filter(EventGeometry.event_uuid == event1_uuid, EventGeometry.name == "GEOMETRY_NAME", func.ST_AsText(EventGeometry.value) == polygon, EventGeometry.level_position == 5, EventGeometry.parent_level == 0, EventGeometry.parent_position == 0).all()) == 1

        # Insert annotation configuration
        annotation_cnf_uuid = uuid.uuid1(node = os.getpid(), clock_seq = random.getrandbits(14))
        annotation_cnf = AnnotationCnf (annotation_cnf_uuid, "ANNOTATION_CNF_NAME", dim_signature)

        self.session.add (annotation_cnf)
        self.session.commit()
        
        assert len (self.session.query(AnnotationCnf).filter(AnnotationCnf.name == "ANNOTATION_CNF_NAME").all()) == 1

        # Insert annotation
        annotation_time = datetime.datetime.now()
        annotation1_uuid = uuid.uuid1(node = os.getpid(), clock_seq = random.getrandbits(14))
        annotation1 = Annotation(annotation1_uuid, annotation_time, annotation_cnf, explicit_ref1, dim_processing)
        self.session.add (annotation1)
        self.session.commit()
        
        assert len (self.session.query(Annotation).filter(Annotation.annotation_uuid == annotation1_uuid, Annotation.annotation_cnf_id == annotation_cnf.annotation_cnf_id, Annotation.processing_uuid == dim_processing.processing_uuid).all()) == 1

        # Associate to an annotation a text value
        self.session.add (AnnotationText("TEXT_NAME", "TEXT_VALUE", 0, 0, 0, annotation1))
        self.session.commit()
        
        assert len (self.session.query(AnnotationText).filter(AnnotationText.annotation_uuid == annotation1_uuid, AnnotationText.name == "TEXT_NAME", AnnotationText.value == "TEXT_VALUE", AnnotationText.level_position == 0, AnnotationText.parent_level == 0, AnnotationText.parent_position == 0).all()) == 1

        # Associate to an annotation a double value
        self.session.add (AnnotationDouble("DOUBLE_NAME", 1.5, 1, 0, 0, annotation1))
        self.session.commit()
        
        assert len (self.session.query(AnnotationDouble).filter(AnnotationDouble.annotation_uuid == annotation1_uuid, AnnotationDouble.name == "DOUBLE_NAME", AnnotationDouble.value == 1.5, AnnotationDouble.level_position == 1, AnnotationDouble.parent_level == 0, AnnotationDouble.parent_position == 0).all()) == 1

        # Associate to an annotation a boolean value
        self.session.add (AnnotationBoolean("BOOLEAN_NAME", True, 2, 0, 0, annotation1))
        self.session.commit()
        
        assert len (self.session.query(AnnotationBoolean).filter(AnnotationBoolean.annotation_uuid == annotation1_uuid, AnnotationBoolean.name == "BOOLEAN_NAME", AnnotationBoolean.value == True, AnnotationBoolean.level_position == 2, AnnotationBoolean.parent_level == 0, AnnotationBoolean.parent_position == 0).all()) == 1

        # Associate to an annotation a timestamp value
        timestamp = datetime.datetime.now()        
        self.session.add (AnnotationTimestamp("TIMESTAMP_NAME", timestamp, 3, 0, 0, annotation1))
        self.session.commit()
        
        assert len (self.session.query(AnnotationTimestamp).filter(AnnotationTimestamp.annotation_uuid == annotation1_uuid, AnnotationTimestamp.name == "TIMESTAMP_NAME", AnnotationTimestamp.value == timestamp, AnnotationTimestamp.level_position == 3, AnnotationTimestamp.parent_level == 0, AnnotationTimestamp.parent_position == 0).all()) == 1

        # Associate to an annotation an object
        self.session.add (AnnotationObject("OBJECT_NAME", 4, 0, 0, annotation1))
        self.session.commit()
        
        assert len (self.session.query(AnnotationObject).filter(AnnotationObject.annotation_uuid == annotation1_uuid, AnnotationObject.name == "OBJECT_NAME", AnnotationObject.level_position == 4, AnnotationObject.parent_level == 0, AnnotationObject.parent_position == 0).all()) == 1

        # Associate to an annotation a geometry
        polygon = "POLYGON((3 0,6 0,6 3,3 3,3 0))"
        self.session.add (AnnotationGeometry("GEOMETRY_NAME", polygon, 5, 0, 0, annotation1))
        self.session.commit()
        
        assert len (self.session.query(AnnotationGeometry).filter(AnnotationGeometry.annotation_uuid == annotation1_uuid, AnnotationGeometry.name == "GEOMETRY_NAME", func.ST_AsText(AnnotationGeometry.value) == polygon, AnnotationGeometry.level_position == 5, AnnotationGeometry.parent_level == 0, AnnotationGeometry.parent_position == 0).all()) == 1
