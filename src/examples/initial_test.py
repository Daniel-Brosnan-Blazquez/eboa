"""
Test: initial test

Written by DEIMOS Space S.L. (dibb)

module gsdm
"""
import os
import sys

from gsdm.datamodel.base import Session, engine, Base
from gsdm.datamodel.dim_signatures import DimSignature
from gsdm.datamodel.events import Event, EventLink, EventText, EventDouble, EventObject, EventGeometry
from gsdm.datamodel.gauges import Gauge
from gsdm.datamodel.dim_processings import DimProcessing, DimProcessingStatus
from gsdm.datamodel.explicit_refs import ExplicitRef, ExplicitRefGrp, ExplicitRefLink
from gsdm.datamodel.annotations import Annotation, AnnotationCnf, AnnotationText, AnnotationDouble, AnnotationObject, AnnotationGeometry
import datetime
import uuid
import pprint
from math import pi
import random

# Create session to connect to the database
session = Session()

# Clear all tables before executing the test
for table in reversed(Base.metadata.sorted_tables):
    engine.execute(table.delete())

################
# DIM Signatures
################
# Create dim_signature
dimSignature1 = DimSignature ('TEST', 'TEST')

# Insert dim_signature into database
session.add (dimSignature1)
session.commit()

if len (session.query(DimSignature).filter(DimSignature.dim_signature == 'TEST').all()) != 1:
    raise Exception("The DIM signature was not committed")

################
# DIM Processing
################
# Create dim_processing
processingTime = datetime.datetime.now()
processingUuid = uuid.uuid1(node = os.getpid(), clock_seq = random.getrandbits(14))
dimProcessing1 = DimProcessing (processingUuid, 'TEST', processingTime, "1.0", dimSignature1)

# Insert dim_processing into database
session.add (dimProcessing1)
session.commit()

if len (session.query(DimProcessing).filter(DimProcessing.name == 'TEST').all()) != 1:
    raise Exception("The DIM processing was not committed")

################
# Explicit references
################
# Create explicit reference
explicitRefTime = datetime.datetime.now()
explicitRef1 = ExplicitRef (explicitRefTime, 'TEST')

# Insert explicit reference into database
session.add (explicitRef1)
session.commit()

if len (session.query(ExplicitRef).filter(ExplicitRef.explicit_ref == 'TEST').all()) != 1:
    raise Exception("The Explicit Reference was not committed")

# Create explicit reference with group configuration
explicitRefGroup = ExplicitRefGrp('TEST')
explicitRef2 = ExplicitRef (explicitRefTime, 'TEST2', explicitRefGroup)

# Insert explicit reference into database
session.add (explicitRef2)
session.commit()

if len (session.query(ExplicitRef).filter(ExplicitRef.explicit_ref == 'TEST2').all()) != 1:
    raise Exception("The Explicit Reference was not committed")
if len (session.query(ExplicitRefGrp).filter(ExplicitRef.explicit_ref == 'TEST').all()) != 1:
    raise Exception("The group of Explicit References was not committed")

# Create link between explicit references
explicitRef1Id = session.query(ExplicitRef).filter(ExplicitRef.explicit_ref == 'TEST').all()[0].explicit_ref_id
erlink = ExplicitRefLink(explicitRef1Id, 'TEST', explicitRef2)

# Insert the link between explicit references into the database
session.add (erlink)
session.commit()

if len (session.query(ExplicitRefLink).filter(ExplicitRefLink.name == 'TEST').all()) != 1:
    raise Exception("The link between Explicit References was not committed")

################
# Events
################
# Create gauge
gauge1 = Gauge ('TEST', dimSignature1, 'TEST')

# Insert gauge into database
session.add (gauge1)
session.commit()

if len (session.query(Gauge).filter(Gauge.name == 'TEST').all()) != 1:
    raise Exception("The Gauge was not committed")

# Create event
event1Time = datetime.datetime.now()
event1Uuid = uuid.uuid1(node = os.getpid(), clock_seq = random.getrandbits(14))
event1 = Event (event1Uuid, event1Time, event1Time, event1Time,gauge1, dim_processing = dimProcessing1)

# Insert the event into the database
session.add (event1)
session.commit()

if len (session.query(Event).filter(Event.event_uuid == event1Uuid).all()) != 1:
    raise Exception("The Event was not committed")

# Create another event
event2Time = datetime.datetime.now()
event2Uuid = uuid.uuid1(node = os.getpid(), clock_seq = random.getrandbits(14))
event2 = Event (event2Uuid, event2Time, event2Time, event2Time,gauge1, dim_processing = dimProcessing1)

# Insert the event into the database
session.add (event2)
session.commit()

if len (session.query(Event).filter(Event.event_uuid == event2Uuid).all()) != 1:
    raise Exception("The Event was not committed")

# Create link between events
event1Id = session.query(Event).filter(Event.event_uuid == event1Uuid).all()[0].event_uuid
evlink = EventLink(event1Id, 'TEST', event2)

# Insert the event link into the database
session.add (evlink)
session.commit()

if len (session.query(EventLink).filter(EventLink.name == 'TEST').all()) != 1:
    raise Exception("The link between events was not committed")

# Add text to the event
name = 'TEST'
eventText1 = EventText (name, 'TEST', 0, 0, 0, event1)

# Insert the text into the database
session.add (eventText1)
session.commit()

if len (session.query(EventText).filter(EventText.event_uuid == event1Uuid).all()) != 1:
    raise Exception("The text was not committed")

# Add double to the event
name = 'TEST'
eventDouble1 = EventDouble (name, pi, 1, 0, 0, event1)

# Insert the float into the database
session.add (eventDouble1)
session.commit()

if len (session.query(EventDouble).filter(EventDouble.event_uuid == event1Uuid).all()) != 1:
    raise Exception("The double was not committed")

# Add object to the event
name = 'TEST'
eventObject1 = EventObject (name, 1, 0, 0, event1)

# Insert the object into the database
session.add (eventObject1)
session.commit()

if len (session.query(EventObject).filter(EventObject.event_uuid == event1Uuid).all()) != 1:
    raise Exception("The object was not committed")

# Add geometry to the event
name = 'TEST'
eventGeometry1 = EventGeometry (name, 'POLYGON((3 0,6 0,6 3,3 3,3 0))', 1, 0, 0, event1)

# Insert the geometry into the database
session.add (eventGeometry1)
session.commit()

if len (session.query(EventGeometry).filter(EventGeometry.event_uuid == event1Uuid).all()) != 1:
    raise Exception("The geometry was not committed")

################
# Annotations
################
# Create annotation configuration
annotationCnf1 = AnnotationCnf ('TEST', dimSignature1)

# Insert annotationcnf configuration into database
session.add (annotationCnf1)
session.commit()

if len (session.query(AnnotationCnf).filter(AnnotationCnf.name == 'TEST').all()) != 1:
    raise Exception("The Annotation was not committed")

# Create annotation
annotation1Uuid = uuid.uuid1(node = os.getpid(), clock_seq = random.getrandbits(14))
annotation1Time = datetime.datetime.now()
annotation1 = Annotation (annotation1Uuid, annotation1Time,annotationCnf1,explicitRef1, dimProcessing1)

# Insert annotation into database
session.add (annotation1)
session.commit()

if len (session.query(Annotation).filter(Annotation.annotation_uuid == annotation1Uuid).all()) != 1:
    raise Exception("The Annotation was not committed")

# Add text to the annotation
name = 'TEST'
annotationText1 = AnnotationText (name, 'TEST', 0, 0, 0, annotation1)

# Insert the text into the database
session.add (annotationText1)
session.commit()

if len (session.query(AnnotationText).filter(AnnotationText.annotation_uuid == annotation1Uuid).all()) != 1:
    raise Exception("The text was not committed")

# Add double to the annotation
name = 'TEST'
annotationDouble1 = AnnotationDouble (name, pi, 1, 0, 0, annotation1)

# Insert the float into the database
session.add (annotationDouble1)
session.commit()

if len (session.query(AnnotationDouble).filter(AnnotationDouble.annotation_uuid == annotation1Uuid).all()) != 1:
    raise Exception("The double was not committed")

# Add object to the annotation
name = 'TEST'
annotationObject1 = AnnotationObject (name, 1, 0, 0, annotation1)

# Insert the object into the database
session.add (annotationObject1)
session.commit()

if len (session.query(AnnotationObject).filter(AnnotationObject.annotation_uuid == annotation1Uuid).all()) != 1:
    raise Exception("The object was not committed")

# Add geometry to the annotation
name = 'TEST'
annotationGeometry1 = AnnotationGeometry (name, 'POLYGON((3 0,6 0,6 3,3 3,3 0))', 1, 0, 0, annotation1)

# Insert the geometry into the database
session.add (annotationGeometry1)
session.commit()

if len (session.query(AnnotationGeometry).filter(AnnotationGeometry.annotation_uuid == annotation1Uuid).all()) != 1:
    raise Exception("The geometry was not committed")

print ('Inserted DIM Signatures ({}):'.format(len (session.query(DimSignature).all())))
for idx, dimSignature in enumerate(session.query(DimSignature).all()):
    print ('DIM signature {}:'.format(idx))
    pprint.pprint(dimSignature.__dict__)

print ('\nInserted gauges ({}):'.format(len(session.query(Gauge).all())))
for idx, gauge in enumerate(session.query(Gauge).all()):
    print ('Gauge {}:'.format(idx))
    pprint.pprint(gauge.__dict__)

print ('\nInserted events ({}):'.format(len(session.query(Event).all())))
for idx, event in enumerate(session.query(Event).all()):
    print ('Event {}:'.format(idx))
    pprint.pprint(event.__dict__)

print ('\nInserted annotations ({}):'.format(len(session.query(Annotation).all())))
for idx, annotation in enumerate(session.query(Annotation).all()):
    print ('Annotation {}:'.format(idx))
    pprint.pprint(annotation.__dict__)

print ('\nInserted explicit references ({}):'.format(len(session.query(ExplicitRef).all())))
for idx, er in enumerate(session.query(ExplicitRef).all()):
    print ('Explicit reference {}:'.format(idx))
    pprint.pprint(er.__dict__)

print ('\nInserted DIM processings ({}):'.format(len(session.query(DimProcessing).all())))
for idx, dimProcessing in enumerate(session.query(DimProcessing).all()):
    print ('DIM processing {}:'.format(idx))
    pprint.pprint(dimProcessing.__dict__)

print ()
print ('Searching events by gauge name TEST\n>>> session.query(Event).join(Gauge).filter(Gauge.name == \'TEST\').all()')
eventsGaugeTest = session.query(Event).join(Gauge).filter(Gauge.name == 'TEST').all()
for idx, event in enumerate(eventsGaugeTest):
    print ('Event {}:'.format(idx))
    pprint.pprint(event.__dict__)
    print ()

# Returning connection to the pool
session.close()

# Closing the pool of connections
engine.dispose()
