"""
Test: initial test

Written by DEIMOS Space S.L. (dibb)

module gsdm
"""
import os
import sys

# Adding path to the datamodel package
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from datamodel.base import Session, engine, Base
from datamodel.dim_signatures import DimSignature
from datamodel.events import Event, EventLink
from datamodel.gauges import Gauge
from datamodel.dim_processings import DimProcessing, DimProcessingStatus
from datamodel.explicit_refs import ExplicitRef, ExplicitRefGrp, ExplicitRefLink
from datamodel.annotations import Annotation, AnnotationCnf
import datetime
import uuid
import pprint

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
gauge1 = Gauge ('TEST', dimSignature1, 'TEST', 'TEST')

# Insert gauge into database
session.add (gauge1)
session.commit()

if len (session.query(Gauge).filter(Gauge.name == 'TEST').all()) != 1:
    raise Exception("The Gauge was not committed")

# Create event
event1Time = datetime.datetime.now()
event1Uuid = uuid.uuid1()
event1 = Event (event1Uuid, event1Time, event1Time, event1Time, event1Time,gauge1)

# Insert the event into the database
session.add (event1)
session.commit()

if len (session.query(Event).filter(Event.event_uuid == event1Uuid).all()) != 1:
    raise Exception("The Event was not committed")

# Create another event
event2Time = datetime.datetime.now()
event2Uuid = uuid.uuid1()
event2 = Event (event2Uuid, event2Time, event2Time, event2Time, event2Time,gauge1)

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
annotation1Uuid = uuid.uuid1()
annotation1Time = datetime.datetime.now()
annotation1 = Annotation (annotation1Uuid, annotation1Time, annotation1Time,annotationCnf1,explicitRef1)

# Insert annotation into database
session.add (annotation1)
session.commit()

if len (session.query(Annotation).filter(Annotation.annotation_uuid == annotation1Uuid).all()) != 1:
    raise Exception("The Annotation was not committed")

print ('Inserted DIM Signatures ({}):'.format(len (session.query(DimSignature).all())))
for idx, dimSignature in enumerate(session.query(DimSignature).all()):
    print ('DIM signature {}:'.format(idx))
    pprint.pprint(dimSignature.__dict__)
    print ()
print ('\nInserted gauges ({}):'.format(len(session.query(Gauge).all())))
for idx, gauge in enumerate(session.query(Gauge).all()):
    print ('Gauge {}:'.format(idx))
    pprint.pprint(gauge.__dict__)
    print ()
print ('\nInserted events ({}):'.format(len(session.query(Event).all())))
for idx, event in enumerate(session.query(Event).all()):
    print ('Event {}:'.format(idx))
    pprint.pprint(event.__dict__)
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
